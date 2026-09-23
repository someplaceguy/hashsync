//! `hashsync run [--no-update] [--ssh CMD] [USER@]HOST -- COMMAND ...`
//! (DESIGN.md §3, §5.6, §6): the receiver.
//!
//! The run updates the index (or opens it read-only), exports and
//! publishes the hash list, prepares the pin file and the lookup workers,
//! starts both SSH sessions, and completes the exchange. It then
//! reconstructs stdout from literals, runs, and references, while the
//! lookup workers answer the sender's queries (DESIGN.md §6.4), and it
//! returns the command's status once both sessions exited successfully.
//! Compression arrives with PLAN.md step 5.

use std::ffi::OsStr;
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::process::{ChildStdin, ChildStdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::cache::{self, ListStore};
use crate::config::Config;
use crate::die::{self, die};
use crate::fsid::FsCache;
use crate::fsops;
use crate::hash::weak;
use crate::hashlist::{self, ListId};
use crate::index::{self, Settings};
use crate::lookup;
use crate::pins::{PinFile, Pins};
use crate::protocol::{
    self, Answer, End, FrameReader, FrameWriter, HashList, Query, ReceiverHello, Rle, SenderHello,
    frame,
};
use crate::rendezvous;
use crate::roots::{self, Location};
use crate::ssh;
use crate::sys;
use crate::update;

pub fn run(
    config: &Config,
    no_update: bool,
    cli_ssh: Option<&OsStr>,
    host: &OsStr,
    command: &[std::ffi::OsString],
) -> ! {
    // Before any other thread starts: SIGINT and SIGTERM end the run with
    // `128 + signal`, and the end kills the SSH children (DESIGN.md §6.6).
    die::handle_termination_signals();

    let options = config.transfer_options(host.as_bytes(), cli_ssh.map(OsStr::as_bytes));

    // Hold index access through the whole run (DESIGN.md §3): the update
    // keeps its writer, a read-only run keeps its keeper connection.
    let (index, locations) = if no_update {
        open_read_only(config)
    } else {
        update::perform(config, false, None)
    };
    let meta = index.read_meta().expect("the index lost its metadata");

    // Export and hash the target from one database snapshot, before the
    // sessions start (DESIGN.md §3 and §5.6).
    let encoded = hashlist::encode(&index.export_weak_hashes());
    let target = ListId {
        algorithm: config.strong_hash,
        digest: config.strong_hash.digest(&encoded),
    };

    // The pin file and the lookup workers, which answer queries as soon as
    // they arrive (DESIGN.md §6.4).
    let (answer_tx, answer_rx) = unbounded();
    let pin_file = PinFile::create(&config.pin_directory, config.block_size);
    let pins = Arc::new(Pins::new(
        pin_file,
        config.strong_hash,
        config.block_size,
        answer_tx,
    ));
    let lookups = lookup::start(&index, locations, &pins, config);

    let account_id = cache::account_id();
    let token = rendezvous::new_token();
    sys::reset_sigchld();
    let (data, control) = ssh::start_sessions(
        &options.ssh,
        &options.sender_command,
        host.as_bytes(),
        &token,
    );
    let sessions = [(data.pid(), "data"), (control.pid(), "control")];

    // Both sides send their protocol version first, without waiting for
    // the peer's (DESIGN.md §6.2). The relay delivers it once `attach`
    // connects. The receiver closes neither session's stdin before it
    // exits (DESIGN.md §6.7), so the control writer gets a reference that
    // lives as long as the process.
    let control_stdin: &'static ChildStdin = Box::leak(Box::new(control.stdin));
    let mut control_out = FrameWriter::new(BufWriter::new(control_stdin));
    if let Err(e) = control_out
        .send_version()
        .and_then(|()| control_out.flush())
    {
        die(format!("control session: {e}"));
    }
    {
        let pins = Arc::clone(&pins);
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || write_answers(control_out, &answer_rx, &pins))
            .expect("cannot start the control writer");
    }
    {
        let pins = Arc::clone(&pins);
        let control_stdout = control.stdout;
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || read_queries(control_stdout, &pins, &lookups))
            .expect("cannot start the control reader");
    }

    // The data session, with received bytes counted for the summary.
    let wire_bytes = Arc::new(AtomicU64::new(0));
    let mut data_out = FrameWriter::new(data.stdin);
    let mut data_in = FrameReader::new(BufReader::new(CountingReader {
        inner: data.stdout,
        count: Arc::clone(&wire_bytes),
    }));

    let hello = ReceiverHello {
        block_size: config.block_size,
        weak_hash_id: weak::ID.to_string(),
        strong_hash: config.strong_hash,
        account_id,
        index_id: meta.index_id,
        target: target.digest,
        sender_cache: options.sender_cache,
        hashlist_max_age: config.hashlist_max_age,
        lookahead: config.lookahead,
        // Compression arrives with PLAN.md step 5: negotiate it
        // disabled.
        compression_level: 0,
        compression_threads: 0,
        negative_cache: config.negative_cache,
        command: command.iter().map(|arg| arg.as_bytes().to_vec()).collect(),
    };
    // HELLO follows the version at once. The sender uses its parameters
    // or ends the run, so its reply does not repeat them.
    if let Err(e) = data_out.send_version() {
        die(format!("data session: {e}"));
    }
    send_data(&mut data_out, frame::HELLO, &hello.encode());
    flush_data(&mut data_out);

    check_sender_version(&mut data_in, host);
    let sender = SenderHello::decode(expect_data(&mut data_in, frame::HELLO, "HELLO"));

    // Durably publish the target in the sender's peer directory before
    // sending list data; a publication failure is fatal and ends setup
    // (DESIGN.md §5.6).
    let store = ListStore::receiver_side(&sender.account_id, &meta.index_id);
    let published = store.publish(&target, &encoded);

    // The full list, whose bytes follow HASHLIST directly; cached bases
    // and deltas arrive with PLAN.md step 8.
    let header = HashList {
        length: encoded.len() as u64,
    };
    send_data(&mut data_out, frame::HASHLIST, &header.encode());
    if let Err(e) = data_out.send_bytes(&encoded) {
        die(format!("data session: {e}"));
    }
    flush_data(&mut data_out);
    let hash_list_bytes = encoded.len() as u64;
    drop(encoded);

    // The run has one target, so the acknowledgement names none.
    let payload = expect_data(&mut data_in, frame::CACHE_RESULT, "CACHE_RESULT");
    assert!(
        payload.is_empty(),
        "protocol error: CACHE_RESULT with a payload"
    );
    // Renew the target after CACHE_RESULT; with a base this would renew
    // it too (DESIGN.md §5.6). Then expire both cache trees.
    published.renew();
    drop(published);
    cache::expire(config.hashlist_max_age);

    // Reconstruct stdout in order (DESIGN.md §6.4). `output_bytes` is
    // where the frame's output ends, checked before anything is written.
    // Once the output is written, the pins learn the new offset, which
    // releases the pins below it.
    let mut stdout = fsops::stdout();
    let block_size = u64::from(config.block_size);
    let mut block = vec![0u8; config.block_size as usize];
    let mut output_bytes = 0u64;
    let mut literal_bytes = 0u64;
    let mut reference_bytes = 0u64;
    let mut run_bytes = 0u64;
    let end = loop {
        let received = match data_in.next() {
            Ok(Some(received)) => received,
            Ok(None) => die("the data session ended before END"),
            Err(e) => die_data(&e),
        };
        match received.kind {
            frame::LIT => {
                assert!(!received.payload.is_empty(), "protocol error: empty LIT");
                let length = received.payload.len() as u64;
                output_bytes = advance(output_bytes, length);
                write_output(&mut stdout, received.payload);
                literal_bytes += length;
            }
            frame::REF => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: REF with a payload"
                );
                output_bytes = advance(output_bytes, block_size);
                pins.read_reference(&mut block);
                write_output(&mut stdout, &block);
                reference_bytes += block_size;
            }
            frame::RLE => {
                // The sender emits runs from step 5 on; accept any
                // positive length (DESIGN.md §6.4).
                let rle = Rle::decode(received.payload);
                output_bytes = advance(output_bytes, rle.length);
                run_bytes += rle.length;
                let chunk = vec![rle.byte; rle.length.min(64 * 1024) as usize];
                let mut remaining = rle.length;
                while remaining > 0 {
                    let n = remaining.min(chunk.len() as u64) as usize;
                    write_output(&mut stdout, &chunk[..n]);
                    remaining -= n as u64;
                }
            }
            frame::END => break End::decode(received.payload),
            kind => panic!("protocol error: unexpected data frame type {kind}"),
        }
        pins.reached(output_bytes);
    };

    // Completion (DESIGN.md §6.7). Compression is disabled, so END is
    // sufficient framing; flush reconstructed stdout, enter Completing,
    // send the single END_ACK, and drain the session to a clean EOF.
    if let Err(e) = stdout.flush() {
        die(format!("cannot write to stdout: {e}"));
    }
    pins.complete();
    send_data(&mut data_out, frame::END_ACK, b"");
    flush_data(&mut data_out);
    match data_in.drain_to_eof() {
        Ok(true) => {}
        Ok(false) => die("trailing data after END"),
        Err(e) => die_data(&e),
    }

    // Both sessions must exit successfully before the command's status
    // is returned, without reaping (DESIGN.md §6.6–6.7).
    for (pid, name) in sessions {
        let status = sys::wait_exit_status(pid);
        if status != 0 {
            die(format!("the {name} session exited with status {status}"));
        }
    }

    if end.status == 127 && !end.error.is_empty() {
        die::note(format!("remote command failed to start: {}", end.error));
    }
    die::note(format!(
        "{output_bytes} bytes output ({literal_bytes} literal, {reference_bytes} reference, \
         {run_bytes} run), {} bytes wire, {hash_list_bytes} bytes hash list, 0 bytes delta",
        wire_bytes.load(Relaxed),
    ));
    die::finish(end.status)
}

/// Read-only index access for `--no-update` (DESIGN.md §4.4 and §8):
/// resolve current roots, then require a compatible index with its block
/// indexes. Returns the index with the current root locations, from which
/// candidates are read.
fn open_read_only(config: &Config) -> (index::Index, Vec<Location>) {
    fsops::raise_descriptor_limit();
    let fs_cache = FsCache::new();
    let locations = roots::expand(config, &fs_cache);

    let path = config.index_path.as_bstr();
    let Some(index) = index::open_read_only(&config.index_path) else {
        die(format!("no index at {path}; run `hashsync update`"));
    };
    let Some(meta) = index.read_meta() else {
        die(format!("index {path} has no schema; run `hashsync update`"));
    };
    if !meta.compatible(&Settings::from_config(config)) {
        die(format!(
            "index {path} does not match the configuration; run `hashsync update`"
        ));
    }
    if !index.has_block_indexes() {
        die(format!(
            "index {path} is missing its block indexes; run `hashsync update`"
        ));
    }
    (index, locations)
}

/// Counts the bytes received on the data session, for the summary's
/// wire count.
struct CountingReader<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Relaxed);
        Ok(n)
    }
}

fn die_data(e: &std::io::Error) -> ! {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        die("the data session ended unexpectedly");
    }
    die(format!("data session: {e}"))
}

fn send_data<W: Write>(out: &mut FrameWriter<W>, kind: u8, payload: &[u8]) {
    if let Err(e) = out.send(kind, payload) {
        die(format!("data session: {e}"));
    }
}

fn flush_data<W: Write>(out: &mut FrameWriter<W>) {
    if let Err(e) = out.flush() {
        die(format!("data session: {e}"));
    }
}

fn expect_data<'a, R: Read>(
    data_in: &'a mut FrameReader<R>,
    kind: u8,
    what: &'static str,
) -> &'a [u8] {
    match data_in.expect(kind, what) {
        Ok(payload) => payload,
        Err(e) => die_data(&e),
    }
}

/// The sender's protocol version, which it sends before anything else
/// (DESIGN.md §6.2). EOF before it means that no usable `hashsync` ran.
fn check_sender_version<R: Read>(data_in: &mut FrameReader<R>, host: &OsStr) {
    match data_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "sender"),
        Ok(None) => die(format!(
            "the data session ended before the sender's protocol version; \
             is a compatible hashsync installed on {}?",
            host.as_bytes().as_bstr()
        )),
        Err(e) => die_data(&e),
    }
}

fn write_output(stdout: &mut fsops::FdWriter, bytes: &[u8]) {
    if let Err(e) = stdout.write_all(bytes) {
        die(format!("cannot write to stdout: {e}"));
    }
}

/// Advance the output offset; overflow is a protocol error
/// (DESIGN.md §6.2).
fn advance(offset: u64, length: u64) -> u64 {
    offset
        .checked_add(length)
        .expect("protocol error: output offset overflow")
}

/// The owner of the control session's incoming direction: the sender's
/// protocol version, then queries for the lookup workers. Loss before
/// Completing is fatal (DESIGN.md §6.7); in Completing, EOF, errors, and a
/// partial frame are expected closure. Reading goes on until closure, so
/// that the sender's relay never blocks on a full pipe.
fn read_queries(stdout: ChildStdout, pins: &Pins, lookups: &Sender<Query>) {
    let mut control_in = FrameReader::new(BufReader::new(stdout));
    let closed = || {
        if !pins.is_completing() {
            die("the control session closed unexpectedly");
        }
    };
    match control_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "sender's control session"),
        Ok(None) | Err(_) => return closed(),
    }
    loop {
        match control_in.next() {
            Ok(Some(received)) => {
                assert!(
                    received.kind == frame::QUERY,
                    "protocol error: unexpected control frame type {}",
                    received.kind
                );
                // The workers live as long as the process.
                let _ = lookups.send(Query::decode(received.payload));
            }
            Ok(None) | Err(_) => return closed(),
        }
    }
}

/// The owner of the control session's outgoing direction after the
/// version: answers in the order the workers queue them, flushed whenever
/// the queue runs empty. In Completing, a failed write is expected
/// closure (DESIGN.md §6.7).
fn write_answers(
    mut control_out: FrameWriter<BufWriter<&ChildStdin>>,
    answers: &Receiver<Answer>,
    pins: &Pins,
) {
    loop {
        let result = match control_out.next_from(answers) {
            Ok(Some(answer)) => control_out.send(frame::ANSWER, &answer.encode()),
            Ok(None) => return,
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            if !pins.is_completing() {
                die(format!("control session: {e}"));
            }
            return;
        }
    }
}
