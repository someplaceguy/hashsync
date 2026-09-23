//! `hashsync serve --token T` (DESIGN.md §3, §5.6, §6): the sender.
//!
//! `serve` first listens on the rendezvous socket, exchanges protocol
//! versions and data HELLOs, accepts `attach`, and completes the list
//! exchange and CACHE_RESULT before starting the command. This milestone
//! sends all command stdout as literals; the scanner arrives with PLAN.md
//! step 4.
//!
//! Each incoming direction has one owner for the whole run, so session
//! loss is fatal at any moment — during rendezvous, setup, startup
//! failure, and transfer alike (DESIGN.md §6.7). The data reader owns
//! stdin and walks the receiver's fixed sequence: the protocol version,
//! HELLO, HASHLIST and the list bytes, END_ACK, closure. A watcher owns the
//! control socket from acceptance. The main thread only writes: the
//! protocol versions, the HELLO reply, the CACHE_RESULT, the LIT pump,
//! and END; command success and startup failure share one completion
//! path.

use std::io::{BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use bstr::ByteSlice;
use crossbeam_channel::{Sender, bounded};
use rustix::process::Pid;

use crate::cache::{self, ListStore, PublishedList};
use crate::die::{self, die};
use crate::fsops::{self, retry_interrupted};
use crate::hash;
use crate::hashlist::{self, ListId};
use crate::hashtable::HashTable;
use crate::protocol::{
    self, End, FrameReader, FrameWriter, HashList, ReceiverHello, SenderHello, frame,
};
use crate::rendezvous;
use crate::sys;

/// The read chunk of the command pump; each chunk becomes one LIT.
const CHUNK: usize = 1 << 20;

pub fn run(token: &str) -> ! {
    // Listen before anything else, so `attach`'s retries can end.
    let listener = rendezvous::listen(token);

    let completing = Arc::new(AtomicBool::new(false));
    let end_sent = Arc::new(AtomicBool::new(false));
    let (hello_tx, hello_rx) = bounded(1);
    let (list_tx, list_rx) = bounded(1);
    let (ack_tx, ack_rx) = bounded(1);
    {
        let end_sent = Arc::clone(&end_sent);
        let completing = Arc::clone(&completing);
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || {
                let data_in = FrameReader::new(BufReader::new(fsops::stdin()));
                data_reader(data_in, hello_tx, list_tx, ack_tx, &end_sent, &completing)
            })
            .expect("cannot start the data reader");
    }
    let mut data_out = FrameWriter::new(fsops::stdout());

    // This side's protocol version goes first, without waiting for the
    // receiver's, which the data reader checks (DESIGN.md §6.2).
    if let Err(e) = data_out.send_version().and_then(|()| data_out.flush()) {
        die(format!("data session: {e}"));
    }

    // Data HELLOs: the receiver speaks first, this side replies only
    // after it accepted the receiver's parameters.
    let hello: ReceiverHello = hello_rx.recv().expect("the data reader ended before HELLO");
    validate_hello(&hello);
    let reply = SenderHello {
        account_id: cache::account_id(),
        // Cached-list reuse arrives with PLAN.md step 8: advertise
        // explicit absence and expect a full list.
        base: None,
    };
    send_data(&mut data_out, frame::HELLO, &reply.encode());
    flush_data(&mut data_out);

    // The control session: accept `attach`, then both sides send their
    // protocol version immediately. The watcher owns the incoming
    // direction and reports readiness once the receiver's version
    // matched.
    let control = listener.accept();
    {
        let mut control_out = FrameWriter::new(&control);
        if let Err(e) = control_out
            .send_version()
            .and_then(|()| control_out.flush())
        {
            die(format!("control session: {e}"));
        }
    }
    let (control_ready_tx, control_ready_rx) = bounded(1);
    {
        let completing = Arc::clone(&completing);
        let control_in = clone_control(&control);
        std::thread::Builder::new()
            .name("control".into())
            .spawn(move || control_watcher(control_in, control_ready_tx, &completing))
            .expect("cannot start the control watcher");
    }

    // The list, assembled and digest-verified by the data reader; then the
    // canonical decoding and the table (DESIGN.md §5.6).
    let encoded: Vec<u8> = list_rx
        .recv()
        .expect("the data reader ended before the hash list");
    let values = hashlist::decode(&encoded)
        .unwrap_or_else(|e| panic!("protocol error: bad full hash list: {e}"));
    // The scanner of PLAN.md step 4 probes this table; building it here
    // completes the setup that CACHE_RESULT promises.
    let _table = HashTable::build(&values);
    drop(values);

    // Durable publication, or cleanup with caching disabled; failures
    // are fatal inside the cache module (DESIGN.md §5.6). Acknowledge
    // the accepted target only afterwards, then renew and expire.
    let store = ListStore::sender_side(&hello.account_id, &hello.index_id);
    let published: Option<PublishedList> = if hello.sender_cache {
        let target = ListId {
            algorithm: hello.strong_hash,
            digest: hello.target,
        };
        Some(store.publish(&target, &encoded))
    } else {
        store.remove_all();
        None
    };
    drop(encoded);
    send_data(&mut data_out, frame::CACHE_RESULT, b"");
    flush_data(&mut data_out);
    if let Some(list) = &published {
        list.renew();
    }
    cache::expire(hello.hashlist_max_age);

    // The receiver's versions on both sessions must match before the
    // command starts (DESIGN.md §6.2). Both sessions stay monitored
    // during this wait: the data reader awaits END_ACK on stdin and the
    // watcher holds the socket.
    control_ready_rx
        .recv()
        .expect("the control watcher ended before the protocol version");

    // The command. Keep it unreaped and in its own process group
    // (DESIGN.md §6.6); a fatal exit SIGKILLs the group, and the spawn
    // is atomic with that registration.
    sys::reset_sigchld();
    let argv: Vec<&std::ffi::OsStr> = hello
        .command
        .iter()
        .map(|arg| std::ffi::OsStr::from_bytes(arg))
        .collect();
    let mut command = Command::new(argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let (status, error, stderr_relay) =
        match die::spawn_guarded(die::KillGuard::ProcessGroup, &mut command) {
            Ok(mut child) => {
                // Command stderr to this process's stderr; SSH forwards
                // it outside the protocol. The relay must finish before
                // successful exit.
                let mut command_stderr = child.stderr.take().unwrap();
                let relay = std::thread::Builder::new()
                    .name("stderr-relay".into())
                    .spawn(move || relay_stderr(&mut command_stderr))
                    .expect("cannot start the stderr relay");

                // The pump: command stdout to LIT frames, in order, with
                // backpressure from the data session. Stdout EOF and the
                // exit status are separate conditions; wait for both
                // (DESIGN.md §6.3).
                let mut command_stdout = child.stdout.take().unwrap();
                let mut buf = vec![0u8; CHUNK];
                loop {
                    let n = match retry_interrupted(|| command_stdout.read(&mut buf)) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => die(format!("cannot read the command's output: {e}")),
                    };
                    send_data(&mut data_out, frame::LIT, &buf[..n]);
                }
                let status = sys::wait_exit_status(Pid::from_child(&child));
                (status, String::new(), Some(relay))
            }
            // Startup failure (DESIGN.md §6.2): status 127 and the error
            // text; there are no command streams to drain.
            Err(e) => (127, format!("{}: {e}", argv[0].as_bytes().as_bstr()), None),
        };

    // The completion conditions of DESIGN.md §6.7, one path for command
    // success and startup failure: END, one valid END_ACK, and a
    // finished stderr relay when command streams exist. Set `end_sent`
    // first so an immediate acknowledgement is valid.
    end_sent.store(true, SeqCst);
    let end = End { status, error };
    send_data(&mut data_out, frame::END, &end.encode());
    flush_data(&mut data_out);
    ack_rx
        .recv()
        .expect("the data reader ended without acknowledgement");
    if let Some(relay) = stderr_relay {
        relay.join().expect("the stderr relay panicked");
    }

    // Normal cleanup once (DESIGN.md §6.7): close the control socket so
    // `attach` flushes and exits zero, then end normally, which removes
    // the rendezvous socket. Sender stdout stays open until process exit.
    completing.store(true, SeqCst);
    let _ = control.shutdown(Shutdown::Both);
    die::finish(0)
}

/// The single owner of the data session's incoming direction, for the
/// whole run: the protocol version, HELLO, the list, END_ACK, then
/// closure. EOF before the normal exit is the receiver's disappearance
/// and is fatal (DESIGN.md §6.7).
fn data_reader<R: Read>(
    mut data_in: FrameReader<R>,
    hello_tx: Sender<ReceiverHello>,
    list_tx: Sender<Vec<u8>>,
    ack_tx: Sender<()>,
    end_sent: &AtomicBool,
    completing: &AtomicBool,
) {
    match data_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "receiver"),
        Ok(None) => die("the receiver disappeared"),
        Err(e) => die_data(&e),
    }
    let payload = data_in
        .expect(frame::HELLO, "HELLO")
        .unwrap_or_else(|e| die_data(&e));
    let hello = ReceiverHello::decode(payload);
    let target = ListId {
        algorithm: hello.strong_hash,
        digest: hello.target,
    };
    let _ = hello_tx.send(hello);

    // The receiver sends the list only after this side's HELLO reply;
    // reading ahead of the main thread is safe because this thread only
    // validates and never replies.
    let encoded = receive_list(&mut data_in, target);
    let _ = list_tx.send(encoded);

    // END_ACK is the receiver's only remaining data-session frame
    // (DESIGN.md §6.2), and it must answer a sent END.
    match data_in.next() {
        Ok(Some(received)) => {
            assert!(
                received.kind == frame::END_ACK,
                "protocol error: expected END_ACK, received frame type {}",
                received.kind
            );
            assert!(
                received.payload.is_empty(),
                "protocol error: END_ACK with a payload"
            );
            assert!(end_sent.load(SeqCst), "protocol error: unsolicited END_ACK");
        }
        Ok(None) => die("the receiver disappeared"),
        Err(e) => die_data(&e),
    }
    let _ = ack_tx.send(());

    // Nothing else may arrive, and the receiver keeps the session open
    // until this process exits: closure before the normal exit remains
    // fatal, so a receiver lost while command stderr still drains kills
    // the command instead of leaving the run half-alive.
    match data_in.next() {
        Ok(Some(extra)) => {
            panic!("protocol error: frame type {} after END_ACK", extra.kind)
        }
        Ok(None) | Err(_) => {
            if !completing.load(SeqCst) {
                die("the receiver disappeared after END_ACK");
            }
        }
    }
}

/// The owner of the control session's incoming direction: the protocol
/// version, whose check gates command startup, then closure. No control
/// frame follows in this milestone; answers arrive with PLAN.md step 4.
fn control_watcher(stream: UnixStream, ready_tx: Sender<()>, completing: &AtomicBool) {
    let mut control_in = FrameReader::new(BufReader::new(stream));
    match control_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "receiver's control session"),
        Ok(None) => die("the control session closed before the protocol version"),
        Err(e) => die(format!("control session: {e}")),
    }
    let _ = ready_tx.send(());
    match control_in.next() {
        Ok(Some(received)) => panic!(
            "protocol error: unexpected control frame type {}",
            received.kind
        ),
        Ok(None) | Err(_) => {
            if !completing.load(SeqCst) {
                die("the control session closed unexpectedly");
            }
        }
    }
}

/// Copy command stderr to this process's stderr through EOF, with
/// prompt writes. Failures are fatal (DESIGN.md §6.5).
fn relay_stderr(command_stderr: &mut std::process::ChildStderr) {
    let mut stderr = fsops::stderr();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match retry_interrupted(|| command_stderr.read(&mut buf)) {
            Ok(0) => return,
            Ok(n) => {
                if let Err(e) = stderr.write_all(&buf[..n]) {
                    die(format!("stderr relay: write: {e}"));
                }
            }
            Err(e) => die(format!("stderr relay: read: {e}")),
        }
    }
}

fn die_data(e: &std::io::Error) -> ! {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        die("the receiver disappeared");
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

fn clone_control(control: &UnixStream) -> UnixStream {
    match control.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the control socket: {e}")),
    }
}

/// The receiver's HELLO parameters are the run's: use them, or end the
/// run before command startup (DESIGN.md §6.2, §6.5). Decoding already
/// checked the strong hash.
fn validate_hello(hello: &ReceiverHello) {
    if hello.weak_hash_id != hash::weak::ID {
        die(format!(
            "incompatible receiver: weak hash {} (this sender: {})",
            hello.weak_hash_id,
            hash::weak::ID
        ));
    }
    // Compression arrives with PLAN.md step 5; this version negotiates
    // it disabled.
    if hello.compression_level != 0 {
        die(format!(
            "incompatible receiver: compression level {} (this sender supports none)",
            hello.compression_level
        ));
    }
}

/// Receive the full canonical list: HASHLIST, then the list bytes that
/// follow it directly, verified against the target digest
/// (DESIGN.md §5.6).
fn receive_list<R: Read>(data_in: &mut FrameReader<R>, target: ListId) -> Vec<u8> {
    let payload = data_in
        .expect(frame::HASHLIST, "HASHLIST")
        .unwrap_or_else(|e| die_data(&e));
    let header = HashList::decode(payload);
    let encoded = data_in
        .bytes(header.length)
        .unwrap_or_else(|e| die_data(&e));
    assert!(
        target.algorithm.digest(&encoded) == target.digest,
        "protocol error: the full hash list does not match {target}"
    );
    encoded
}
