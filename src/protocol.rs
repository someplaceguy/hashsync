//! The transfer protocol's version exchange, frames, and payload layouts
//! (DESIGN.md §6.2).
//!
//! Both roles share these definitions. The version exchange never
//! changes; DESIGN.md §11 says which other changes require a protocol
//! version bump.
//!
//! Readers grow their buffers only as bytes arrive, so no length or count
//! needs a bound beyond its field: a corrupt value costs only the bytes
//! that really follow it.
//!
//! After its HELLO, the sender's data-session frames through END share one
//! zstd stream unless compression is disabled: `Compressor` writes it and
//! `Decompressor` reads it. DONE follows the stream, outside it.
//!
//! Malformed payloads, invalid compressed data, and violated frame rules
//! are bugs in a compatible peer: parsers panic (DESIGN.md §6.5). Transport
//! failures, including EOF inside a frame, surface as `io::Error` for the
//! caller to turn into `die`.

use std::io::{BufRead, Read, Write};
use std::ops::RangeInclusive;

use crossbeam_channel::{Receiver, TryRecvError};
use zstd_sys::ZSTD_EndDirective;

use crate::die::die;
use crate::hash::{Strong, StrongHash};
use crate::sys::{ZstdCCtx, ZstdDCtx};

/// The first bytes of each session, in both directions, as a
/// little-endian `u32`.
pub const PROTOCOL_VERSION: u32 = 1;

/// Frame types. `type u8, length u32, payload`, integers little endian.
pub mod frame {
    // The full frame space is defined here (DESIGN.md §6.2); later
    // milestones send the remaining types.
    #![allow(dead_code)]

    pub const HELLO: u8 = 1;
    pub const HASHLIST: u8 = 2;
    pub const HASHDELTA: u8 = 3;
    pub const HASHKEEP: u8 = 4;
    pub const HASHFULL: u8 = 5;
    pub const CACHE_RESULT: u8 = 6;
    pub const LIT: u8 = 7;
    pub const REF: u8 = 8;
    pub const RLE: u8 = 9;
    pub const END: u8 = 10;
    pub const END_ACK: u8 = 11;
    pub const QUERY: u8 = 12;
    pub const ANSWER: u8 = 13;
    pub const DONE: u8 = 14;
}

/// Require the peer's protocol version, read before any frame, to equal
/// this side's; another version can change everything that follows.
/// `who` names the peer for the failure message.
pub fn check_version(version: u32, who: &str) {
    if version != PROTOCOL_VERSION {
        die(format!(
            "the {who} speaks protocol version {version}, \
             this hashsync {} speaks {PROTOCOL_VERSION}",
            env!("CARGO_PKG_VERSION")
        ));
    }
}

/// HELLO, receiver to sender. Its block size and hashes are the run's:
/// the sender uses them or ends the run. Every digest of the run,
/// including the target's, uses `strong_hash` (DESIGN.md §6.2).
pub struct ReceiverHello {
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash: StrongHash,
    /// The digest of the receiver's hash list.
    pub target: Strong,
    pub account_id: [u8; 16],
    pub index_id: [u8; 16],
    pub sender_cache: bool,
    /// Seconds.
    pub hashlist_max_age: u64,
    pub compression_level: i32,
    /// zstd long-distance matching (DESIGN.md §7).
    pub compression_long: bool,
    pub compression_threads: u32,
    /// Threads that probe the weak table for the scanner; 0 selects half
    /// the sender's cores, at least one (DESIGN.md §6.3).
    pub probe_threads: u32,
    pub lookahead: u64,
    /// Bytes.
    pub negative_cache: u64,
    /// The command's argv; argv[0] is found in PATH and run directly.
    pub command: Vec<Vec<u8>>,
}

impl ReceiverHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u32(self.block_size);
        enc.bytes(self.weak_hash_id.as_bytes());
        enc.bytes(self.strong_hash.id().as_bytes());
        enc.raw(&self.target);
        enc.raw(&self.account_id);
        enc.raw(&self.index_id);
        enc.u8(self.sender_cache.into());
        enc.u64(self.hashlist_max_age);
        enc.i32(self.compression_level);
        enc.u8(self.compression_long.into());
        enc.u32(self.compression_threads);
        enc.u32(self.probe_threads);
        enc.u64(self.lookahead);
        enc.u64(self.negative_cache);
        enc.u32(list_len(self.command.len()));
        for arg in &self.command {
            enc.bytes(arg);
        }
        enc.buf
    }

    /// The target's length depends on the strong hash, so an unsupported
    /// strong hash ends the run here, before the target is read
    /// (DESIGN.md §6.2). The sender checks the other parameters after
    /// decoding.
    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let block_size = dec.u32();
        let weak_hash_id = dec.string();
        let strong_hash_id = dec.string();
        let Some(strong_hash) = StrongHash::from_id(&strong_hash_id) else {
            die(format!(
                "incompatible receiver: strong hash {strong_hash_id}"
            ));
        };
        let hello = ReceiverHello {
            block_size,
            weak_hash_id,
            strong_hash,
            target: dec.array(),
            account_id: dec.array(),
            index_id: dec.array(),
            sender_cache: dec.flag(),
            hashlist_max_age: dec.u64(),
            compression_level: dec.i32(),
            compression_long: dec.flag(),
            compression_threads: dec.u32(),
            probe_threads: dec.u32(),
            lookahead: dec.u64(),
            negative_cache: dec.u64(),
            command: dec.list(Dec::string_bytes),
        };
        dec.done();
        assert!(hello.block_size > 0, "protocol error: block size 0");
        assert!(
            hello.lookahead >= u64::from(hello.block_size),
            "protocol error: lookahead {} below the block size",
            hello.lookahead
        );
        assert!(!hello.command.is_empty(), "protocol error: empty command");
        hello
    }
}

/// HELLO, sender to receiver. The sender sends it only after it
/// accepted the receiver's HELLO, so it does not repeat those
/// parameters. The base's digest uses the run's strong hash.
pub struct SenderHello {
    pub account_id: [u8; 16],
    pub base: Option<Strong>,
}

impl SenderHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.raw(&self.account_id);
        match &self.base {
            None => enc.u8(0),
            Some(base) => {
                enc.u8(1);
                enc.raw(base);
            }
        }
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let hello = SenderHello {
            account_id: dec.array(),
            base: match dec.flag() {
                false => None,
                true => Some(dec.array()),
            },
        };
        dec.done();
        hello
    }
}

/// HASHLIST: the encoded length of the target list, whose bytes follow the
/// frame directly.
pub struct HashList {
    pub length: u64,
}

impl HashList {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u64(self.length);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HASHLIST");
        let header = HashList { length: dec.u64() };
        dec.done();
        header
    }
}

/// END: the command's exit status and, for a startup failure, error text.
pub struct End {
    pub status: i32,
    pub error: String,
}

impl End {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.i32(self.status);
        enc.bytes(self.error.as_bytes());
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "END");
        let end = End {
            status: dec.i32(),
            error: dec.string(),
        };
        dec.done();
        end
    }
}

/// QUERY: a request for the verified block whose full strong hash is
/// `strong`, for the window at output offset `offset`.
pub struct Query {
    pub offset: u64,
    pub strong: Strong,
}

impl Query {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u64(self.offset);
        enc.raw(&self.strong);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "QUERY");
        let query = Query {
            offset: dec.u64(),
            strong: dec.array(),
        };
        dec.done();
        query
    }
}

/// ANSWER: whether the receiver pinned a verified block for the query at
/// `offset`.
pub struct Answer {
    pub offset: u64,
    pub found: bool,
}

impl Answer {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u64(self.offset);
        enc.u8(self.found.into());
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "ANSWER");
        let answer = Answer {
            offset: dec.u64(),
            found: dec.flag(),
        };
        dec.done();
        answer
    }
}

/// RLE: a repeated byte value and length.
pub struct Rle {
    pub byte: u8,
    pub length: u64,
}

impl Rle {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u8(self.byte);
        enc.u64(self.length);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "RLE");
        let rle = Rle {
            byte: dec.u8(),
            length: dec.u64(),
        };
        dec.done();
        assert!(rle.length > 0, "protocol error: empty RLE frame");
        rle
    }
}

fn list_len(len: usize) -> u32 {
    u32::try_from(len).expect("protocol error: list too long")
}

/// A payload builder for the little-endian layouts above.
pub struct Enc {
    pub buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Self {
        Enc { buf: Vec::new() }
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Bytes preceded by their `u32` length: a string or byte string.
    pub fn bytes(&mut self, v: &[u8]) {
        self.u32(list_len(v.len()));
        self.buf.extend_from_slice(v);
    }

    /// Bytes with a length fixed by the protocol, without a prefix.
    pub fn raw(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }
}

/// A payload parser. Truncation, trailing bytes, and invalid values are
/// protocol errors and panic (DESIGN.md §6.5).
pub struct Dec<'a> {
    buf: &'a [u8],
    what: &'static str,
}

impl<'a> Dec<'a> {
    pub fn new(buf: &'a [u8], what: &'static str) -> Self {
        Dec { buf, what }
    }

    fn take(&mut self, len: usize) -> &'a [u8] {
        assert!(
            len <= self.buf.len(),
            "protocol error: truncated {} frame",
            self.what
        );
        let (taken, rest) = self.buf.split_at(len);
        self.buf = rest;
        taken
    }

    pub fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }

    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take(4).try_into().unwrap())
    }

    pub fn i32(&mut self) -> i32 {
        i32::from_le_bytes(self.take(4).try_into().unwrap())
    }

    pub fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }

    /// A 0-or-1 byte.
    pub fn flag(&mut self) -> bool {
        match self.u8() {
            0 => false,
            1 => true,
            v => panic!("protocol error: invalid flag {v} in {} frame", self.what),
        }
    }

    pub fn array<const N: usize>(&mut self) -> [u8; N] {
        self.take(N).try_into().unwrap()
    }

    /// A length-prefixed byte string.
    pub fn string_bytes(&mut self) -> Vec<u8> {
        let len = self.u32() as usize;
        self.take(len).to_vec()
    }

    /// A length-prefixed UTF-8 string, such as an algorithm or error text.
    pub fn string(&mut self) -> String {
        String::from_utf8(self.string_bytes()).unwrap_or_else(|_| {
            panic!(
                "protocol error: invalid UTF-8 string in {} frame",
                self.what
            )
        })
    }

    /// A `u32` item count, then the items. The list grows only as items
    /// parse: `collect` would reserve space for the announced count
    /// first, so a corrupt count could cost far more memory than the
    /// payload holds (DESIGN.md §3).
    pub fn list<T>(&mut self, mut item: impl FnMut(&mut Self) -> T) -> Vec<T> {
        let count = self.u32();
        let mut items = Vec::new();
        for _ in 0..count {
            items.push(item(self));
        }
        items
    }

    /// Require that the whole payload was consumed.
    pub fn done(self) {
        assert!(
            self.buf.is_empty(),
            "protocol error: excess bytes in {} frame",
            self.what
        );
    }
}

/// One received frame, borrowing the reader's payload buffer.
#[derive(Debug)]
pub struct Frame<'a> {
    pub kind: u8,
    pub payload: &'a [u8],
}

/// Reads frames from a transport, reusing one payload buffer.
pub struct FrameReader<R: Read> {
    reader: R,
    payload: Vec<u8>,
}

impl<R: Read> FrameReader<R> {
    pub fn new(reader: R) -> Self {
        FrameReader {
            reader,
            payload: Vec::new(),
        }
    }

    /// The transport. Nothing after the last frame read is lost: frames
    /// are read without reading ahead.
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// The peer's protocol version, which precedes its first frame;
    /// `None` at a clean EOF before it. A truncated version is
    /// `UnexpectedEof`.
    pub fn version(&mut self) -> std::io::Result<Option<u32>> {
        let mut bytes = [0u8; 4];
        Ok(self
            .read_whole(&mut bytes)?
            .then(|| u32::from_le_bytes(bytes)))
    }

    /// The next frame; `None` at a clean EOF between frames. EOF inside a
    /// frame is `UnexpectedEof`: a truncated stream, for the caller's
    /// `die`.
    pub fn next(&mut self) -> std::io::Result<Option<Frame<'_>>> {
        let mut header = [0u8; 5];
        if !self.read_whole(&mut header)? {
            return Ok(None);
        }
        let kind = header[0];
        let length = u32::from_le_bytes(header[1..].try_into().unwrap());
        self.payload.clear();
        read_exactly(&mut self.reader, length.into(), &mut self.payload)?;
        Ok(Some(Frame {
            kind,
            payload: &self.payload,
        }))
    }

    /// Exactly `length` bytes that follow a frame outside any frame: the
    /// list bytes after HASHLIST or HASHDELTA. EOF first is
    /// `UnexpectedEof`.
    pub fn bytes(&mut self, length: u64) -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        read_exactly(&mut self.reader, length, &mut bytes)?;
        Ok(bytes)
    }

    /// The next frame, which must exist and have this type.
    pub fn expect(&mut self, kind: u8, what: &'static str) -> std::io::Result<&[u8]> {
        let frame = self.next()?.ok_or(std::io::ErrorKind::UnexpectedEof)?;
        assert!(
            frame.kind == kind,
            "protocol error: expected {what}, received frame type {}",
            frame.kind
        );
        Ok(frame.payload)
    }

    /// Fill `buf`; `false` at a clean EOF before its first byte. The first
    /// byte is read separately to tell that EOF from a truncated `buf`,
    /// which is `UnexpectedEof`.
    fn read_whole(&mut self, buf: &mut [u8]) -> std::io::Result<bool> {
        loop {
            match self.reader.read(&mut buf[..1]) {
                Ok(0) => return Ok(false),
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        self.reader.read_exact(&mut buf[1..])?;
        Ok(true)
    }
}

/// Read `reader` to EOF, requiring that no byte arrives. `false` when
/// trailing bytes arrived.
pub fn drain_to_eof(mut reader: impl Read) -> std::io::Result<bool> {
    let mut buf = [0u8; 4096];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => return Ok(true),
            Ok(_) => return Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// Writes frames to a transport: one header write and one payload write
/// per frame. A writer that sends many small frames wraps its transport in
/// a `BufWriter`. A control writer flushes before it waits (`next_from`).
/// On the data session, a side flushes only before it waits for a reply
/// (DESIGN.md §6.2).
pub struct FrameWriter<W: Write> {
    writer: W,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(writer: W) -> Self {
        FrameWriter { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    /// This side's protocol version: the session's first bytes, sent
    /// without waiting for the peer's.
    pub fn send_version(&mut self) -> std::io::Result<()> {
        self.writer.write_all(&PROTOCOL_VERSION.to_le_bytes())
    }

    pub fn send(&mut self, kind: u8, payload: &[u8]) -> std::io::Result<()> {
        let length = u32::try_from(payload.len())
            .unwrap_or_else(|_| panic!("frame type {kind} with {} bytes", payload.len()));
        let mut header = [kind, 0, 0, 0, 0];
        header[1..].copy_from_slice(&length.to_le_bytes());
        self.writer.write_all(&header)?;
        self.writer.write_all(payload)
    }

    /// Bytes that follow a frame outside any frame: the list bytes after
    /// HASHLIST or HASHDELTA.
    pub fn send_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(bytes)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// The next item from `queue`, for a control writer, which writes what
    /// the queue delivers. When nothing is queued, flush first: the
    /// scanner waits for answers, so queries and answers must not stay in
    /// a buffer while this thread waits (DESIGN.md §6.6). `None`, after a
    /// flush, once the queue is closed and empty.
    pub fn next_from<T>(&mut self, queue: &Receiver<T>) -> std::io::Result<Option<T>> {
        match queue.try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                self.flush()?;
                Ok(queue.recv().ok())
            }
        }
    }
}

/// The compression levels that a sender supports (DESIGN.md §7): zstd
/// levels 1 to 19, and 0, which disables compression.
pub const COMPRESSION_LEVELS: RangeInclusive<i32> = 0..=19;

/// The window log of long-distance matching: a 128 MiB window, as with
/// `zstd --long=27`. A zstd decoder accepts windows up to this size by
/// default, so the receiver needs no setting for it.
const LONG_WINDOW_LOG: u32 = 27;

/// The sender's data session after its HELLO (DESIGN.md §6.2): one zstd
/// stream for all frames, or the frames as they are when compression is
/// disabled. `flush` makes every byte written so far decodable by the
/// receiver, for a frame that it must answer, and `finish` ends the stream
/// after END. The zstd frame has a checksum: the trailer that the receiver
/// checks (DESIGN.md §6.7).
///
/// Transport failures are `io::Error`s for the caller's `die`. A failure of
/// zstd itself panics.
pub struct Compressor<W: Write> {
    transport: W,
    /// `None` when compression is disabled.
    zstd: Option<Encoding>,
}

struct Encoding {
    encoder: ZstdCCtx,
    /// Compressed bytes on their way to the transport.
    buffer: Box<[u8]>,
}

impl<W: Write> Compressor<W> {
    /// Compress at zstd `level` on `threads` worker threads, with
    /// long-distance matching when `long`; level 0 disables compression.
    pub fn new(transport: W, level: i32, long: bool, threads: u32) -> Self {
        assert!(
            COMPRESSION_LEVELS.contains(&level),
            "compression level {level}"
        );
        let zstd = (level != 0).then(|| Encoding {
            encoder: ZstdCCtx::new(level, threads, long.then_some(LONG_WINDOW_LOG))
                .unwrap_or_else(zstd_failed),
            buffer: vec![0; ZstdCCtx::out_size()].into_boxed_slice(),
        });
        Compressor { transport, zstd }
    }

    /// End the stream, flush the transport, and return it.
    pub fn finish(mut self) -> std::io::Result<W> {
        if let Some(zstd) = &mut self.zstd {
            zstd.drain(&mut self.transport, ZSTD_EndDirective::ZSTD_e_end)?;
        }
        self.transport.flush()?;
        Ok(self.transport)
    }
}

impl Encoding {
    /// Repeat `end_op` without input, which flushes (`ZSTD_e_flush`) or
    /// ends the frame (`ZSTD_e_end`), and pass the output to `transport`,
    /// until zstd holds no more.
    fn drain(
        &mut self,
        transport: &mut impl Write,
        end_op: ZSTD_EndDirective,
    ) -> std::io::Result<()> {
        loop {
            let status = self
                .encoder
                .compress_stream(&[], &mut self.buffer, end_op)
                .unwrap_or_else(zstd_failed);
            transport.write_all(&self.buffer[..status.written])?;
            if status.remaining == 0 {
                return Ok(());
            }
        }
    }
}

impl<W: Write> Write for Compressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(zstd) = &mut self.zstd else {
            return self.transport.write(buf);
        };
        // Each call takes input or, when every worker is busy, waits for
        // output.
        let mut read = 0;
        while read < buf.len() {
            let status = zstd
                .encoder
                .compress_stream(
                    &buf[read..],
                    &mut zstd.buffer,
                    ZSTD_EndDirective::ZSTD_e_continue,
                )
                .unwrap_or_else(zstd_failed);
            read += status.read;
            self.transport.write_all(&zstd.buffer[..status.written])?;
        }
        Ok(buf.len())
    }

    /// Make every byte written so far decodable, then flush the transport.
    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(zstd) = &mut self.zstd {
            zstd.drain(&mut self.transport, ZSTD_EndDirective::ZSTD_e_flush)?;
        }
        self.transport.flush()
    }
}

/// The receiver's end of the sender's data session after HELLO
/// (DESIGN.md §6.2): the decoded bytes of one zstd frame, or the bytes of
/// the transport when compression is disabled. After END, `finish` checks
/// that the frame ends there.
///
/// EOF of the transport inside the frame is `UnexpectedEof`: a truncated
/// stream. Other transport failures are `io::Error`s too, for the caller's
/// `die`. Invalid compressed data, including a wrong checksum, and a frame
/// that ends before END are protocol errors and panic (DESIGN.md §6.5): a
/// truncated stream cannot end a frame, because its end needs the last
/// block and the checksum.
pub struct Decompressor<R: BufRead> {
    transport: R,
    /// `None` when compression is disabled.
    zstd: Option<Decoding>,
}

struct Decoding {
    decoder: ZstdDCtx,
    /// Decoded bytes, of which `[start, end)` are not read yet.
    buffer: Box<[u8]>,
    start: usize,
    end: usize,
    /// zstd decoded the whole frame and verified its checksum.
    ended: bool,
}

impl<R: BufRead> Decompressor<R> {
    pub fn new(transport: R, compressed: bool) -> Self {
        let zstd = compressed.then(|| Decoding {
            decoder: ZstdDCtx::new(),
            buffer: vec![0; ZstdDCtx::out_size()].into_boxed_slice(),
            start: 0,
            end: 0,
            ended: false,
        });
        Decompressor { transport, zstd }
    }

    /// After END: read the rest of the zstd frame, which must hold no more
    /// decoded bytes, and return the transport, right after the frame
    /// (DESIGN.md §6.7). Only the frame is read: this does not wait for the
    /// transport's EOF. Without compression, END ends the stream.
    pub fn finish(mut self) -> std::io::Result<R> {
        if let Some(zstd) = &mut self.zstd {
            let more = zstd.available(&mut self.transport)?;
            assert!(!more, "protocol error: data after END");
        }
        Ok(self.transport)
    }
}

impl Decoding {
    /// True when decoded bytes wait to be read, after decoding more if the
    /// buffer holds none; false at the end of the frame. Interrupted
    /// transport reads are retried (DESIGN.md §3).
    fn available(&mut self, transport: &mut impl BufRead) -> std::io::Result<bool> {
        if self.start < self.end {
            return Ok(true);
        }
        // zstd can hold decoded bytes back. Ask for them without input
        // first, so that they never wait for more of the transport.
        let mut input_needed = false;
        while !self.ended {
            let input = if input_needed {
                // Retry here, not in the callers: zstd fails after 16 steps
                // without progress, and each call begins with a step
                // without input.
                loop {
                    match transport.fill_buf() {
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        result => break result?,
                    }
                }
            } else {
                &[]
            };
            if input_needed && input.is_empty() {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            let status = self
                .decoder
                .decompress_stream(input, &mut self.buffer)
                .unwrap_or_else(|e| panic!("protocol error: invalid compressed data: {e}"));
            transport.consume(status.read);
            // At the end of the frame, zstd consumed nothing after it.
            self.ended = status.remaining == 0;
            (self.start, self.end) = (0, status.written);
            if self.end > 0 {
                return Ok(true);
            }
            input_needed = true;
        }
        Ok(false)
    }
}

impl<R: BufRead> Read for Decompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let Some(zstd) = &mut self.zstd else {
            return self.transport.read(buf);
        };
        if buf.is_empty() {
            return Ok(0);
        }
        // The sender ends the frame only after END, and only `finish`
        // reads after END.
        assert!(
            zstd.available(&mut self.transport)?,
            "protocol error: the compressed stream ended before END"
        );
        let n = buf.len().min(zstd.end - zstd.start);
        buf[..n].copy_from_slice(&zstd.buffer[zstd.start..zstd.start + n]);
        zstd.start += n;
        Ok(n)
    }
}

/// zstd reports its own failures, such as a failed allocation, as errors.
/// They are not transport failures: like a failed Rust allocation, they
/// end the process with a core dump.
fn zstd_failed<T>(e: &str) -> T {
    panic!("zstd: {e}")
}

/// Append exactly `length` bytes from `reader` to `out`. `out` grows only
/// as bytes arrive, so a corrupt length costs no more memory than the
/// bytes that really follow it. EOF first is `UnexpectedEof`.
fn read_exactly<R: Read>(reader: R, length: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
    let start = out.len();
    reader.take(length).read_to_end(out)?;
    if ((out.len() - start) as u64) < length {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::tests::noise;

    fn read_all(bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut reader = FrameReader::new(bytes);
        let mut frames = Vec::new();
        while let Some(frame) = reader.next().unwrap() {
            frames.push((frame.kind, frame.payload.to_vec()));
        }
        frames
    }

    #[test]
    fn frames_roundtrip() {
        let mut out = Vec::new();
        let mut writer = FrameWriter::new(&mut out);
        writer.send(frame::LIT, b"abc").unwrap();
        writer.send(frame::END_ACK, b"").unwrap();
        assert_eq!(out[..5], [frame::LIT, 3, 0, 0, 0]);
        assert_eq!(
            read_all(&out),
            vec![(frame::LIT, b"abc".to_vec()), (frame::END_ACK, Vec::new())]
        );
    }

    #[test]
    fn truncated_frames_are_unexpected_eof() {
        let mut out = Vec::new();
        FrameWriter::new(&mut out)
            .send(frame::LIT, b"abcd")
            .unwrap();
        for cut in 1..out.len() {
            let mut reader = FrameReader::new(&out[..cut]);
            let error = reader.next().unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn a_corrupt_length_costs_only_the_bytes_that_arrive() {
        // The header announces 4 GiB, but only three payload bytes follow.
        let bytes = [frame::LIT, 0xff, 0xff, 0xff, 0xff, 1, 2, 3];
        let mut reader = FrameReader::new(&bytes[..]);
        let error = reader.next().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(reader.payload.capacity() < 1 << 20);
    }

    #[test]
    fn list_bytes_follow_their_frame_directly() {
        let mut out = Vec::new();
        let mut writer = FrameWriter::new(&mut out);
        let header = HashList { length: 3 };
        writer.send(frame::HASHLIST, &header.encode()).unwrap();
        writer.send_bytes(b"abc").unwrap();
        writer.send(frame::END_ACK, b"").unwrap();

        let mut reader = FrameReader::new(&out[..]);
        let payload = reader.expect(frame::HASHLIST, "HASHLIST").unwrap();
        let length = HashList::decode(payload).length;
        assert_eq!(reader.bytes(length).unwrap(), b"abc");
        assert_eq!(reader.next().unwrap().unwrap().kind, frame::END_ACK);

        let mut short = FrameReader::new(&b"ab"[..]);
        let error = short.bytes(3).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn the_version_precedes_the_first_frame() {
        let mut out = Vec::new();
        let mut writer = FrameWriter::new(&mut out);
        writer.send_version().unwrap();
        writer.send(frame::HELLO, b"x").unwrap();
        assert_eq!(out[..4], PROTOCOL_VERSION.to_le_bytes());
        let mut reader = FrameReader::new(&out[..]);
        assert_eq!(reader.version().unwrap(), Some(PROTOCOL_VERSION));
        assert_eq!(reader.next().unwrap().unwrap().kind, frame::HELLO);

        assert_eq!(FrameReader::new(&out[..0]).version().unwrap(), None);
        for cut in 1..4 {
            let error = FrameReader::new(&out[..cut]).version().unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    fn receiver_hello() -> ReceiverHello {
        ReceiverHello {
            block_size: 4096,
            weak_hash_id: "weak".into(),
            strong_hash: StrongHash::Sha256,
            target: [3; 32],
            account_id: [7; 16],
            index_id: [9; 16],
            sender_cache: true,
            hashlist_max_age: 90 * 24 * 3600,
            compression_level: 0,
            compression_long: true,
            compression_threads: 0,
            probe_threads: 5,
            lookahead: 256 << 20,
            negative_cache: 128 << 20,
            command: vec![b"printf".to_vec(), b"%s".to_vec(), b"\xff\xfe".to_vec()],
        }
    }

    /// The peak virtual size of this process in bytes. It includes
    /// reservations that overcommit grants without memory.
    fn peak_virtual_size() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let line = status.lines().find(|l| l.starts_with("VmPeak:")).unwrap();
        let kib: u64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
        kib << 10
    }

    #[test]
    fn a_corrupt_argument_count_costs_only_the_arguments_that_arrive() {
        // The count announces u32::MAX arguments, but none follow. A list
        // reserved for that count takes 96 GiB, which overcommit can grant
        // without memory, so compare the peak virtual size. Tests that
        // run in parallel add much less than the bound.
        let mut payload = ReceiverHello {
            command: Vec::new(),
            ..receiver_hello()
        }
        .encode();
        let count = payload.len() - 4;
        payload[count..].copy_from_slice(&u32::MAX.to_le_bytes());
        let before = peak_virtual_size();
        let Err(unwound) = std::panic::catch_unwind(|| ReceiverHello::decode(&payload)) else {
            panic!("a HELLO without its arguments must not decode");
        };
        let message = unwound.downcast::<String>().unwrap();
        assert!(message.contains("truncated HELLO frame"), "{message}");
        assert!(peak_virtual_size() - before < 16 << 30);
    }

    #[test]
    fn receiver_hello_roundtrips() {
        let hello = receiver_hello();
        let decoded = ReceiverHello::decode(&hello.encode());
        assert_eq!(decoded.block_size, 4096);
        assert_eq!(decoded.weak_hash_id, "weak");
        assert_eq!(decoded.strong_hash, StrongHash::Sha256);
        assert_eq!(decoded.account_id, [7; 16]);
        assert_eq!(decoded.target, hello.target);
        assert!(decoded.sender_cache);
        assert!(decoded.compression_long);
        assert_eq!(decoded.probe_threads, 5);
        assert_eq!(decoded.command, hello.command);
    }

    #[test]
    fn sender_hello_roundtrips_with_and_without_base() {
        let mut hello = SenderHello {
            account_id: [1; 16],
            base: None,
        };
        let decoded = SenderHello::decode(&hello.encode());
        assert_eq!(decoded.account_id, [1; 16]);
        assert!(decoded.base.is_none());
        hello.base = Some([5; 32]);
        assert_eq!(SenderHello::decode(&hello.encode()).base, Some([5; 32]));
    }

    #[test]
    fn queries_and_answers_roundtrip() {
        let query = Query::decode(
            &Query {
                offset: u64::MAX - 1,
                strong: [6; 32],
            }
            .encode(),
        );
        assert_eq!((query.offset, query.strong), (u64::MAX - 1, [6; 32]));
        for found in [false, true] {
            let answer = Answer::decode(&Answer { offset: 17, found }.encode());
            assert_eq!((answer.offset, answer.found), (17, found));
        }
    }

    #[test]
    #[should_panic(expected = "invalid flag 2 in ANSWER")]
    fn an_answer_is_zero_or_one() {
        let mut payload = Answer {
            offset: 0,
            found: true,
        }
        .encode();
        *payload.last_mut().unwrap() = 2;
        Answer::decode(&payload);
    }

    #[test]
    #[should_panic(expected = "truncated QUERY")]
    fn a_query_carries_the_full_digest() {
        let mut enc = Enc::new();
        enc.u64(0);
        enc.raw(&[0; 16]);
        Query::decode(&enc.buf);
    }

    #[test]
    #[should_panic(expected = "lookahead 4095 below the block size")]
    fn the_lookahead_covers_a_window() {
        ReceiverHello::decode(
            &ReceiverHello {
                lookahead: 4095,
                ..receiver_hello()
            }
            .encode(),
        );
    }

    #[test]
    #[should_panic(expected = "excess bytes")]
    fn trailing_payload_bytes_are_rejected() {
        let mut payload = End {
            status: 0,
            error: String::new(),
        }
        .encode();
        payload.push(0);
        End::decode(&payload);
    }

    #[test]
    fn rle_roundtrips() {
        let rle = Rle::decode(
            &Rle {
                byte: 0xfe,
                length: u64::MAX,
            }
            .encode(),
        );
        assert_eq!((rle.byte, rle.length), (0xfe, u64::MAX));
    }

    fn end_payload() -> Vec<u8> {
        End {
            status: 3,
            error: String::new(),
        }
        .encode()
    }

    /// The frames of the sender's data session after HELLO, through a
    /// `Compressor`, and the finished stream.
    fn compressed(level: i32, long: bool, threads: u32, frames: &[(u8, &[u8])]) -> Vec<u8> {
        let mut writer = FrameWriter::new(Compressor::new(Vec::new(), level, long, threads));
        for &(kind, payload) in frames {
            writer.send(kind, payload).unwrap();
        }
        writer.into_inner().finish().unwrap()
    }

    #[test]
    fn frames_roundtrip_through_the_compressed_stream() {
        let zeros = vec![0; 100_000];
        let run = Rle {
            byte: 7,
            length: 1 << 40,
        }
        .encode();
        let frames = [
            (frame::CACHE_RESULT, &b""[..]),
            (frame::LIT, &zeros),
            (frame::RLE, &run),
            (frame::END, &end_payload()),
        ];
        let plain = compressed(0, false, 0, &frames);
        let cases = [(0, false, 0), (1, false, 1), (19, false, 3), (19, true, 3)];
        for (level, long, threads) in cases {
            let mut stream = compressed(level, long, threads, &frames);
            if level != 0 {
                assert!(stream.len() < plain.len() / 10, "level {level}");
            }
            // Bytes that follow the stream stay in the transport.
            stream.extend_from_slice(b"after");
            let mut reader = FrameReader::new(Decompressor::new(&stream[..], level != 0));
            for &(kind, payload) in &frames {
                assert_eq!(reader.expect(kind, "a frame").unwrap(), payload);
            }
            let mut rest = Vec::new();
            let mut transport = reader.into_inner().finish().unwrap();
            transport.read_to_end(&mut rest).unwrap();
            assert_eq!(rest, b"after", "level {level}");
        }
    }

    #[test]
    fn long_distance_matching_finds_repeats_beyond_the_window_of_the_level() {
        // Level 1 has a 512 KiB window. Without long-distance matching,
        // noise that repeats after 4 MiB of other noise costs its full size
        // again. The receiver decodes the 128 MiB window without a setting.
        let repeated = noise(256 << 10, 1);
        let gap = noise(4 << 20, 2);
        let end = end_payload();
        let frames = [
            (frame::LIT, &repeated[..]),
            (frame::LIT, &gap[..]),
            (frame::LIT, &repeated[..]),
            (frame::END, &end[..]),
        ];
        let short = compressed(1, false, 2, &frames);
        let long = compressed(1, true, 2, &frames);
        assert!(
            long.len() + (200 << 10) < short.len(),
            "{} bytes with long-distance matching, {} without",
            long.len(),
            short.len()
        );
        let mut reader = FrameReader::new(Decompressor::new(&long[..], true));
        for &(kind, payload) in &frames {
            assert!(reader.expect(kind, "a frame").unwrap() == payload);
        }
        reader.into_inner().finish().unwrap();
    }

    /// A transport whose bytes stay visible while it is in use.
    #[derive(Clone, Default)]
    struct Shared(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_flush_makes_every_frame_so_far_decodable() {
        // The bytes sent until a flush decode to every frame written
        // before it, although the stream goes on: the sender flushes
        // before it waits for a reply (DESIGN.md §6.2).
        for (long, threads) in [(false, 1), (false, 3), (true, 1), (true, 3)] {
            let sent = Shared::default();
            let mut writer = FrameWriter::new(Compressor::new(sent.clone(), 9, long, threads));
            writer.send(frame::CACHE_RESULT, b"").unwrap();
            writer.send(frame::LIT, &[1; 1000]).unwrap();
            writer.flush().unwrap();
            let so_far = sent.0.borrow().clone();
            let mut reader = FrameReader::new(Decompressor::new(&so_far[..], true));
            assert_eq!(reader.expect(frame::CACHE_RESULT, "").unwrap(), b"");
            assert_eq!(reader.expect(frame::LIT, "LIT").unwrap(), [1; 1000]);
            let error = reader.next().unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn a_truncated_stream_is_unexpected_eof() {
        // Wherever the stream ends early, even inside the trailer after
        // END, reading reports UnexpectedEof, never a clean end
        // (DESIGN.md §6.7).
        let stream = compressed(
            3,
            false,
            1,
            &[(frame::LIT, b"literal"), (frame::END, &end_payload())],
        );
        for cut in 0..stream.len() {
            let mut reader = FrameReader::new(Decompressor::new(&stream[..cut], true));
            let result = (move || {
                reader.expect(frame::LIT, "LIT")?;
                reader.expect(frame::END, "END")?;
                reader.into_inner().finish()
            })();
            let error = result.err().unwrap_or_else(|| panic!("cut at {cut}"));
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof,
                "cut at {cut}"
            );
        }
    }

    /// A transport that gives one byte at a time, and whose reads are
    /// interrupted 20 times before each byte.
    struct Interrupting<'a> {
        bytes: &'a [u8],
        interruptions: usize,
        /// The first byte is in the buffer.
        buffered: bool,
    }

    impl Read for Interrupting<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.fill_buf()?.len().min(buf.len());
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.consume(n);
            Ok(n)
        }
    }

    impl BufRead for Interrupting<'_> {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            if !self.buffered {
                if self.interruptions > 0 {
                    self.interruptions -= 1;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                (self.interruptions, self.buffered) = (20, true);
            }
            Ok(&self.bytes[..self.bytes.len().min(1)])
        }

        fn consume(&mut self, n: usize) {
            self.bytes = &self.bytes[n..];
            self.buffered &= n == 0;
        }
    }

    #[test]
    fn interrupted_transport_reads_are_retried() {
        // Before each byte, also in the trailer, the transport is
        // interrupted more times in a row than zstd accepts steps without
        // progress (DESIGN.md §3).
        let stream = compressed(
            3,
            false,
            1,
            &[(frame::LIT, b"literal"), (frame::END, &end_payload())],
        );
        let transport = Interrupting {
            bytes: &stream,
            interruptions: 20,
            buffered: false,
        };
        let mut reader = FrameReader::new(Decompressor::new(transport, true));
        assert_eq!(reader.expect(frame::LIT, "LIT").unwrap(), b"literal");
        assert_eq!(reader.expect(frame::END, "END").unwrap(), end_payload());
        let transport = reader.into_inner().finish().unwrap();
        assert!(transport.bytes.is_empty(), "the frame did not end");
    }

    #[test]
    #[should_panic(expected = "protocol error: data after END")]
    fn data_after_end_is_a_protocol_error() {
        let stream = compressed(
            3,
            false,
            1,
            &[(frame::END, &end_payload()), (frame::LIT, b"late")],
        );
        let mut reader = FrameReader::new(Decompressor::new(&stream[..], true));
        reader.expect(frame::END, "END").unwrap();
        let _ = reader.into_inner().finish();
    }

    /// LIT, then `partial`, in a zstd frame that ends without END.
    fn ended_before_end(partial: &[u8]) -> Vec<u8> {
        let mut writer = FrameWriter::new(Compressor::new(Vec::new(), 3, false, 1));
        writer.send(frame::LIT, b"literal").unwrap();
        let mut compressor = writer.into_inner();
        compressor.write_all(partial).unwrap();
        compressor.finish().unwrap()
    }

    /// Read LIT, then try to read the next frame.
    fn read_after_lit(stream: &[u8]) {
        let mut reader = FrameReader::new(Decompressor::new(stream, true));
        reader.expect(frame::LIT, "LIT").unwrap();
        let _ = reader.next();
    }

    #[test]
    #[should_panic(expected = "protocol error: the compressed stream ended before END")]
    fn a_frame_that_ends_before_end_is_a_protocol_error() {
        // Only the sender ends the zstd frame, after END; a truncated
        // stream cannot end it (DESIGN.md §6.5).
        read_after_lit(&ended_before_end(b""));
    }

    #[test]
    #[should_panic(expected = "protocol error: the compressed stream ended before END")]
    fn a_frame_that_ends_inside_a_frame_is_a_protocol_error() {
        // Two bytes of a frame header, which a check after the frame
        // reader would see as UnexpectedEof, like a truncation.
        read_after_lit(&ended_before_end(&[frame::END, 0]));
    }

    #[test]
    #[should_panic(expected = "protocol error: invalid compressed data")]
    fn a_wrong_checksum_is_a_protocol_error() {
        let mut stream = compressed(3, false, 1, &[(frame::END, &end_payload())]);
        *stream.last_mut().unwrap() ^= 1;
        let mut reader = FrameReader::new(Decompressor::new(&stream[..], true));
        reader.expect(frame::END, "END").unwrap();
        let _ = reader.into_inner().finish();
    }

    #[test]
    #[should_panic(expected = "empty RLE")]
    fn empty_rle_is_rejected() {
        let mut enc = Enc::new();
        enc.u8(0);
        enc.u64(0);
        Rle::decode(&enc.buf);
    }
}
