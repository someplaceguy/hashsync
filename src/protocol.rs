//! The transfer protocol's frames and payload layouts (DESIGN.md §6.2).
//!
//! Both roles share these definitions (DESIGN.md §11). Changing a frame
//! layout, a hash constant, the list encoding, or a HELLO field requires a
//! protocol version bump.
//!
//! Malformed payloads and violated frame rules are bugs in a compatible
//! peer: parsers panic (DESIGN.md §6.5). Transport failures, including EOF
//! inside a frame, surface as `io::Error` for the caller to turn into
//! `die`.

use std::io::{Read, Write};

use crate::hash::{STRONG_LEN, Strong, StrongHash};

pub const PROTOCOL_VERSION: u32 = 2;
pub const PROGRAM_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The hash-list value width `W` (DESIGN.md §5.4): no hash bits are
/// dropped.
pub const WIDTH: u8 = 64;

/// Frame types. `type u8, length u32, payload`, integers little endian.
pub mod frame {
    // The full frame space is defined here (DESIGN.md §6.2); later
    // milestones send the remaining types.
    #![allow(dead_code)]

    pub const HELLO: u8 = 1;
    pub const HASHLIST: u8 = 2;
    pub const HASHDELTA: u8 = 3;
    pub const HASHKEEP: u8 = 4;
    pub const LISTDATA: u8 = 5;
    pub const HASHFULL: u8 = 6;
    pub const CACHE_RESULT: u8 = 7;
    pub const LIT: u8 = 8;
    pub const REF: u8 = 9;
    pub const RLE: u8 = 10;
    pub const END: u8 = 11;
    pub const END_ACK: u8 = 12;
    pub const QUERY: u8 = 13;
    pub const ANSWER: u8 = 14;
}

/// A LISTDATA frame carries at most 1 MiB of the announced bytes.
pub const MAX_LISTDATA: usize = 1 << 20;

/// Bound on any frame payload this implementation produces or accepts.
/// LIT payloads stay within the sender's read chunk, and a HELLO is
/// bounded by the command line the receiver's kernel accepted.
pub const MAX_FRAME: usize = 8 << 20;

/// The peer's protocol version leads every HELLO payload: check it
/// before trusting the rest of the layout, which another version may
/// have changed. `who` names the peer for the failure message.
pub fn check_protocol_version(payload: &[u8], who: &str) {
    assert!(payload.len() >= 4, "protocol error: truncated HELLO frame");
    let version = u32::from_le_bytes(payload[..4].try_into().unwrap());
    if version != PROTOCOL_VERSION {
        crate::die::die(format!(
            "the {who} speaks protocol version {version}, \
             this hashsync {PROGRAM_VERSION} speaks {PROTOCOL_VERSION}"
        ));
    }
}

/// A hash list's identity: its strong-hash algorithm and full digest
/// (DESIGN.md §5.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ListId {
    pub algorithm: StrongHash,
    pub digest: Strong,
}

impl std::fmt::Display for ListId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-", self.algorithm.id())?;
        for byte in self.digest {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// HELLO, receiver to sender, on the data session.
pub struct ReceiverHello {
    pub protocol_version: u32,
    pub program_version: String,
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash_id: String,
    pub width: u8,
    pub account_id: [u8; 16],
    pub index_id: [u8; 16],
    pub target: ListId,
    pub sender_cache: bool,
    /// Seconds.
    pub hashlist_max_age: u64,
    pub lookahead: u64,
    pub compression_level: i32,
    pub compression_threads: u32,
    /// Bytes.
    pub negative_cache: u64,
    /// The command's argv; argv[0] is found in PATH and run directly.
    pub command: Vec<Vec<u8>>,
}

impl ReceiverHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u32(self.protocol_version);
        enc.bytes(self.program_version.as_bytes());
        enc.u32(self.block_size);
        enc.bytes(self.weak_hash_id.as_bytes());
        enc.bytes(self.strong_hash_id.as_bytes());
        enc.u8(self.width);
        enc.raw(&self.account_id);
        enc.raw(&self.index_id);
        enc.list_id(&self.target);
        enc.u8(self.sender_cache.into());
        enc.u64(self.hashlist_max_age);
        enc.u64(self.lookahead);
        enc.i32(self.compression_level);
        enc.u32(self.compression_threads);
        enc.u64(self.negative_cache);
        enc.u32(list_len(self.command.len()));
        for arg in &self.command {
            enc.bytes(arg);
        }
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let hello = ReceiverHello {
            protocol_version: dec.u32(),
            program_version: dec.string(),
            block_size: dec.u32(),
            weak_hash_id: dec.string(),
            strong_hash_id: dec.string(),
            width: dec.u8(),
            account_id: dec.array(),
            index_id: dec.array(),
            target: dec.list_id(),
            sender_cache: dec.flag(),
            hashlist_max_age: dec.u64(),
            lookahead: dec.u64(),
            compression_level: dec.i32(),
            compression_threads: dec.u32(),
            negative_cache: dec.u64(),
            command: {
                let count = dec.u32();
                (0..count).map(|_| dec.string_bytes()).collect()
            },
        };
        dec.done();
        assert!(!hello.command.is_empty(), "protocol error: empty command");
        hello
    }
}

/// HELLO, sender to receiver, on the data session. It echoes the
/// parameters the sender accepted, so the receiver checks them for
/// equality.
pub struct SenderHello {
    pub protocol_version: u32,
    pub program_version: String,
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash_id: String,
    pub width: u8,
    pub account_id: [u8; 16],
    pub base: Option<ListId>,
}

impl SenderHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u32(self.protocol_version);
        enc.bytes(self.program_version.as_bytes());
        enc.u32(self.block_size);
        enc.bytes(self.weak_hash_id.as_bytes());
        enc.bytes(self.strong_hash_id.as_bytes());
        enc.u8(self.width);
        enc.raw(&self.account_id);
        match &self.base {
            None => enc.u8(0),
            Some(base) => {
                enc.u8(1);
                enc.list_id(base);
            }
        }
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let hello = SenderHello {
            protocol_version: dec.u32(),
            program_version: dec.string(),
            block_size: dec.u32(),
            weak_hash_id: dec.string(),
            strong_hash_id: dec.string(),
            width: dec.u8(),
            account_id: dec.array(),
            base: match dec.flag() {
                false => None,
                true => Some(dec.list_id()),
            },
        };
        dec.done();
        hello
    }
}

/// HELLO, both directions, on the control session. Both sides send it
/// immediately.
pub struct ControlHello {
    pub protocol_version: u32,
    pub program_version: String,
}

impl ControlHello {
    pub fn current() -> Self {
        ControlHello {
            protocol_version: PROTOCOL_VERSION,
            program_version: PROGRAM_VERSION.to_string(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u32(self.protocol_version);
        enc.bytes(self.program_version.as_bytes());
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let hello = ControlHello {
            protocol_version: dec.u32(),
            program_version: dec.string(),
        };
        dec.done();
        hello
    }
}

/// HASHLIST: the announced target and encoded length; the bytes follow in
/// LISTDATA frames.
pub struct HashList {
    pub target: ListId,
    pub length: u64,
}

impl HashList {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.list_id(&self.target);
        enc.u64(self.length);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HASHLIST");
        let header = HashList {
            target: dec.list_id(),
            length: dec.u64(),
        };
        dec.done();
        header
    }
}

/// CACHE_RESULT: the sender's single acknowledgement of the accepted
/// target, after verification, table construction, and, with caching
/// enabled, durable publication. There is no storage-success flag: cache
/// failures are fatal (DESIGN.md §5.6).
pub struct CacheResult {
    pub target: ListId,
}

impl CacheResult {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.list_id(&self.target);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "CACHE_RESULT");
        let result = CacheResult {
            target: dec.list_id(),
        };
        dec.done();
        result
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

/// RLE: a repeated byte value and length.
pub struct Rle {
    pub byte: u8,
    pub length: u64,
}

impl Rle {
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

    pub fn list_id(&mut self, id: &ListId) {
        self.bytes(id.algorithm.id().as_bytes());
        self.raw(&id.digest);
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

    /// A length-prefixed UTF-8 string, such as an algorithm or version.
    pub fn string(&mut self) -> String {
        String::from_utf8(self.string_bytes()).unwrap_or_else(|_| {
            panic!(
                "protocol error: invalid UTF-8 string in {} frame",
                self.what
            )
        })
    }

    pub fn list_id(&mut self) -> ListId {
        let name = self.string();
        let algorithm = StrongHash::from_id(&name).unwrap_or_else(|| {
            panic!(
                "protocol error: unsupported strong hash {name:?} in {} frame",
                self.what
            )
        });
        // Every supported algorithm has one fixed digest length
        // (DESIGN.md §5.3).
        let digest = self.array::<STRONG_LEN>();
        ListId { algorithm, digest }
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

    /// The next frame; `None` at a clean EOF between frames. EOF inside a
    /// frame is `UnexpectedEof`: a truncated stream, for the caller's
    /// `die`. An oversized frame is a protocol error.
    pub fn next(&mut self) -> std::io::Result<Option<Frame<'_>>> {
        let mut header = [0u8; 5];
        // Read the first byte separately to tell a clean EOF from a
        // truncated header.
        loop {
            match self.reader.read(&mut header[..1]) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        self.reader.read_exact(&mut header[1..])?;
        let kind = header[0];
        let length = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
        assert!(
            length <= MAX_FRAME,
            "protocol error: frame type {kind} with {length} bytes"
        );
        self.payload.resize(length, 0);
        self.reader.read_exact(&mut self.payload)?;
        Ok(Some(Frame {
            kind,
            payload: &self.payload,
        }))
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

    /// Read to EOF, requiring that no byte follows. `false` when trailing
    /// bytes arrived.
    pub fn drain_to_eof(&mut self) -> std::io::Result<bool> {
        let mut buf = [0u8; 4096];
        loop {
            match self.reader.read(&mut buf) {
                Ok(0) => return Ok(true),
                Ok(_) => return Ok(false),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }
}

/// Writes frames to a transport. Unbuffered: one header write and one
/// payload write per frame.
pub struct FrameWriter<W: Write> {
    writer: W,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(writer: W) -> Self {
        FrameWriter { writer }
    }

    pub fn send(&mut self, kind: u8, payload: &[u8]) -> std::io::Result<()> {
        assert!(
            payload.len() <= MAX_FRAME,
            "frame type {kind} with {} bytes",
            payload.len()
        );
        let mut header = [kind, 0, 0, 0, 0];
        header[1..].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        self.writer.write_all(&header)?;
        self.writer.write_all(payload)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn receiver_hello_roundtrips() {
        let hello = ReceiverHello {
            protocol_version: PROTOCOL_VERSION,
            program_version: "0.1.0".into(),
            block_size: 4096,
            weak_hash_id: "weak".into(),
            strong_hash_id: "blake3".into(),
            width: WIDTH,
            account_id: [7; 16],
            index_id: [9; 16],
            target: ListId {
                algorithm: StrongHash::Blake3,
                digest: [3; 32],
            },
            sender_cache: true,
            hashlist_max_age: 90 * 24 * 3600,
            lookahead: 256 << 20,
            compression_level: 0,
            compression_threads: 0,
            negative_cache: 100 << 20,
            command: vec![b"printf".to_vec(), b"%s".to_vec(), b"\xff\xfe".to_vec()],
        };
        let decoded = ReceiverHello::decode(&hello.encode());
        assert_eq!(decoded.program_version, "0.1.0");
        assert_eq!(decoded.block_size, 4096);
        assert_eq!(decoded.account_id, [7; 16]);
        assert_eq!(decoded.target, hello.target);
        assert!(decoded.sender_cache);
        assert_eq!(decoded.command, hello.command);
    }

    #[test]
    fn sender_hello_roundtrips_with_and_without_base() {
        let mut hello = SenderHello {
            protocol_version: PROTOCOL_VERSION,
            program_version: PROGRAM_VERSION.into(),
            block_size: 8192,
            weak_hash_id: "weak".into(),
            strong_hash_id: "sha256".into(),
            width: WIDTH,
            account_id: [1; 16],
            base: None,
        };
        assert!(SenderHello::decode(&hello.encode()).base.is_none());
        let base = ListId {
            algorithm: StrongHash::Sha256,
            digest: [5; 32],
        };
        hello.base = Some(base);
        assert_eq!(SenderHello::decode(&hello.encode()).base, Some(base));
    }

    #[test]
    #[should_panic(expected = "excess bytes")]
    fn trailing_payload_bytes_are_rejected() {
        let mut payload = ControlHello::current().encode();
        payload.push(0);
        ControlHello::decode(&payload);
    }

    #[test]
    #[should_panic(expected = "unsupported strong hash")]
    fn unsupported_algorithms_are_rejected() {
        let mut enc = Enc::new();
        enc.list_id(&ListId {
            algorithm: StrongHash::Blake3,
            digest: [0; 32],
        });
        enc.u64(1);
        let mut payload = HashList {
            target: ListId {
                algorithm: StrongHash::Blake3,
                digest: [0; 32],
            },
            length: 1,
        }
        .encode();
        payload[4..10].copy_from_slice(b"md5NO!");
        HashList::decode(&payload);
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
