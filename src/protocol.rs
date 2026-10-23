//! The transfer protocol's version exchange, features, frames, and payload
//! layouts (DESIGN.md §6.2).
//!
//! Both hosts share these definitions. The version exchange never
//! changes; DESIGN.md §11 says which other changes require a protocol
//! version bump or a new feature.
//!
//! Readers grow their buffers only as bytes arrive, so no length or count
//! needs a bound beyond its field: a corrupt value costs only the bytes
//! that really follow it.
//!
//! Each session has two levels of frames. Session frames are never
//! compressed. On the data session, they are REQUEST, the HELLO of each
//! deduplicated stream, CHUNK, STREAM_END and END. The stream frames of a
//! stream, LIT, REF and RLE, form one zstd frame, unless compression is
//! disabled, and the payloads of the stream's CHUNK frames are consecutive
//! pieces of it: the stream's data writer writes it through a `Compressor`
//! (`stream::write_stream`), and a `StreamReader` reads it back. So a reader
//! finds the session frames without decoding a stream, and a stopped stream
//! is discarded without being decoded. On the control session, the session
//! frames are CHUNK and the frames of the list exchanges. There, the CHUNK
//! payloads of each direction are the pieces of one zstd stream, which
//! carries the control frames QUERY, ANSWER and STOP, and which the closure
//! of the session ends. The hash lists stay outside it: zstd cannot make
//! them smaller (DESIGN.md §10).
//!
//! Malformed payloads, invalid compressed data, and violated frame rules
//! are bugs in a compatible peer: parsers panic (DESIGN.md §6.7). Transport
//! failures, including EOF inside a frame, surface as `io::Error` for the
//! caller to turn into `die`; `send_data` and `flush_data` do that for the
//! writes of the data session.

use std::io::{BufRead, IoSlice, Read, Write};
use std::ops::RangeInclusive;

use zstd_sys::ZSTD_EndDirective;

use crate::config::{self, Source};
use crate::die::die;
use crate::escapes::quoted;
use crate::hash::{Strong, StrongHash};
use crate::settings::{Deduplicated, RemoteCommand, RunSettings};
use crate::sys::{ZstdCCtx, ZstdDCtx};

/// The first bytes of each session, in both directions, as a
/// little-endian `u32`.
pub const PROTOCOL_VERSION: u32 = 1;

/// The optional features of the protocol that this build supports, one
/// bit each. Each side sends them as a little-endian `u64` directly after
/// its protocol version. The field exists so that a later build can add a
/// feature and still work with this one: a side uses a feature only when
/// both sides have its bit, and it ignores the bits that it does not know.
/// This build supports no features: they come only after the first
/// release (DESIGN.md §6.2 and §11).
pub const FEATURES: u64 = 0;

/// Frame types. `type u8, length u32, payload`, integers little endian.
/// The numbers follow the levels of frames (DESIGN.md §6.2).
pub mod frame {
    // Session frames of the data session. CHUNK is a session frame of the
    // control session too.
    pub const REQUEST: u8 = 1;
    pub const HELLO: u8 = 2;
    pub const CHUNK: u8 = 3;
    pub const STREAM_END: u8 = 4;
    pub const END: u8 = 5;
    // Session frames of the control session: the frames of the list
    // exchanges, and UPDATED.
    pub const INDEX: u8 = 6;
    pub const ADVERTISE: u8 = 7;
    pub const HASHLIST: u8 = 8;
    pub const HASHDELTA: u8 = 9;
    pub const HASHKEEP: u8 = 10;
    pub const HASHNONE: u8 = 11;
    pub const HASHDATA: u8 = 12;
    pub const HASHFULL: u8 = 13;
    pub const CACHE_RESULT: u8 = 14;
    pub const UPDATED: u8 = 15;
    // Stream frames: inside the zstd frame of a stream.
    pub const LIT: u8 = 16;
    pub const REF: u8 = 17;
    pub const RLE: u8 = 18;
    // Control frames: inside the control stream of the control session.
    pub const QUERY: u8 = 19;
    pub const ANSWER: u8 = 20;
    pub const STOP: u8 = 21;
}

/// The size of a frame header, `type u8, length u32`.
const FRAME_HEADER: usize = 5;

/// When the bytes of a stream go on from the buffers on their way to the
/// stream's destination (DESIGN.md §6.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// At each idle point of their source, when it has no byte ready, and
    /// when a buffer is full: a command and a consumer that answer each
    /// other never wait for bytes that a buffer holds.
    Eager,
    /// Only when a buffer is full and at the end of the stream, which costs
    /// fewer bytes and less CPU when nothing waits for the bytes.
    Batch,
}

/// REQUEST, client to server: the first frame of the data session after
/// the protocol version and the features. It holds the command and every
/// setting of the run, and each host applies the settings that concern its
/// roles (DESIGN.md §6.2).
#[derive(Debug, PartialEq, Eq)]
pub struct Request {
    pub command: RemoteCommand,
    /// The streams that the run deduplicates, which give the server its
    /// roles.
    pub deduplicated: Deduplicated,
    pub settings: RunSettings,
    /// The client's global `-q`, which the server applies to its own
    /// notes, such as those of an update of its index.
    pub quiet: bool,
    /// The opposite of the client's global `--no-human-readable`, which the
    /// server applies to its own notes too.
    pub human_readable: bool,
    /// The server's configuration file: by default its own file of the
    /// profile `default`, else the one that `--server-profile` or
    /// `--server-config` names (DESIGN.md §7).
    pub config: Source,
}

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        let words = match &self.command {
            RemoteCommand::Shell(line) => {
                enc.u8(0);
                std::slice::from_ref(line)
            }
            RemoteCommand::Argv(words) => {
                enc.u8(1);
                &words[..]
            }
        };
        enc.u32(list_len(words.len()));
        for word in words {
            enc.bytes(word);
        }
        // One bit per stream: 1 the forward stream, 2 the backward one.
        enc.u8(match self.deduplicated {
            Deduplicated::Stdin => 1,
            Deduplicated::Stdout => 2,
            Deduplicated::Both => 3,
        });
        let settings = &self.settings;
        enc.u8(settings.no_update.into());
        enc.u8(settings.scan_immutable.into());
        enc.u8(settings.compact.into());
        enc.u8(settings.sender_cache.into());
        enc.i32(settings.compression_level);
        enc.u8(settings.compression_long.into());
        enc.u32(settings.compression_threads);
        enc.u32(settings.probe_threads);
        // 0 for the default, which depends on the block size of the
        // stream's receiver. An explicit 0 is below every block size, so
        // the client ends the run before the server could use it.
        enc.u64(settings.lookahead.unwrap_or(0));
        enc.u64(settings.negative_cache);
        enc.u64(settings.spool_limit);
        enc.u8(settings.batch_stdin.into());
        enc.u8(settings.batch_stdout.into());
        enc.u8(settings.wait.into());
        enc.u8(self.quiet.into());
        enc.u8(self.human_readable.into());
        match &self.config {
            Source::Default => enc.u8(0),
            Source::Profile(name) => {
                enc.u8(1);
                enc.bytes(name);
            }
            Source::File(path) => {
                enc.u8(2);
                enc.bytes(path);
            }
        }
        enc.buf
    }

    /// The server checks the values that it must support, such as the
    /// compression level, after decoding (DESIGN.md §6.2).
    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "REQUEST");
        let shell = match dec.u8() {
            0 => true,
            1 => false,
            v => panic!("protocol error: invalid command kind {v} in REQUEST frame"),
        };
        let mut words = dec.list(Dec::argument);
        let command = if shell {
            assert!(
                words.len() == 1,
                "protocol error: a command line of {} words",
                words.len()
            );
            RemoteCommand::Shell(words.pop().unwrap())
        } else {
            assert!(!words.is_empty(), "protocol error: empty command");
            RemoteCommand::Argv(words)
        };
        let deduplicated = match dec.u8() {
            1 => Deduplicated::Stdin,
            2 => Deduplicated::Stdout,
            3 => Deduplicated::Both,
            v => panic!("protocol error: invalid deduplicated streams {v} in REQUEST frame"),
        };
        let settings = RunSettings {
            no_update: dec.flag(),
            scan_immutable: dec.flag(),
            compact: dec.flag(),
            sender_cache: dec.flag(),
            compression_level: dec.i32(),
            compression_long: dec.flag(),
            compression_threads: dec.u32(),
            probe_threads: dec.u32(),
            lookahead: Some(dec.u64()).filter(|&lookahead| lookahead != 0),
            negative_cache: dec.u64(),
            spool_limit: dec.u64(),
            batch_stdin: dec.flag(),
            batch_stdout: dec.flag(),
            wait: dec.flag(),
        };
        let (quiet, human_readable) = (dec.flag(), dec.flag());
        let config = match dec.u8() {
            0 => Source::Default,
            1 => Source::Profile(dec.argument()),
            2 => Source::File(dec.argument()),
            v => panic!("protocol error: invalid configuration kind {v} in REQUEST frame"),
        };
        // The client's options accept only profile names, which become part
        // of a path on this host.
        if let Source::Profile(name) = &config
            && let Err(e) = config::check_profile_name(name)
        {
            panic!("protocol error: {e} in REQUEST frame");
        }
        let request = Request {
            command,
            deduplicated,
            settings,
            quiet,
            human_readable,
            config,
        };
        dec.done();
        request
    }
}

/// HELLO, receiver to sender, on the data session: the block size and the
/// hashes of a deduplicated stream, which its sender uses, or ends the run.
/// The receiver sends it at once, from its configuration, which its index
/// matches once it is ready for the run: the scanner needs the block size
/// before it scans a byte, and the index can take long to update
/// (DESIGN.md §6.2). Every digest of the stream's list exchange uses
/// `strong_hash`.
pub struct Hello {
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash: StrongHash,
}

impl Hello {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.u32(self.block_size);
        enc.bytes(self.weak_hash_id.as_bytes());
        enc.bytes(self.strong_hash.id().as_bytes());
        enc.buf
    }

    /// The strong hash sets the length of every list ID that the frames of
    /// the exchange carry, so an unsupported strong hash ends the run here
    /// (DESIGN.md §6.2). The sender checks the weak hash after decoding.
    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "HELLO");
        let block_size = dec.u32();
        let weak_hash_id = dec.string();
        let strong_hash_id = dec.string();
        let Some(strong_hash) = StrongHash::from_id(&strong_hash_id) else {
            die(format!(
                "incompatible receiver: strong hash {}",
                quoted(strong_hash_id.as_bytes())
            ));
        };
        dec.done();
        assert!(block_size > 0, "protocol error: block size 0");
        Hello {
            block_size,
            weak_hash_id,
            strong_hash,
        }
    }
}

/// INDEX, receiver to sender: the IDs that partition the list caches, the
/// receiver's account and the ID of its index. The receiver sends it once
/// its index is ready for the run, and the sender then lists its cache
/// directory for them (DESIGN.md §5.6).
pub struct IndexIds {
    pub account_id: [u8; 16],
    pub index_id: [u8; 16],
}

impl IndexIds {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.raw(&self.account_id);
        enc.raw(&self.index_id);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "INDEX");
        let ids = IndexIds {
            account_id: dec.array(),
            index_id: dec.array(),
        };
        dec.done();
        ids
    }
}

/// ADVERTISE, sender to receiver: the sender's account, and the IDs of the
/// lists that its cache holds for the receiver and the index of INDEX. They
/// are digests of the stream's strong hash (DESIGN.md §5.6).
pub struct Advertise {
    pub account_id: [u8; 16],
    pub lists: Vec<Strong>,
}

impl Advertise {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.raw(&self.account_id);
        enc.u32(list_len(self.lists.len()));
        for list in &self.lists {
            enc.raw(list);
        }
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "ADVERTISE");
        let advertise = Advertise {
            account_id: dec.array(),
            lists: dec.list(Dec::array),
        };
        dec.done();
        advertise
    }
}

/// A session frame of the control session other than CHUNK, with a payload
/// of its own: what the control reader passes to a side of a list exchange,
/// and what a side queues for the control writer (DESIGN.md §6.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeFrame {
    pub kind: u8,
    pub payload: Vec<u8>,
}

/// The most list bytes of one HASHDATA frame. The lists share their
/// direction of the control session with the control frames, which go
/// first, but a control frame that comes while a HASHDATA frame goes out
/// waits for it: at 1 MB/s, up to 16 ms.
pub const HASHDATA_BYTES: usize = 16 << 10;

/// The message in which the receiver hands the sender its target
/// (DESIGN.md §5.6): a frame that names the target, followed by HASHDATA
/// frames with the encoded lists that it announces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetList {
    /// HASHLIST, with the full target.
    Full { target: Strong, list: Vec<u8> },
    /// HASHKEEP: the target is one of the lists that the sender advertised.
    Keep { target: Strong },
    /// HASHDELTA: the removed and added lists that turn `base`, one of the
    /// advertised lists, into the target.
    Delta {
        target: Strong,
        base: Strong,
        removed: Vec<u8>,
        added: Vec<u8>,
    },
}

impl TargetList {
    /// The digest of the target that the message hands over.
    pub fn target(&self) -> Strong {
        match self {
            TargetList::Full { target, .. }
            | TargetList::Keep { target }
            | TargetList::Delta { target, .. } => *target,
        }
    }

    /// The type and payload of the frame. The payload holds the target,
    /// the base, if there is one, and the lengths of the lists that follow.
    fn frame(&self) -> (u8, Vec<u8>) {
        let mut enc = Enc::new();
        enc.raw(&self.target());
        let kind = match self {
            TargetList::Full { list, .. } => {
                enc.u64(list.len() as u64);
                frame::HASHLIST
            }
            TargetList::Keep { .. } => frame::HASHKEEP,
            TargetList::Delta {
                base,
                removed,
                added,
                ..
            } => {
                enc.raw(base);
                enc.u64(removed.len() as u64);
                enc.u64(added.len() as u64);
                frame::HASHDELTA
            }
        };
        (kind, enc.buf)
    }

    /// The lists that follow the frame, in order: the removed list of a
    /// delta comes before its added list.
    fn lists(&self) -> Vec<&[u8]> {
        match self {
            TargetList::Full { list, .. } => vec![list],
            TargetList::Keep { .. } => Vec::new(),
            TargetList::Delta { removed, added, .. } => vec![removed, added],
        }
    }

    /// The number of bytes that the message takes on the control session:
    /// the frame, the bytes of the lists, and the headers of the HASHDATA
    /// frames that carry them.
    pub fn size(&self) -> usize {
        let lists: usize = self.lists().iter().map(|list| list.len()).sum();
        let pieces = lists.div_ceil(HASHDATA_BYTES);
        FRAME_HEADER + self.frame().1.len() + lists + pieces * FRAME_HEADER
    }

    /// Give `send` the frames of the message, one after the other: the
    /// frame, then the HASHDATA frames of its lists, each with at most
    /// `HASHDATA_BYTES` of them. The lists of a delta go as one sequence of
    /// bytes, so one HASHDATA frame can hold the end of the removed list
    /// and the start of the added list. A frame is made only once `send`
    /// has taken the one before, so a long list is not copied whole.
    pub fn send(&self, mut send: impl FnMut(ExchangeFrame)) {
        let (kind, payload) = self.frame();
        send(ExchangeFrame { kind, payload });
        let mut piece = Vec::with_capacity(HASHDATA_BYTES);
        for mut bytes in self.lists() {
            while !bytes.is_empty() {
                let n = bytes.len().min(HASHDATA_BYTES - piece.len());
                piece.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if piece.len() == HASHDATA_BYTES {
                    let payload = std::mem::replace(&mut piece, Vec::with_capacity(HASHDATA_BYTES));
                    send(ExchangeFrame {
                        kind: frame::HASHDATA,
                        payload,
                    });
                }
            }
        }
        if !piece.is_empty() {
            send(ExchangeFrame {
                kind: frame::HASHDATA,
                payload: piece,
            });
        }
    }
}

/// The frame that answers ADVERTISE, as the sender reads it (DESIGN.md
/// §5.6): HASHLIST, HASHDELTA and HASHKEEP name the target, and HASHLIST
/// and HASHDELTA announce the bytes of the lists that follow them in
/// HASHDATA frames. HASHNONE ends the exchange without a target.
#[derive(Debug, PartialEq, Eq)]
pub enum TargetFrame {
    Full {
        target: Strong,
        length: u64,
    },
    Keep {
        target: Strong,
    },
    Delta {
        target: Strong,
        base: Strong,
        removed: u64,
        added: u64,
    },
    None,
}

impl TargetFrame {
    /// The frame of type `kind` with `payload`. Any other type than these
    /// four is a protocol error.
    pub fn decode(kind: u8, payload: &[u8]) -> Self {
        match kind {
            frame::HASHLIST => {
                let mut dec = Dec::new(payload, "HASHLIST");
                let (target, length) = (dec.array(), dec.u64());
                dec.done();
                TargetFrame::Full { target, length }
            }
            frame::HASHKEEP => {
                let mut dec = Dec::new(payload, "HASHKEEP");
                let target = dec.array();
                dec.done();
                TargetFrame::Keep { target }
            }
            frame::HASHDELTA => {
                let mut dec = Dec::new(payload, "HASHDELTA");
                let (target, base) = (dec.array(), dec.array());
                let (removed, added) = (dec.u64(), dec.u64());
                dec.done();
                TargetFrame::Delta {
                    target,
                    base,
                    removed,
                    added,
                }
            }
            frame::HASHNONE => {
                assert!(
                    payload.is_empty(),
                    "protocol error: HASHNONE with a payload"
                );
                TargetFrame::None
            }
            kind => panic!("protocol error: expected the target list, received frame type {kind}"),
        }
    }
}

/// END: the command's exit status, the error text of a startup failure,
/// and the server's counts of its ends of both streams: of the forward
/// stream, whose decoder it runs, and whose lookups too when the stream is
/// deduplicated, and of the backward stream, whose source reader it runs.
/// The client counts its own ends, so with END it has what the summary
/// needs of both streams, also of a stream that stopped (DESIGN.md §8).
pub struct End {
    pub status: i32,
    pub error: String,
    pub forward: Counts,
    pub backward: SourceRead,
}

impl End {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.i32(self.status);
        enc.bytes(self.error.as_bytes());
        self.forward.encode(&mut enc);
        self.backward.encode(&mut enc);
        enc.buf
    }

    pub fn decode(payload: &[u8]) -> Self {
        let mut dec = Dec::new(payload, "END");
        let end = End {
            status: dec.i32(),
            error: dec.string(),
            forward: Counts::decode(&mut dec),
            backward: SourceRead::decode(&mut dec),
        };
        dec.done();
        end
    }
}

/// What the source reader of a stream read, for the summary (DESIGN.md
/// §8): the bytes that it read from the stream's source, and whether it
/// reached the EOF of that source: it read the EOF, or, as the stdin reader
/// can at the stop, found the source at its EOF without a read. A stream is
/// whole when its source reader reached the EOF, and its destination took
/// every byte that the source reader read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceRead {
    pub bytes: u64,
    pub eof: bool,
}

impl SourceRead {
    fn encode(&self, enc: &mut Enc) {
        enc.u64(self.bytes);
        enc.u8(self.eof.into());
    }

    fn decode(dec: &mut Dec) -> Self {
        SourceRead {
            bytes: dec.u64(),
            eof: dec.flag(),
        }
    }
}

/// What the receiving end of a stream counted, for the summary (DESIGN.md
/// §8): the bytes that its decoder wrote to the stream's destination, by the
/// kind of stream frame that carried them, the bytes of the REF and RLE
/// frames and of the CHUNK payloads that it decoded, and in a deduplicated
/// stream, the blocks that its lookups did not find.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// All of them.
    pub bytes: u64,
    pub literal: u64,
    pub reference: u64,
    pub run: u64,
    /// The bytes of the REF frames and of the RLE frames that the decoder
    /// read, before compression: what the references and the runs cost in
    /// the stream. A frame that a stop cut short counts whole.
    pub reference_frames: u64,
    pub run_frames: u64,
    /// The bytes of the CHUNK payloads that the decoder consumed: of the
    /// stream's zstd frame, or of its stream frames without compression.
    pub wire: u64,
    pub misses: Misses,
}

/// The queries that the receiver of a stream answered no after it looked
/// their blocks up in its index, by the reason, for the summary (DESIGN.md
/// §8). A query that the receiver answered without the index, from its pins
/// or its cache of verified blocks, or that it answered no because the
/// output had passed it, counts in neither.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Misses {
    /// The index has no candidate, no block whose strong hash starts with
    /// the same 16 bytes (DESIGN.md §6.4): the window had the weak hash of
    /// an indexed block, but other bytes.
    pub false_positives: u64,
    /// The index has candidates, but none of those that the lookup tried,
    /// at most `candidate_attempts`, verified: their files changed or could
    /// not be read, for example.
    pub not_found: u64,
}

impl Counts {
    fn encode(&self, enc: &mut Enc) {
        for count in [
            self.bytes,
            self.literal,
            self.reference,
            self.run,
            self.reference_frames,
            self.run_frames,
            self.wire,
            self.misses.false_positives,
            self.misses.not_found,
        ] {
            enc.u64(count);
        }
    }

    fn decode(dec: &mut Dec) -> Self {
        Counts {
            bytes: dec.u64(),
            literal: dec.u64(),
            reference: dec.u64(),
            run: dec.u64(),
            reference_frames: dec.u64(),
            run_frames: dec.u64(),
            wire: dec.u64(),
            misses: Misses {
                false_positives: dec.u64(),
                not_found: dec.u64(),
            },
        }
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

/// A frame of the control session, in the queue of a host's control
/// writer, which sends the frames in queue order (DESIGN.md §6.2): the
/// queries of the stream that the host scans, the answers for the stream
/// that it receives, and STOP, which the host that receives a stream sends
/// when it stops that stream (DESIGN.md §6.6).
pub enum ControlFrame {
    Query(Query),
    Answer(Answer),
    Stop,
}

impl ControlFrame {
    /// The type and the payload of the frame.
    pub fn encode(&self) -> (u8, Vec<u8>) {
        match self {
            ControlFrame::Query(query) => (frame::QUERY, query.encode()),
            ControlFrame::Answer(answer) => (frame::ANSWER, answer.encode()),
            ControlFrame::Stop => (frame::STOP, Vec::new()),
        }
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
/// protocol errors and panic (DESIGN.md §6.7).
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

    /// A byte string from a command-line argument of the client, such as a
    /// word of the command or a path. A NUL byte, which no argument can
    /// hold, is a protocol error (DESIGN.md §6.2).
    fn argument(&mut self) -> Vec<u8> {
        let bytes = self.string_bytes();
        assert!(
            !bytes.contains(&0),
            "protocol error: a NUL byte in a byte string of the {} frame",
            self.what
        );
        bytes
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

impl Frame<'_> {
    /// The number of bytes that the frame takes in its session or stream:
    /// its header and its payload.
    pub fn size(&self) -> usize {
        FRAME_HEADER + self.payload.len()
    }
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

    /// What the peer sends before its first frame: its protocol version,
    /// then its features (DESIGN.md §6.2). This function exists so that
    /// every session checks the version before it reads anything more: a
    /// peer with another protocol version can send anything after its
    /// version, or nothing. A different version ends the run with `die`,
    /// and `who` names the peer in the message. This build supports no
    /// features, so the peer's bits enable none, and this function ignores
    /// them. `false` at a clean EOF before the version; EOF after its
    /// first byte is `UnexpectedEof`.
    pub fn read_version_and_features(&mut self, who: &str) -> std::io::Result<bool> {
        let mut version = [0u8; 4];
        if !self.read_whole(&mut version)? {
            return Ok(false);
        }
        let version = u32::from_le_bytes(version);
        if version != PROTOCOL_VERSION {
            die(format!(
                "the {who} speaks protocol version {version}, \
                 this hashsync {} speaks {PROTOCOL_VERSION}",
                env!("CARGO_PKG_VERSION")
            ));
        }
        let mut features = [0u8; 8];
        self.reader.read_exact(&mut features)?;
        Ok(true)
    }

    /// The next frame; `None` at a clean EOF between frames. EOF inside a
    /// frame is `UnexpectedEof`: a truncated stream, for the caller's
    /// `die`.
    pub fn next(&mut self) -> std::io::Result<Option<Frame<'_>>> {
        let mut header = [0u8; FRAME_HEADER];
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

/// Writes frames to a transport. Each frame goes out in one write call,
/// whenever the transport takes it whole (`send`). The many small frames
/// inside a compressed stream go to a buffer first, which makes fewer
/// calls: a `ChunkWriter` for the stream frames of the data session, and
/// the buffer of the control stream (`control::write`), which go to the
/// session in CHUNK frames (DESIGN.md §6.2).
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

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// This side's protocol version and then its features: the session's
    /// first bytes, sent without waiting for the peer's (DESIGN.md §6.2).
    pub fn send_version_and_features(&mut self) -> std::io::Result<()> {
        let mut bytes = [0u8; 12];
        bytes[..4].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        bytes[4..].copy_from_slice(&FEATURES.to_le_bytes());
        self.writer.write_all(&bytes)
    }

    /// Write one frame: its header and its payload in one vectored write
    /// call, and the rest in more calls if the transport takes only a part
    /// of it. This function exists so that SSH does not send the header of
    /// a frame alone: SSH sends what one read of its stdin returns in
    /// packets of its own, so with two write calls, it can read the header
    /// before the payload arrives, and the header costs a packet
    /// (DESIGN.md §6.2).
    pub fn send(&mut self, kind: u8, payload: &[u8]) -> std::io::Result<()> {
        let header = frame_header(kind, payload);
        self.write_parts(&mut [IoSlice::new(&header), IoSlice::new(payload)])
    }

    /// Write `parts` with one vectored write call, and the rest with more
    /// calls if the transport takes only some of the bytes.
    fn write_parts(&mut self, mut parts: &mut [IoSlice<'_>]) -> std::io::Result<()> {
        while !parts.is_empty() {
            match self.writer.write_vectored(parts) {
                Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                Ok(n) => IoSlice::advance_slices(&mut parts, n),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// The header of a frame of type `kind` with `payload`: `type u8,
/// length u32`.
fn frame_header(kind: u8, payload: &[u8]) -> [u8; FRAME_HEADER] {
    let length = u32::try_from(payload.len())
        .unwrap_or_else(|_| panic!("frame type {kind} with {} bytes", payload.len()));
    let mut header = [0; FRAME_HEADER];
    header[0] = kind;
    header[1..].copy_from_slice(&length.to_le_bytes());
    header
}

/// Send one frame on the data session, whose other end is `peer`. A
/// failure ends the run (`die_data`).
pub fn send_data<W: Write>(out: &mut FrameWriter<W>, kind: u8, payload: &[u8], peer: &str) {
    if let Err(e) = out.send(kind, payload) {
        die_data(peer, &e);
    }
}

/// Flush the data session. A failure ends the run, as in `send_data`.
pub fn flush_data<W: Write>(out: &mut FrameWriter<W>, peer: &str) {
    if let Err(e) = out.flush() {
        die_data(peer, &e);
    }
}

/// End the run because `peer`, the other host, disappeared: one of its
/// sessions ended while this host still needed it (DESIGN.md §6.9). This
/// function exists so that each host words the loss of its peer one way,
/// whichever of its threads, and whichever session, sees it first.
pub fn lost(peer: &str) -> ! {
    die(format!("the {peer} disappeared"))
}

/// End the run after a read or a write of the data session failed
/// (`die_session`).
pub fn die_data(peer: &str, e: &std::io::Error) -> ! {
    die_session("data session", peer, e)
}

/// End the run after a read or a write of `session` failed. EOF inside a
/// frame, and a write to a session that `peer` closed, are the loss of
/// `peer` (`lost`); any other failure is reported as a failure of
/// `session`.
pub fn die_session(session: &str, peer: &str, e: &std::io::Error) -> ! {
    use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
    if matches!(e.kind(), UnexpectedEof | BrokenPipe | ConnectionReset) {
        lost(peer);
    }
    die(format!("{session}: {e}"))
}

/// The compression levels that a sender supports (DESIGN.md §7): zstd
/// levels 1 to 19, and 0, which disables compression.
pub const COMPRESSION_LEVELS: RangeInclusive<i32> = 0..=19;

/// The window log of long-distance matching: a 128 MiB window, as with
/// `zstd --long=27`. A zstd decoder accepts windows up to this size by
/// default, so the receiver needs no setting for it.
const LONG_WINDOW_LOG: u32 = 27;

/// One compressed stream (DESIGN.md §6.2): the stream frames of a stream
/// on the data session, in one zstd frame, or the control stream of one
/// direction of the control session. With compression disabled, the bytes
/// pass as they are. `flush` makes every byte written so far decodable by
/// the peer. `finish` ends the zstd frame of a stream, whose checksum the
/// reader checks; no frame ends a control stream, which ends with the
/// closure of the session.
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

    /// The control stream of one direction of the control session:
    /// compress at zstd `level` in the writer's own thread, without
    /// long-distance matching; level 0 disables compression. Its frames are
    /// small, and the writer flushes whenever its queue runs empty, so
    /// worker threads and a long window would only add cost (DESIGN.md
    /// §6.2).
    pub fn control(transport: W, level: i32) -> Self {
        Self::new(transport, level, false, 0)
    }

    /// End the zstd frame, flush the transport, and return it.
    pub fn finish(mut self) -> std::io::Result<W> {
        if let Some(zstd) = &mut self.zstd {
            zstd.drain(&mut self.transport, ZSTD_EndDirective::ZSTD_e_end)?;
        }
        self.transport.flush()?;
        Ok(self.transport)
    }

    /// The transport, without ending the stream, as a control writer ends:
    /// the bytes that zstd still holds are lost.
    pub fn into_transport(self) -> W {
        self.transport
    }

    /// The transport, which the control writer empties into CHUNK frames.
    pub fn transport_mut(&mut self) -> &mut W {
        &mut self.transport
    }

    /// Start the zstd frame of the stream now, which writes nothing: the
    /// frame header goes out with the first block. The start of a frame
    /// allocates the tables of the context and clears them, which takes
    /// milliseconds, tens of them on a busy host, and waits for the other
    /// threads of the process that map memory meanwhile, such as the decoder
    /// of a stream that takes a window of up to 128 MiB. The control writer
    /// calls this at its start, so that its first frame, such as STOP, waits
    /// for neither (DESIGN.md §6.2). Ending a frame here and starting
    /// another would clear the tables again at the first frame.
    pub fn prepare(&mut self) {
        if let Some(zstd) = &mut self.zstd {
            let status = zstd
                .encoder
                .compress_stream(&[], &mut zstd.buffer, ZSTD_EndDirective::ZSTD_e_continue)
                .unwrap_or_else(zstd_failed);
            assert_eq!(status.written, 0, "zstd wrote without input");
        }
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

/// The most bytes of one CHUNK frame (DESIGN.md §6.2).
pub const CHUNK_BYTES: usize = 256 << 10;

/// The magic number of a zstd frame, little-endian on the wire, and the
/// bit of its frame header descriptor, the byte after it, that announces
/// the checksum at the end of the frame (RFC 8878, section 3.1.1).
const ZSTD_MAGIC: [u8; 4] = 0xFD2F_B528u32.to_le_bytes();
const ZSTD_CHECKSUM_FLAG: u8 = 1 << 2;

/// The decoding of a zstd frame, which `StreamReader` and `Decompressor`
/// share.
struct Decoding {
    decoder: ZstdDCtx,
    /// Decoded bytes, of which `[start, end)` are not read yet.
    buffer: Box<[u8]>,
    start: usize,
    end: usize,
    /// zstd decoded the whole frame, and verified its checksum if the frame
    /// has one.
    ended: bool,
    /// The first bytes that zstd took: the magic number of the frame and its
    /// frame header descriptor, which tells whether the frame has a
    /// checksum. zstd itself accepts a frame without one.
    start_of_frame: [u8; 5],
    /// How many bytes of `start_of_frame` zstd took so far.
    taken: usize,
    /// The bytes of the transport that zstd took so far.
    consumed: u64,
}

impl Decoding {
    fn new() -> Self {
        Decoding {
            decoder: ZstdDCtx::new(),
            buffer: vec![0; ZstdDCtx::out_size()].into_boxed_slice(),
            start: 0,
            end: 0,
            ended: false,
            start_of_frame: [0; 5],
            taken: 0,
            consumed: 0,
        }
    }

    /// Start afresh, before a new frame, with the memory that the context
    /// and the buffer have taken.
    fn reset(&mut self) {
        self.decoder.reset_session().unwrap_or_else(zstd_failed);
        self.start = 0;
        self.end = 0;
        self.ended = false;
        self.start_of_frame = [0; 5];
        self.taken = 0;
        self.consumed = 0;
    }

    /// True when the frame is a zstd frame that announces its checksum,
    /// which zstd then verified at its end.
    fn has_checksum(&self) -> bool {
        let [magic @ .., descriptor] = self.start_of_frame;
        self.taken == self.start_of_frame.len()
            && magic == ZSTD_MAGIC
            && descriptor & ZSTD_CHECKSUM_FLAG != 0
    }

    /// True when decoded bytes wait to be read, after decoding more if the
    /// buffer holds none; false at the end of the frame. EOF of the
    /// transport inside the frame is `UnexpectedEof`: a truncated stream
    /// cannot end the frame, because its end needs the last block and the
    /// checksum. Invalid compressed data, including a wrong checksum, is a
    /// protocol error and panics (DESIGN.md §6.7). Interrupted transport
    /// reads are retried (DESIGN.md §3).
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
            let n = (self.start_of_frame.len() - self.taken).min(status.read);
            self.start_of_frame[self.taken..self.taken + n].copy_from_slice(&input[..n]);
            self.taken += n;
            transport.consume(status.read);
            self.consumed += status.read as u64;
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

    /// Move decoded bytes into `buf`, which `available` found.
    fn take(&mut self, buf: &mut [u8]) -> usize {
        let n = buf.len().min(self.end - self.start);
        buf[..n].copy_from_slice(&self.buffer[self.start..self.start + n]);
        self.start += n;
        n
    }
}

/// The reading end of a stream on the data session (DESIGN.md §6.2): its
/// stream frames, decoded from its zstd frame, or the bytes of the
/// transport as they are when compression is disabled. A read returns 0 at
/// the end of the zstd frame, or at the EOF of the transport without
/// compression: the normal end of the stream frames, which `finish` then
/// checks. EOF of the transport inside the zstd frame is `UnexpectedEof`.
pub struct StreamReader<R: BufRead> {
    transport: R,
    /// `None` when compression is disabled.
    zstd: Option<Decoding>,
    /// The bytes of the transport read so far without compression.
    consumed: u64,
}

impl<R: BufRead> StreamReader<R> {
    pub fn new(transport: R, compressed: bool) -> Self {
        StreamReader {
            transport,
            zstd: compressed.then(Decoding::new),
            consumed: 0,
        }
    }

    /// The bytes of the transport that this reader decoded so far, which a
    /// buffer of the transport does not count: at a stop, the bytes that
    /// wait there are dropped undecoded (DESIGN.md §8).
    pub fn consumed(&self) -> u64 {
        match &self.zstd {
            Some(zstd) => zstd.consumed,
            None => self.consumed,
        }
    }

    /// After a read returned 0: require the normal end of the stream, a
    /// zstd frame that ended with its checksum, and right after it, the end
    /// of the transport. Without compression, the transport has ended
    /// already. A frame without a checksum, and bytes after the frame, are
    /// protocol errors and panic (DESIGN.md §6.6).
    pub fn finish(mut self) -> std::io::Result<()> {
        if let Some(zstd) = &self.zstd {
            assert!(
                zstd.has_checksum(),
                "protocol error: the zstd frame of a stream has no checksum"
            );
        }
        let rest = loop {
            match self.transport.fill_buf() {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                result => break result?,
            }
        };
        assert!(
            rest.is_empty(),
            "protocol error: bytes after the zstd frame of a stream"
        );
        Ok(())
    }
}

impl<R: BufRead> Read for StreamReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let Some(zstd) = &mut self.zstd else {
            let n = self.transport.read(buf)?;
            self.consumed += n as u64;
            return Ok(n);
        };
        if buf.is_empty() || !zstd.available(&mut self.transport)? {
            return Ok(0);
        }
        Ok(zstd.take(buf))
    }
}

/// The reading end of the control stream of one direction of the control
/// session (DESIGN.md §6.2): the decoded bytes of a zstd frame that never
/// ends, or the bytes of the transport when compression is disabled. The
/// transport gives the payloads of the CHUNK frames of the session. The
/// closure of the session ends the stream: EOF of the transport is
/// `UnexpectedEof` inside the frame. The end of the frame is a protocol
/// error and panics.
pub struct Decompressor<R: BufRead> {
    transport: R,
    /// `None` when compression is disabled.
    zstd: Option<Decoding>,
}

impl<R: BufRead> Decompressor<R> {
    pub fn new(transport: R, compressed: bool) -> Self {
        Decompressor {
            transport,
            zstd: compressed.then(Decoding::new),
        }
    }

    /// Decode a control stream that a control writer at `level` sends, with
    /// STOP alone, which reads nothing of the transport, and start afresh,
    /// with the window buffer that zstd took for it, for the reason of
    /// `Compressor::prepare`: the control reader calls this before the
    /// first frame of the session. A new frame of a decoder clears no
    /// table.
    pub fn prepare(&mut self, level: i32) {
        let Some(zstd) = &mut self.zstd else {
            return;
        };
        let mut frames = FrameWriter::new(Compressor::control(Vec::new(), level));
        frames
            .send(frame::STOP, b"")
            .and_then(|()| frames.flush())
            .expect("a write to memory failed");
        let stream = frames.into_inner().into_transport();
        let decoded = zstd
            .available(&mut &stream[..])
            .expect("a read from memory failed");
        assert!(decoded, "zstd decoded nothing of a flushed frame");
        zstd.reset();
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
        assert!(
            zstd.available(&mut self.transport)?,
            "protocol error: the zstd frame of the control session ended"
        );
        Ok(zstd.take(buf))
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
    use proptest::prelude::*;

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
        writer.send(frame::STREAM_END, b"").unwrap();
        assert_eq!(out[..5], [frame::LIT, 3, 0, 0, 0]);
        assert_eq!(
            read_all(&out),
            vec![
                (frame::LIT, b"abc".to_vec()),
                (frame::STREAM_END, Vec::new())
            ]
        );
    }

    /// The frame that `prepare` compresses goes nowhere: a prepared control
    /// writer sends the stream of a fresh one, byte for byte, and a prepared
    /// reader decodes it as a fresh one does (DESIGN.md §6.2).
    #[test]
    fn a_prepared_control_stream_is_the_stream_of_a_fresh_one() {
        let query = [7u8; 40];
        for level in [0, 3, 9] {
            let stream = |prepared: bool| {
                let mut compressor = Compressor::control(Vec::new(), level);
                if prepared {
                    compressor.prepare();
                }
                let mut writer = FrameWriter::new(compressor);
                writer.send(frame::QUERY, &query).unwrap();
                writer.flush().unwrap();
                writer.send(frame::STOP, b"").unwrap();
                writer.flush().unwrap();
                writer.into_inner().into_transport()
            };
            let prepared = stream(true);
            assert_eq!(prepared, stream(false), "level {level}");
            let mut decompressor = Decompressor::new(&prepared[..], level != 0);
            decompressor.prepare(level);
            let mut reader = FrameReader::new(decompressor);
            let first = reader.next().unwrap().unwrap();
            assert_eq!((first.kind, first.payload), (frame::QUERY, &query[..]));
            let second = reader.next().unwrap().unwrap();
            assert_eq!(second.kind, frame::STOP, "level {level}");
        }
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

    /// The frames of a target list, as `TargetList::send` gives them.
    fn sent(list: &TargetList) -> Vec<ExchangeFrame> {
        let mut frames = Vec::new();
        list.send(|frame| frames.push(frame));
        frames
    }

    #[test]
    fn target_lists_carry_their_lists_in_hashdata_frames() {
        // The target, and the base of a delta, travel as just their
        // digests, and the frame announces the bytes of the lists, which
        // follow in HASHDATA frames, the removed list first (DESIGN.md
        // §6.2).
        let full = TargetList::Full {
            target: [1; 32],
            list: b"full".to_vec(),
        };
        let keep = TargetList::Keep { target: [2; 32] };
        let delta = TargetList::Delta {
            target: [3; 32],
            base: [4; 32],
            removed: b"rm".to_vec(),
            added: b"add".to_vec(),
        };
        let data = |payload: &[u8]| ExchangeFrame {
            kind: frame::HASHDATA,
            payload: payload.to_vec(),
        };
        let hashlist = ExchangeFrame {
            kind: frame::HASHLIST,
            payload: [&[1; 32][..], &4u64.to_le_bytes()].concat(),
        };
        assert_eq!(sent(&full), [hashlist, data(b"full")]);
        let hashkeep = ExchangeFrame {
            kind: frame::HASHKEEP,
            payload: vec![2; 32],
        };
        assert_eq!(sent(&keep), [hashkeep]);
        let hashdelta = ExchangeFrame {
            kind: frame::HASHDELTA,
            payload: [
                &[3; 32][..],
                &[4; 32],
                &2u64.to_le_bytes(),
                &3u64.to_le_bytes(),
            ]
            .concat(),
        };
        assert_eq!(sent(&delta), [hashdelta, data(b"rmadd")]);
        let sizes: Vec<usize> = [&full, &keep, &delta].map(TargetList::size).to_vec();
        assert_eq!(sizes, [54, 37, 95]);

        for list in [full, keep, delta] {
            let frames = sent(&list);
            let first = TargetFrame::decode(frames[0].kind, &frames[0].payload);
            let expected = match list {
                TargetList::Full { target, list } => TargetFrame::Full {
                    target,
                    length: list.len() as u64,
                },
                TargetList::Keep { target } => TargetFrame::Keep { target },
                TargetList::Delta {
                    target,
                    base,
                    removed,
                    added,
                } => TargetFrame::Delta {
                    target,
                    base,
                    removed: removed.len() as u64,
                    added: added.len() as u64,
                },
            };
            assert_eq!(first, expected);
        }
        assert_eq!(TargetFrame::decode(frame::HASHNONE, b""), TargetFrame::None);
    }

    #[test]
    fn a_long_list_goes_in_full_hashdata_frames() {
        // Each HASHDATA frame but the last carries `HASHDATA_BYTES`; a delta
        // fills them across the end of its removed list, and the sizes
        // count every header (DESIGN.md §6.2).
        let removed = noise(HASHDATA_BYTES + 100, 1);
        let added = noise(2 * HASHDATA_BYTES, 2);
        let delta = TargetList::Delta {
            target: [3; 32],
            base: [4; 32],
            removed: removed.clone(),
            added: added.clone(),
        };
        let frames = sent(&delta);
        let pieces: Vec<&ExchangeFrame> = frames[1..].iter().collect();
        assert!(pieces.iter().all(|piece| piece.kind == frame::HASHDATA));
        let lengths: Vec<usize> = pieces.iter().map(|piece| piece.payload.len()).collect();
        assert_eq!(
            lengths,
            [HASHDATA_BYTES, HASHDATA_BYTES, HASHDATA_BYTES, 100]
        );
        let bytes: Vec<u8> = pieces
            .iter()
            .flat_map(|piece| piece.payload.clone())
            .collect();
        assert_eq!(bytes, [removed, added].concat());
        let size: usize = frames.iter().map(|frame| 5 + frame.payload.len()).sum();
        assert_eq!(delta.size(), size);
    }

    #[test]
    #[should_panic(expected = "protocol error: expected the target list, received frame type 4")]
    fn another_frame_in_place_of_the_target_list_is_a_protocol_error() {
        TargetFrame::decode(frame::STREAM_END, b"");
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHNONE with a payload")]
    fn hashnone_is_empty() {
        TargetFrame::decode(frame::HASHNONE, b"x");
    }

    #[test]
    fn the_version_and_the_features_precede_the_first_frame() {
        let mut out = Vec::new();
        let mut writer = FrameWriter::new(&mut out);
        writer.send_version_and_features().unwrap();
        writer.send(frame::HELLO, b"x").unwrap();
        assert_eq!(out[..4], PROTOCOL_VERSION.to_le_bytes());
        assert_eq!(out[4..12], FEATURES.to_le_bytes());
        let mut reader = FrameReader::new(&out[..]);
        assert!(reader.read_version_and_features("peer").unwrap());
        assert_eq!(reader.next().unwrap().unwrap().kind, frame::HELLO);

        // A later peer can send bits that this build does not know.
        let mut unknown = out.clone();
        unknown[4..12].copy_from_slice(&u64::MAX.to_le_bytes());
        let mut reader = FrameReader::new(&unknown[..]);
        assert!(reader.read_version_and_features("peer").unwrap());
        assert_eq!(reader.next().unwrap().unwrap().kind, frame::HELLO);

        let mut empty = FrameReader::new(&out[..0]);
        assert!(!empty.read_version_and_features("peer").unwrap());
        for cut in 1..12 {
            let error = FrameReader::new(&out[..cut])
                .read_version_and_features("peer")
                .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    /// A transport that keeps the bytes of each write call apart, and
    /// takes at most `most` bytes in one call.
    struct Calls {
        calls: Vec<Vec<u8>>,
        most: usize,
    }

    impl Write for Calls {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.write_vectored(&[IoSlice::new(buf)])
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
            let mut call: Vec<u8> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
            call.truncate(self.most);
            let n = call.len();
            self.calls.push(call);
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_frame_is_one_write_call() {
        // SSH sends what one read of its stdin returns in packets of its
        // own, so the header of a frame must not arrive alone (DESIGN.md
        // §6.2). A transport that takes only a part of a frame gets the
        // rest in more calls.
        let chunk = [&[frame::CHUNK, 7, 0, 0, 0][..], b"payload"].concat();
        let mut writer = FrameWriter::new(Calls {
            calls: Vec::new(),
            most: usize::MAX,
        });
        writer.send(frame::CHUNK, b"payload").unwrap();
        writer.send(frame::STREAM_END, b"").unwrap();
        let stream_end = vec![frame::STREAM_END, 0, 0, 0, 0];
        assert_eq!(writer.into_inner().calls, [chunk.clone(), stream_end]);
        let mut writer = FrameWriter::new(Calls {
            calls: Vec::new(),
            most: 5,
        });
        writer.send(frame::CHUNK, b"payload").unwrap();
        let calls = writer.into_inner().calls;
        assert_eq!(calls.len(), 3);
        assert_eq!(calls.concat(), chunk);
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
        // The command of REQUEST announces u32::MAX arguments, but none
        // follow. A list reserved for that count takes 96 GiB, which
        // overcommit can grant without memory, so compare the peak virtual
        // size. Tests that run in parallel add much less than the bound.
        let mut payload = vec![1];
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let before = peak_virtual_size();
        let Err(unwound) = std::panic::catch_unwind(|| Request::decode(&payload)) else {
            panic!("a REQUEST without its arguments must not decode");
        };
        let message = unwound.downcast::<String>().unwrap();
        assert!(message.contains("truncated REQUEST frame"), "{message}");
        assert!(peak_virtual_size() - before < 16 << 30);
    }

    /// A REQUEST for `command` with the defaults of `hashsync ssh`.
    fn request(command: RemoteCommand) -> Request {
        Request {
            command,
            deduplicated: Deduplicated::Stdout,
            settings: crate::settings::tests::settings(&[]),
            quiet: false,
            human_readable: true,
            config: Source::Default,
        }
    }

    #[test]
    fn requests_roundtrip() {
        let settings = crate::settings::tests::settings(&[
            "--batch-stdout",
            "--wait",
            "--lookahead",
            "64KiB",
            "--spool-limit",
            "unlimited",
            "--no-sender-cache",
            "--compression-level",
            "3",
        ]);
        for command in [
            RemoteCommand::Shell(b"printf %s \xff\xfe".to_vec()),
            RemoteCommand::Shell(Vec::new()),
            RemoteCommand::Argv(vec![
                b"printf".to_vec(),
                b"%s".to_vec(),
                b"\xff\xfe".to_vec(),
            ]),
        ] {
            for deduplicated in [
                Deduplicated::Stdout,
                Deduplicated::Stdin,
                Deduplicated::Both,
            ] {
                let request = Request {
                    deduplicated,
                    settings: settings.clone(),
                    quiet: true,
                    human_readable: false,
                    ..request(command.clone())
                };
                assert_eq!(Request::decode(&request.encode()), request);
            }
        }
        // The default lookahead travels as 0.
        let defaults = request(RemoteCommand::Shell(b"true".to_vec()));
        assert_eq!(Request::decode(&defaults.encode()), defaults);
        // The server's configuration file, as each option selects it.
        for config in [
            Source::Profile(b"\xff.b".to_vec()),
            Source::File(b"relative/\xff path".to_vec()),
            Source::File(Vec::new()),
        ] {
            let request = Request {
                config,
                ..request(RemoteCommand::Shell(b"true".to_vec()))
            };
            assert_eq!(Request::decode(&request.encode()), request);
        }
    }

    /// The server's configuration file is its profile `default`, a profile
    /// that `--server-profile` names, or a file that `--server-config`
    /// names: no other kind exists, and a profile name is one nonempty file
    /// name, as the client's options require (DESIGN.md §6.2 and §7).
    #[test]
    fn the_server_reads_a_profile_or_a_file() {
        let payload = request(RemoteCommand::Shell(b"true".to_vec())).encode();
        // The kind of the profile `default` ends the payload.
        let at = payload.len() - 1;
        assert_eq!(payload[at], 0, "the profile default");
        for value in [3, 255] {
            let mut payload = payload.clone();
            payload[at] = value;
            let message = caught(|| Request::decode(&payload)).unwrap_err();
            let expected = format!("protocol error: invalid configuration kind {value} in REQUEST");
            assert!(message.contains(&expected), "{message}");
        }
        for config in [
            Source::Profile(Vec::new()),
            Source::Profile(b"a/b".to_vec()),
            Source::Profile(b"/".to_vec()),
        ] {
            let request = Request {
                config,
                ..request(RemoteCommand::Shell(b"true".to_vec()))
            };
            let message = caught(|| Request::decode(&request.encode())).unwrap_err();
            let expected = "protocol error: a profile name must not be empty or contain '/'";
            assert!(message.contains(expected), "{message}");
        }
    }

    /// The command and the server's configuration file come from the
    /// client's command line, whose arguments hold no NUL byte, so one in
    /// REQUEST is a protocol error (DESIGN.md §6.2). Any other byte is
    /// valid.
    #[test]
    fn the_byte_strings_of_a_request_hold_no_nul() {
        let shell = |line: &[u8]| request(RemoteCommand::Shell(line.to_vec()));
        let argv = |word: &[u8]| request(RemoteCommand::Argv(vec![b"a".to_vec(), word.to_vec()]));
        let config = |config| Request {
            config,
            ..shell(b"true")
        };
        for request in [
            shell(b"echo a\0b"),
            argv(b"\0"),
            config(Source::Profile(b"a\0b".to_vec())),
            config(Source::File(b"/a\0b".to_vec())),
        ] {
            let message = caught(|| Request::decode(&request.encode())).unwrap_err();
            let expected = "protocol error: a NUL byte in a byte string of the REQUEST frame";
            assert!(message.contains(expected), "{message}");
        }
        let all: Vec<u8> = (1..=255).collect();
        for request in [
            shell(&all),
            argv(&all),
            config(Source::Profile(
                all.iter().copied().filter(|&b| b != b'/').collect(),
            )),
            config(Source::File(all.clone())),
        ] {
            assert_eq!(Request::decode(&request.encode()), request);
        }
    }

    #[test]
    #[should_panic(expected = "invalid command kind 2 in REQUEST")]
    fn a_command_is_a_line_or_words() {
        let mut payload = request(RemoteCommand::Shell(b"true".to_vec())).encode();
        payload[0] = 2;
        Request::decode(&payload);
    }

    #[test]
    #[should_panic(expected = "protocol error: a command line of 2 words")]
    fn a_command_line_is_one_string() {
        let mut payload = request(RemoteCommand::Argv(vec![b"a".to_vec(), b"b".to_vec()])).encode();
        payload[0] = 0;
        Request::decode(&payload);
    }

    #[test]
    fn a_run_deduplicates_one_stream_or_both() {
        // Only 1, 2 and 3 name deduplicated streams: none is no run, and
        // the values above 3 name nothing (DESIGN.md §6.2).
        let mut payload = request(RemoteCommand::Shell(b"true".to_vec())).encode();
        // After the command kind, the count of the words, and the one word
        // with its length.
        let at = 1 + 4 + 4 + 4;
        assert_eq!(payload[at], 2, "the backward stream");
        for value in [0, 4, 255] {
            payload[at] = value;
            let message = caught(|| Request::decode(&payload)).unwrap_err();
            let expected =
                format!("protocol error: invalid deduplicated streams {value} in REQUEST");
            assert!(message.contains(&expected), "{message}");
        }
    }

    #[test]
    fn hello_roundtrips() {
        let hello = Hello {
            block_size: 4096,
            weak_hash_id: "weak".into(),
            strong_hash: StrongHash::Sha256,
        };
        let decoded = Hello::decode(&hello.encode());
        assert_eq!(decoded.block_size, 4096);
        assert_eq!(decoded.weak_hash_id, "weak");
        assert_eq!(decoded.strong_hash, StrongHash::Sha256);
    }

    #[test]
    fn index_ids_roundtrip() {
        let ids = IndexIds {
            account_id: [7; 16],
            index_id: [9; 16],
        };
        let decoded = IndexIds::decode(&ids.encode());
        assert_eq!((decoded.account_id, decoded.index_id), ([7; 16], [9; 16]));
    }

    #[test]
    fn advertise_roundtrips_with_and_without_lists() {
        let mut advertise = Advertise {
            account_id: [1; 16],
            lists: Vec::new(),
        };
        let decoded = Advertise::decode(&advertise.encode());
        assert_eq!(decoded.account_id, [1; 16]);
        assert!(decoded.lists.is_empty());
        advertise.lists = vec![[5; 32], [6; 32]];
        assert_eq!(
            Advertise::decode(&advertise.encode()).lists,
            [[5; 32], [6; 32]]
        );
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
    #[should_panic(expected = "excess bytes")]
    fn trailing_payload_bytes_are_rejected() {
        let mut payload = End {
            status: 0,
            error: String::new(),
            forward: Counts::default(),
            backward: SourceRead::default(),
        }
        .encode();
        payload.push(0);
        End::decode(&payload);
    }

    #[test]
    fn end_carries_the_counts_of_both_streams() {
        // The server's counts of its end of the forward stream, then of its
        // end of the backward stream, follow the status and the error text
        // (DESIGN.md §6.2). Each count differs from the others, so that a
        // decoder that reads them in another order fails.
        let forward = Counts {
            bytes: 7,
            literal: 1,
            reference: 4,
            run: 2,
            reference_frames: 5,
            run_frames: 14,
            wire: u64::MAX,
            misses: Misses {
                false_positives: 3,
                not_found: 6,
            },
        };
        for eof in [false, true] {
            let backward = SourceRead { bytes: 9, eof };
            let end = End {
                status: -3,
                error: "missing".into(),
                forward: forward.clone(),
                backward: backward.clone(),
            };
            let decoded = End::decode(&end.encode());
            assert_eq!((decoded.status, &decoded.error[..]), (-3, "missing"));
            assert_eq!(
                (decoded.forward, decoded.backward),
                (forward.clone(), backward)
            );
        }
    }

    #[test]
    #[should_panic(expected = "protocol error: invalid flag 2 in END frame")]
    fn the_eof_of_the_backward_source_is_a_flag() {
        let mut payload = End {
            status: 0,
            error: String::new(),
            forward: Counts::default(),
            backward: SourceRead::default(),
        }
        .encode();
        *payload.last_mut().unwrap() = 2;
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

    /// The zstd frame of a stream with `frames`, through a `Compressor` at
    /// zstd `level`, or the frames as they are at level 0.
    fn compressed(level: i32, long: bool, threads: u32, frames: &[(u8, &[u8])]) -> Vec<u8> {
        let mut writer = FrameWriter::new(Compressor::new(Vec::new(), level, long, threads));
        for &(kind, payload) in frames {
            writer.send(kind, payload).unwrap();
        }
        writer.into_inner().finish().unwrap()
    }

    /// Read the stream frames of `stream` through a `StreamReader`, until
    /// their normal end, which `finish` checks.
    fn read_stream(stream: &[u8], compressed: bool) -> Vec<(u8, Vec<u8>)> {
        let mut reader = FrameReader::new(StreamReader::new(stream, compressed));
        let mut frames = Vec::new();
        while let Some(frame) = reader.next().unwrap() {
            frames.push((frame.kind, frame.payload.to_vec()));
        }
        reader.into_inner().finish().unwrap();
        frames
    }

    #[test]
    fn frames_roundtrip_through_the_compressed_stream() {
        // A stream's frames form one zstd frame, whose end is the normal
        // end of the frames; at level 0 the frames are as they are, and the
        // end of the transport ends them (DESIGN.md §6.2).
        let zeros = vec![0; 100_000];
        let run = Rle {
            byte: 7,
            length: 1 << 40,
        }
        .encode();
        let frames = [
            (frame::LIT, &zeros[..]),
            (frame::REF, &b""[..]),
            (frame::RLE, &run),
        ];
        let plain = compressed(0, false, 0, &frames);
        let cases = [(0, false, 0), (1, false, 1), (19, false, 3), (19, true, 3)];
        for (level, long, threads) in cases {
            let stream = compressed(level, long, threads, &frames);
            if level != 0 {
                assert!(stream.len() < plain.len() / 10, "level {level}");
            }
            let expected: Vec<(u8, Vec<u8>)> = frames
                .iter()
                .map(|&(kind, payload)| (kind, payload.to_vec()))
                .collect();
            assert_eq!(read_stream(&stream, level != 0), expected, "level {level}");
        }
        // An empty stream is an empty zstd frame, or no bytes at all.
        assert!(read_stream(&compressed(3, false, 0, &[]), true).is_empty());
        assert!(read_stream(&[], false).is_empty());
    }

    #[test]
    #[should_panic(expected = "protocol error: the zstd frame of a stream has no checksum")]
    fn a_zstd_frame_without_its_checksum_is_a_protocol_error() {
        // The normal end of a stream is the end of a zstd frame with its
        // checksum (DESIGN.md §6.6). The same frame without its checksum,
        // which zstd itself takes as whole, is not.
        let mut stream = compressed(3, false, 0, &[(frame::LIT, b"literal")]);
        assert_ne!(stream[4] & ZSTD_CHECKSUM_FLAG, 0, "no checksum to remove");
        stream[4] &= !ZSTD_CHECKSUM_FLAG;
        stream.truncate(stream.len() - 4);
        let mut reader = FrameReader::new(StreamReader::new(&stream[..], true));
        assert_eq!(reader.expect(frame::LIT, "LIT").unwrap(), b"literal");
        assert!(reader.next().unwrap().is_none(), "the frame did not end");
        let _ = reader.into_inner().finish();
    }

    #[test]
    fn long_distance_matching_finds_repeats_beyond_the_window_of_the_level() {
        // Level 1 has a 512 KiB window. Without long-distance matching,
        // noise that repeats after 4 MiB of other noise costs its full size
        // again. The receiver decodes the 128 MiB window without a setting.
        let repeated = noise(256 << 10, 1);
        let gap = noise(4 << 20, 2);
        let frames = [
            (frame::LIT, &repeated[..]),
            (frame::LIT, &gap[..]),
            (frame::LIT, &repeated[..]),
        ];
        let short = compressed(1, false, 2, &frames);
        let long = compressed(1, true, 2, &frames);
        assert!(
            long.len() + (200 << 10) < short.len(),
            "{} bytes with long-distance matching, {} without",
            long.len(),
            short.len()
        );
        let decoded = read_stream(&long, true);
        assert!(decoded.len() == 3 && decoded[2].1 == repeated);
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
        // before it, although the stream goes on: the data writer flushes
        // at the idle points of eager delivery (DESIGN.md §6.2).
        for (long, threads) in [(false, 0), (false, 1), (false, 3), (true, 1), (true, 3)] {
            let sent = Shared::default();
            let mut writer = FrameWriter::new(Compressor::new(sent.clone(), 9, long, threads));
            writer.send(frame::REF, b"").unwrap();
            writer.send(frame::LIT, &[1; 1000]).unwrap();
            writer.flush().unwrap();
            let so_far = sent.0.borrow().clone();
            let mut reader = FrameReader::new(StreamReader::new(&so_far[..], true));
            assert_eq!(reader.expect(frame::REF, "REF").unwrap(), b"");
            assert_eq!(reader.expect(frame::LIT, "LIT").unwrap(), [1; 1000]);
            let error = reader.next().unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn a_control_stream_decodes_at_each_flush_and_ends_at_closure() {
        // A control writer flushes whenever its queue runs empty, and it
        // never ends its zstd frame: the closure of the session ends the
        // stream, as EOF inside the frame (DESIGN.md §6.2). Answers that
        // share a flush take a small part of their plain size.
        let answers: Vec<Vec<u8>> = (0..1000u64)
            .map(|i| {
                let found = i % 7 != 0;
                Answer {
                    offset: i * 4096,
                    found,
                }
                .encode()
            })
            .collect();
        let sent = Shared::default();
        let mut writer = FrameWriter::new(Compressor::control(sent.clone(), 9));
        writer.send(frame::ANSWER, &answers[0]).unwrap();
        writer.flush().unwrap();
        let alone = sent.0.borrow().len();
        for answer in &answers[1..] {
            writer.send(frame::ANSWER, answer).unwrap();
        }
        writer.flush().unwrap();
        let batch = sent.0.borrow().len() - alone;
        let plain = 999 * (5 + answers[0].len());
        assert!(batch < plain / 4, "{batch} bytes for {plain} plain bytes");
        writer.into_inner().into_transport();
        let stream = sent.0.borrow().clone();
        assert_eq!(stream.len(), alone + batch, "nothing ends the frame");

        let mut reader = FrameReader::new(Decompressor::new(&stream[..], true));
        for answer in &answers {
            assert_eq!(reader.expect(frame::ANSWER, "ANSWER").unwrap(), answer);
        }
        let error = reader.next().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    #[should_panic(expected = "protocol error: the zstd frame of the control session ended")]
    fn the_end_of_a_control_frame_is_a_protocol_error() {
        // Only the closure of the session ends a control stream.
        let stream = compressed(3, false, 0, &[(frame::ANSWER, &[0; 9])]);
        let mut reader = FrameReader::new(Decompressor::new(&stream[..], true));
        reader.expect(frame::ANSWER, "ANSWER").unwrap();
        let _ = reader.next();
    }

    #[test]
    fn a_truncated_stream_is_unexpected_eof() {
        // Wherever the stream ends early, also inside the trailer of its
        // zstd frame, reading reports UnexpectedEof, never the normal end
        // (DESIGN.md §6.6). Without compression, only the frames show the
        // cut.
        let frames = [(frame::LIT, &b"literal"[..]), (frame::REF, &b""[..])];
        for compressed_stream in [true, false] {
            let level = if compressed_stream { 3 } else { 0 };
            let stream = compressed(level, false, 1, &frames);
            for cut in 0..stream.len() {
                let mut reader =
                    FrameReader::new(StreamReader::new(&stream[..cut], compressed_stream));
                let result = (|| {
                    reader.expect(frame::LIT, "LIT")?;
                    reader.expect(frame::REF, "REF")?;
                    reader.next().map(|_| ())
                })();
                let error = result.err().unwrap_or_else(|| panic!("cut at {cut}"));
                assert_eq!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof,
                    "cut at {cut}"
                );
            }
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
        let stream = compressed(3, false, 1, &[(frame::LIT, b"literal")]);
        let transport = Interrupting {
            bytes: &stream,
            interruptions: 20,
            buffered: false,
        };
        let mut reader = FrameReader::new(StreamReader::new(transport, true));
        assert_eq!(reader.expect(frame::LIT, "LIT").unwrap(), b"literal");
        assert!(reader.next().unwrap().is_none(), "the frame did not end");
        reader.into_inner().finish().unwrap();
    }

    #[test]
    #[should_panic(expected = "protocol error: bytes after the zstd frame of a stream")]
    fn bytes_after_the_zstd_frame_are_a_protocol_error() {
        let mut stream = compressed(3, false, 1, &[(frame::LIT, b"literal")]);
        stream.push(0);
        read_stream(&stream, true);
    }

    #[test]
    fn a_zstd_frame_that_ends_inside_a_stream_frame_is_unexpected_eof() {
        // Two bytes of a frame header, then the end of the zstd frame: the
        // frame reader sees the end inside a frame, as for a truncation.
        let mut writer = FrameWriter::new(Compressor::new(Vec::new(), 3, false, 1));
        writer.send(frame::LIT, b"literal").unwrap();
        let mut compressor = writer.into_inner();
        compressor.write_all(&[frame::REF, 0]).unwrap();
        let stream = compressor.finish().unwrap();
        let mut reader = FrameReader::new(StreamReader::new(&stream[..], true));
        reader.expect(frame::LIT, "LIT").unwrap();
        let error = reader.next().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    #[should_panic(expected = "protocol error: invalid compressed data")]
    fn a_wrong_checksum_is_a_protocol_error() {
        let mut stream = compressed(3, false, 1, &[(frame::LIT, b"literal")]);
        *stream.last_mut().unwrap() ^= 1;
        read_stream(&stream, true);
    }

    #[test]
    #[should_panic(expected = "empty RLE")]
    fn empty_rle_is_rejected() {
        let mut enc = Enc::new();
        enc.u8(0);
        enc.u64(0);
        Rle::decode(&enc.buf);
    }

    /// The decoders of the payloads of the session frames, for the fuzzing
    /// below.
    #[derive(Clone, Copy, Debug)]
    enum Payload {
        Request,
        Hello,
        IndexIds,
        Advertise,
        End,
        Query,
        Answer,
        Rle,
        /// The frame of this type that answers ADVERTISE: HASHLIST,
        /// HASHKEEP or HASHDELTA.
        Target(u8),
    }

    /// The payload of the frame that `target` describes.
    fn target_payload(target: TargetFrame) -> Vec<u8> {
        let mut enc = Enc::new();
        match target {
            TargetFrame::Full { target, length } => {
                enc.raw(&target);
                enc.u64(length);
            }
            TargetFrame::Keep { target } => enc.raw(&target),
            TargetFrame::Delta {
                target,
                base,
                removed,
                added,
            } => {
                enc.raw(&target);
                enc.raw(&base);
                enc.u64(removed);
                enc.u64(added);
            }
            TargetFrame::None => {}
        }
        enc.buf
    }

    impl Payload {
        /// Decode `bytes`, and encode the value again.
        fn roundtrip(self, bytes: &[u8]) -> Vec<u8> {
            match self {
                Payload::Request => Request::decode(bytes).encode(),
                Payload::Hello => Hello::decode(bytes).encode(),
                Payload::IndexIds => IndexIds::decode(bytes).encode(),
                Payload::Advertise => Advertise::decode(bytes).encode(),
                Payload::End => End::decode(bytes).encode(),
                Payload::Query => Query::decode(bytes).encode(),
                Payload::Answer => Answer::decode(bytes).encode(),
                Payload::Rle => Rle::decode(bytes).encode(),
                Payload::Target(kind) => target_payload(TargetFrame::decode(kind, bytes)),
            }
        }

        /// The bytes of `valid`, a valid encoding, that the damage must
        /// leave alone, and where the bytes after them start. Only HELLO has
        /// such bytes: the length of the weak-hash ID, which tells where the
        /// strong-hash ID starts, and the strong-hash ID. An unknown
        /// strong-hash ID ends the run with `die`, which would end this test
        /// process; the serve tests check that end.
        fn protected(self, valid: &[u8]) -> (Vec<std::ops::Range<usize>>, usize) {
            let Payload::Hello = self else {
                return (Vec::new(), 0);
            };
            let length = |at: usize| u32::from_le_bytes(valid[at..at + 4].try_into().unwrap());
            let strong = 8 + length(4) as usize;
            let end = strong + 4 + length(strong) as usize;
            (vec![4..8, strong..end], end)
        }
    }

    /// Whether `outcome`, of damaged bytes, is one that a decoder may have:
    /// a value, or a protocol error.
    fn allowed(outcome: &Result<Vec<u8>, String>) -> bool {
        match outcome {
            Ok(_) => true,
            Err(message) => message.contains("protocol error"),
        }
    }

    /// Run `decode`, and return its value, or the message of its panic.
    fn caught<T>(decode: impl FnOnce() -> T) -> Result<T, String> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(decode)).map_err(|payload| {
            payload
                .downcast::<String>()
                .map(|message| *message)
                .or_else(|payload| {
                    payload
                        .downcast::<&str>()
                        .map(|message| message.to_string())
                })
                .unwrap_or_default()
        })
    }

    /// A valid encoding of a payload of each kind, from random values.
    fn any_payload() -> impl Strategy<Value = (Payload, Vec<u8>)> {
        use proptest::collection::vec;
        // The bytes of a command-line argument: any but NUL.
        let word = || vec(1u8.., 0..6);
        let command = prop_oneof![
            word().prop_map(RemoteCommand::Shell),
            vec(word(), 1..4).prop_map(RemoteCommand::Argv),
        ];
        let deduplicated = prop_oneof![
            Just(Deduplicated::Stdout),
            Just(Deduplicated::Stdin),
            Just(Deduplicated::Both),
        ];
        let settings = (
            any::<[bool; 8]>(),
            any::<i32>(),
            any::<[u32; 2]>(),
            any::<[u64; 3]>(),
        )
            .prop_map(|(flags, compression_level, threads, sizes)| {
                let [
                    no_update,
                    scan_immutable,
                    compact,
                    sender_cache,
                    compression_long,
                    batch_stdin,
                    batch_stdout,
                    wait,
                ] = flags;
                let [lookahead, negative_cache, spool_limit] = sizes;
                RunSettings {
                    no_update,
                    scan_immutable,
                    compact,
                    sender_cache,
                    compression_level,
                    compression_long,
                    compression_threads: threads[0],
                    probe_threads: threads[1],
                    lookahead: Some(lookahead).filter(|&lookahead| lookahead != 0),
                    negative_cache,
                    spool_limit,
                    batch_stdin,
                    batch_stdout,
                    wait,
                }
            });
        let profile = || {
            vec(any::<u8>(), 1..6).prop_filter("a profile name", |name| {
                config::check_profile_name(name).is_ok()
            })
        };
        let config = prop_oneof![
            Just(Source::Default),
            profile().prop_map(Source::Profile),
            word().prop_map(Source::File),
        ];
        let request = (command, deduplicated, settings, any::<[bool; 2]>(), config).prop_map(
            |(command, deduplicated, settings, [quiet, human_readable], config)| {
                let request = Request {
                    command,
                    deduplicated,
                    settings,
                    quiet,
                    human_readable,
                    config,
                };
                (Payload::Request, request.encode())
            },
        );
        let strong_hash = prop_oneof![Just(StrongHash::Blake3), Just(StrongHash::Sha256)];
        let hello = (1u32.., "\\PC{0,10}", strong_hash).prop_map(
            |(block_size, weak_hash_id, strong_hash)| {
                let hello = Hello {
                    block_size,
                    weak_hash_id,
                    strong_hash,
                };
                (Payload::Hello, hello.encode())
            },
        );
        let index_ids = any::<[[u8; 16]; 2]>().prop_map(|ids| {
            let ids = IndexIds {
                account_id: ids[0],
                index_id: ids[1],
            };
            (Payload::IndexIds, ids.encode())
        });
        let advertise =
            (any::<[u8; 16]>(), vec(any::<Strong>(), 0..4)).prop_map(|(account_id, lists)| {
                let advertise = Advertise { account_id, lists };
                (Payload::Advertise, advertise.encode())
            });
        let end = (
            any::<i32>(),
            "\\PC{0,12}",
            any::<[u64; 9]>(),
            any::<(u64, bool)>(),
        )
            .prop_map(|(status, error, forward, (bytes, eof))| {
                let [
                    bytes_written,
                    literal,
                    reference,
                    run,
                    reference_frames,
                    run_frames,
                    wire,
                    false_positives,
                    not_found,
                ] = forward;
                let end = End {
                    status,
                    error,
                    forward: Counts {
                        bytes: bytes_written,
                        literal,
                        reference,
                        run,
                        reference_frames,
                        run_frames,
                        wire,
                        misses: Misses {
                            false_positives,
                            not_found,
                        },
                    },
                    backward: SourceRead { bytes, eof },
                };
                (Payload::End, end.encode())
            });
        let query = (any::<u64>(), any::<Strong>())
            .prop_map(|(offset, strong)| (Payload::Query, Query { offset, strong }.encode()));
        let answer = (any::<u64>(), any::<bool>())
            .prop_map(|(offset, found)| (Payload::Answer, Answer { offset, found }.encode()));
        let rle = (any::<u8>(), 1u64..)
            .prop_map(|(byte, length)| (Payload::Rle, Rle { byte, length }.encode()));
        let encoded = |kind, frame| (Payload::Target(kind), target_payload(frame));
        let target_frame = prop_oneof![
            (any::<Strong>(), any::<u64>()).prop_map(move |(target, length)| {
                encoded(frame::HASHLIST, TargetFrame::Full { target, length })
            }),
            any::<Strong>()
                .prop_map(move |target| encoded(frame::HASHKEEP, TargetFrame::Keep { target })),
            (any::<[Strong; 2]>(), any::<[u64; 2]>()).prop_map(move |(ids, lengths)| {
                let delta = TargetFrame::Delta {
                    target: ids[0],
                    base: ids[1],
                    removed: lengths[0],
                    added: lengths[1],
                };
                encoded(frame::HASHDELTA, delta)
            }),
        ];
        prop_oneof![
            request,
            hello,
            index_ids,
            advertise,
            end,
            query,
            answer,
            rle,
            target_frame,
        ]
    }

    proptest! {
        /// The decoders of the payloads of the session frames under damage
        /// (DESIGN.md §6.7 and §11). A valid encoding decodes to the value
        /// that it encodes. Cut short, it is a protocol error. With a
        /// changed byte, and as random bytes, it decodes to some value or is
        /// a protocol error. No input hangs a decoder, takes more memory than
        /// it holds, or panics otherwise.
        #[test]
        fn session_payloads_decode_or_are_protocol_errors(
            (payload, valid) in any_payload(),
            cut in any::<prop::sample::Index>(),
            changed in any::<prop::sample::Index>(),
            flip in 1u8..=255,
            random in proptest::collection::vec(any::<u8>(), 0..96),
        ) {
            prop_assert_eq!(caught(|| payload.roundtrip(&valid)), Ok(valid.clone()));

            let at = cut.index(valid.len());
            let outcome = caught(|| payload.roundtrip(&valid[..at]));
            let cut_short = matches!(&outcome, Err(message) if message.contains("protocol error: truncated"));
            prop_assert!(cut_short, "{:?} cut at {}: {:?}", payload, at, outcome);

            let (protected, after) = payload.protected(&valid);
            let free: Vec<usize> = (0..valid.len())
                .filter(|at| !protected.iter().any(|range| range.contains(at)))
                .collect();
            if !free.is_empty() {
                let mut damaged = valid.clone();
                damaged[free[changed.index(free.len())]] ^= flip;
                let outcome = caught(|| payload.roundtrip(&damaged));
                prop_assert!(allowed(&outcome), "{:?} {:?}: {:?}", payload, damaged, outcome);
            }

            let random = [&valid[..after], &random].concat();
            let outcome = caught(|| payload.roundtrip(&random));
            prop_assert!(allowed(&outcome), "{:?} {:?}: {:?}", payload, random, outcome);
        }
    }
}
