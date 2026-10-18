//! The list exchange of one deduplicated stream (DESIGN.md §5.6): its
//! receiver hands its sender the hash list of its index, the target, in the
//! cheapest form that both sides' list caches allow, and the sender
//! verifies it, gives it to the hash table of its scanner, and acknowledges
//! it.
//!
//! The exchange runs while the streams run, on the control session, and
//! each side runs its part on a thread of its own. A side talks to the
//! other through a `Link`: the control reader passes it the frames of the
//! other side, and the control writer sends its own. Before the exchange,
//! the receiver's HELLO, which sets the block size and the hashes of the
//! stream, goes on the data session at the start of the run (`hello`), so
//! that the sender's scanner can start at once, with an empty table.
//!
//! The exchange starts once the receiver's index is ready for the run.
//! INDEX names the receiver's account and index, which partition the
//! caches, and the sender answers with ADVERTISE, the lists of its cache,
//! and prepares the first of them while the receiver exports its target.
//! The receiver then sends HASHKEEP, HASHDELTA, or HASHLIST, each of which
//! names the target. The sender can ask once for the full list with
//! HASHFULL, and replies CACHE_RESULT. A receiver whose stream has ended or
//! stopped before it sends its target sends HASHNONE instead, at either of
//! its steps, and the exchange ends there: no list goes out for a stream
//! that needs none. The receiver's side has two steps: `start` sends INDEX
//! and starts the export, so that no caller can send them out of order,
//! and `finish` does the rest. The sender's side is one call (`send`). The
//! cache storage itself is `cache::Exchange`, which uses `Rc`: each side
//! runs its part on one thread, and only the receiver's export runs on a
//! thread of its own.
//!
//! The sender gives the scanner its table as soon as it can
//! (`TableUpdate`). A kept list or a delta gives a whole table, which the
//! sender builds on its own thread. A full list gives a table sized for
//! its values, which the scanner fills while the list arrives, so that the
//! scanner finds more blocks as more of the list is in (DESIGN.md §6.3).

use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};

use crate::cache::{self, Base, Exchange};
use crate::config::Config;
use crate::die::die;
use crate::hash::{Strong, StrongHash, weak};
use crate::hashlist::{self, ListDecoder, ListId};
use crate::hashtable::HashTable;
use crate::index::Reader;
use crate::protocol::{Advertise, ExchangeFrame, Hello, IndexIds, TargetFrame, TargetList, frame};

/// One side's connection to the other side of its exchange, which the
/// control session carries (DESIGN.md §6.2): the frames of the other side,
/// which the control reader passes on, and the queue of the control writer
/// for this side's frames. The control reader and the control writer
/// outlive the exchange until the end of the run, before which a failure of
/// the session ends the run, so neither end of a link ends while the
/// exchange uses it.
pub struct Link {
    input: Receiver<ExchangeFrame>,
    output: Sender<ExchangeFrame>,
}

impl Link {
    pub fn new(input: Receiver<ExchangeFrame>, output: Sender<ExchangeFrame>) -> Link {
        Link { input, output }
    }

    /// Send the frame `kind` with `payload` to the other side.
    pub fn send(&self, kind: u8, payload: Vec<u8>) {
        self.send_frame(ExchangeFrame { kind, payload });
    }

    fn send_frame(&self, frame: ExchangeFrame) {
        self.output
            .send(frame)
            .expect("the control writer ended before the list exchange");
    }

    /// The next frame of the other side.
    pub fn next(&self) -> ExchangeFrame {
        self.input
            .recv()
            .expect("the control reader ended before the list exchange")
    }

    /// The payload of the next frame, which must have type `kind`.
    pub fn expect(&self, kind: u8, what: &str) -> Vec<u8> {
        let frame = self.next();
        assert!(
            frame.kind == kind,
            "protocol error: expected {what}, received frame type {}",
            frame.kind
        );
        frame.payload
    }

    /// The next frame, which must have type `kind` and no payload.
    pub fn expect_empty(&self, kind: u8, what: &str) {
        let payload = self.expect(kind, what);
        assert!(payload.is_empty(), "protocol error: {what} with a payload");
    }

    /// The next bytes of the lists of a target message, from the next
    /// frame, which must be HASHDATA with at least one byte and at most
    /// `most` of them.
    fn data(&self, most: u64) -> Vec<u8> {
        let piece = self.expect(frame::HASHDATA, "HASHDATA");
        assert!(
            !piece.is_empty() && piece.len() as u64 <= most,
            "protocol error: HASHDATA with {} bytes where at most {most} remain",
            piece.len()
        );
        piece
    }

    /// The next `length` bytes of the lists of a target message, from
    /// HASHDATA frames. The bytes grow only as they arrive, so a corrupt
    /// length costs only the bytes that follow it (DESIGN.md §3).
    fn list_bytes(&self, length: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        while (bytes.len() as u64) < length {
            bytes.extend_from_slice(&self.data(length - bytes.len() as u64));
        }
        bytes
    }
}

/// The payload of the receiver's HELLO, from `config`, the configuration
/// of the receiver's host: the block size and the strong hash of its index,
/// which the index matches once it is ready for the run, and the weak hash
/// of this build (DESIGN.md §6.2).
pub fn hello(config: &Config) -> Vec<u8> {
    Hello {
        block_size: config.block_size,
        weak_hash_id: weak::ID.to_string(),
        strong_hash: config.strong_hash,
    }
    .encode()
}

/// The receiver's HELLO from `payload`, which the sender's scanner uses
/// for its stream. An unsupported strong hash or weak hash ends the run
/// (DESIGN.md §6.2).
pub fn read_hello(payload: &[u8]) -> Hello {
    let hello = Hello::decode(payload);
    if hello.weak_hash_id != weak::ID {
        die(format!(
            "incompatible receiver: weak hash {} (this sender: {})",
            hello.weak_hash_id,
            weak::ID
        ));
    }
    hello
}

/// The hash list of a receiver's index for one run, the target of its
/// exchange: its ID, its values, and its encoding.
pub struct Target {
    pub id: ListId,
    values: Vec<u64>,
    encoded: Vec<u8>,
}

impl Target {
    /// Export and hash the list of the index that `reader` reads, from one
    /// database snapshot, with the strong hash `strong` (DESIGN.md §3 and
    /// §5.6). The values stay until the receiver knows whether it can send
    /// the target as a delta.
    pub fn export(reader: &Reader, strong: StrongHash) -> Target {
        let values = reader.export_weak_hashes();
        let encoded = hashlist::encode(&values);
        let id = ListId {
            algorithm: strong,
            digest: strong.digest(&encoded),
        };
        Target {
            id,
            values,
            encoded,
        }
    }
}

/// The bytes of the hash lists of an exchange, which the receiver sent and
/// the sender received, for the summary (DESIGN.md §8).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListBytes {
    /// Full lists, after HASHLIST.
    pub full: u64,
    /// The removed and added lists of a delta, after HASHDELTA.
    pub delta: u64,
}

impl ListBytes {
    /// Count the lists that `message` carries.
    fn count(&mut self, message: &TargetList) {
        match message {
            TargetList::Full { list, .. } => self.full += list.len() as u64,
            TargetList::Keep { .. } => {}
            TargetList::Delta { removed, added, .. } => {
                self.delta += (removed.len() + added.len()) as u64;
            }
        }
    }
}

/// End the receiver's side of the exchange on `link` with HASHNONE,
/// because its stream has ended or stopped before this side sent its
/// target, and expire the list caches with their `max_age`, as the end of
/// every exchange does (DESIGN.md §5.6).
pub fn send_none(link: &Link, max_age: u64) {
    link.send(frame::HASHNONE, Vec::new());
    cache::expire(max_age);
}

/// The receiver's part of the exchange (DESIGN.md §5.6, steps 1, 3, 4 and 7).
pub struct ReceiverExchange {
    link: Link,
    /// The ID of the receiver's index, which partitions the list caches.
    index_id: [u8; 16],
    /// The strong hash of the stream, whose digests name its lists.
    strong: StrongHash,
    /// The export of the target, which runs on a thread of its own from
    /// INDEX until `finish` needs the target.
    export: JoinHandle<Target>,
}

impl ReceiverExchange {
    /// Send INDEX on `link`, with the IDs that partition the list caches:
    /// this side's account ID, and `index_id`, the ID of the index that
    /// `reader` reads (DESIGN.md §6.2). Then start the export of the target
    /// from `reader`. INDEX names no target, so the export runs while the
    /// sender lists its cache and prepares its cached list, and while this
    /// side reads the sender's ADVERTISE and the base of a delta.
    pub fn start(link: Link, strong: StrongHash, index_id: [u8; 16], reader: Reader) -> Self {
        let ids = IndexIds {
            account_id: cache::account_id(),
            index_id,
        };
        link.send(frame::INDEX, ids.encode());
        let export = std::thread::Builder::new()
            .name("list-export".into())
            .spawn(move || Target::export(&reader, strong))
            .expect("cannot start the export of the hash list");
        ReceiverExchange {
            link,
            index_id,
            strong,
            export,
        }
    }

    /// Read the sender's ADVERTISE, and read the base of a delta, the first
    /// of the lists that the sender advertised that this side has, while the
    /// export of the target still runs. Then wait for the target. If
    /// `done`, which says whether the stream has ended or stopped, is true
    /// by then, end the exchange with HASHNONE. Otherwise, with sender
    /// caching (`sender_cache`), durably publish the target before any list
    /// data, so that this side has every list that the sender can advertise
    /// later: keep the copy of it that this side's listing holds, or link a
    /// copy from another of this side's peer/index directories, or else
    /// write it. Then send the target in the smallest form that the
    /// sender's lists allow, answer one HASHFULL with the full list, and
    /// wait for CACHE_RESULT. Then end the exchange of this side, with the
    /// list cache's `max_age`. Returns the bytes of the lists sent.
    ///
    /// The lookups keep the index open, and so its lock, until the stream
    /// has ended or stopped. `done` is false only while they do, so the two
    /// listings of an exchange that goes on, the sender's before ADVERTISE
    /// and this side's after it, come while this side holds its lock. A
    /// run with another target needs the exclusive lock first, so neither
    /// listing holds a list of a later run. After HASHNONE, neither side
    /// removes a listed file (DESIGN.md §5.6).
    pub fn finish(self, sender_cache: bool, max_age: u64, done: impl Fn() -> bool) -> ListBytes {
        let ReceiverExchange {
            link,
            index_id,
            strong,
            export,
        } = self;
        let sender = Advertise::decode(&link.expect(frame::ADVERTISE, "ADVERTISE"));
        // This side lists its directory for the sender and the index.
        // Without sender caching, the sender keeps no list, so this side
        // keeps none either.
        let mut exchange = Exchange::receiver(&sender.account_id, &index_id);
        // The base is read before the target is known, so that the read
        // happens during the export. It is of no use if the sender
        // advertised the target itself, which HASHKEEP then names.
        let base = exchange.find_base(&sender.lists, strong);
        let target = export.join().expect("the export of the hash list panicked");
        // Without CACHE_RESULT, this side removes no listed file
        // (DESIGN.md §5.6), so the exchange ends here with its listing as
        // it is.
        if done() {
            send_none(&link, max_age);
            return ListBytes::default();
        }
        if sender_cache && !exchange.keep(&target.id) && !exchange.link(&target.id) {
            exchange.publish(&target.id, &target.encoded);
        }
        let Offer {
            message,
            base,
            fallback,
        } = Offer::choose(&sender.lists, target, base);
        let list_bytes = give_target(&link, message, fallback);
        // After CACHE_RESULT the sender holds the target, or no list at all
        // if caching is disabled, and the end of this side's exchange
        // matches that: it renews the target and the base of HASHDELTA,
        // even if the sender asked for the full list instead, removes the
        // other listed files, and expires both cache trees.
        exchange.finish(base, max_age);
        list_bytes
    }
}

/// How the receiver hands the sender its target (DESIGN.md §5.6).
struct Offer {
    /// The first message: HASHKEEP, HASHDELTA, or HASHLIST.
    message: TargetList,
    /// The base of HASHDELTA. The end of the exchange renews it, even if
    /// the sender asks for the full list instead of using the delta.
    base: Option<Base>,
    /// The full list, which the sender may ask for once with HASHFULL
    /// after HASHKEEP or HASHDELTA.
    fallback: Option<TargetList>,
}

impl Offer {
    /// Choose how to send `target`: HASHKEEP if the sender advertised it;
    /// otherwise HASHDELTA from `base`, the first of the `advertised` lists
    /// that the receiver found, with its values, if the delta takes fewer
    /// bytes than the full list; otherwise HASHLIST, which is also the
    /// choice when the first advertised list that the receiver found was
    /// damaged, and `base` is `None`.
    fn choose(advertised: &[Strong], target: Target, base: Option<(Base, Vec<u64>)>) -> Offer {
        let digest = target.id.digest;
        let full = TargetList::Full {
            target: digest,
            list: target.encoded,
        };
        if advertised.contains(&digest) {
            return Offer {
                message: TargetList::Keep { target: digest },
                base: None,
                fallback: Some(full),
            };
        }
        if let Some((base, base_values)) = base {
            let (removed, added) = hashlist::delta(&base_values, &target.values);
            let delta = TargetList::Delta {
                target: digest,
                base: base.digest,
                removed: hashlist::encode(&removed),
                added: hashlist::encode(&added),
            };
            if delta.size() < full.size() {
                return Offer {
                    message: delta,
                    base: Some(base),
                    fallback: Some(full),
                };
            }
        }
        Offer {
            message: full,
            base: None,
            fallback: None,
        }
    }
}

/// Send the sender its target as `message`, then wait for CACHE_RESULT.
/// After HASHKEEP or HASHDELTA, the sender asks once for the full list with
/// HASHFULL if it cannot use the list that the message names, or if the
/// delta does not produce the target (DESIGN.md §5.6). This function then
/// sends `fallback`, the full list. Returns the bytes of the lists sent.
fn give_target(link: &Link, message: TargetList, mut fallback: Option<TargetList>) -> ListBytes {
    let mut sent = ListBytes::default();
    message.send(|frame| link.send_frame(frame));
    sent.count(&message);
    loop {
        let reply = link.next();
        let empty = reply.payload.is_empty();
        match reply.kind {
            // The stream has one target, so the acknowledgement names none.
            frame::CACHE_RESULT => {
                assert!(empty, "protocol error: CACHE_RESULT with a payload");
                return sent;
            }
            frame::HASHFULL => {
                assert!(empty, "protocol error: HASHFULL with a payload");
                let Some(full) = fallback.take() else {
                    panic!("protocol error: HASHFULL after the full list");
                };
                full.send(|frame| link.send_frame(frame));
                sent.count(&full);
            }
            kind => panic!("protocol error: expected CACHE_RESULT, received frame type {kind}"),
        }
    }
}

/// What the sender's side of the exchange gives the scanner of its stream,
/// in order: the table, then, for a full list, the values of the list as
/// they arrive (DESIGN.md §6.3). The end of the channel says that the
/// table is whole, or, without a table before it, that the exchange ended
/// without a target, at HASHNONE.
pub enum TableUpdate {
    /// The table: whole for a kept list or a delta, and for a full list
    /// empty and sized for its values.
    Table(HashTable),
    /// The next values of the full list, sorted, after those before.
    Values(Vec<u64>),
}

/// The sender's side of the exchange (DESIGN.md §5.6, steps 2, 5 and 6),
/// from INDEX to CACHE_RESULT, on `link`, for a stream of the strong hash
/// `strong`, which the receiver's HELLO named. List the cache directory for
/// the receiver and index of INDEX, which must happen before ADVERTISE:
/// then the end of the exchange cannot remove the target of a later run,
/// even when this side outlives its receiver. With sender caching
/// (`sender_cache`), advertise every listed list, and with caching
/// disabled, none, and prepare the first advertised list while the
/// receiver exports its target (`Prepared`). Then get the target, with a
/// single HASHFULL if neither the prepared list nor the cached list that
/// the receiver names gives it, and give it to the scanner through `table`
/// as soon as it can. A scanner that has ended takes nothing. With sender
/// caching, durably publish the target, or keep the listed list that
/// HASHKEEP named, which this side has just verified. Then end the
/// exchange of this side, with the list cache's `max_age`, and only then
/// acknowledge the target with CACHE_RESULT. HASHNONE in place of INDEX or
/// of the target ends the exchange without a target, and with expiry alone.
/// Returns the bytes of the lists received.
pub fn send(
    link: &Link,
    strong: StrongHash,
    sender_cache: bool,
    max_age: u64,
    table: Sender<TableUpdate>,
) -> ListBytes {
    let first = link.next();
    let ids = match first.kind {
        frame::INDEX => IndexIds::decode(&first.payload),
        frame::HASHNONE => {
            assert!(
                first.payload.is_empty(),
                "protocol error: HASHNONE with a payload"
            );
            cache::expire(max_age);
            return ListBytes::default();
        }
        kind => panic!("protocol error: expected INDEX, received frame type {kind}"),
    };
    let mut exchange = Exchange::sender(&ids.account_id, &ids.index_id);
    let advertised = if sender_cache {
        exchange.listed_ids(strong)
    } else {
        Vec::new()
    };
    let advertise = Advertise {
        account_id: cache::account_id(),
        lists: advertised.clone(),
    };
    link.send(frame::ADVERTISE, advertise.encode());
    let prepared = advertised.first().map(|&digest| {
        let id = ListId {
            algorithm: strong,
            digest,
        };
        Prepared::read(&exchange, id)
    });
    let mut received = ListBytes::default();
    let resolved = resolve_target(
        link,
        &exchange,
        &advertised,
        strong,
        prepared,
        &mut received,
        &table,
    );
    let Some(Resolved { target, encoded }) = resolved else {
        cache::expire(max_age);
        return received;
    };
    // The table is whole: the scanner can start, also with `--wait`.
    drop(table);
    if sender_cache {
        match encoded {
            // If the list can no longer be kept, a concurrent exchange
            // has removed or replaced it, which is normal absence
            // (DESIGN.md §5.6).
            None => {
                exchange.keep(&target);
            }
            Some(encoded) => exchange.publish(&target, &encoded),
        }
    }
    exchange.finish(None, max_age);
    link.send(frame::CACHE_RESULT, Vec::new());
    received
}

/// The first list that the sender advertised, read, verified and decoded
/// while the receiver exports its target, with the hash table built from
/// it. HASHKEEP of that list needs only the table, and HASHDELTA from it
/// needs only its values, so whichever comes, the work that took most of
/// the time is done (DESIGN.md §5.6). The list is usually the only one
/// that the sender advertised.
struct Prepared {
    id: ListId,
    /// The values and the table, or `None` if the cached list turned out
    /// to be invalid, which removed it, or gone.
    list: Option<(Vec<u64>, HashTable)>,
}

impl Prepared {
    /// Prepare the list `id` from the listing of `exchange`.
    fn read(exchange: &Exchange, id: ListId) -> Prepared {
        let list = exchange.read_listed(&id, hashlist::decode).map(|values| {
            let table = HashTable::build(&values);
            (values, table)
        });
        Prepared { id, list }
    }
}

/// The target of an exchange, as the sender resolved it: its ID, and its
/// bytes, which the sender publishes, or `None` for the listed list that
/// HASHKEEP named, which the sender keeps as it is.
struct Resolved {
    target: ListId,
    encoded: Option<Vec<u8>>,
}

/// Read the receiver's answer to ADVERTISE from `link`, and resolve the
/// target that it names, of the stream's strong hash `strong` (DESIGN.md
/// §5.6), or `None` for HASHNONE. A full list must be the target itself
/// (`receive_full`). HASHKEEP and HASHDELTA name one of the `advertised`
/// lists: `prepared`, when it is that list, and otherwise the list that
/// this function reads through the listing of `exchange`. For HASHKEEP it
/// is the target, and for HASHDELTA it is the base that the delta turns
/// into the target. If that list is invalid, or the delta does not produce
/// the target, this sends HASHFULL on `link` and uses the full list that
/// the receiver sends back, which must name the same target; the result of
/// a bad delta is never used. The table of the target goes to `table` as
/// soon as it is known. Nothing after the target list, or after that full
/// list, is read here. `received` counts the lists that arrive.
fn resolve_target(
    link: &Link,
    exchange: &Exchange,
    advertised: &[Strong],
    strong: StrongHash,
    prepared: Option<Prepared>,
    received: &mut ListBytes,
    table: &Sender<TableUpdate>,
) -> Option<Resolved> {
    let first = link.next();
    let message = TargetFrame::decode(first.kind, &first.payload);
    let id = |digest| ListId {
        algorithm: strong,
        digest,
    };
    // The prepared list serves only if the message names it. Any other
    // goes now, before the target takes memory of its own: at 21 million
    // values, a prepared list takes 415 MB.
    let named = match message {
        TargetFrame::None => return None,
        TargetFrame::Full { .. } => None,
        TargetFrame::Keep { target } => Some(target),
        TargetFrame::Delta { base, .. } => Some(base),
    };
    let prepared = prepared.filter(|prepared| Some(prepared.id.digest) == named);
    // A prepared list that turned out to be invalid is not read again: it
    // gives `None`, as the read would. A scanner that has ended takes no
    // table.
    let resolved = match message {
        TargetFrame::None => unreachable!("HASHNONE names no target"),
        TargetFrame::Full { target, length } => {
            return Some(receive_full(link, id(target), length, table, received));
        }
        TargetFrame::Keep { target } => {
            let target = id(target);
            assert!(
                advertised.contains(&target.digest),
                "protocol error: HASHKEEP, but {target} was not advertised"
            );
            let whole = match prepared {
                Some(prepared) => prepared.list.map(|(_, table)| table),
                // A list that only feeds the table is decoded straight
                // into it.
                None => exchange.read_listed(&target, HashTable::from_list),
            };
            whole.map(|whole| {
                let _ = table.send(TableUpdate::Table(whole));
                Resolved {
                    target,
                    encoded: None,
                }
            })
        }
        TargetFrame::Delta {
            target,
            base,
            removed,
            added,
        } => {
            let (target, base) = (id(target), id(base));
            assert!(
                advertised.contains(&base.digest),
                "protocol error: HASHDELTA from {base}, which was not advertised"
            );
            let length = removed
                .checked_add(added)
                .expect("protocol error: the lists of HASHDELTA overflow");
            let lists = link.list_bytes(length);
            received.delta += length;
            let (removed, added) = lists.split_at(removed as usize);
            let decode = |list: &[u8], which: &str| {
                hashlist::decode(list).unwrap_or_else(|e| {
                    panic!("protocol error: bad {which} list in HASHDELTA: {e}")
                })
            };
            let (removed, added) = (decode(removed, "removed"), decode(added, "added"));
            let base_values = match prepared {
                Some(prepared) => prepared.list.map(|(values, _)| values),
                None => exchange.read_listed(&base, hashlist::decode),
            };
            base_values.and_then(|base_values| {
                let values = hashlist::apply(&base_values, &removed, &added);
                let encoded = hashlist::encode(&values);
                (strong.digest(&encoded) == target.digest).then(|| {
                    let _ = table.send(TableUpdate::Table(HashTable::build(&values)));
                    Resolved {
                        target,
                        encoded: Some(encoded),
                    }
                })
            })
        }
    };
    if resolved.is_some() {
        return resolved;
    }
    // The receiver answers with the full list, and the sender waits for it.
    link.send(frame::HASHFULL, Vec::new());
    let full = link.next();
    let TargetFrame::Full {
        target: named,
        length,
    } = TargetFrame::decode(full.kind, &full.payload)
    else {
        panic!("protocol error: the answer to HASHFULL is not the full list");
    };
    let target = id(match message {
        TargetFrame::Keep { target } | TargetFrame::Delta { target, .. } => target,
        TargetFrame::Full { .. } | TargetFrame::None => unreachable!("no HASHFULL after them"),
    });
    assert!(
        named == target.digest,
        "protocol error: the full list after HASHFULL is not {target}"
    );
    Some(receive_full(link, target, length, table, received))
}

/// Receive the full list `target`, of `length` bytes, from HASHDATA
/// frames on `link`, and give its values to `table` while the list arrives
/// (DESIGN.md §6.3): first a table sized for them, once the bytes so far
/// can hold as many values as the list announces, so that a corrupt header
/// allocates no more than the bytes that arrive allow, then the values of
/// each HASHDATA frame. The values are not verified yet, but they are only
/// hints: every block that they find is verified by its strong hash. Then
/// require the canonical form and the digest of `target`: a bad full list
/// is a protocol error (DESIGN.md §5.6). `received` counts it.
fn receive_full(
    link: &Link,
    target: ListId,
    length: u64,
    table: &Sender<TableUpdate>,
    received: &mut ListBytes,
) -> Resolved {
    let bad = |e: &str| -> ! { panic!("protocol error: bad full hash list: {e}") };
    let mut encoded = Vec::new();
    let mut decoder: Option<ListDecoder> = None;
    let mut sized = false;
    while (encoded.len() as u64) < length {
        encoded.extend_from_slice(&link.data(length - encoded.len() as u64));
        if decoder.is_none() {
            decoder = ListDecoder::start(&encoded);
        }
        let Some(decoder) = &mut decoder else {
            continue;
        };
        if !sized {
            if !decoder.fits(&encoded) {
                continue;
            }
            let _ = table.send(TableUpdate::Table(HashTable::for_values(decoder.len())));
            sized = true;
        }
        let mut values = Vec::new();
        decoder
            .decode(&encoded, |value| values.push(value))
            .unwrap_or_else(|e| bad(&e));
        if !values.is_empty() {
            let _ = table.send(TableUpdate::Values(values));
        }
    }
    received.full += length;
    let decoder = decoder.unwrap_or_else(|| bad("truncated header"));
    if !sized {
        bad("more values than encoded bits");
    }
    decoder.finish(&encoded).unwrap_or_else(|e| bad(&e));
    assert!(
        target.algorithm.digest(&encoded) == target.digest,
        "protocol error: the full hash list does not match {target}"
    );
    Resolved {
        target,
        encoded: Some(encoded),
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::unbounded;

    use super::*;
    use crate::cache::tests::with_home;
    use crate::hashlist::tests::list;
    use crate::names::hex;

    /// What the receiver offers for a target with `values`, when the sender
    /// advertised `advertised` and the receiver's directory for the sender
    /// holds `cached`. The base comes from the receiver's caches, as
    /// `ReceiverExchange::finish` finds it.
    fn offer(values: &[u64], advertised: &[&ListId], cached: &[(&ListId, &[u8])]) -> Offer {
        let (peer, index) = ([5; 16], [6; 16]);
        let mut earlier = Exchange::receiver(&peer, &index);
        for &(id, encoded) in cached {
            earlier.publish(id, encoded);
        }
        let mut exchange = Exchange::receiver(&peer, &index);
        let (id, encoded) = list(values);
        let target = Target {
            id,
            values: values.to_vec(),
            encoded,
        };
        let advertised: Vec<Strong> = advertised.iter().map(|id| id.digest).collect();
        let base = exchange.find_base(&advertised, StrongHash::Blake3);
        Offer::choose(&advertised, target, base)
    }

    /// The full list of `values`, as HASHLIST hands it over.
    fn full(values: &[u64]) -> TargetList {
        let (id, encoded) = list(values);
        TargetList::Full {
            target: id.digest,
            list: encoded,
        }
    }

    /// The receiver sends no list bytes for a target that the sender has, a
    /// delta from a list both sides have when that is smaller than the full
    /// list, and the full list otherwise. The sender can ask for the full
    /// list only after HASHKEEP or HASHDELTA (DESIGN.md §5.6).
    #[test]
    fn the_receiver_offers_the_smallest_form_of_the_target() {
        with_home(|_| {
            let wide: Vec<u64> = (0..100).map(|i| i << 56).collect();
            let (target, _) = list(&wide);
            let keep = offer(&wide, &[&target], &[]);
            let message = TargetList::Keep {
                target: target.digest,
            };
            assert_eq!((keep.message, keep.fallback), (message, Some(full(&wide))));

            // If the receiver has none of the advertised lists, it sends
            // the full list.
            let (base, base_bytes) = list(&wide);
            let mut changed = wide.clone();
            changed[50] += 1;
            let (changed_id, _) = list(&changed);
            for advertised in [&[&base][..], &[]] {
                let offered = offer(&changed, advertised, &[]);
                assert_eq!(
                    (&offered.message, &offered.fallback),
                    (&full(&changed), &None)
                );
                assert!(offered.base.is_none());
            }

            // With one value changed in a list of 100, the delta is smaller.
            let delta = offer(&changed, &[&base], &[(&base, &base_bytes)]);
            let expected = TargetList::Delta {
                target: changed_id.digest,
                base: base.digest,
                removed: hashlist::encode(&[wide[50]]),
                added: hashlist::encode(&[changed[50]]),
            };
            assert_eq!(delta.message, expected);
            assert_eq!(delta.base.unwrap().digest, base.digest);
            assert_eq!(delta.fallback, Some(full(&changed)));

            // When every value of a short list changes, the delta takes
            // more bytes than the full list.
            let (small, small_bytes) = list(&[1]);
            let offered = offer(&[2], &[&small], &[(&small, &small_bytes)]);
            assert_eq!(offered.message, full(&[2]));
        });
    }

    /// A link whose other side has sent `frames`, and the frames that this
    /// side sends on it.
    fn link(frames: &[ExchangeFrame]) -> (Link, Receiver<ExchangeFrame>, Sender<ExchangeFrame>) {
        let (input_tx, input) = unbounded();
        for frame in frames {
            input_tx.send(frame.clone()).unwrap();
        }
        let (output, sent) = unbounded();
        (Link::new(input, output), sent, input_tx)
    }

    /// The frames of `message`.
    fn frames(message: &TargetList) -> Vec<ExchangeFrame> {
        let mut frames = Vec::new();
        message.send(|frame| frames.push(frame));
        frames
    }

    fn empty(kind: u8) -> ExchangeFrame {
        ExchangeFrame {
            kind,
            payload: Vec::new(),
        }
    }

    /// Run `give_target` with `message` and `fallback` against `replies`,
    /// the sender's frames. Returns what the receiver sent and the list
    /// bytes that it counted.
    fn give(
        message: TargetList,
        fallback: Option<TargetList>,
        replies: &[ExchangeFrame],
    ) -> (Vec<ExchangeFrame>, ListBytes) {
        let (link, sent, _input) = link(replies);
        let bytes = give_target(&link, message, fallback);
        (sent.try_iter().collect(), bytes)
    }

    /// After HASHKEEP or HASHDELTA, the sender can ask for the full list once
    /// with HASHFULL: the receiver sends it and waits for CACHE_RESULT
    /// again. After a full list, only CACHE_RESULT can follow (DESIGN.md
    /// §5.6).
    #[test]
    fn hashfull_gets_the_full_list_once() {
        let values = [1, 2, 3];
        let (id, list) = list(&values);
        let keep = TargetList::Keep { target: id.digest };
        let result = empty(frame::CACHE_RESULT);
        let replies = [empty(frame::HASHFULL), result.clone()];
        let (sent, bytes) = give(keep.clone(), Some(full(&values)), &replies);
        assert_eq!(sent, [frames(&keep), frames(&full(&values))].concat());
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));

        let (sent, bytes) = give(full(&values), None, &[result]);
        assert_eq!(sent, frames(&full(&values)));
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));
    }

    /// UPDATED is empty (DESIGN.md §6.2), as are CACHE_RESULT, HASHFULL and
    /// HASHNONE: a payload is a protocol error.
    #[test]
    #[should_panic(expected = "protocol error: UPDATED with a payload")]
    fn updated_with_a_payload_is_a_protocol_error() {
        let updated = ExchangeFrame {
            kind: frame::UPDATED,
            payload: vec![0],
        };
        let (link, _sent, _input) = link(&[updated]);
        link.expect_empty(frame::UPDATED, "UPDATED");
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn hashfull_after_the_full_list_is_a_protocol_error() {
        give(full(&[1]), None, &[empty(frame::HASHFULL)]);
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn a_second_hashfull_is_a_protocol_error() {
        let (id, _) = list(&[1]);
        let hashfull = empty(frame::HASHFULL);
        give(
            TargetList::Keep { target: id.digest },
            Some(full(&[1])),
            &[hashfull.clone(), hashfull],
        );
    }

    /// The receiver's account and index in the sender's cache tests.
    const PEER: [u8; 16] = [1; 16];
    const INDEX: [u8; 16] = [2; 16];

    /// Publish `encoded` as the list `id` in the sender's cache for `PEER`
    /// and `INDEX`, as an earlier run would have.
    fn cache(id: &ListId, encoded: &[u8]) {
        Exchange::sender(&PEER, &INDEX).publish(id, encoded);
    }

    /// The values of the table that the scanner got from `updates`, once
    /// the exchange ended: the values of `asked` that the table holds, and
    /// whether a table came at all.
    fn table_values(updates: &Receiver<TableUpdate>, asked: &[u64]) -> Option<Vec<u64>> {
        let mut table: Option<HashTable> = None;
        for update in updates.try_iter() {
            match update {
                TableUpdate::Table(whole) => {
                    assert!(table.is_none(), "a second table");
                    table = Some(whole);
                }
                TableUpdate::Values(values) => {
                    let table = table.as_mut().expect("values before the table");
                    for value in values {
                        table.insert(value);
                    }
                }
            }
        }
        table.map(|table| {
            asked
                .iter()
                .copied()
                .filter(|&value| table.contains_value(value))
                .collect()
        })
    }

    /// Run `resolve_target` on `lists`, the target lists that the receiver
    /// sends, in a new exchange for `PEER` and `INDEX` that advertised
    /// `advertised`, or every list in its listing for `None`. With
    /// `prepare`, the first advertised list is prepared first, as `send`
    /// prepares it. Returns the resolved target, the frames that it sent,
    /// how many frames of `lists` it left unread, and what it gave the
    /// scanner.
    fn resolve(
        advertised: Option<&[Strong]>,
        prepare: bool,
        lists: &[TargetList],
    ) -> (Resolved, Vec<ExchangeFrame>, usize, Receiver<TableUpdate>) {
        let exchange = Exchange::sender(&PEER, &INDEX);
        let advertised = match advertised {
            Some(advertised) => advertised.to_vec(),
            None => exchange.listed_ids(StrongHash::Blake3),
        };
        let prepared = advertised.first().filter(|_| prepare).map(|&digest| {
            let id = ListId {
                algorithm: StrongHash::Blake3,
                digest,
            };
            Prepared::read(&exchange, id)
        });
        let sent_frames: Vec<ExchangeFrame> = lists.iter().flat_map(frames).collect();
        let (link, sent, _input) = link(&sent_frames);
        let (table, updates) = unbounded();
        let mut received = ListBytes::default();
        let resolved = resolve_target(
            &link,
            &exchange,
            &advertised,
            StrongHash::Blake3,
            prepared,
            &mut received,
            &table,
        );
        let left = link.input.len();
        (resolved.unwrap(), sent.try_iter().collect(), left, updates)
    }

    /// HASHLIST carries the target itself, HASHKEEP takes it from the
    /// listing, and HASHDELTA turns the base from the listing into it, all
    /// without HASHFULL, and whichever list was prepared. The scanner gets
    /// a table of the target. The sender keeps the list that HASHKEEP
    /// names, and publishes the others. Nothing after the target list is
    /// read: here an unrequested full list follows, and it stays unread
    /// (DESIGN.md §5.6).
    #[test]
    fn each_target_list_gives_the_target() {
        with_home(|_| {
            let (base, base_bytes) = list(&[1, 2, 3]);
            let (target, encoded) = list(&[2, 3, 4]);
            cache(&base, &base_bytes);
            cache(&target, &encoded);
            let lists = [
                ("HASHLIST", full(&[2, 3, 4]), Some(encoded.clone())),
                (
                    "HASHKEEP",
                    TargetList::Keep {
                        target: target.digest,
                    },
                    None,
                ),
                (
                    "HASHDELTA",
                    TargetList::Delta {
                        target: target.digest,
                        base: base.digest,
                        removed: hashlist::encode(&[1]),
                        added: hashlist::encode(&[4]),
                    },
                    Some(encoded.clone()),
                ),
            ];
            for (name, message, published) in lists {
                for advertised in [[target.digest, base.digest], [base.digest, target.digest]] {
                    for prepare in [false, true] {
                        let unasked = full(&[2, 3, 4]);
                        let unread = frames(&unasked).len();
                        let (resolved, sent, left, updates) =
                            resolve(Some(&advertised), prepare, &[message.clone(), unasked]);
                        let case = format!("{name}, prepared {prepare}");
                        assert_eq!(resolved.target, target, "{case}");
                        assert_eq!(resolved.encoded, published, "{case}");
                        let asked = [1, 2, 3, 4, 5];
                        assert_eq!(
                            table_values(&updates, &asked),
                            Some(vec![2, 3, 4]),
                            "{case}"
                        );
                        assert_eq!((sent, left), (Vec::new(), unread), "{case}");
                    }
                }
            }
        });
    }

    /// The values of a full list reach the scanner while the list arrives:
    /// the table, sized once enough bytes are in, then the values that each
    /// HASHDATA frame completes, before the next frame arrives, in the order
    /// of the list (DESIGN.md §6.3).
    #[test]
    fn a_full_list_reaches_the_scanner_while_it_arrives() {
        let mut values: Vec<u64> = (0..20_000u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect();
        values.sort_unstable();
        let mut sent = frames(&full(&values)).into_iter();
        let first = sent.next().unwrap();
        let pieces: Vec<ExchangeFrame> = sent.collect();
        assert!(pieces.len() > 4, "the list fills too few frames");
        let TargetFrame::Full { target, length } = TargetFrame::decode(first.kind, &first.payload)
        else {
            panic!("not a full list");
        };
        let target = ListId {
            algorithm: StrongHash::Blake3,
            digest: target,
        };
        let (link, _sent, input) = link(&[]);
        let (table, updates) = unbounded();
        let thread = std::thread::spawn(move || {
            let mut received = ListBytes::default();
            receive_full(&link, target, length, &table, &mut received);
            received
        });
        let (mut got, mut sized) = (Vec::new(), false);
        for piece in pieces {
            input.send(piece).unwrap();
            // Each frame of this list holds thousands of values. No
            // deadline: a sender that waits for more frames hangs the test.
            loop {
                match updates.recv().unwrap() {
                    TableUpdate::Table(_) => {
                        assert!(!sized, "a second table");
                        sized = true;
                    }
                    TableUpdate::Values(batch) => {
                        assert!(sized, "values before the table");
                        got.extend(batch);
                        break;
                    }
                }
            }
        }
        let received = thread.join().unwrap();
        assert_eq!(got, values);
        assert_eq!(received.full, length);
        assert!(
            updates.try_recv().is_err(),
            "more after the end of the list"
        );
    }

    /// HASHNONE in place of INDEX, or in place of the target, ends the
    /// sender's exchange without a table, so a scanner that waits for its
    /// table keeps waiting for the stop of its stream (DESIGN.md §5.6).
    #[test]
    fn hashnone_ends_the_exchange_of_the_sender_without_a_table() {
        with_home(|_| {
            let ids = IndexIds {
                account_id: PEER,
                index_id: INDEX,
            };
            let index = ExchangeFrame {
                kind: frame::INDEX,
                payload: ids.encode(),
            };
            for frames in [
                vec![empty(frame::HASHNONE)],
                vec![index, empty(frame::HASHNONE)],
            ] {
                let after_index = frames.len() > 1;
                let (link, sent, _input) = link(&frames);
                let (table, updates) = unbounded();
                let received = send(&link, StrongHash::Blake3, true, 1 << 30, table);
                assert_eq!(received, ListBytes::default());
                let sent: Vec<u8> = sent.try_iter().map(|frame| frame.kind).collect();
                let expected: &[u8] = if after_index {
                    &[frame::ADVERTISE]
                } else {
                    &[]
                };
                assert_eq!(sent, expected);
                assert!(updates.try_recv().is_err(), "a table without a target");
                assert!(updates.is_empty());
            }
        });
    }

    /// A delta that does not produce the target, and an invalid listed
    /// list, each make the sender ask for the full list with a single
    /// HASHFULL and use that instead, whether the list was prepared or not.
    /// The invalid list is removed (DESIGN.md §5.6). The scanner gets only
    /// the table of the full list.
    #[test]
    fn an_unusable_cached_list_asks_once_for_the_full_list() {
        with_home(|home| {
            let (base, base_bytes) = list(&[1, 2, 3]);
            let (target, encoded) = list(&[2, 3, 4]);
            let path = home.join(format!(
                "cache/hashsync/remote-hashlists/{}-{}/{target}.cache",
                hex(&PEER),
                hex(&INDEX)
            ));
            for prepare in [false, true] {
                cache(&base, &base_bytes);
                let wrong = TargetList::Delta {
                    target: target.digest,
                    base: base.digest,
                    removed: hashlist::encode(&[1]),
                    added: hashlist::encode(&[5]),
                };
                let advertised = [base.digest];
                let (resolved, sent, left, updates) =
                    resolve(Some(&advertised), prepare, &[wrong, full(&[2, 3, 4])]);
                assert_eq!(resolved.encoded, Some(encoded.clone()));
                assert_eq!((sent, left), (vec![empty(frame::HASHFULL)], 0));
                let asked = [1, 2, 3, 4, 5];
                assert_eq!(table_values(&updates, &asked), Some(vec![2, 3, 4]));

                cache(&target, &encoded);
                std::fs::write(&path, b"damaged").unwrap();
                let keep = TargetList::Keep {
                    target: target.digest,
                };
                let advertised = [target.digest];
                let (resolved, sent, left, updates) =
                    resolve(Some(&advertised), prepare, &[keep, full(&[2, 3, 4])]);
                assert_eq!(resolved.encoded, Some(encoded.clone()));
                assert_eq!(table_values(&updates, &asked), Some(vec![2, 3, 4]));
                assert_eq!((sent, left), (vec![empty(frame::HASHFULL)], 0));
                assert!(!path.exists(), "the invalid list must be removed");
            }
        });
    }

    /// A full list must match the digest of the target that it names
    /// (DESIGN.md §5.6).
    #[test]
    #[should_panic(expected = "protocol error: the full hash list does not match")]
    fn a_bad_full_list_is_a_protocol_error() {
        with_home(|_| {
            let (target, _) = list(&[1]);
            let other = TargetList::Full {
                target: target.digest,
                list: hashlist::encode(&[2]),
            };
            resolve(Some(&[]), false, &[other]);
        });
    }

    /// A full list must be canonical too.
    #[test]
    #[should_panic(expected = "protocol error: bad full hash list: excess bytes")]
    fn a_full_list_that_is_not_canonical_is_a_protocol_error() {
        with_home(|_| {
            let (target, _) = list(&[1]);
            let mut bytes = hashlist::encode(&[1]);
            bytes.push(0);
            let other = TargetList::Full {
                target: target.digest,
                list: bytes,
            };
            resolve(Some(&[]), false, &[other]);
        });
    }

    /// So must the full list that HASHFULL asks for, here after a HASHKEEP
    /// of a list that the listing does not have (DESIGN.md §5.6).
    #[test]
    #[should_panic(expected = "protocol error: the full hash list does not match")]
    fn a_bad_full_list_after_hashfull_is_a_protocol_error() {
        with_home(|_| {
            let (target, _) = list(&[1]);
            let keep = TargetList::Keep {
                target: target.digest,
            };
            let other = TargetList::Full {
                target: target.digest,
                list: hashlist::encode(&[2]),
            };
            resolve(Some(&[target.digest]), false, &[keep, other]);
        });
    }

    /// And the full list after HASHFULL must name the target that HASHKEEP
    /// or HASHDELTA named: the exchange has one target (DESIGN.md §6.2).
    #[test]
    #[should_panic(expected = "protocol error: the full list after HASHFULL is not")]
    fn a_full_list_of_another_target_after_hashfull_is_a_protocol_error() {
        with_home(|_| {
            let (target, _) = list(&[1]);
            let keep = TargetList::Keep {
                target: target.digest,
            };
            resolve(Some(&[target.digest]), false, &[keep, full(&[2])]);
        });
    }

    /// HASHKEEP must name a list that the sender advertised. With caching
    /// disabled, the sender advertises none, even though its listing may
    /// hold the target (DESIGN.md §5.6).
    #[test]
    #[should_panic(expected = "protocol error: HASHKEEP, but")]
    fn hashkeep_of_a_list_that_was_not_advertised_is_a_protocol_error() {
        with_home(|_| {
            let (target, encoded) = list(&[3]);
            cache(&target, &encoded);
            let keep = TargetList::Keep {
                target: target.digest,
            };
            resolve(Some(&[]), false, &[keep]);
        });
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHDELTA from")]
    fn hashdelta_from_a_list_that_was_not_advertised_is_a_protocol_error() {
        with_home(|_| {
            let (base, base_bytes) = list(&[3]);
            let (target, _) = list(&[4]);
            cache(&base, &base_bytes);
            let delta = TargetList::Delta {
                target: target.digest,
                base: base.digest,
                removed: hashlist::encode(&[3]),
                added: hashlist::encode(&[4]),
            };
            resolve(Some(&[]), false, &[delta]);
        });
    }

    /// The lists of a delta must be canonical, just like a full list
    /// (DESIGN.md §6.2): here the added list announces a value that never
    /// follows.
    #[test]
    #[should_panic(expected = "protocol error: bad added list in HASHDELTA")]
    fn a_delta_list_that_is_not_canonical_is_a_protocol_error() {
        with_home(|_| {
            let (base, base_bytes) = list(&[3]);
            let (target, _) = list(&[3, 4]);
            cache(&base, &base_bytes);
            let delta = TargetList::Delta {
                target: target.digest,
                base: base.digest,
                removed: hashlist::encode(&[]),
                added: 1u64.to_le_bytes().to_vec(),
            };
            resolve(None, false, &[delta]);
        });
    }

    /// The HASHDATA frames of a list carry exactly its announced bytes: one
    /// that goes past them is a protocol error (DESIGN.md §6.2).
    #[test]
    #[should_panic(expected = "protocol error: HASHDATA with 9 bytes where at most 8 remain")]
    fn hashdata_past_the_announced_bytes_is_a_protocol_error() {
        let (target, _) = list(&[1]);
        let hashlist = ExchangeFrame {
            kind: frame::HASHLIST,
            payload: [&target.digest[..], &8u64.to_le_bytes()].concat(),
        };
        let data = ExchangeFrame {
            kind: frame::HASHDATA,
            payload: vec![0; 9],
        };
        let (link, _sent, _input) = link(&[hashlist, data]);
        let (table, _updates) = unbounded();
        let first = link.next();
        let TargetFrame::Full { length, .. } = TargetFrame::decode(first.kind, &first.payload)
        else {
            unreachable!()
        };
        receive_full(&link, target, length, &table, &mut ListBytes::default());
    }
}
