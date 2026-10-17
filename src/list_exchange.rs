//! The list exchange of one deduplicated stream (DESIGN.md §5.6): its
//! receiver hands its sender the hash list of its index, the target, in the
//! cheapest form that both sides' list caches allow, and the sender
//! verifies it, builds its hash table, and acknowledges it.
//!
//! Both sides read and write session frames of the data session, which are
//! never compressed, so the same code serves either direction of it, and
//! either host can play either side. The exchange begins with the HELLOs:
//! the receiver's sets the block size and the hashes of the stream, and the
//! sender's advertises the lists of its cache. The receiver sends its HELLO
//! before it exports its target, so the two sides work at the same time:
//! while the receiver exports, the sender reads the first list that it
//! advertised, and builds its hash table from it. The receiver then sends
//! HASHKEEP, HASHDELTA, or HASHLIST, each of which names the target; the
//! sender can ask once for the full list with HASHFULL, and replies
//! CACHE_RESULT. Each side has two steps: `start` sends its HELLO, so that
//! no caller can send it out of order, and `finish` does the rest. Between
//! them, a host can do other work: the client of a pull sends its HELLO
//! right after REQUEST, before it reads anything from the server, and the
//! server of a pull accepts the control session while the client exports
//! its target. The cache storage itself is `cache::Exchange`, which uses
//! `Rc`: both sides run their part on the main thread of their host, and
//! only the receiver's export runs on a thread of its own.
//!
//! A failure of the data session is an `io::Error` for the caller, which
//! knows which host is on the other side, and ends the run with
//! `protocol::die_data`. EOF before a frame that the exchange waits for is
//! `UnexpectedEof`.

use std::io::{ErrorKind, Read, Write};
use std::thread::JoinHandle;

use crate::cache::{self, Base, Exchange};
use crate::config::Config;
use crate::die::die;
use crate::hash::{Strong, StrongHash, weak};
use crate::hashlist::{self, ListId};
use crate::hashtable::HashTable;
use crate::index::Reader;
use crate::protocol::{FrameReader, FrameWriter, ReceiverHello, SenderHello, TargetList, frame};

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

/// The receiver's part of the exchange (DESIGN.md §5.6, steps 1, 3 and 6).
pub struct ReceiverExchange {
    /// The ID of the receiver's index, which partitions the list caches.
    index_id: [u8; 16],
    /// The strong hash of the stream, whose digests name its lists.
    strong: StrongHash,
    /// The export of the target, which runs on a thread of its own from
    /// the HELLO until `finish` needs the target.
    export: JoinHandle<Target>,
}

impl ReceiverExchange {
    /// Send the receiver's HELLO on `output`, with the block size and the
    /// hashes of `config`, the host's configuration, which are those of the
    /// stream, and the IDs that partition the list caches: this side's
    /// account ID, and `index_id`, the ID of the index that `reader` reads
    /// (DESIGN.md §6.2). The sender replies to it, so it must not stay in a
    /// buffer. Then start the export of the target from `reader`. The HELLO
    /// names no target, so the export runs while the sender lists its cache
    /// and prepares its cached list, and while this side reads the sender's
    /// HELLO and the base of a delta.
    pub fn start<W: Write>(
        output: &mut FrameWriter<W>,
        config: &Config,
        index_id: [u8; 16],
        reader: Reader,
    ) -> std::io::Result<ReceiverExchange> {
        let hello = ReceiverHello {
            block_size: config.block_size,
            weak_hash_id: weak::ID.to_string(),
            strong_hash: config.strong_hash,
            account_id: cache::account_id(),
            index_id,
        };
        output.send(frame::HELLO, &hello.encode())?;
        output.flush()?;
        let strong = config.strong_hash;
        let export = std::thread::Builder::new()
            .name("list-export".into())
            .spawn(move || Target::export(&reader, strong))
            .expect("cannot start the export of the hash list");
        Ok(ReceiverExchange {
            index_id,
            strong,
            export,
        })
    }

    /// Read the sender's HELLO from `input`, and read the base of a delta,
    /// the first of the lists that the sender advertised that this side
    /// has, while the export of the target still runs. Then wait for the
    /// target. With sender caching (`sender_cache`), durably publish the
    /// target before any list data, so that this side has every list that
    /// the sender can advertise later: keep the copy of it that this side's
    /// listing holds, or link a copy from another of this side's
    /// peer/index directories, or else write it. Then send the target in
    /// the smallest form that the sender's lists allow, answer one HASHFULL
    /// with the full list, and wait for CACHE_RESULT. Then end the exchange
    /// of this side, with the list cache's `max_age`. Returns the bytes of
    /// the lists sent.
    pub fn finish<R: Read, W: Write>(
        self,
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        sender_cache: bool,
        max_age: u64,
    ) -> std::io::Result<ListBytes> {
        let sender = SenderHello::decode(input.expect(frame::HELLO, "HELLO")?);
        // This side lists its directory for the sender and the index.
        // Without sender caching, the sender keeps no list, so this side
        // keeps none either.
        let mut exchange = Exchange::receiver(&sender.account_id, &self.index_id);
        // The base is read before the target is known, so that the read
        // happens during the export. It is of no use if the sender
        // advertised the target itself, which HASHKEEP then names.
        let base = exchange.find_base(&sender.lists, self.strong);
        let target = self
            .export
            .join()
            .expect("the export of the hash list panicked");
        if sender_cache && !exchange.keep(&target.id) && !exchange.link(&target.id) {
            exchange.publish(&target.id, &target.encoded);
        }
        let Offer {
            message,
            base,
            fallback,
        } = Offer::choose(&sender.lists, target, base);
        let list_bytes = give_target(output, input, message, fallback)?;
        // After CACHE_RESULT the sender holds the target, or no list at all
        // if caching is disabled, and the end of this side's exchange
        // matches that: it renews the target and the base of HASHDELTA,
        // even if the sender asked for the full list instead, removes the
        // other listed files, and expires both cache trees.
        exchange.finish(base, max_age);
        Ok(list_bytes)
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
fn give_target<R: Read, W: Write>(
    output: &mut FrameWriter<W>,
    input: &mut FrameReader<R>,
    message: TargetList,
    mut fallback: Option<TargetList>,
) -> std::io::Result<ListBytes> {
    let mut sent = ListBytes::default();
    send_target(output, &message, &mut sent)?;
    loop {
        let reply = input.next()?.ok_or(ErrorKind::UnexpectedEof)?;
        let empty = reply.payload.is_empty();
        match reply.kind {
            // The stream has one target, so the acknowledgement names none.
            frame::CACHE_RESULT => {
                assert!(empty, "protocol error: CACHE_RESULT with a payload");
                return Ok(sent);
            }
            frame::HASHFULL => {
                assert!(empty, "protocol error: HASHFULL with a payload");
                let Some(full) = fallback.take() else {
                    panic!("protocol error: HASHFULL after the full list");
                };
                send_target(output, &full, &mut sent)?;
            }
            kind => panic!("protocol error: expected CACHE_RESULT, received frame type {kind}"),
        }
    }
}

/// Send the target list `message` and count its lists in `sent`. The
/// sender replies to it, so it must not stay in a buffer.
fn send_target<W: Write>(
    output: &mut FrameWriter<W>,
    message: &TargetList,
    sent: &mut ListBytes,
) -> std::io::Result<()> {
    message.send(output)?;
    output.flush()?;
    sent.count(message);
    Ok(())
}

/// The sender's part of the exchange, from the receiver's HELLO to
/// CACHE_RESULT (DESIGN.md §5.6, steps 2, 4 and 5).
pub struct SenderExchange {
    /// The receiver's HELLO, whose parameters the stream uses.
    pub receiver: ReceiverHello,
    exchange: Exchange,
    /// The lists that this side's HELLO advertised.
    advertised: Vec<Strong>,
    sender_cache: bool,
}

impl SenderExchange {
    /// Read the receiver's HELLO from `input`, and end the run if this
    /// build does not support its hashes (DESIGN.md §6.2). Then `accept`
    /// checks the receiver's parameters against the run, and ends it if they
    /// do not fit, before this side's HELLO: so the receiver does no work,
    /// such as the export of its target, for a run that cannot go on
    /// (DESIGN.md §8). Then list the cache directory for that receiver and
    /// index, which must happen before this side's HELLO: then the end of
    /// the exchange cannot remove the target of a later run, even when this
    /// side outlives its receiver. With sender caching (`sender_cache`),
    /// this side advertises every listed list, and with caching disabled,
    /// none. Then send this side's HELLO on `output`, which the receiver
    /// needs for the list. Returns the exchange, and what `accept` returned.
    pub fn start<R: Read, W: Write, T>(
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        sender_cache: bool,
        accept: impl FnOnce(&ReceiverHello) -> T,
    ) -> std::io::Result<(SenderExchange, T)> {
        let receiver = ReceiverHello::decode(input.expect(frame::HELLO, "HELLO")?);
        if receiver.weak_hash_id != weak::ID {
            die(format!(
                "incompatible receiver: weak hash {} (this sender: {})",
                receiver.weak_hash_id,
                weak::ID
            ));
        }
        let accepted = accept(&receiver);
        let exchange = Exchange::sender(&receiver.account_id, &receiver.index_id);
        let advertised = if sender_cache {
            exchange.listed_ids(receiver.strong_hash)
        } else {
            Vec::new()
        };
        let hello = SenderHello {
            account_id: cache::account_id(),
            lists: advertised.clone(),
        };
        output.send(frame::HELLO, &hello.encode())?;
        output.flush()?;
        let exchange = SenderExchange {
            receiver,
            exchange,
            advertised,
            sender_cache,
        };
        Ok((exchange, accepted))
    }

    /// Prepare the first list that this side advertised while the
    /// receiver exports its target (`Prepared`). Then get the target from
    /// `input`, with a single HASHFULL on `output` if neither the prepared
    /// list nor the cached list that the receiver names gives it, together
    /// with its hash table. With sender caching, durably publish the
    /// target, or keep the listed list that HASHKEEP named, which this side
    /// has just verified. Then end the exchange of this side, with the list
    /// cache's `max_age`, and only then acknowledge the target with
    /// CACHE_RESULT. Returns the table, and the bytes of the lists
    /// received.
    pub fn finish<R: Read, W: Write>(
        self,
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        max_age: u64,
    ) -> std::io::Result<(HashTable, ListBytes)> {
        let SenderExchange {
            receiver,
            mut exchange,
            advertised,
            sender_cache,
        } = self;
        let strong = receiver.strong_hash;
        let prepared = advertised.first().map(|&digest| {
            let id = ListId {
                algorithm: strong,
                digest,
            };
            Prepared::read(&exchange, id)
        });
        let mut received = ListBytes::default();
        let Resolved {
            target,
            table,
            encoded,
        } = resolve_target(
            input,
            output,
            &exchange,
            &advertised,
            strong,
            prepared,
            &mut received,
        )?;
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
        output.send(frame::CACHE_RESULT, b"")?;
        output.flush()?;
        Ok((table, received))
    }
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

/// The target of an exchange, as the sender resolved it: its ID, its hash
/// table, and its bytes, which the sender publishes, or `None` for the
/// listed list that HASHKEEP named, which the sender keeps as it is.
struct Resolved {
    target: ListId,
    table: HashTable,
    encoded: Option<Vec<u8>>,
}

/// Read the receiver's target list from `input`, and resolve the target
/// that it names, of the stream's strong hash `strong` (DESIGN.md §5.6). A
/// full list must be the target itself (`full_list`). HASHKEEP and
/// HASHDELTA name one of the `advertised` lists: `prepared`, when it is
/// that list, and otherwise the list that this function reads through the
/// listing of `exchange`. For HASHKEEP it is the target, and for HASHDELTA
/// it is the base that the delta turns into the target. If that list is
/// invalid, or the delta does not produce the target, this sends HASHFULL
/// on `output` and uses the full list that the receiver sends back, which
/// must name the same target; the result of a bad delta is never used.
/// Nothing after the target list, or after that full list, is read here.
/// `received` counts the lists that arrive.
fn resolve_target<R: Read, W: Write>(
    input: &mut FrameReader<R>,
    output: &mut FrameWriter<W>,
    exchange: &Exchange,
    advertised: &[Strong],
    strong: StrongHash,
    prepared: Option<Prepared>,
    received: &mut ListBytes,
) -> std::io::Result<Resolved> {
    let message = TargetList::receive(input)?;
    received.count(&message);
    let target = ListId {
        algorithm: strong,
        digest: message.target(),
    };
    // The prepared list serves only if the message names it. Any other
    // goes now, before the target takes memory of its own: at 21 million
    // values, a prepared list takes 415 MB.
    let named = match &message {
        TargetList::Full { .. } => None,
        TargetList::Keep { target } => Some(*target),
        TargetList::Delta { base, .. } => Some(*base),
    };
    let prepared = prepared.filter(|prepared| Some(prepared.id.digest) == named);
    // A prepared list that turned out to be invalid is not read again: it
    // gives `None`, as the read would.
    let resolved = match message {
        TargetList::Full { list, .. } => return Ok(full_list(list, target)),
        TargetList::Keep { .. } => {
            assert!(
                advertised.contains(&target.digest),
                "protocol error: HASHKEEP, but {target} was not advertised"
            );
            let table = match prepared {
                Some(prepared) => prepared.list.map(|(_, table)| table),
                // A list that only feeds the table is decoded straight
                // into it.
                None => exchange.read_listed(&target, HashTable::from_list),
            };
            table.map(|table| Resolved {
                target,
                table,
                encoded: None,
            })
        }
        TargetList::Delta {
            base,
            removed,
            added,
            ..
        } => {
            let base = ListId {
                algorithm: strong,
                digest: base,
            };
            assert!(
                advertised.contains(&base.digest),
                "protocol error: HASHDELTA from {base}, which was not advertised"
            );
            let decode = |list: &[u8], which: &str| {
                hashlist::decode(list).unwrap_or_else(|e| {
                    panic!("protocol error: bad {which} list in HASHDELTA: {e}")
                })
            };
            let (removed, added) = (decode(&removed, "removed"), decode(&added, "added"));
            let base_values = match prepared {
                Some(prepared) => prepared.list.map(|(values, _)| values),
                None => exchange.read_listed(&base, hashlist::decode),
            };
            base_values.and_then(|base_values| {
                let values = hashlist::apply(&base_values, &removed, &added);
                let encoded = hashlist::encode(&values);
                (strong.digest(&encoded) == target.digest).then(|| Resolved {
                    target,
                    table: HashTable::build(&values),
                    encoded: Some(encoded),
                })
            })
        }
    };
    if let Some(resolved) = resolved {
        return Ok(resolved);
    }
    // The receiver answers with the full list, and the sender waits for it,
    // so the request must not stay in a buffer.
    output.send(frame::HASHFULL, b"")?;
    output.flush()?;
    let full = TargetList::receive(input)?;
    received.count(&full);
    let TargetList::Full {
        target: named,
        list,
    } = full
    else {
        panic!("protocol error: the answer to HASHFULL is not the full list");
    };
    assert!(
        named == target.digest,
        "protocol error: the full list after HASHFULL is not {target}"
    );
    Ok(full_list(list, target))
}

/// The full list `encoded` resolved as the target, which it must be. A
/// list that does not match the digest of `target`, or is not canonical,
/// is a bad full list and a protocol error (DESIGN.md §5.6). Nothing but
/// the table needs its values, so they go straight into it.
fn full_list(encoded: Vec<u8>, target: ListId) -> Resolved {
    assert!(
        target.algorithm.digest(&encoded) == target.digest,
        "protocol error: the full hash list does not match {target}"
    );
    let table = HashTable::from_list(&encoded)
        .unwrap_or_else(|e| panic!("protocol error: bad full hash list: {e}"));
    Resolved {
        target,
        table,
        encoded: Some(encoded),
    }
}

#[cfg(test)]
mod tests {
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

    /// Run `give_target` with `message` and `fallback` against `replies`,
    /// the sender's frames. Returns what the receiver sent and the list
    /// bytes that it counted.
    fn give(
        message: TargetList,
        fallback: Option<TargetList>,
        replies: &[u8],
    ) -> (Vec<u8>, ListBytes) {
        let mut output = FrameWriter::new(Vec::new());
        let mut input = FrameReader::new(replies);
        let sent = give_target(&mut output, &mut input, message, fallback).unwrap();
        (output.into_inner(), sent)
    }

    /// One frame.
    fn frame_bytes(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = FrameWriter::new(Vec::new());
        out.send(kind, payload).unwrap();
        out.into_inner()
    }

    /// The frame of `message` followed by its lists.
    fn message_bytes(message: &TargetList) -> Vec<u8> {
        let mut out = FrameWriter::new(Vec::new());
        message.send(&mut out).unwrap();
        out.into_inner()
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
        let result = frame_bytes(frame::CACHE_RESULT, b"");
        let replies = [frame_bytes(frame::HASHFULL, b""), result.clone()].concat();
        let (sent, bytes) = give(keep, Some(full(&values)), &replies);
        let keep_then_full = [
            message_bytes(&TargetList::Keep { target: id.digest }),
            message_bytes(&full(&values)),
        ];
        assert_eq!(sent, keep_then_full.concat());
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));

        let (sent, bytes) = give(full(&values), None, &result);
        assert_eq!(sent, message_bytes(&full(&values)));
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn hashfull_after_the_full_list_is_a_protocol_error() {
        give(full(&[1]), None, &frame_bytes(frame::HASHFULL, b""));
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn a_second_hashfull_is_a_protocol_error() {
        let (id, _) = list(&[1]);
        let hashfull = frame_bytes(frame::HASHFULL, b"");
        give(
            TargetList::Keep { target: id.digest },
            Some(full(&[1])),
            &[hashfull.clone(), hashfull].concat(),
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

    /// Run `resolve_target` on `lists`, the target lists that the receiver
    /// sends, in a new exchange for `PEER` and `INDEX` that advertised
    /// `advertised`, or every list in its listing for `None`. With
    /// `prepare`, the first advertised list is prepared first, as
    /// `SenderExchange::finish` prepares it. Returns the resolved target,
    /// the bytes of the frames that it sent, and how many bytes of `lists`
    /// it left unread.
    fn resolve(
        advertised: Option<&[Strong]>,
        prepare: bool,
        lists: &[TargetList],
    ) -> (Resolved, Vec<u8>, usize) {
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
        let mut session = FrameWriter::new(Vec::new());
        for list in lists {
            list.send(&mut session).unwrap();
        }
        let session = session.into_inner();
        let mut input = FrameReader::new(&session[..]);
        let mut output = FrameWriter::new(Vec::new());
        let mut received = ListBytes::default();
        let resolved = resolve_target(
            &mut input,
            &mut output,
            &exchange,
            &advertised,
            StrongHash::Blake3,
            prepared,
            &mut received,
        );
        (
            resolved.unwrap(),
            output.into_inner(),
            input.into_inner().len(),
        )
    }

    /// Whether `table` holds exactly the values `holds` of the values
    /// `asked`, without false positives among them.
    fn table_holds(table: &HashTable, asked: &[u64], holds: &[u64]) -> bool {
        asked
            .iter()
            .all(|value| table.contains_value(*value) == holds.contains(value))
    }

    /// HASHLIST carries the target itself, HASHKEEP takes it from the
    /// listing, and HASHDELTA turns the base from the listing into it, all
    /// without HASHFULL, and whichever list was prepared. The sender keeps
    /// the list that HASHKEEP names, and publishes the others. Nothing
    /// after the target list is read: here an unrequested full list
    /// follows, and it stays unread (DESIGN.md §5.6).
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
                        let unread = unasked.size();
                        let (resolved, sent, left) =
                            resolve(Some(&advertised), prepare, &[message.clone(), unasked]);
                        let case = format!("{name}, prepared {prepare}");
                        assert_eq!(resolved.target, target, "{case}");
                        assert_eq!(resolved.encoded, published, "{case}");
                        let asked = [1, 2, 3, 4, 5];
                        assert!(table_holds(&resolved.table, &asked, &[2, 3, 4]), "{case}");
                        assert_eq!((sent, left), (Vec::new(), unread), "{case}");
                    }
                }
            }
        });
    }

    /// A delta that does not produce the target, and an invalid listed
    /// list, each make the sender ask for the full list with a single
    /// HASHFULL and use that instead, whether the list was prepared or not.
    /// The invalid list is removed (DESIGN.md §5.6).
    #[test]
    fn an_unusable_cached_list_asks_once_for_the_full_list() {
        with_home(|home| {
            let (base, base_bytes) = list(&[1, 2, 3]);
            let (target, encoded) = list(&[2, 3, 4]);
            let hashfull = vec![frame::HASHFULL, 0, 0, 0, 0];
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
                let (resolved, sent, left) =
                    resolve(Some(&advertised), prepare, &[wrong, full(&[2, 3, 4])]);
                assert_eq!(resolved.encoded, Some(encoded.clone()));
                assert_eq!((sent, left), (hashfull.clone(), 0));

                cache(&target, &encoded);
                std::fs::write(&path, b"damaged").unwrap();
                let keep = TargetList::Keep {
                    target: target.digest,
                };
                let advertised = [target.digest];
                let (resolved, sent, left) =
                    resolve(Some(&advertised), prepare, &[keep, full(&[2, 3, 4])]);
                assert_eq!(resolved.encoded, Some(encoded.clone()));
                assert!(table_holds(&resolved.table, &[1, 2, 3, 4], &[2, 3, 4]));
                assert_eq!((sent, left), (hashfull.clone(), 0));
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
}
