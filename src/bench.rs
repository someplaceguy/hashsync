//! `hashsync bench` (DESIGN.md §8): how fast this host computes the hashes
//! that deduplication uses, on one thread, and scans output for blocks, on
//! one thread and on the probe threads.
//!
//! An update computes the weak hash of every block that it indexes from the
//! whole block, and the block's strong hash. The sender's scanner rolls the
//! weak hash from one window to the next and looks each window up in the
//! table of the receiver's list. It computes the weak hash of a whole
//! window only when it reinitializes the rolling hash, as after each
//! matched block, and the strong hash only of the windows that it queries.
//! So the first table gives the rate of each hash at a few block sizes, and
//! the second the rate of the scanner's search, which depends on the size
//! of the list more than on the block size. The probe threads search a
//! long stretch of windows in parts at once, so the second table gives the
//! search's rate with them too, which shows what `--probe-threads` gains.

use std::hint::black_box;
use std::io::Write;
use std::time::{Duration, Instant};

use rayon::ThreadPool;

use crate::blocks;
use crate::config::{DEFAULT_BLOCK_SIZE, MIN_RECOMMENDED_BLOCK_SIZE};
use crate::dedup;
use crate::die::die;
use crate::fsops;
use crate::hash::StrongHash;
use crate::hash::weak::{WeakHasher, mix64};
use crate::hashtable::HashTable;
use crate::scanner::{Lookahead, Scanner};
use crate::settings;
use crate::stream::READ_SIZE;
use crate::units::{Bytes, ExactBytes, ExactCount};

/// The block sizes that the command measures without `--block-size`: the
/// smallest one that an update does not warn about, the default, and two
/// larger ones.
const BLOCK_SIZES: [u32; 4] = [
    MIN_RECOMMENDED_BLOCK_SIZE,
    DEFAULT_BLOCK_SIZE,
    64 << 10,
    1 << 20,
];

/// The sizes of the receiver's list that the scanner searches against: one
/// whose table fits in the CPU caches, and one as large as the list of a
/// big index, whose table does not. An index of `/nix/store` at 4 KiB
/// blocks has about 14 million values.
const LIST_VALUES: [u64; 2] = [10_000, 10_000_000];

/// How long each measurement lasts.
const DURATION: Duration = Duration::from_millis(500);

/// Print the table of the hashes, then, after an empty line, the table of
/// the scanner, for blocks of `block_size` bytes, or by default of a few
/// sizes for the hashes and of the default size for the scanner. The
/// scanner's second rate uses `probe_threads`, which `--probe-threads`
/// gives.
pub fn run(block_size: Option<u32>, probe_threads: u32) {
    let sizes = match block_size {
        Some(size) => vec![size],
        None => BLOCK_SIZES.to_vec(),
    };
    print_hashes(&sizes);
    print("");
    print_scans(block_size.unwrap_or(DEFAULT_BLOCK_SIZE), probe_threads);
}

/// A table with one line per block size: the block size, then the rate of
/// the weak hash of whole blocks, of the weak hash rolled from one window
/// to the next, and of each strong hash. A line is printed as soon as it is
/// measured, so that the user sees the progress.
fn print_hashes(sizes: &[u32]) {
    let mut header = format!("{:>12}{:>16}{:>16}", "block size", "weak", "rolling");
    for strong in StrongHash::ALL {
        header += &format!("{:>16}", strong.id());
    }
    print(&header);
    for &size in sizes {
        let label = ExactBytes(size.into()).to_string();
        let batch = blocks::max_batch(size.into()) as usize;
        let size = size as usize;
        // The blocks of one read of an update, which the hashes hash over
        // and over, and then a copy of their first block. With the copy,
        // the window after the last one of the read is the first one
        // again, so that the rolling hash goes on from there in the next
        // pass, without a polynomial of a whole window.
        let mut data = vec![0; batch + size];
        Noise::new().fill(&mut data);
        data.copy_within(..size, batch);
        let blocks = &data[..batch];
        let weak = WeakHasher::new(size);
        let whole = rate(|| {
            for block in blocks.chunks_exact(size) {
                black_box(weak.hash(block));
            }
            batch as u64
        });
        let mut poly = weak.polynomial(&data[..size]);
        let rolling = rate(|| {
            for (&out, &new) in data.iter().zip(&data[size..]) {
                poly = weak.roll(poly, out, new);
                black_box(mix64(poly));
            }
            batch as u64
        });
        let mut line = format!("{label:>12}{whole:>16}{rolling:>16}");
        for strong in StrongHash::ALL {
            let rate = rate(|| {
                for block in blocks.chunks_exact(size) {
                    black_box(strong.digest(block));
                }
                batch as u64
            });
            line += &format!("{rate:>16}");
        }
        print(&line);
    }
}

/// A table with one line per size of the receiver's list: the number of
/// values, then the rate at which the scanner searches output that matches
/// none of them, for blocks of `block_size` bytes, first on one thread,
/// then on `probe_threads` probe threads, or half the cores for 0. With one
/// probe thread, the scanner probes on its own thread, as a sender's does,
/// so the second rate would only repeat the first and is left out. The
/// header gives the thread count of each rate.
fn print_scans(block_size: u32, probe_threads: u32) {
    // The sender's own probe threads, so that the second rate is the rate
    // of a sender with the same `--probe-threads`.
    let pool = dedup::start_probe_pool(probe_threads);
    // The thread count of each rate, and the probe threads of its scanner:
    // without them, the scanner probes on its own thread.
    let mut rates = vec![("1 thread".to_string(), None)];
    if let Some(pool) = pool.as_deref() {
        let threads = format!("{} threads", pool.current_num_threads());
        rates.push((threads, Some(pool)));
    }
    let mut header = format!("{:>11}", "list values");
    for (threads, _) in &rates {
        header += &format!("{threads:>16}");
    }
    print(&header);
    for values in LIST_VALUES {
        let mut table = HashTable::for_values(values as usize);
        for value in 1..=values {
            table.insert(mix64(value));
        }
        let label = ExactCount(values).to_string();
        let mut line = format!("{label:>11}");
        // Each scanner needs a table of its own. A copy costs much less than
        // inserting the values again, and the last scanner takes the
        // original.
        let tables = std::iter::repeat_n(table, rates.len());
        for ((_, pool), table) in rates.iter().zip(tables) {
            // One scanner at a time, so that the command never holds the
            // memory of two: a scanner's buffer is twice its lookahead, 512
            // MiB by default. Each scanner and its table go before the next
            // scanner starts.
            let mut scanner = scanner(block_size, *pool, table);
            line += &format!("{:>16}", scan_rate(&mut scanner));
        }
        print(&line);
    }
}

/// A scanner as a sender's, which searches against `table`, with the
/// default lookahead for blocks of `block_size` bytes, and probes long
/// stretches on `pool`, or with `None` on its own thread.
fn scanner(block_size: u32, pool: Option<&ThreadPool>, table: HashTable) -> Scanner<'_> {
    let lookahead = settings::default_lookahead(block_size);
    let reserved = Lookahead::reserve(lookahead, block_size).unwrap_or_else(|e| {
        die(format!(
            "cannot reserve the scanner's buffer for a lookahead of {lookahead} bytes: {e}"
        ))
    });
    Scanner::new(table, block_size, StrongHash::DEFAULT, reserved, 0, pool)
}

/// The rate at which `scanner`, which is new, searches noise, measured over
/// `DURATION` of scanning. Before each call of `scan`, pieces of the noise
/// fill the buffer while it has room, as the lookahead reader of a fast
/// source fills it, so that the scanner never runs out of output. The clock
/// stops meanwhile, because making the output and copying it are not part
/// of the search, and because the first write to each page of the buffer
/// costs a page fault, which in a virtual machine can cost more than the
/// search itself. On many probe threads, the scanner searches more than its
/// lookahead in `DURATION`, so it needs these pieces while it scans.
///
/// The rate counts the bytes that the scanner scanned, not those that it
/// emitted: the frames of the last literal bytes wait until those bytes
/// fill a block, and with large blocks that can be all that the scanner
/// searches in `DURATION`.
fn scan_rate(scanner: &mut Scanner) -> String {
    let mut noise = Noise::new();
    let mut piece = vec![0; READ_SIZE];
    let mut queries = Vec::new();
    let mut elapsed = Duration::ZERO;
    while elapsed < DURATION {
        while scanner.has_room() {
            noise.fill(&mut piece);
            scanner.push(&piece);
        }
        let start = Instant::now();
        scanner.scan(&mut |offset, _| queries.push(offset));
        // A window of noise is in the list only by chance. The answer no
        // lets the scanner go on, as the receiver's answer would.
        for offset in queries.drain(..) {
            scanner.answer(offset, false);
        }
        while scanner.next_frame().is_some() {}
        elapsed += start.elapsed();
    }
    per_second(scanner.scanned(), elapsed)
}

/// Bytes that are all written and never repeat: each 8 of them are the
/// next value of a counter, mixed. A buffer of zeros could stay unwritten,
/// so that every page would map the same zero page and the whole buffer
/// would fit in the CPU caches, whatever its size. And in output that
/// repeats, a window that hits the table without being in the list would
/// come back in each repetition, where a sender's output has such windows
/// only by chance. Each one ends a stretch of the probe threads, so the
/// scanner's rate would depend on whether the repeated bytes happen to
/// hold one.
struct Noise {
    /// The counter value of the next 8 bytes.
    next: u64,
}

impl Noise {
    fn new() -> Self {
        Noise { next: 1 }
    }

    /// Write the next bytes over all of `buf`.
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            chunk.copy_from_slice(&mix64(self.next).to_le_bytes()[..chunk.len()]);
            self.next += 1;
        }
    }
}

/// The rate of `pass`, which returns the bytes that it processed, called
/// over and over until `DURATION` has passed, as an amount of bytes per
/// second. The clock is read once per pass, not once per block or window,
/// so that reading it adds nothing measurable.
fn rate(mut pass: impl FnMut() -> u64) -> String {
    let start = Instant::now();
    let mut bytes = 0u64;
    loop {
        bytes += pass();
        let elapsed = start.elapsed();
        if elapsed >= DURATION {
            return per_second(bytes, elapsed);
        }
    }
}

/// `bytes` processed in `elapsed`, as an amount of bytes per second.
fn per_second(bytes: u64, elapsed: Duration) -> String {
    let rate = (bytes as f64 / elapsed.as_secs_f64()).round() as u64;
    format!("{}/s", Bytes(rate))
}

/// Write `line` to stdout in one call, which is not buffered. The tables
/// are the output of the command, so a failure to write them is an
/// external failure, as for the report of `hashsync stats`.
fn print(line: &str) {
    if let Err(e) = fsops::stdout().write_all(format!("{line}\n").as_bytes()) {
        die(format!("cannot write to stdout: {e}"));
    }
}
