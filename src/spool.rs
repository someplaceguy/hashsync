//! Spools (DESIGN.md §6.5): pipes whose bytes wait in segments, so that
//! the producer of the bytes waits for their consumer only at the limit of
//! the spool.
//!
//! The writer of a spool appends bytes to extents of the spool's own
//! segments, in stream order, and the reader takes them in the same order.
//! Once the reader has passed an extent, the extent is released, so a
//! sealed segment closes once the reader has passed its last byte. With a
//! limit, the writer waits while that many bytes wait for the reader;
//! without one, it never waits for the reader. The writer is the thread
//! of the spool, which copies a source such as the command's stdout until
//! EOF (`Spool::start`), or another thread, which writes through a
//! `SpoolWriter` (`Spool::pipe`).
//!
//! The writer and the reader share one lock. Segment writes, reads, and
//! closes happen outside it; the creation of a segment happens under it,
//! once per segment, as for the pins.
//!
//! The source of the thread of a spool can tell when it has no byte ready:
//! a read then fails with `WouldBlock`, and the next read waits. The
//! command's stdout does that in eager delivery of the output, also right
//! before its EOF (DESIGN.md §6.2). The thread then marks an idle point,
//! which the reader reports (`take_next`) once it has taken every byte
//! before it, unless more bytes arrive first, and before the end of the
//! stream.
//!
//! At the normal end, after the reader has read EOF, `close` joins the
//! thread of the spool and closes the spool's segments (DESIGN.md §6.9). A
//! role can also stop a spool, from any thread, when its bytes have no use
//! any more (DESIGN.md §6.6). The stop closes all the segments of the
//! spool, which discards the bytes that wait, and the writer discards the
//! bytes that it appends later. Spool failures end the run with `die`
//! (DESIGN.md §6.7).

use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;

use crate::die::die;
use crate::fsops::retry_interrupted;
use crate::segments::{Directory, Extent, SEGMENT_MINIMUM, Segments};

/// The bytes of one extent of a spool.
const EXTENT: u64 = 1 << 20;

/// The most bytes of one read of the thread of a spool from its source.
const READ_SIZE: usize = 256 << 10;

/// The reading end of a pipe whose bytes wait in segments (DESIGN.md
/// §6.5). Reads wait for bytes or for EOF, without a timeout, and never
/// fail. After a stop, a read gives EOF at once.
pub struct Spool {
    shared: Arc<Shared>,
    /// The thread that fills the spool from its source, if it has one.
    thread: Option<JoinHandle<()>>,
}

/// The writing end of a spool: the caller's, from `Spool::pipe`, or that
/// of the thread of a spool, from `Spool::start`. A write appends all its
/// bytes, then waits while `limit` bytes, and at least one, wait for the
/// reader. After a stop, a write discards its bytes and does not wait.
/// Writes never fail. The drop of the writer ends the stream: the reader
/// reads EOF after the bytes that wait.
pub struct SpoolWriter {
    shared: Arc<Shared>,
}

/// Stops a spool from any thread (`Stopper::stop`).
pub struct Stopper(Arc<Shared>);

/// What one `Spool::take_next` found.
#[derive(Debug, PartialEq, Eq)]
pub enum Taken {
    /// This many bytes, at the start of the buffer.
    Bytes(usize),
    /// An idle point: the reader has taken every byte, and the source had
    /// no byte ready after the last one (DESIGN.md §6.5).
    Idle,
    /// The end of the stream, or a stop.
    End,
}

/// What the writer and the reader share.
struct Shared {
    /// The tmp_data directory, for new segments and failure messages.
    dir: Arc<Directory>,
    /// The bytes that can wait for the reader before the writer waits. The
    /// writer appends when no byte waits, also with a limit of 0.
    limit: u64,
    state: Mutex<State>,
    /// Wakes the reader when bytes or EOF arrive, and at a stop.
    appended: Condvar,
    /// Wakes the writer when the reader takes bytes, and at a stop.
    taken: Condvar,
}

struct State {
    /// The open segments of the spool.
    segments: Segments,
    /// The extents that are allocated and not released, in stream order.
    /// The reader is in the first one, which starts at `read` rounded down
    /// to a whole extent, and the writer appends to the last one.
    extents: VecDeque<Extent>,
    /// The bytes that the writer has appended.
    written: u64,
    /// The bytes that the reader has taken.
    read: u64,
    /// The writer appended all its bytes: the stream ended.
    ended: bool,
    /// A role stopped the spool: it has no extents and no open segments
    /// any more, and every later byte is discarded.
    stopped: bool,
    /// An idle point: the source had no byte ready after the last append,
    /// and the reader has not reported it yet. The next append removes it.
    idle: bool,
    /// The reader waits for `appended`, or the writer for `taken`. A side
    /// signals the condition variable only when the other side waits.
    reader_waits: bool,
    writer_waits: bool,
}

impl Spool {
    /// A spool in the tmp_data directory `dir`, that a new thread fills
    /// from `source` until EOF. The thread stops reading `source` while
    /// `limit` bytes, and at least one, wait for the reader; `u64::MAX` sets
    /// no limit. A read of `source` that fails with `WouldBlock` tells an
    /// idle point, and the next read must wait for bytes or EOF. `what`
    /// names the source in failure messages.
    pub fn start(
        what: &'static str,
        source: impl Read + Send + 'static,
        dir: Arc<Directory>,
        limit: u64,
    ) -> Spool {
        Self::with_sizes(what, source, dir, limit, EXTENT, SEGMENT_MINIMUM)
    }

    /// A spool in the tmp_data directory `dir`, that the returned writer
    /// fills. The writer waits while `limit` bytes, and at least one, wait
    /// for the reader; `u64::MAX` sets no limit.
    pub fn pipe(dir: Arc<Directory>, limit: u64) -> (SpoolWriter, Spool) {
        Self::pipe_with_sizes(dir, limit, EXTENT, SEGMENT_MINIMUM)
    }

    /// `start`, with extents of `extent` bytes, and a current segment that
    /// is full at `minimum` bytes while no sealed segment is open.
    fn with_sizes(
        what: &'static str,
        mut source: impl Read + Send + 'static,
        dir: Arc<Directory>,
        limit: u64,
        extent: u64,
        minimum: u64,
    ) -> Spool {
        let (mut writer, mut spool) = Self::pipe_with_sizes(dir, limit, extent, minimum);
        let thread = std::thread::Builder::new()
            .name("spool".into())
            .spawn(move || {
                // A write returns once fewer than `limit` bytes wait, so
                // the thread reads its source only then. The drop of the
                // writer at EOF ends the stream.
                let mut buf = vec![0u8; READ_SIZE];
                loop {
                    let n = match retry_interrupted(|| source.read(&mut buf)) {
                        Ok(0) => break,
                        Ok(n) => n,
                        // The source has no byte ready, and its next read
                        // waits (DESIGN.md §6.5, step W2).
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            writer.shared.mark_idle();
                            continue;
                        }
                        Err(e) => die(format!("cannot read {what}: {e}")),
                    };
                    writer.write_all(&buf[..n]).expect("a spool write failed");
                }
            })
            .expect("cannot start a spool thread");
        spool.thread = Some(thread);
        spool
    }

    /// `pipe`, with the sizes of `with_sizes`.
    fn pipe_with_sizes(
        dir: Arc<Directory>,
        limit: u64,
        extent: u64,
        minimum: u64,
    ) -> (SpoolWriter, Spool) {
        let shared = Arc::new(Shared {
            dir,
            limit,
            state: Mutex::new(State {
                segments: Segments::new(extent, minimum),
                extents: VecDeque::new(),
                written: 0,
                read: 0,
                ended: false,
                stopped: false,
                idle: false,
                reader_waits: false,
                writer_waits: false,
            }),
            appended: Condvar::new(),
            taken: Condvar::new(),
        });
        let writer = SpoolWriter {
            shared: Arc::clone(&shared),
        };
        (
            writer,
            Spool {
                shared,
                thread: None,
            },
        )
    }

    /// The normal end of the spool, after its reader has read EOF: join
    /// the thread of the spool, if it has one, which ended at the EOF of
    /// its source, and close the segments of the spool, which frees their
    /// storage (DESIGN.md §6.9).
    pub fn close(self) {
        // This function exists so that the normal end joins the thread of
        // the spool, as it joins every other thread. Before EOF, the
        // thread can still wait for its source or for room, so a join
        // could wait forever: that is a bug of the caller. The check
        // releases the lock before the assertion, so that a failed check
        // does not poison the lock for the thread.
        let ended = {
            let state = self.shared.lock();
            state.ended && state.read == state.written && !state.stopped
        };
        assert!(ended, "a spool closed before its end");
        if let Some(thread) = self.thread {
            thread.join().expect("a spool thread panicked");
        }
    }

    /// A handle that stops this spool from any thread.
    pub fn stopper(&self) -> Stopper {
        Stopper(Arc::clone(&self.shared))
    }

    /// True once a role has stopped the spool. This function exists so
    /// that the reader can tell an EOF that the stop gave from the end of
    /// the stream.
    pub fn is_stopped(&self) -> bool {
        self.shared.lock().stopped
    }
}

impl Spool {
    /// Take the next bytes into `buf`, which must not be empty, or else
    /// report an idle point, or the end of the stream (DESIGN.md §6.5,
    /// steps T1 to T3). Waits without a timeout until one of them comes,
    /// and never fails. After a stop, it reports the end at once.
    pub fn take_next(&mut self, buf: &mut [u8]) -> Taken {
        assert!(!buf.is_empty(), "a take into an empty buffer");
        let (file, offset, available) = {
            let mut state = self.shared.lock();
            loop {
                if state.stopped {
                    return Taken::End;
                }
                if let Some(next) = state.next_bytes() {
                    break next;
                }
                // No byte follows the idle point: an append would have
                // removed it. The end can follow it at once, as at the EOF
                // of the command's stdout, and must not hide it.
                if state.idle {
                    state.idle = false;
                    return Taken::Idle;
                }
                if state.ended {
                    // The reader took every byte, so only the last extent
                    // can remain: it is not full, so `took` did not
                    // release it. If its segment is sealed, the release
                    // here closes the segment at once, not in `close`. The
                    // current segment keeps its file until `close`.
                    let closed = state.release_rest();
                    drop(state);
                    drop(closed);
                    return Taken::End;
                }
                // Ends when the writer appends, marks an idle point, or
                // ends the stream, or at a stop; no timeout (DESIGN.md §3).
                state.reader_waits = true;
                state = self.shared.appended.wait(state).unwrap();
                state.reader_waits = false;
            }
        };
        let n = available.min(buf.len() as u64) as usize;
        if let Err(e) = file.read_exact_at(&mut buf[..n], offset) {
            self.shared.dir.fail("read", e);
        }
        // The files that the release gives back close here, after the lock
        // is released. A stop during the read discards these bytes too.
        let (closed, stopped) = self.shared.took(n as u64);
        drop(closed);
        if stopped { Taken::End } else { Taken::Bytes(n) }
    }
}

/// The bytes of the spool, without its idle points: a read waits through
/// them.
impl Read for Spool {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            match self.take_next(buf) {
                Taken::Bytes(n) => return Ok(n),
                Taken::Idle => {}
                Taken::End => return Ok(0),
            }
        }
    }
}

impl SpoolWriter {
    /// The bytes appended so far: the writer's `written` (DESIGN.md §6.5).
    pub fn written(&self) -> u64 {
        self.shared.lock().written
    }
}

impl Write for SpoolWriter {
    /// Append all of `buf`, then wait while `limit` bytes, and at least
    /// one, wait for the reader (DESIGN.md §6.5, steps W1 and W3). After a
    /// stop, discard `buf` without a wait.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.shared.append(buf);
        self.shared.wait_for_room();
        Ok(buf.len())
    }

    /// Nothing to do: the reader can take the bytes of a write as soon as
    /// the write has appended them.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for SpoolWriter {
    /// The end of the stream: the reader reads EOF after the bytes that
    /// wait.
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.ended = true;
        if state.reader_waits {
            self.shared.appended.notify_one();
        }
    }
}

impl Stopper {
    /// Stop the spool, before or after its end (DESIGN.md §6.5): wake the
    /// writer and the reader, and close all the segments of the spool,
    /// which discards the bytes that wait. From then on, a read gives EOF
    /// at once, and a write discards its bytes without a wait.
    pub fn stop(&self) {
        let storage = {
            let mut state = self.0.lock();
            state.stopped = true;
            if state.reader_waits {
                self.0.appended.notify_one();
            }
            if state.writer_waits {
                self.0.taken.notify_one();
            }
            // The spool uses no extent after a stop, so the extents need no
            // release one by one: the stop takes them, with the segments.
            (std::mem::take(&mut state.extents), state.segments.take())
        };
        // The files close here, after the lock is released. A file that
        // the writer or the reader still uses outside the lock closes when
        // that I/O ends.
        drop(storage);
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// The source had no byte ready after the last append: mark an idle
    /// point, and wake the reader (DESIGN.md §6.5, step W2).
    fn mark_idle(&self) {
        let mut state = self.lock();
        state.idle = true;
        if state.reader_waits {
            self.appended.notify_one();
        }
    }

    /// Wait until fewer than `limit` bytes wait for the reader, or none,
    /// or until a stop. Ends when the reader takes bytes, or at the stop;
    /// no timeout (DESIGN.md §3).
    fn wait_for_room(&self) {
        let mut state = self.lock();
        while !state.stopped
            && state.written > state.read
            && state.written - state.read >= self.limit
        {
            state.writer_waits = true;
            state = self.taken.wait(state).unwrap();
        }
        state.writer_waits = false;
    }

    /// Append `bytes` at the end of the stream, in new extents where the
    /// last one is full. A stop discards the bytes that are not appended
    /// yet.
    fn append(&self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            // The reference to the file ends with the write, before the
            // reader can take these bytes. So the reader, which releases
            // the extent, also closes the file (DESIGN.md §6.5, step R2).
            let n = {
                let Some((file, offset, room)) = self.lock().append_at(&self.dir) else {
                    return;
                };
                let n = room.min(bytes.len() as u64) as usize;
                // Only the writer writes, and only past the bytes that the
                // reader can take.
                if let Err(e) = file.write_all_at(&bytes[..n], offset) {
                    self.dir.fail("write", e);
                }
                n
            };
            let mut state = self.lock();
            if state.stopped {
                return;
            }
            state.written += n as u64;
            // These bytes follow the idle point: it is not the end of the
            // bytes any more.
            state.idle = false;
            if state.reader_waits {
                self.appended.notify_one();
            }
            bytes = &bytes[n..];
        }
    }

    /// The reader took `n` bytes. Release the first extent if the reader
    /// has passed it (DESIGN.md §6.5, steps R1 to R3). Returns the files
    /// of the segments that this closes, for the caller to drop outside
    /// the lock, and whether a stop came first, which discarded the bytes.
    fn took(&self, n: u64) -> ([Option<Arc<File>>; 2], bool) {
        let mut state = self.lock();
        if state.stopped {
            return ([None, None], true);
        }
        state.read += n;
        if state.writer_waits {
            self.taken.notify_one();
        }
        // The reader has passed the first extent when `read` is at the
        // start of the next one: a multiple of the extent size.
        if !state.read.is_multiple_of(state.segments.extent_size()) {
            return ([None, None], false);
        }
        let extent = state.extents.pop_front().expect("read past the extents");
        (state.segments.release(extent), false)
    }
}

impl State {
    /// Where the writer appends next: the file, the offset in it, and the
    /// bytes left in the extent; `None` after a stop. When `written` is a
    /// multiple of the extent size, the last extent is full, or there is
    /// none, so a new extent is necessary (DESIGN.md §6.5, steps A1 to A3).
    fn append_at(&mut self, dir: &Directory) -> Option<(Arc<File>, u64, u64)> {
        if self.stopped {
            return None;
        }
        let extent = self.segments.extent_size();
        let used = self.written % extent;
        if used == 0 {
            let new = dir.allocate(&mut self.segments);
            self.extents.push_back(new);
        }
        let last = self.extents.back().expect("an extent was allocated");
        Some((Arc::clone(last.file()), last.offset() + used, extent - used))
    }

    /// The bytes that the reader can take now, which are all in the first
    /// extent: the file, the offset in it, and the count. `None` when no
    /// byte waits for the reader.
    fn next_bytes(&self) -> Option<(Arc<File>, u64, u64)> {
        let first = self.extents.front()?;
        let extent = self.segments.extent_size();
        let at = self.read % extent;
        let available = (self.written - self.read).min(extent - at);
        (available > 0).then(|| (Arc::clone(first.file()), first.offset() + at, available))
    }

    /// Release the extent that can remain after the reader took the last
    /// byte: the last one, which is not full, so `took` did not release it
    /// (DESIGN.md §6.5, step T1). Returns the files of the segments that
    /// this closes, for the caller to drop outside the lock.
    fn release_rest(&mut self) -> [Option<Arc<File>>; 2] {
        let Some(extent) = self.extents.pop_front() else {
            return [None, None];
        };
        self.segments.release(extent)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    use super::*;
    use crate::scanner::tests::noise;
    use crate::segments::{self, descriptors};

    /// The tmp_data directory of a test.
    struct Setup {
        dir: Arc<Directory>,
        path: PathBuf,
        _tempdir: tempfile::TempDir,
    }

    fn setup() -> Setup {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        Setup {
            dir,
            path: std::fs::canonicalize(path).unwrap(),
            _tempdir: tempdir,
        }
    }

    /// A source that returns `bytes` in reads of the sizes of `sizes`,
    /// in turn, then EOF.
    struct Chunked {
        bytes: Vec<u8>,
        at: usize,
        sizes: Vec<usize>,
        next: usize,
    }

    impl Read for Chunked {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let size = self.sizes[self.next % self.sizes.len()];
            self.next += 1;
            let n = size.min(buf.len()).min(self.bytes.len() - self.at);
            buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
            self.at += n;
            Ok(n)
        }
    }

    /// Spin until `done` holds for the state of `spool`. The thread of the
    /// spool makes progress on its own, so this ends without a timeout.
    fn wait_until(spool: &Spool, done: impl Fn(&State) -> bool) {
        while !done(&spool.shared.lock()) {
            std::thread::yield_now();
        }
    }

    /// The open segments of `spool`, oldest first: the count of the
    /// extents in use of each one.
    fn open_segments(spool: &Spool, setup: &Setup) -> Vec<usize> {
        segments::open_segments(&spool.shared.lock().segments, &setup.path)
    }

    #[test]
    fn bytes_come_out_in_order_and_segments_close_behind_the_reader() {
        // Extents of 1000 bytes, a minimum of two extents, and reads of
        // odd sizes on both sides. The reader starts after the thread has
        // appended everything, so the segments double: 2, 4, 8, and 16
        // extents, then the rest. Each sealed segment closes once the
        // reader has passed its last byte.
        let setup = setup();
        let bytes = noise(29_500, 1);
        let source = Chunked {
            bytes: bytes.clone(),
            at: 0,
            sizes: vec![1, 999, 4000, 1700],
            next: 0,
        };
        let mut spool = Spool::with_sizes(
            "the test source",
            source,
            Arc::clone(&setup.dir),
            u64::MAX,
            1000,
            2000,
        );
        wait_until(&spool, |state| state.ended);
        assert_eq!(open_segments(&spool, &setup), [2, 4, 8, 16]);
        let mut read = Vec::new();
        let mut buf = [0u8; 777];
        let mut open = Vec::new();
        loop {
            let n = spool.read(&mut buf).unwrap();
            read.extend_from_slice(&buf[..n]);
            let now = open_segments(&spool, &setup).len();
            if open.last() != Some(&now) {
                open.push(now);
            }
            if n == 0 {
                break;
            }
        }
        assert!(read == bytes, "the bytes differ");
        // The last extent is not full. The reader releases it at EOF, so
        // the last segment closes before the spool does.
        assert_eq!(open, [4, 3, 2, 1, 0]);
        spool.close();
        drop(setup.dir);
        assert_eq!(descriptors(&setup.path), (0, 0));
    }

    #[test]
    fn the_current_segment_is_sealed_when_an_older_segment_closes() {
        // Extents of 1000 bytes, and a minimum of two extents. Of 5000
        // bytes, the first segment gets 2000 and is sealed. The second one
        // gets 3000: it is not full while the first one is open (4000).
        // When the reader passes the first segment, that segment closes,
        // and the second one is full: step R3 seals it (DESIGN.md §6.5).
        // So the second segment also closes once the reader has passed
        // it, while the source gives no more bytes. As the current
        // segment, it would stay open until the next bytes.
        let setup = setup();
        let (source, mut input) = std::io::pipe().unwrap();
        let mut spool = Spool::with_sizes(
            "the test source",
            source,
            Arc::clone(&setup.dir),
            u64::MAX,
            1000,
            2000,
        );
        let bytes = noise(5000, 5);
        input.write_all(&bytes).unwrap();
        wait_until(&spool, |state| state.written == 5000);
        assert_eq!(open_segments(&spool, &setup), [2, 3]);
        let mut read = vec![0u8; 5000];
        spool.read_exact(&mut read[..2000]).unwrap();
        assert_eq!(open_segments(&spool, &setup), [3]);
        spool.read_exact(&mut read[2000..]).unwrap();
        assert!(read == bytes, "the bytes differ");
        assert_eq!(open_segments(&spool, &setup), []);
        drop(input);
        assert_eq!(spool.read(&mut read).unwrap(), 0);
        spool.close();
    }

    #[test]
    fn the_thread_does_not_wait_for_the_reader_without_a_limit() {
        // Five extents of the default size, read in one call each.
        let setup = setup();
        let bytes = noise(5 * EXTENT as usize, 2);
        let mut spool = Spool::start(
            "the test source",
            Cursor::new(bytes.clone()),
            Arc::clone(&setup.dir),
            u64::MAX,
        );
        wait_until(&spool, |state| state.ended);
        assert_eq!(spool.shared.lock().written, bytes.len() as u64);
        let mut read = Vec::new();
        spool.read_to_end(&mut read).unwrap();
        assert!(read == bytes, "the bytes differ");
        spool.close();
    }

    #[test]
    fn the_thread_stops_reading_its_source_at_the_limit() {
        // The source gives 256 KiB per read. With a limit of 1 MiB, the
        // thread reads four times, then waits for the reader, which takes
        // 300 KiB: two more reads fill the spool to 1236 KiB.
        let setup = setup();
        let bytes = noise(4 << 20, 3);
        let mut spool = Spool::start(
            "the test source",
            Cursor::new(bytes.clone()),
            Arc::clone(&setup.dir),
            1 << 20,
        );
        let waiting = |state: &State| state.writer_waits || state.ended;
        wait_until(&spool, waiting);
        {
            let state = spool.shared.lock();
            assert!(!state.ended, "the thread read past the limit");
            assert_eq!(state.written, 1 << 20);
        }
        let mut read = vec![0u8; 300 << 10];
        spool.read_exact(&mut read).unwrap();
        wait_until(&spool, |state| state.written > 1 << 20 && waiting(state));
        {
            let state = spool.shared.lock();
            assert!(!state.ended, "the thread read past the limit");
            assert_eq!(state.written - state.read, 1236 << 10);
        }
        spool.read_to_end(&mut read).unwrap();
        assert!(read == bytes, "the bytes differ");
        spool.close();
    }

    #[test]
    fn a_limit_of_zero_passes_one_read_at_a_time() {
        // The thread reads its source when no byte waits for the reader.
        let setup = setup();
        let bytes = noise(3 * READ_SIZE + 5, 4);
        let mut spool = Spool::start(
            "the test source",
            Cursor::new(bytes.clone()),
            Arc::clone(&setup.dir),
            0,
        );
        let mut read = Vec::new();
        spool.read_to_end(&mut read).unwrap();
        assert!(read == bytes, "the bytes differ");
        spool.close();
    }

    #[test]
    fn an_empty_source_gives_eof() {
        let setup = setup();
        let mut spool = Spool::start(
            "the test source",
            std::io::empty(),
            Arc::clone(&setup.dir),
            u64::MAX,
        );
        let mut buf = [0u8; 16];
        assert_eq!(spool.read(&mut buf).unwrap(), 0);
        assert_eq!(spool.read(&mut buf).unwrap(), 0);
        spool.close();
    }

    #[test]
    #[should_panic(expected = "a spool closed before its end")]
    fn a_spool_closes_only_after_its_end() {
        let setup = setup();
        let spool = Spool::start(
            "the test source",
            Cursor::new(vec![1u8; 10]),
            Arc::clone(&setup.dir),
            u64::MAX,
        );
        spool.close();
    }

    #[test]
    fn a_writer_waits_at_the_limit_and_its_drop_ends_the_stream() {
        // A spool without a thread: its writer waits once 3000 bytes wait,
        // after its fourth write of 777 bytes, as the thread of a spool
        // waits. The drop of the writer ends the stream, which is not a
        // stop (DESIGN.md §6.5).
        let setup = setup();
        let (mut writer, mut spool) =
            Spool::pipe_with_sizes(Arc::clone(&setup.dir), 3000, 1000, 2000);
        let bytes = noise(29_500, 6);
        let writes = {
            let bytes = bytes.clone();
            std::thread::spawn(move || {
                for chunk in bytes.chunks(777) {
                    writer.write_all(chunk).unwrap();
                }
            })
        };
        wait_until(&spool, |state| state.writer_waits);
        assert_eq!(spool.shared.lock().written, 4 * 777);
        let mut read = Vec::new();
        spool.read_to_end(&mut read).unwrap();
        writes.join().unwrap();
        assert!(read == bytes, "the bytes differ");
        assert!(!spool.is_stopped());
        spool.close();
        drop(setup.dir);
        assert_eq!(descriptors(&setup.path), (0, 0));
    }

    #[test]
    fn a_stop_closes_all_segments_and_wakes_the_writer() {
        // Extents of 1000 bytes and a minimum of two extents: a write of
        // 3000 bytes fills a first segment, which is sealed, and puts one
        // extent in the current segment. Then it waits at the limit of 500
        // bytes. The stop closes both segments at once, the current one
        // too, although it is not full, and the write returns. A later
        // write discards its bytes without a wait, and a read gives EOF,
        // which `is_stopped` tells from the end of the stream (DESIGN.md
        // §6.5).
        let setup = setup();
        let (mut writer, mut spool) =
            Spool::pipe_with_sizes(Arc::clone(&setup.dir), 500, 1000, 2000);
        let writes = std::thread::spawn(move || {
            writer.write_all(&noise(3000, 7)).unwrap();
            writer
        });
        wait_until(&spool, |state| state.writer_waits);
        assert_eq!(open_segments(&spool, &setup), [2, 1]);
        spool.stopper().stop();
        let mut writer = writes.join().unwrap();
        assert_eq!(open_segments(&spool, &setup), []);
        writer.write_all(&noise(5000, 8)).unwrap();
        assert_eq!(open_segments(&spool, &setup), []);
        assert_eq!(spool.read(&mut [0u8; 10]).unwrap(), 0);
        assert!(spool.is_stopped());
        drop((writer, spool));
        drop(setup.dir);
        assert_eq!(descriptors(&setup.path), (0, 0));
    }

    /// A source that gives what the test sends, one item per read: bytes,
    /// or `None`, for no byte ready (`WouldBlock`). It gives EOF once the
    /// test drops the sending end. A read waits for the next item.
    struct Scripted(crossbeam_channel::Receiver<Option<Vec<u8>>>);

    impl Read for Scripted {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self.0.recv() {
                Ok(Some(bytes)) => {
                    buf[..bytes.len()].copy_from_slice(&bytes);
                    Ok(bytes.len())
                }
                Ok(None) => Err(ErrorKind::WouldBlock.into()),
                Err(_) => Ok(0),
            }
        }
    }

    #[test]
    fn the_reader_reports_an_idle_point_only_when_no_byte_follows_it() {
        // The source has no byte ready after "abc": the reader takes the
        // bytes, then reports the idle point. Then the source gives "def",
        // has no byte ready, and gives "gh" before the reader takes "def":
        // the append of "gh" removes that idle point, so the reader takes
        // all five bytes, and no idle point (DESIGN.md §6.5).
        let setup = setup();
        let (steps, script) = crossbeam_channel::unbounded();
        let mut spool = Spool::start(
            "the test source",
            Scripted(script),
            Arc::clone(&setup.dir),
            u64::MAX,
        );
        let mut buf = [0u8; 16];
        steps.send(Some(b"abc".to_vec())).unwrap();
        steps.send(None).unwrap();
        wait_until(&spool, |state| state.idle);
        assert_eq!(spool.take_next(&mut buf), Taken::Bytes(3));
        assert_eq!(buf[..3], *b"abc");
        assert_eq!(spool.take_next(&mut buf), Taken::Idle);
        steps.send(Some(b"def".to_vec())).unwrap();
        steps.send(None).unwrap();
        wait_until(&spool, |state| state.idle);
        steps.send(Some(b"gh".to_vec())).unwrap();
        wait_until(&spool, |state| state.written == 8);
        assert!(!spool.shared.lock().idle, "an idle point before a byte");
        assert_eq!(spool.take_next(&mut buf), Taken::Bytes(5));
        assert_eq!(buf[..5], *b"defgh");

        // A read waits through an idle point: here until the source gives
        // "ij", which the test sends only once the read waits.
        steps.send(None).unwrap();
        wait_until(&spool, |state| state.idle);
        let shared = Arc::clone(&spool.shared);
        let sends = std::thread::spawn(move || {
            while !shared.lock().reader_waits {
                std::thread::yield_now();
            }
            steps.send(Some(b"ij".to_vec())).unwrap();
            steps
        });
        assert_eq!(spool.read(&mut buf).unwrap(), 2);
        assert_eq!(buf[..2], *b"ij");
        drop(sends.join().unwrap());
        assert_eq!(spool.take_next(&mut buf), Taken::End);
        spool.close();
    }

    #[test]
    fn an_idle_point_right_before_the_end_comes_first() {
        // The source has no byte ready after "abc", then ends, all before
        // the reader looks. The reader takes the bytes, then reports the
        // idle point, and only then the end, which must not hide it: at the
        // EOF of the command's stdout, the bytes before it would wait for
        // the end of the command (DESIGN.md §6.5, step T1).
        let setup = setup();
        let (steps, script) = crossbeam_channel::unbounded();
        let mut spool = Spool::start(
            "the test source",
            Scripted(script),
            Arc::clone(&setup.dir),
            u64::MAX,
        );
        steps.send(Some(b"abc".to_vec())).unwrap();
        steps.send(None).unwrap();
        drop(steps);
        wait_until(&spool, |state| state.ended);
        let mut buf = [0u8; 16];
        assert_eq!(spool.take_next(&mut buf), Taken::Bytes(3));
        assert_eq!(spool.take_next(&mut buf), Taken::Idle);
        assert_eq!(spool.take_next(&mut buf), Taken::End);
        spool.close();
    }

    #[test]
    fn a_stop_wakes_a_reader_that_waits() {
        // The reader waits for the first byte, and a stop from another
        // thread ends the wait with EOF.
        let setup = setup();
        let (writer, mut spool) = Spool::pipe(Arc::clone(&setup.dir), u64::MAX);
        let stopper = spool.stopper();
        let reads = std::thread::spawn(move || {
            let n = spool.read(&mut [0u8; 10]).unwrap();
            (n, spool.is_stopped())
        });
        while !stopper.0.lock().reader_waits {
            std::thread::yield_now();
        }
        stopper.stop();
        assert_eq!(reads.join().unwrap(), (0, true));
        drop(writer);
    }
}
