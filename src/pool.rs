//! Rayon pools that join their threads when they drop (DESIGN.md §3): the
//! pools of the reconciliation and of prune (§4.2), of the scan (§4.6), and
//! of the export (§5.4), and the sender's probe threads (§6.3). The owner of
//! a pool drops it after the work of the pool, explicitly where a later step
//! must come after the join.
//!
//! Dropping a rayon pool only tells its threads to end: it does not wait
//! for them. So a `Pool` starts its threads itself, with
//! `std::thread::Builder`, and keeps their join handles. Such a join
//! returns only after the thread has exited, the destructors of its
//! thread-local values included. A join through `std::thread::scope`,
//! which rayon's `build_scoped` uses, can return before those destructors
//! run.
//!
//! A fatal end drops no pool: `die` and panics end the process without
//! destructors, so without joins (DESIGN.md §6.9).

use std::ops::Deref;
use std::thread::JoinHandle;

/// A rayon pool with the join handles of its threads. It dereferences to
/// the rayon pool, which runs the work. Dropping it ends the pool and
/// waits until all its threads have ended.
pub struct Pool {
    // The fields drop in the order of their declaration: first the rayon
    // pool, which tells the threads to end, then the join handles, whose
    // drop waits for the threads.
    pool: rayon::ThreadPool,
    _threads: Threads,
}

/// The join handles of the threads of a pool. Dropping them joins the
/// threads.
struct Threads(Vec<JoinHandle<()>>);

impl Pool {
    /// Start a pool of `count` threads, or of rayon's default for 0: one
    /// thread per core. The name of each thread is `name` followed by the
    /// index of the thread in the pool, for example `scan0`. A thread that
    /// cannot start is a fatal error of this host.
    pub fn start(name: &str, count: usize) -> Pool {
        Pool::try_start(name, count).unwrap_or_else(|e| panic!("cannot start the {name} pool: {e}"))
    }

    /// As `start`, but a thread that cannot start is an error for the
    /// caller, for a count that the peer chose. The threads that did start
    /// have ended when this returns the error.
    pub fn try_start(name: &str, count: usize) -> Result<Pool, rayon::ThreadPoolBuildError> {
        // The list grows as the threads start, with no reservation for
        // `count`: the client's REQUEST can set it, to any value, and rayon
        // starts at most 65,535 threads whatever it is.
        let mut threads = Vec::new();
        let built = rayon::ThreadPoolBuilder::new()
            .num_threads(count)
            .spawn_handler(|thread| {
                let spawned = std::thread::Builder::new()
                    .name(format!("{name}{}", thread.index()))
                    .spawn(|| thread.run())?;
                threads.push(spawned);
                Ok(())
            })
            .build();
        // After a failed start, rayon has told the threads that started to
        // end, and the drop of their handles waits for them.
        let threads = Threads(threads);
        Ok(Pool {
            pool: built?,
            _threads: threads,
        })
    }
}

impl Deref for Pool {
    type Target = rayon::ThreadPool;

    fn deref(&self) -> &rayon::ThreadPool {
        &self.pool
    }
}

impl Drop for Threads {
    fn drop(&mut self) {
        for thread in self.0.drain(..) {
            thread.join().expect("a pool thread panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use super::Pool;

    /// The threads that have destroyed their `EXIT` value.
    static EXITED: AtomicUsize = AtomicUsize::new(0);

    /// A thread-local value that counts its thread in `EXITED` when the
    /// thread exits and destroys it.
    struct Exit;

    impl Drop for Exit {
        fn drop(&mut self) {
            EXITED.fetch_add(1, SeqCst);
        }
    }

    thread_local! {
        static EXIT: Exit = const { Exit };
    }

    /// The drop of a pool returns only after every thread of the pool has
    /// exited, the destructors of its thread-local values included: each
    /// thread has an `Exit` value, and all of them have counted when the
    /// drop returns. No thread exits before the drop, and no other thread
    /// uses `EXIT`, so `EXITED` is exact before and after the drop. The
    /// threads have the name of the pool and their index.
    #[test]
    fn drop_waits_for_the_threads() {
        let pool = Pool::start("test", 3);
        let named = pool.broadcast(|context| {
            EXIT.with(|_| {});
            std::thread::current().name() == Some(format!("test{}", context.index()).as_str())
        });
        assert_eq!(named, [true; 3], "the names of the threads");
        assert_eq!(EXITED.load(SeqCst), 0, "a thread exited before the drop");
        drop(pool);
        assert_eq!(
            EXITED.load(SeqCst),
            3,
            "the drop returned before every thread exited"
        );
    }
}
