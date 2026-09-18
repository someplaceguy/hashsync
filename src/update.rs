//! `hashsync update [--rebuild]` (DESIGN.md §4 and §8).

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use bstr::ByteSlice;
use crossbeam_channel::bounded;
use rustix::process::{Resource, getrlimit, setrlimit};

use crate::config::Config;
use crate::fsid::FsCache;
use crate::index::{self, Settings, writer};
use crate::roots;
use crate::scan::{self, ScanRoot};

/// Messages in flight between workers and the writer. Each message is
/// bounded (`blocks::BATCH_ROWS` rows or one path), so this bounds memory.
const CHANNEL_CAPACITY: usize = 1024;

pub fn run(config: &Config, rebuild: bool) {
    let start = Instant::now();
    raise_descriptor_limit();

    let index = index::open_modifying(&config.index_path);
    let settings = Settings::from_config(config);
    let rebuild = rebuild
        || match index.read_meta() {
            None => {
                eprintln!("hashsync: creating index {}", config.index_path.as_bstr());
                true
            }
            Some(meta) if meta.compatible(&settings) => false,
            Some(meta) => {
                eprintln!(
                    "hashsync: index settings changed (schema {} -> {}, block size {} -> {}, \
                     weak hash {} -> {}, strong hash {} -> {}); rebuilding",
                    meta.schema_version,
                    index::SCHEMA_VERSION,
                    meta.settings.block_size,
                    settings.block_size,
                    meta.settings.weak_hash_id,
                    settings.weak_hash_id,
                    meta.settings.strong_hash_id,
                    settings.strong_hash_id
                );
                true
            }
        };
    if rebuild {
        index.reset(&settings);
    }

    let fs_cache = FsCache::new();
    let locations = roots::expand(config, &fs_cache);
    let paths: Vec<&[u8]> = locations.iter().map(|l| l.path.as_slice()).collect();
    let root_ids = index.reconcile_roots(&paths);
    let roots: Vec<ScanRoot> = locations
        .into_iter()
        .zip(root_ids)
        .map(|(location, root_id)| ScanRoot { location, root_id })
        .collect();

    let (tx, rx) = bounded(CHANNEL_CAPACITY);
    let fs_cache = &fs_cache;
    let (counters, (index, summary)) = std::thread::scope(|s| {
        let writer = std::thread::Builder::new()
            .name("writer".into())
            .spawn_scoped(s, move || writer::run(index, fs_cache, rx))
            .expect("cannot start the writer thread");
        let counters = scan::run(config, roots, fs_cache, tx);
        (counters, writer.join().expect("the writer thread panicked"))
    });

    let orphans = index.remove_orphans();
    index.ensure_secondary_indexes();
    drop(index);

    eprintln!(
        "hashsync: {} in {:.1}s: {} directories, {} files hashed ({} bytes), \
         {} hard links, {} small files, {} indexed blocks ({} periodic, {} in holes), \
         {} paths, {} stale inodes removed",
        if rebuild {
            "rebuilt index"
        } else {
            "updated index"
        },
        start.elapsed().as_secs_f64(),
        counters.directories.load(Relaxed),
        counters.files_hashed.load(Relaxed),
        counters.bytes_hashed.load(Relaxed),
        counters.files_linked.load(Relaxed),
        counters.files_small.load(Relaxed),
        summary.blocks,
        counters.blocks_periodic.load(Relaxed),
        counters.blocks_in_holes.load(Relaxed),
        summary.paths,
        orphans
    );
}

/// Raise the soft descriptor limit to the hard limit: every pending task
/// may hold its parent's descriptor.
fn raise_descriptor_limit() {
    let mut limit = getrlimit(Resource::Nofile);
    if limit.current != limit.maximum {
        limit.current = limit.maximum;
        // Failure only means fewer descriptors; opens report it later.
        let _ = setrlimit(Resource::Nofile, limit);
    }
}
