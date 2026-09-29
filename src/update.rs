//! `hashsync update [--rebuild]` (DESIGN.md §4 and §8).

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use bstr::ByteSlice;

use crate::config::Config;
use crate::die;
use crate::fsid::FsCache;
use crate::fsops;
use crate::index::{self, Settings};
use crate::roots::{self, Location};
use crate::scan::{self, ScanRoot};
use crate::units::Bytes;

pub fn run(config: &Config, rebuild: bool, jobs: Option<u32>) {
    let (index, _) = perform(config, rebuild, jobs);
    index.close();
}

/// Update the index and return it still open, with the current root
/// locations that the update resolved. A normal `run` keeps this index
/// open through the transfer, so there is no gap in exclusion between
/// update and use (DESIGN.md §4.4), and it reads candidate blocks through
/// these locations.
pub fn perform(config: &Config, rebuild: bool, jobs: Option<u32>) -> (index::Index, Vec<Location>) {
    let start = Instant::now();
    fsops::raise_descriptor_limit();

    // Resolve and validate the roots before the index is opened: a rejected
    // configuration must not change the index (DESIGN.md §3 and §4.2).
    let fs_cache = FsCache::new();
    let locations = roots::expand(config, &fs_cache);

    let index = index::open_modifying(&config.index_path);
    let settings = Settings::from_config(config);
    let rebuild = rebuild
        || match index.read_meta() {
            None => {
                die::note(format!("creating index {}", config.index_path.as_bstr()));
                true
            }
            Some(meta) if meta.compatible(&settings) => false,
            Some(meta) => {
                die::note(format!(
                    "index settings changed (schema {} -> {}, block size {} -> {}, \
                     weak hash {} -> {}, strong hash {} -> {}); rebuilding",
                    meta.schema_version,
                    index::SCHEMA_VERSION,
                    meta.settings.block_size,
                    settings.block_size,
                    meta.settings.weak_hash_id,
                    settings.weak_hash_id,
                    meta.settings.strong_hash_id,
                    settings.strong_hash_id
                ));
                true
            }
        };
    if rebuild {
        // Warn only when the index gets its block size, not on each update.
        if settings.block_size < 4096 {
            die::warn(format!(
                "block_size ({} bytes) is less than 4 KiB, so performance is much \
                 reduced; a block size of at least 4 KiB is strongly recommended",
                settings.block_size
            ));
        }
        index.reset(&settings);
    }

    let paths: Vec<&[u8]> = locations.iter().map(|l| l.path.as_slice()).collect();
    let root_ids = index.reconcile_roots(&paths);
    let roots: Vec<ScanRoot> = locations
        .iter()
        .zip(root_ids)
        .map(|(location, root_id)| ScanRoot { location, root_id })
        .collect();

    let (counters, summary) = scan::run(config, roots, &fs_cache, &index, jobs);

    index.finish_update();

    die::note(format!(
        "{} in {:.1}s: {} directories, {} files hashed ({}), \
         {} unchanged, {} hard links, {} small files, {} newly indexed blocks \
         (skipped: {} periodic, {} in holes), {} paths, {} stale inodes removed",
        if rebuild {
            "rebuilt index"
        } else {
            "updated index"
        },
        start.elapsed().as_secs_f64(),
        counters.directories.load(Relaxed),
        counters.files_hashed.load(Relaxed),
        Bytes(counters.bytes_hashed.load(Relaxed)),
        counters.files_unchanged.load(Relaxed),
        counters.files_linked.load(Relaxed),
        counters.files_small.load(Relaxed),
        summary.blocks,
        counters.blocks_periodic.load(Relaxed),
        counters.blocks_in_holes.load(Relaxed),
        summary.paths,
        summary.orphans
    ));
    (index, locations)
}
