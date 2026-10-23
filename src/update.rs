//! `hashsync update [--rebuild] [--scan-immutable] [--compact]` (DESIGN.md
//! §4 and §8).

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use crate::compact;
use crate::config::Config;
use crate::escapes::quoted;
use crate::fsid::FsCache;
use crate::fsops;
use crate::index::{self, CurrentRoot, Settings};
use crate::report;
use crate::roots::{self, Location};
use crate::scan;
use crate::units::Bytes;

/// The options of an update: those of `hashsync update`, of which the
/// update of a run's receiver takes `scan_immutable` and `compact` from the
/// options of `hashsync ssh` (DESIGN.md §8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Options {
    /// Remove every entry of the index and build it again.
    pub rebuild: bool,
    /// Scan the completed immutable groups again too, which an update
    /// otherwise reuses without a scan (DESIGN.md §4.2).
    pub scan_immutable: bool,
    /// End with a compaction of the index, as a prune does (DESIGN.md
    /// §4.4).
    pub compact: bool,
    /// The thread count of the scan, in place of the configured one; 0
    /// selects the default.
    pub jobs: Option<u32>,
}

pub fn run(config: &Config, options: Options) {
    let (index, _) = perform(config, options);
    index.close();
}

/// Update the index as `options` say, and return it still open, with the
/// current root locations that the update resolved. The receiver of a run
/// turns the index into a read-only index on the same lock
/// (`Index::into_read_only`), so no modifying process comes between the
/// update and its use, or, in a run with both streams deduplicated, closes
/// it until the other host's update is done too (DESIGN.md §4.4). Its
/// lookups read candidate blocks through these locations.
pub fn perform(config: &Config, options: Options) -> (index::Index, Vec<Location>) {
    fsops::raise_descriptor_limit();

    // Resolve and validate the roots before the index is opened: a rejected
    // configuration must not change the index (DESIGN.md §3 and §4.2).
    let fs_cache = FsCache::new();
    let locations = roots::resolve(config, &fs_cache);

    let index = index::open_modifying(&config.index_path);
    // The time of the update, in its note, starts once the index is
    // locked: a wait for the lock of another process is no update work.
    let start = Instant::now();
    let settings = Settings::from_config(config);
    let rebuild = options.rebuild
        || match index.read_meta() {
            None => {
                report::note(format_args!(
                    "creating index {}",
                    quoted(&config.index_path)
                ));
                true
            }
            Some(meta) if meta.compatible(&settings) => false,
            Some(meta) => {
                report::note(format_args!(
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
            report::warn(format!(
                "block_size ({} bytes) is less than 4 KiB, so performance is much \
                 reduced; a block size of at least 4 KiB is strongly recommended",
                settings.block_size
            ));
        }
        index.reset(&settings);
    }

    let current: Vec<CurrentRoot> = locations
        .iter()
        .map(|location| CurrentRoot {
            path: &location.path,
            identifier: location.identifier.as_deref(),
        })
        .collect();
    let reconciliation = index.reconcile_roots(&current, options.scan_immutable);

    let (counters, summary) = scan::run(
        config,
        &locations,
        &reconciliation,
        &fs_cache,
        &index,
        options.jobs,
    );

    index.finish_writes();

    report::note(format_args!(
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
    // A configuration with immutable roots says what became of their
    // groups: the reuse of a completed group is what makes such a root
    // cheap, and an incomplete one is scanned again by the next update.
    let groups = summary.groups_reused + summary.groups_completed + summary.groups_incomplete;
    if groups > 0 {
        report::note(format_args!(
            "immutable roots: {} reused, {} completed, {} incomplete",
            summary.groups_reused, summary.groups_completed, summary.groups_incomplete
        ));
    }
    if options.compact {
        compact::perform(&index);
    }
    (index, locations)
}
