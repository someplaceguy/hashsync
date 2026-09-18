//! `hashsync stats` (DESIGN.md §8): a read-only report of the index.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use bstr::ByteSlice;

use crate::config::Config;
use crate::die::{EXIT_FAILURE, die, warn};
use crate::index::{self, Settings};

pub fn run(config: &Config) {
    let path = config.index_path.as_bstr();
    let Some(index) = index::open_read_only(&config.index_path) else {
        die(
            EXIT_FAILURE,
            format!("no index at {path}; run `hashsync update`"),
        );
    };
    let Some(meta) = index.read_meta() else {
        die(
            EXIT_FAILURE,
            format!("index {path} has no schema; run `hashsync update`"),
        );
    };
    let stats = index.stats();
    drop(index);

    let file_size = |suffix: &str| -> u64 {
        let mut p = config.index_path.clone();
        p.extend_from_slice(suffix.as_bytes());
        std::fs::metadata(OsStr::from_bytes(&p)).map_or(0, |m| m.len())
    };
    let index_id: String = meta.index_id.iter().map(|b| format!("{b:02x}")).collect();
    println!("index path:       {path}");
    println!("schema version:   {}", meta.schema_version);
    println!("block size:       {}", meta.settings.block_size);
    println!("strong hash:      {}", meta.settings.strong_hash_id);
    println!("weak hash:        {}", meta.settings.weak_hash_id);
    println!("index id:         {index_id}");
    println!("filesystems:      {}", stats.filesystems);
    println!(
        "roots:            {} ({} complete immutable)",
        stats.roots, stats.roots_complete
    );
    println!(
        "inodes:           {} ({} incomplete)",
        stats.inodes, stats.inodes_incomplete
    );
    println!("paths:            {}", stats.paths);
    println!("blocks:           {}", stats.blocks);
    println!("distinct weak:    {}", stats.distinct_weak);
    println!("indexed bytes:    {}", stats.indexed_bytes);
    println!(
        "database bytes:   {} (+{} WAL)",
        file_size(""),
        file_size("-wal")
    );

    if !meta.compatible(&Settings::from_config(config)) {
        warn(
            "the index settings differ from the configuration; run `hashsync update` to rebuild it",
        );
    }
    if !stats.secondary_indexes {
        warn("secondary block indexes are missing; run `hashsync update`");
    }
}
