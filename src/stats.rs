//! `hashsync stats` (DESIGN.md §8): a read-only report of the index.

use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;

use bstr::ByteSlice;

use crate::config::Config;
use crate::die::{die, warn};
use crate::fsops;
use crate::index::{self, Settings};

pub fn run(config: &Config) {
    let path = config.index_path.as_bstr();
    let Some(index) = index::open_read_only(&config.index_path) else {
        die(format!("no index at {path}; run `hashsync update`"));
    };
    let Some(meta) = index.read_meta() else {
        die(format!("index {path} has no schema; run `hashsync update`"));
    };
    let stats = index.stats();
    drop(index);

    let file_size = |suffix: &str| -> u64 {
        let mut p = config.index_path.clone();
        p.extend_from_slice(suffix.as_bytes());
        fsops::retry_interrupted(|| std::fs::metadata(OsStr::from_bytes(&p))).map_or(0, |m| m.len())
    };
    let index_id: String = meta.index_id.iter().map(|b| format!("{b:02x}")).collect();
    // The report is the output of the command, so a failure to write it
    // is an external failure, as for the output of `run`.
    let report = format!(
        "index path:       {path}\n\
         schema version:   {schema_version}\n\
         block size:       {block_size}\n\
         strong hash:      {strong_hash}\n\
         weak hash:        {weak_hash}\n\
         index id:         {index_id}\n\
         filesystems:      {filesystems}\n\
         roots:            {roots} ({roots_complete} complete immutable)\n\
         inodes:           {inodes} ({inodes_incomplete} incomplete)\n\
         paths:            {paths}\n\
         blocks:           {blocks}\n\
         distinct weak:    {distinct_weak}\n\
         indexed bytes:    {indexed_bytes}\n\
         database bytes:   {database} (+{wal} WAL)\n",
        schema_version = meta.schema_version,
        block_size = meta.settings.block_size,
        strong_hash = meta.settings.strong_hash_id,
        weak_hash = meta.settings.weak_hash_id,
        filesystems = stats.filesystems,
        roots = stats.roots,
        roots_complete = stats.roots_complete,
        inodes = stats.inodes,
        inodes_incomplete = stats.inodes_incomplete,
        paths = stats.paths,
        blocks = stats.blocks,
        distinct_weak = stats.distinct_weak,
        indexed_bytes = stats.indexed_bytes,
        database = file_size(""),
        wal = file_size("-wal"),
    );
    if let Err(e) = fsops::stdout().write_all(report.as_bytes()) {
        die(format!("cannot write to stdout: {e}"));
    }

    if !meta.compatible(&Settings::from_config(config)) {
        warn(
            "the index settings differ from the configuration; run `hashsync update` to rebuild it",
        );
    }
    if !stats.deferred_block_index {
        warn("the deferred block index is missing; run `hashsync update`");
    }
}
