//! `hashsync stats` (DESIGN.md §8): a read-only report of the index.

use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use bstr::ByteSlice;

use crate::config::Config;
use crate::die::{die, warn};
use crate::fsops;
use crate::index::{self, Settings};

pub fn run(config: &Config) {
    // RocksDB keeps every file of the index open (DESIGN.md §4.4).
    fsops::raise_descriptor_limit();
    let path = config.index_path.as_bstr();
    let Some(index) = index::open_read_only(&config.index_path) else {
        die(format!("no index at {path}; run `hashsync update`"));
    };
    let Some(meta) = index.read_meta() else {
        die(format!("index {path} has no schema; run `hashsync update`"));
    };
    let stats = index.stats();
    // The lock keeps modifying processes away while the files are listed.
    let (database, wal) = file_sizes(&config.index_path);
    index.close();

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
         database bytes:   {database} (+{wal} write-ahead log)\n",
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
    );
    if let Err(e) = fsops::stdout().write_all(report.as_bytes()) {
        die(format!("cannot write to stdout: {e}"));
    }

    if !meta.compatible(&Settings::from_config(config)) {
        warn(
            "the index settings differ from the configuration; run `hashsync update` to rebuild it",
        );
    }
}

/// The bytes of the index directory's files: the write-ahead logs
/// (`*.log`) and all others. A failure to list them is an external
/// failure.
fn file_sizes(path: &[u8]) -> (u64, u64) {
    let dir = Path::new(OsStr::from_bytes(path));
    let fail =
        |e: std::io::Error| -> ! { die(format!("cannot list index {}: {e}", path.as_bstr())) };
    let (mut database, mut wal) = (0, 0);
    for entry in fsops::retry_interrupted(|| std::fs::read_dir(dir)).unwrap_or_else(|e| fail(e)) {
        let entry = entry.unwrap_or_else(|e| fail(e));
        let size = fsops::retry_interrupted(|| entry.metadata())
            .unwrap_or_else(|e| fail(e))
            .len();
        if entry.file_name().as_bytes().ends_with(b".log") {
            wal += size;
        } else {
            database += size;
        }
    }
    (database, wal)
}
