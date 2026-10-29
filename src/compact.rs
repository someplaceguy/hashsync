// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! `hashsync compact` (DESIGN.md §4.4 and §8): compact the index, as a
//! prune does at its end, so that the entries that updates and prunes
//! removed leave the database files. An update with `--compact` ends with
//! the same compaction, also the update of a run's receiver after the
//! command.

use std::time::Instant;

use crate::config::Config;
use crate::fsops;
use crate::index::{self, Access, Index};
use crate::report::note;

pub fn run(config: &Config) {
    // RocksDB keeps every file of the index open (DESIGN.md §4.4).
    fsops::raise_descriptor_limit();
    let index = index::open_current(config, Access::Modifying);
    perform(&index);
    index.close();
}

/// Compact `index`, the read/write index of a modifying process, at the
/// end of its work: a prune, an update with `--compact`, or `hashsync
/// compact`. A note says how long the compaction took, apart from the work
/// before it: on a large index, it can take seconds.
pub fn perform(index: &Index) {
    let start = Instant::now();
    index.compact();
    note(format_args!(
        "compacted index in {:.1}s",
        start.elapsed().as_secs_f64()
    ));
}
