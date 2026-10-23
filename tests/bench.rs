//! `hashsync bench` (DESIGN.md §8).

use std::process::Command;

/// The lines of `hashsync bench --block-size 4KiB` with `options`, split
/// into words, with whole bytes and plain numbers. The command reads no
/// configuration file, so it has no note to print.
fn bench(options: &[&str]) -> Vec<Vec<String>> {
    let output = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .args(["--no-human-readable", "bench", "--block-size", "4KiB"])
        .args(options)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8(output.stdout).unwrap();
    stdout
        .lines()
        .map(|line| line.split_whitespace().map(String::from).collect())
        .collect()
}

/// Check that `words` are `count` rates, each a whole number of bytes and
/// its unit, `B/s`.
fn assert_rates(words: &[String], count: usize) {
    assert_eq!(words.len(), 2 * count, "{words:?}");
    for rate in words.chunks(2) {
        assert!(rate[0].parse::<u64>().unwrap() > 0, "{words:?}");
        assert_eq!(rate[1], "B/s", "{words:?}");
    }
}

/// With `--block-size`, the table of the hashes has a header and one line,
/// for that block size, with the rate of the weak hash of whole blocks, of
/// the rolling weak hash, and of each strong hash. After an empty line, the
/// table of the scanner has a header and one line per list size, with the
/// rate of the search on one thread and on the probe threads.
#[test]
fn bench_measures_the_given_block_size() {
    let lines = bench(&["--probe-threads", "2"]);
    let [hashes, size, empty, scans, small, large] = &lines[..] else {
        panic!("{lines:?}");
    };
    assert_eq!(
        hashes[..],
        ["block", "size", "weak", "rolling", "blake3", "sha256"]
    );
    // Without human-readable output, the block size shows as whole bytes,
    // and the list sizes as plain numbers.
    assert_eq!(size[..2], ["4096", "B"]);
    assert_rates(&size[2..], 4);
    assert!(empty.is_empty(), "{lines:?}");
    assert_eq!(scans[..], ["list", "values", "1", "thread", "2", "threads"]);
    assert_eq!(small[0], "10000");
    assert_rates(&small[1..], 2);
    assert_eq!(large[0], "10000000");
    assert_rates(&large[1..], 2);
}

/// With one probe thread, the scanner probes on its own thread, so the
/// table of the scanner has only the rate on one thread.
#[test]
fn bench_with_one_probe_thread_has_one_scan_rate() {
    let lines = bench(&["--probe-threads", "1"]);
    let [.., scans, small, large] = &lines[..] else {
        panic!("{lines:?}");
    };
    assert_eq!(scans[..], ["list", "values", "1", "thread"]);
    assert_eq!(small[0], "10000");
    assert_rates(&small[1..], 1);
    assert_eq!(large[0], "10000000");
    assert_rates(&large[1..], 1);
}
