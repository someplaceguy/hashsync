//! The settings of one run of `hashsync ssh` (DESIGN.md §7 and §8).
//!
//! Everything about a run is an option of `hashsync ssh` on the client.
//! The client sends the command and these settings to the server in
//! REQUEST, and each host applies the settings that concern its roles. A
//! configuration file describes only its own host, so it is never
//! ambiguous which settings a run uses.

use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::process::Command;

use clap::ArgAction;

use crate::config::parse_size;
use crate::protocol::{COMPRESSION_LEVELS, Delivery};

/// The smallest default lookahead (DESIGN.md §6.3).
const DEFAULT_LOOKAHEAD: u64 = 256 << 20;

/// The streams of a run that are deduplicated, each against the index of
/// the host that receives it (DESIGN.md §6.6 and §8). A stream that is
/// not deduplicated is plain, and at least one of them is deduplicated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deduplicated {
    /// The backward stream, the command's stdout, against the client's
    /// index: a pull, the default.
    Stdout,
    /// The forward stream, the client's stdin, against the server's index:
    /// a push, with `--dedup-stdin`.
    Stdin,
    /// Both streams, with `--dedup-stdin --dedup-stdout`.
    Both,
}

impl Deduplicated {
    /// True when the forward stream, the client's stdin, is deduplicated.
    pub fn stdin(self) -> bool {
        self != Deduplicated::Stdout
    }

    /// True when the backward stream, the command's stdout, is
    /// deduplicated.
    pub fn stdout(self) -> bool {
        self != Deduplicated::Stdin
    }
}

/// The command that the server runs (DESIGN.md §6.1 and §8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteCommand {
    /// COMMAND and its ARGS joined with single spaces, as ssh joins them.
    /// The server runs this line with the user's shell and `-c`, as sshd
    /// does.
    Shell(Vec<u8>),
    /// `-s`: the words as they are. The server finds the first one in PATH
    /// and runs it without a shell.
    Argv(Vec<Vec<u8>>),
}

impl RemoteCommand {
    /// The command of `hashsync ssh`, from its COMMAND and ARGS: joined into
    /// one line for the server's shell, or the words as they are with `-s`
    /// (`no_shell`).
    pub fn from_words(words: &[OsString], no_shell: bool) -> Self {
        let words: Vec<Vec<u8>> = words.iter().map(|w| w.as_bytes().to_vec()).collect();
        if no_shell {
            RemoteCommand::Argv(words)
        } else {
            RemoteCommand::Shell(words.join(&b' '))
        }
    }

    /// The process that runs this command on the server. sshd runs a
    /// command line with the user's shell from the password database, and
    /// sets `$SHELL` to that shell, so the server takes `$SHELL`, or
    /// `/bin/sh` when it is unset or empty, as sshd does for an empty
    /// shell field (DESIGN.md §6.1).
    pub fn process(&self) -> Command {
        match self {
            RemoteCommand::Shell(line) => {
                let shell = std::env::var_os("SHELL")
                    .filter(|shell| !shell.is_empty())
                    .unwrap_or_else(|| "/bin/sh".into());
                let mut command = Command::new(shell);
                command.arg("-c").arg(OsStr::from_bytes(line));
                command
            }
            RemoteCommand::Argv(words) => {
                let mut command = Command::new(OsStr::from_bytes(&words[0]));
                command.args(words[1..].iter().map(|word| OsStr::from_bytes(word)));
                command
            }
        }
    }
}

/// The settings of a run that travel in REQUEST: the options of `hashsync
/// ssh` that the server may need, besides the command. The options that
/// only decide how the client reaches the server, such as `--ssh`, stay on
/// the client, and so does `-n`, which only changes what the client reads.
#[derive(clap::Args, Clone, Debug, PartialEq, Eq)]
pub struct RunSettings {
    /// Skip the update of each index that a stream is deduplicated against,
    /// and share read-only access to it with other runs
    #[arg(long)]
    pub no_update: bool,

    /// Scan the completed immutable roots of each index that a stream is
    /// deduplicated against again too, in its update
    #[arg(long, conflicts_with = "no_update")]
    pub scan_immutable: bool,

    /// Send stdin to the command in batch delivery: only from full buffers
    /// and at its end, not each time stdin waits, which compresses better,
    /// and keeps more references when stdin is deduplicated
    #[arg(long)]
    pub batch_stdin: bool,

    /// Send the command's stdout in batch delivery: only from full buffers
    /// and at its end, not each time it waits, which compresses better, and
    /// keeps more references when stdout is deduplicated
    #[arg(long)]
    pub batch_stdout: bool,

    /// Scan each deduplicated stream only once its sender has the whole
    /// hash list of its receiver, so that the stream sends the fewest
    /// bytes; meanwhile its bytes wait in the spool of its source. Without
    /// it, a deduplicated stream starts at once, and finds more blocks as
    /// the list arrives
    #[arg(long)]
    pub wait: bool,

    /// How far the scanner scans ahead of the bytes it has sent, at least
    /// the block size of the index that it scans against; it also flushes
    /// the stream once it has sent this many bytes since the last flush
    /// [default: the larger of 256MiB and three blocks]
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    pub lookahead: Option<u64>,

    /// The scanner's cache of the hashes that the index does not have
    #[arg(long, value_name = "SIZE", value_parser = parse_size, default_value = "128MiB")]
    pub negative_cache: u64,

    /// The threads that probe the hash table for the scanner; 0 selects
    /// half the cores of the host that scans
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub probe_threads: u32,

    /// The zstd level of the transfer, from 1 to 19; 0 disables compression
    #[arg(long, value_name = "N", value_parser = parse_compression_level, default_value_t = 9)]
    pub compression_level: i32,

    /// Disable zstd long-distance matching, which finds repeats in a 128 MiB
    /// window
    #[arg(long = "no-compression-long", action = ArgAction::SetFalse)]
    pub compression_long: bool,

    /// The zstd worker threads of each stream; 0 selects half the cores of
    /// the host that compresses
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub compression_threads: u32,

    /// The most bytes that wait in each spool before its producer waits, or
    /// "unlimited"
    #[arg(long, value_name = "SIZE", value_parser = parse_spool_limit, default_value = "2GiB")]
    pub spool_limit: u64,

    /// Keep no hash list in the cache of the host that scans, and remove
    /// the lists that earlier runs kept there
    #[arg(long = "no-sender-cache", action = ArgAction::SetFalse)]
    pub sender_cache: bool,
}

impl RunSettings {
    /// The delivery of the client's stdin to the command (DESIGN.md §6.2).
    pub fn stdin_delivery(&self) -> Delivery {
        delivery(self.batch_stdin)
    }

    /// The delivery of the command's stdout to the client's stdout.
    pub fn stdout_delivery(&self) -> Delivery {
        delivery(self.batch_stdout)
    }

    /// The lookahead of a scanner whose stream goes to a receiver with
    /// blocks of `block_size` bytes: `--lookahead`, or by default the
    /// larger of 256 MiB and three blocks, which leaves room for the probe
    /// threads: a stretch of windows splits among them only when it has
    /// two parts of `B` windows, after up to `B` literal bytes that wait for
    /// a whole LIT frame (DESIGN.md §6.3). A scan step needs a whole window
    /// of lookahead, so `--lookahead` below one block is a user error,
    /// which the client reports. `index` names the receiver's index in the
    /// message.
    pub fn lookahead(&self, block_size: u32, index: &str) -> Result<u64, String> {
        let block = u64::from(block_size);
        match self.lookahead {
            None => Ok(DEFAULT_LOOKAHEAD.max(3 * block)),
            Some(lookahead) if lookahead < block => Err(format!(
                "--lookahead must be at least the block size of {index} ({block} bytes)"
            )),
            Some(lookahead) => Ok(lookahead),
        }
    }
}

fn delivery(batch: bool) -> Delivery {
    match batch {
        true => Delivery::Batch,
        false => Delivery::Eager,
    }
}

fn parse_compression_level(text: &str) -> Result<i32, String> {
    text.parse()
        .ok()
        .filter(|level| COMPRESSION_LEVELS.contains(level))
        .ok_or_else(|| {
            format!(
                "expected a level from {} to {}",
                COMPRESSION_LEVELS.start(),
                COMPRESSION_LEVELS.end()
            )
        })
}

/// A spool limit: a size, or `unlimited` for none. No limit is `u64::MAX`
/// bytes, which no spool reaches, so the spools need no special case for
/// it (DESIGN.md §6.5).
pub fn parse_spool_limit(text: &str) -> Result<u64, String> {
    if text.trim().eq_ignore_ascii_case("unlimited") {
        return Ok(u64::MAX);
    }
    parse_size(text).map_err(|e| format!("{e} (or `unlimited`)"))
}

#[cfg(test)]
pub mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Options {
        #[command(flatten)]
        settings: RunSettings,
    }

    /// The settings of `hashsync ssh` with `options`.
    pub fn settings(options: &[&str]) -> RunSettings {
        let words = ["hashsync"].iter().chain(options);
        Options::try_parse_from(words).unwrap().settings
    }

    #[test]
    fn the_defaults() {
        let settings = settings(&[]);
        assert_eq!(
            settings,
            RunSettings {
                no_update: false,
                scan_immutable: false,
                batch_stdin: false,
                batch_stdout: false,
                wait: false,
                lookahead: None,
                negative_cache: 128 << 20,
                probe_threads: 0,
                compression_level: 9,
                compression_long: true,
                compression_threads: 0,
                spool_limit: 2 << 30,
                sender_cache: true,
            }
        );
        assert_eq!(settings.stdin_delivery(), Delivery::Eager);
        assert_eq!(settings.stdout_delivery(), Delivery::Eager);
    }

    #[test]
    fn options_set_each_setting() {
        let settings = settings(&[
            "--no-update",
            "--batch-stdin",
            "--batch-stdout",
            "--wait",
            "--lookahead",
            "1MiB",
            "--negative-cache",
            "0",
            "--probe-threads",
            "5",
            "--compression-level",
            "0",
            "--no-compression-long",
            "--compression-threads",
            "2",
            "--spool-limit",
            " Unlimited ",
            "--no-sender-cache",
        ]);
        assert!(settings.no_update);
        assert!(settings.wait);
        assert_eq!(settings.stdin_delivery(), Delivery::Batch);
        assert_eq!(settings.stdout_delivery(), Delivery::Batch);
        assert_eq!(settings.lookahead, Some(1 << 20));
        assert_eq!(settings.negative_cache, 0);
        assert_eq!(settings.probe_threads, 5);
        assert_eq!(settings.compression_level, 0);
        assert!(!settings.compression_long);
        assert_eq!(settings.compression_threads, 2);
        assert_eq!(settings.spool_limit, u64::MAX);
        assert!(!settings.sender_cache);
    }

    /// `--scan-immutable` changes the update of a receiver, which
    /// `--no-update` skips, so together they are a usage error.
    #[test]
    fn scan_immutable_needs_an_update() {
        assert!(settings(&["--scan-immutable"]).scan_immutable);
        let both = ["hashsync", "--scan-immutable", "--no-update"];
        assert!(Options::try_parse_from(both).is_err());
    }

    #[test]
    fn invalid_values_are_usage_errors() {
        for options in [
            &["--compression-level", "20"][..],
            &["--compression-level", "-1"],
            &["--spool-limit", "lots"],
            &["--lookahead", "1XB"],
            &["--probe-threads", "-1"],
        ] {
            let words = ["hashsync"].iter().chain(options);
            assert!(Options::try_parse_from(words).is_err(), "{options:?}");
        }
    }

    #[test]
    fn the_default_lookahead_has_room_for_three_blocks() {
        let defaults = settings(&[]);
        assert_eq!(defaults.lookahead(64 << 20, "x"), Ok(256 << 20));
        assert_eq!(defaults.lookahead(128 << 20, "x"), Ok(384 << 20));
        assert_eq!(
            defaults.lookahead(u32::MAX, "x"),
            Ok(3 * u64::from(u32::MAX))
        );
        // An explicit lookahead stays as it is, from one block on.
        let explicit = settings(&["--lookahead", "1GiB"]);
        assert_eq!(explicit.lookahead(512 << 20, "x"), Ok(1 << 30));
        assert_eq!(explicit.lookahead(1 << 30, "x"), Ok(1 << 30));
        let error = explicit
            .lookahead((1 << 30) + 1, "the server's index")
            .unwrap_err();
        assert!(
            error.contains("at least the block size of the server's index"),
            "{error}"
        );
    }

    #[test]
    fn a_command_is_one_line_for_the_shell_unless_it_keeps_its_words() {
        let words: Vec<OsString> = ["printf", "%s\\n", "a b", "$HOME"]
            .iter()
            .map(OsString::from)
            .collect();
        assert_eq!(
            RemoteCommand::from_words(&words, false),
            RemoteCommand::Shell(b"printf %s\\n a b $HOME".to_vec())
        );
        assert_eq!(
            RemoteCommand::from_words(&words, true),
            RemoteCommand::Argv(vec![
                b"printf".to_vec(),
                b"%s\\n".to_vec(),
                b"a b".to_vec(),
                b"$HOME".to_vec()
            ])
        );
    }
}
