//! Configuration (DESIGN.md §7).
//!
//! This milestone accepts the options that control indexing, transfers,
//! block references, and compression. Options of later milestones are
//! unknown fields and are rejected.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use bstr::ByteSlice;
use serde::Deserialize;

use crate::die::die;
use crate::fsops;
use crate::glob;
use crate::hash::StrongHash;
use crate::protocol::COMPRESSION_LEVELS;
use crate::ssh;

pub const MIN_BLOCK_SIZE: u64 = 512;
pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024;

/// 90 days (DESIGN.md §5.6), in seconds.
pub const DEFAULT_HASHLIST_MAX_AGE: u64 = 90 * 24 * 3600;

const DEFAULT_LOOKAHEAD: u64 = 256 << 20;
const DEFAULT_NEGATIVE_CACHE: u64 = 128 << 20;
const DEFAULT_CANDIDATE_ATTEMPTS: u32 = 128;
const DEFAULT_COMPRESSION_LEVEL: i32 = 9;

#[derive(Debug)]
pub struct Config {
    pub block_size: u32,
    pub strong_hash: StrongHash,
    /// Path bytes of the index database.
    pub index_path: Vec<u8>,
    /// Scan threads; 0 selects the default count (DESIGN.md §4.6).
    pub threads: u32,
    /// Hash-list cache expiry, in seconds (DESIGN.md §5.6).
    pub hashlist_max_age: u64,
    /// How far the sender scans ahead of its output, in bytes; at least
    /// `block_size` (DESIGN.md §6.3).
    pub lookahead: u64,
    /// The sender's cache of hashes answered no, in bytes.
    pub negative_cache: u64,
    /// The sender's weak-table probe threads; 0 selects half the sender's
    /// cores, at least one (DESIGN.md §6.3).
    pub probe_threads: u32,
    /// Candidates tried per query; at least 1 (DESIGN.md §6.4).
    pub candidate_attempts: u32,
    /// The zstd level of the sender's data stream; 0 disables compression
    /// (DESIGN.md §6.2).
    pub compression_level: i32,
    /// zstd long-distance matching in a 128 MiB window, for repeats
    /// farther back than the window of the level (DESIGN.md §7).
    pub compression_long: bool,
    /// The sender's compression threads; 0 selects half the sender's
    /// cores, at least one.
    pub compression_threads: u32,
    /// Path bytes of the directory that holds the receiver's pin file.
    pub pin_directory: Vec<u8>,
    /// The transfer options a host override can replace; read them
    /// through `transfer_options`.
    ssh: Vec<Vec<u8>>,
    sender_command: Vec<u8>,
    sender_cache: bool,
    hosts: Vec<HostConfig>,
    pub roots: Vec<RootConfig>,
}

/// `[hosts.NAME]`: overrides keyed by the run command's HOST argument.
#[derive(Debug)]
struct HostConfig {
    name: String,
    ssh: Option<Vec<Vec<u8>>>,
    sender_command: Option<Vec<u8>>,
    sender_cache: Option<bool>,
}

/// The transfer options of one run, after host overrides and CLI
/// precedence (DESIGN.md §8).
#[derive(Debug)]
pub struct TransferOptions {
    /// The shell-split SSH command; the first word is the executable.
    pub ssh: Vec<Vec<u8>>,
    /// One executable name or path, not a shell fragment.
    pub sender_command: Vec<u8>,
    pub sender_cache: bool,
}

impl Config {
    /// Resolve the transfer options for `host`. `cli_ssh` is `--ssh`,
    /// which takes precedence over a host override, which takes
    /// precedence over the global options. CLI arguments get no byte
    /// escapes (DESIGN.md §7).
    pub fn transfer_options(&self, host: &[u8], cli_ssh: Option<&[u8]>) -> TransferOptions {
        let overrides = self.hosts.iter().find(|h| h.name.as_bytes() == host);
        let ssh = match cli_ssh {
            Some(text) => match split_ssh_words(text) {
                Ok(words) => words,
                Err(e) => die(format!("--ssh: {e}")),
            },
            None => overrides
                .and_then(|h| h.ssh.clone())
                .unwrap_or_else(|| self.ssh.clone()),
        };
        TransferOptions {
            ssh,
            sender_command: overrides
                .and_then(|h| h.sender_command.clone())
                .unwrap_or_else(|| self.sender_command.clone()),
            sender_cache: overrides
                .and_then(|h| h.sender_cache)
                .unwrap_or(self.sender_cache),
        }
    }
}

#[derive(Debug)]
pub struct RootConfig {
    /// Normalized absolute glob pattern with glob quoting preserved.
    pub pattern: Vec<u8>,
    pub exclude: Exclusions,
    pub one_filesystem: bool,
}

/// The exclusion patterns of one root. Patterns beginning with `/` match
/// absolute paths; others match basenames.
#[derive(Default, Debug)]
pub struct Exclusions {
    names: Vec<Vec<u8>>,
    paths: Vec<Vec<u8>>,
}

impl Exclusions {
    /// True when an entry with basename `name` is excluded. `abs_path` is
    /// called only when a pattern needs the absolute path.
    pub fn excludes(&self, name: &[u8], abs_path: impl FnOnce() -> Vec<u8>) -> bool {
        if self.names.iter().any(|p| glob::matches(p, name)) {
            return true;
        }
        if self.paths.is_empty() {
            return false;
        }
        let path = abs_path();
        self.paths.iter().any(|p| glob::matches(p, &path))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    block_size: Option<Size>,
    strong_hash: Option<StrongHash>,
    index_path: Option<String>,
    threads: Option<u32>,
    hashlist_max_age: Option<String>,
    lookahead: Option<Size>,
    negative_cache: Option<Size>,
    probe_threads: Option<u32>,
    candidate_attempts: Option<u32>,
    compression_level: Option<i32>,
    compression_long: Option<bool>,
    compression_threads: Option<u32>,
    pin_directory: Option<String>,
    ssh: Option<String>,
    sender_command: Option<String>,
    sender_cache: Option<bool>,
    #[serde(default)]
    hosts: std::collections::BTreeMap<String, RawHost>,
    #[serde(default)]
    roots: Vec<RawRoot>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHost {
    ssh: Option<String>,
    sender_command: Option<String>,
    sender_cache: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoot {
    path: String,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    one_filesystem: bool,
    #[serde(default)]
    immutable: bool,
    identifier: Option<toml::Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Size {
    Bytes(u64),
    Text(String),
}

/// Load the configuration file, or die with a message.
pub fn load(path: Option<&OsStr>) -> Config {
    let path = match path {
        Some(p) => p.as_bytes().to_vec(),
        None => default_config_path(),
    };
    let shown = path.as_bstr();
    let text = match fsops::retry_interrupted(|| std::fs::read(OsStr::from_bytes(&path))) {
        Ok(bytes) => bytes,
        Err(e) => die(format!("cannot read configuration file {shown}: {e}")),
    };
    let text = match String::from_utf8(text) {
        Ok(text) => text,
        Err(_) => die(format!("configuration file {shown} is not UTF-8")),
    };
    match parse(&text) {
        Ok(config) => config,
        Err(message) => die(format!("{shown}: {message}")),
    }
}

/// Parse and validate configuration text.
pub fn parse(text: &str) -> Result<Config, String> {
    let raw: RawConfig = toml::from_str(text).map_err(|e| e.to_string())?;
    let block_size = match raw.block_size {
        None => DEFAULT_BLOCK_SIZE,
        Some(size) => {
            let bytes = size.bytes().map_err(|e| format!("block_size: {e}"))?;
            if bytes < MIN_BLOCK_SIZE {
                return Err(format!(
                    "block_size must be at least {MIN_BLOCK_SIZE} bytes"
                ));
            }
            u32::try_from(bytes)
                .map_err(|_| format!("block_size must be at most {} bytes", u32::MAX))?
        }
    };
    let index_path = match raw.index_path {
        None => default_index_path(),
        Some(text) => parse_path(&text).map_err(|e| format!("index_path: {e}"))?,
    };
    let lookahead = match raw.lookahead {
        None => DEFAULT_LOOKAHEAD,
        Some(size) => size.bytes().map_err(|e| format!("lookahead: {e}"))?,
    };
    // A scan step needs a whole window of lookahead (DESIGN.md §6.3).
    if lookahead < u64::from(block_size) {
        return Err(format!(
            "lookahead must be at least block_size ({block_size} bytes)"
        ));
    }
    let negative_cache = match raw.negative_cache {
        None => DEFAULT_NEGATIVE_CACHE,
        Some(size) => size.bytes().map_err(|e| format!("negative_cache: {e}"))?,
    };
    let candidate_attempts = raw.candidate_attempts.unwrap_or(DEFAULT_CANDIDATE_ATTEMPTS);
    if candidate_attempts == 0 {
        return Err("candidate_attempts must be at least 1".into());
    }
    let compression_level = raw.compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL);
    if !COMPRESSION_LEVELS.contains(&compression_level) {
        return Err(format!(
            "compression_level must be from {} to {}",
            COMPRESSION_LEVELS.start(),
            COMPRESSION_LEVELS.end()
        ));
    }
    let pin_directory = match raw.pin_directory {
        None => default_pin_directory(),
        Some(text) => parse_path(&text).map_err(|e| format!("pin_directory: {e}"))?,
    };
    let hashlist_max_age = match raw.hashlist_max_age {
        None => DEFAULT_HASHLIST_MAX_AGE,
        Some(text) => parse_duration(&text).map_err(|e| format!("hashlist_max_age: {e}"))?,
    };
    let ssh = match raw.ssh {
        None => vec![b"ssh".to_vec()],
        Some(text) => parse_ssh(&text).map_err(|e| format!("ssh: {e}"))?,
    };
    let sender_command = match raw.sender_command {
        None => b"hashsync".to_vec(),
        Some(text) => parse_sender_command(&text).map_err(|e| format!("sender_command: {e}"))?,
    };
    let mut hosts = Vec::with_capacity(raw.hosts.len());
    for (name, host) in raw.hosts {
        let parsed = HostConfig {
            ssh: host
                .ssh
                .map(|text| parse_ssh(&text))
                .transpose()
                .map_err(|e| format!("hosts.{name}.ssh: {e}"))?,
            sender_command: host
                .sender_command
                .map(|text| parse_sender_command(&text))
                .transpose()
                .map_err(|e| format!("hosts.{name}.sender_command: {e}"))?,
            sender_cache: host.sender_cache,
            name,
        };
        hosts.push(parsed);
    }
    let mut roots = Vec::with_capacity(raw.roots.len());
    for (i, root) in raw.roots.into_iter().enumerate() {
        roots.push(parse_root(root).map_err(|e| format!("roots[{i}]: {e}"))?);
    }
    Ok(Config {
        block_size,
        strong_hash: raw.strong_hash.unwrap_or(StrongHash::DEFAULT),
        index_path,
        threads: raw.threads.unwrap_or(0),
        hashlist_max_age,
        lookahead,
        negative_cache,
        probe_threads: raw.probe_threads.unwrap_or(0),
        candidate_attempts,
        compression_level,
        compression_long: raw.compression_long.unwrap_or(true),
        compression_threads: raw.compression_threads.unwrap_or(0),
        pin_directory,
        ssh,
        sender_command,
        sender_cache: raw.sender_cache.unwrap_or(true),
        hosts,
        roots,
    })
}

impl Size {
    fn bytes(self) -> Result<u64, String> {
        match self {
            Size::Bytes(n) => Ok(n),
            Size::Text(text) => parse_size(&text),
        }
    }
}

/// A configured path: byte escapes, no NUL, and a leading `~` for HOME.
fn parse_path(text: &str) -> Result<Vec<u8>, String> {
    let decoded = decode_escapes(text, Escapes::Plain)?;
    check_no_nul(&decoded)?;
    Ok(expand_tilde(decoded))
}

/// A configured `ssh` command: shell-split first, then decode the byte
/// escapes of each argv word (DESIGN.md §7).
fn parse_ssh(text: &str) -> Result<Vec<Vec<u8>>, String> {
    let words = ssh::shell_split(text.as_bytes())?;
    words
        .into_iter()
        .map(|word| {
            // Splitting UTF-8 text at ASCII delimiters keeps every word
            // UTF-8.
            let word = decode_escapes(std::str::from_utf8(&word).unwrap(), Escapes::Plain)?;
            check_no_nul(&word)?;
            Ok(word)
        })
        .collect::<Result<Vec<_>, String>>()
        .and_then(require_command)
}

/// `--ssh` from the command line: shell-split without byte escapes
/// (DESIGN.md §7: no configuration escapes for CLI arguments).
fn split_ssh_words(text: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    ssh::shell_split(text).and_then(require_command)
}

fn require_command(words: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, String> {
    if words.is_empty() {
        return Err("empty command".into());
    }
    Ok(words)
}

/// `sender_command` is one executable name or path, not a shell fragment
/// (DESIGN.md §6.1).
fn parse_sender_command(text: &str) -> Result<Vec<u8>, String> {
    let decoded = decode_escapes(text, Escapes::Plain)?;
    check_no_nul(&decoded)?;
    if decoded.is_empty() {
        return Err("empty command".into());
    }
    Ok(decoded)
}

/// Parse a duration such as `90d`, `12h`, `30m`, or `3600` (seconds).
pub fn parse_duration(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let digits_end = text
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(text.len());
    if digits_end == 0 {
        return Err(format!("expected a number: {text:?}"));
    }
    let number: u64 = text[..digits_end]
        .parse()
        .map_err(|_| format!("number too large: {text:?}"))?;
    let multiplier: u64 = match text[digits_end..].trim() {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 24 * 3600,
        unit => return Err(format!("unknown unit {unit:?}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("duration too large: {text:?}"))
}

fn parse_root(raw: RawRoot) -> Result<RootConfig, String> {
    if raw.immutable || raw.identifier.is_some() {
        return Err("immutable roots are not supported by this version".into());
    }
    let pattern = decode_escapes(&raw.path, Escapes::Glob).map_err(|e| format!("path: {e}"))?;
    check_no_nul(&pattern).map_err(|e| format!("path: {e}"))?;
    if pattern.first() != Some(&b'/') {
        return Err("path: root patterns must be absolute".into());
    }
    let mut normalized = Vec::with_capacity(pattern.len());
    for component in glob::components(&pattern) {
        if component == b"." || component == b".." {
            return Err("path: `.` and `..` components are not allowed".into());
        }
        normalized.push(b'/');
        normalized.extend_from_slice(component);
    }
    if normalized.is_empty() {
        normalized.push(b'/');
    }
    let mut exclude = Exclusions::default();
    for text in &raw.exclude {
        let pattern = decode_escapes(text, Escapes::Glob).map_err(|e| format!("exclude: {e}"))?;
        check_no_nul(&pattern).map_err(|e| format!("exclude: {e}"))?;
        if pattern.is_empty() {
            return Err("exclude: empty pattern".into());
        }
        if pattern[0] == b'/' {
            exclude.paths.push(pattern);
        } else {
            exclude.names.push(pattern);
        }
    }
    Ok(RootConfig {
        pattern: normalized,
        exclude,
        one_filesystem: raw.one_filesystem,
    })
}

/// How to decode a byte-string field.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Escapes {
    /// `\xHH` and `\\` only.
    Plain,
    /// Also `\*`, `\?`, `\[`, `\]`, `\-`, `\!`, and `\^`, kept as glob
    /// quoting. Decoded bytes that are glob metacharacters are quoted.
    Glob,
}

/// Decode the byte escapes of DESIGN.md §7 after TOML parsing.
pub fn decode_escapes(text: &str, mode: Escapes) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        let Some(&next) = bytes.get(i + 1) else {
            return Err("trailing backslash".into());
        };
        let value = match next {
            b'x' => {
                let digits = bytes
                    .get(i + 2..i + 4)
                    .filter(|d| d.iter().all(u8::is_ascii_hexdigit))
                    .ok_or_else(|| {
                        "malformed \\x escape: expected two hexadecimal digits".to_string()
                    })?;
                i += 4;
                u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap()
            }
            b'\\' => {
                i += 2;
                b'\\'
            }
            c if mode == Escapes::Glob && glob::QUOTABLE.contains(&c) => {
                i += 2;
                c
            }
            _ => {
                let shown = text[i + 1..].chars().next().unwrap();
                return Err(format!("unsupported escape \\{shown}"));
            }
        };
        match mode {
            Escapes::Glob => glob::push_quoted(&mut out, value),
            Escapes::Plain => out.push(value),
        }
    }
    Ok(out)
}

fn check_no_nul(bytes: &[u8]) -> Result<(), String> {
    if bytes.contains(&0) {
        return Err("NUL byte is not allowed".into());
    }
    Ok(())
}

/// Parse a size such as `32KiB`, `1MB`, or `4096`.
pub fn parse_size(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let digits_end = text
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(text.len());
    if digits_end == 0 {
        return Err(format!("expected a number: {text:?}"));
    }
    let number: u64 = text[..digits_end]
        .parse()
        .map_err(|_| format!("number too large: {text:?}"))?;
    let unit = text[digits_end..].trim();
    let multiplier: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        _ => return Err(format!("unknown unit {unit:?}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size too large: {text:?}"))
}

fn home_dir() -> Vec<u8> {
    match std::env::var_os("HOME") {
        Some(home) if !home.is_empty() => home.as_bytes().to_vec(),
        _ => die("HOME is not set"),
    }
}

/// `$VAR` when it is set to an absolute path, else `$HOME/<fallback>`.
pub fn xdg_dir(var: &str, fallback: &str) -> Vec<u8> {
    if let Some(value) = std::env::var_os(var)
        && value.as_bytes().first() == Some(&b'/')
    {
        return value.as_bytes().to_vec();
    }
    let mut dir = home_dir();
    dir.push(b'/');
    dir.extend_from_slice(fallback.as_bytes());
    dir
}

pub fn default_config_path() -> Vec<u8> {
    let mut path = xdg_dir("XDG_CONFIG_HOME", ".config");
    path.extend_from_slice(b"/hashsync/config.toml");
    path
}

pub fn default_index_path() -> Vec<u8> {
    let mut path = xdg_dir("XDG_STATE_HOME", ".local/state");
    path.extend_from_slice(b"/hashsync/default.db");
    path
}

fn default_pin_directory() -> Vec<u8> {
    let mut path = xdg_dir("XDG_CACHE_HOME", ".cache");
    path.extend_from_slice(b"/hashsync/pins");
    path
}

fn expand_tilde(path: Vec<u8>) -> Vec<u8> {
    if path == b"~" {
        return home_dir();
    }
    if let Some(rest) = path.strip_prefix(b"~/") {
        let mut expanded = home_dir();
        expanded.push(b'/');
        expanded.extend_from_slice(rest);
        return expanded;
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("32KiB").unwrap(), 32 * 1024);
        assert_eq!(parse_size("32 kib").unwrap(), 32 * 1024);
        assert_eq!(parse_size("1MiB").unwrap(), 1 << 20);
        assert_eq!(parse_size("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_size("2GiB").unwrap(), 2 << 30);
        assert!(parse_size("KiB").is_err());
        assert!(parse_size("1XB").is_err());
        assert!(parse_size("99999999999999999999KiB").is_err());
    }

    #[test]
    fn plain_escapes() {
        assert_eq!(decode_escapes("abc", Escapes::Plain).unwrap(), b"abc");
        assert_eq!(
            decode_escapes(r"a\xFFb", Escapes::Plain).unwrap(),
            b"a\xffb"
        );
        assert_eq!(
            decode_escapes(r"a\xffb", Escapes::Plain).unwrap(),
            b"a\xffb"
        );
        assert_eq!(decode_escapes(r"a\\b", Escapes::Plain).unwrap(), b"a\\b");
        assert_eq!(decode_escapes("é", Escapes::Plain).unwrap(), "é".as_bytes());
        assert!(decode_escapes(r"a\", Escapes::Plain).is_err());
        assert!(decode_escapes(r"a\xF", Escapes::Plain).is_err());
        assert!(decode_escapes(r"a\xGG", Escapes::Plain).is_err());
        assert!(decode_escapes(r"a\*", Escapes::Plain).is_err());
        assert!(decode_escapes(r"a\n", Escapes::Plain).is_err());
        assert!(decode_escapes(r"\101", Escapes::Plain).is_err());
        // Decoded bytes are never decoded again.
        assert_eq!(
            decode_escapes(r"\x5Cx41", Escapes::Plain).unwrap(),
            b"\\x41"
        );
    }

    #[test]
    fn glob_escapes_keep_quoting() {
        assert_eq!(decode_escapes(r"\x2A", Escapes::Glob).unwrap(), b"\\*");
        assert_eq!(decode_escapes(r"\*", Escapes::Glob).unwrap(), b"\\*");
        assert_eq!(
            decode_escapes(r"\?\[\]\-\!\^", Escapes::Glob).unwrap(),
            b"\\?\\[\\]\\-\\!\\^"
        );
        assert_eq!(
            decode_escapes(r"back\\slash", Escapes::Glob).unwrap(),
            b"back\\\\slash"
        );
        assert_eq!(decode_escapes(r"\x5C", Escapes::Glob).unwrap(), b"\\\\");
        assert_eq!(decode_escapes(r"\x2F", Escapes::Glob).unwrap(), b"/");
        assert_eq!(decode_escapes(r"\xFF*", Escapes::Glob).unwrap(), b"\xff*");
        assert!(decode_escapes(r"a\n", Escapes::Glob).is_err());
        // The quoted forms match literally.
        assert!(glob::matches(
            &decode_escapes(r"literal\x2Aname", Escapes::Glob).unwrap(),
            b"literal*name"
        ));
        assert!(!glob::matches(
            &decode_escapes(r"literal\x2Aname", Escapes::Glob).unwrap(),
            b"literalXname"
        ));
        assert!(glob::matches(
            &decode_escapes(r"back\\slash", Escapes::Glob).unwrap(),
            b"back\\slash"
        ));
        assert!(glob::matches(
            &decode_escapes(r"[\x2D]", Escapes::Glob).unwrap(),
            b"-"
        ));
    }

    #[test]
    fn parse_full_config() {
        let config = parse(
            r#"
block_size = "64KiB"
strong_hash = "sha256"
index_path = '/tmp/\xFF/default.db'
threads = 24
lookahead = "1MiB"
negative_cache = 0
probe_threads = 5
candidate_attempts = 3
compression_level = 19
compression_long = false
compression_threads = 2
pin_directory = '/var/\xFE/pins'

[[roots]]
path = '/srv/archive-\xFF'
exclude = ['*.tmp', 'literal\x2Aname', 'back\\slash', '/srv/archive-\xFF/skip']

[[roots]]
path = "/nix//store/*/"
one_filesystem = true
"#,
        )
        .unwrap();
        assert_eq!(config.block_size, 64 * 1024);
        assert_eq!(config.strong_hash, StrongHash::Sha256);
        assert_eq!(config.index_path, b"/tmp/\xff/default.db");
        assert_eq!(config.threads, 24);
        assert_eq!(config.lookahead, 1 << 20);
        assert_eq!(config.negative_cache, 0);
        assert_eq!(config.probe_threads, 5);
        assert_eq!(config.candidate_attempts, 3);
        assert_eq!(config.compression_level, 19);
        assert!(!config.compression_long);
        assert_eq!(config.compression_threads, 2);
        assert_eq!(config.pin_directory, b"/var/\xfe/pins");
        assert_eq!(config.roots.len(), 2);
        assert_eq!(config.roots[0].pattern, b"/srv/archive-\xff");
        let exclude = &config.roots[0].exclude;
        assert_eq!(exclude.names.len(), 3);
        assert_eq!(exclude.paths.len(), 1);
        let never = || panic!("basename patterns must not need the path");
        assert!(exclude.excludes(b"x.tmp", never));
        assert!(exclude.excludes(b"literal*name", never));
        assert!(!exclude.excludes(b"literalXname", || {
            b"/srv/archive-\xff/literalXname".to_vec()
        }));
        assert!(exclude.excludes(b"back\\slash", never));
        assert!(exclude.excludes(b"skip", || b"/srv/archive-\xff/skip".to_vec()));
        assert!(!exclude.excludes(b"skip", || b"/srv/archive-\xff/sub/skip".to_vec()));
        assert!(config.roots[1].exclude.names.is_empty());
        assert!(config.roots[1].exclude.paths.is_empty());
        assert_eq!(config.roots[1].pattern, b"/nix/store/*");
        assert!(config.roots[1].one_filesystem);
        assert!(!config.roots[0].one_filesystem);
    }

    #[test]
    fn defaults_and_double_quoted_strings() {
        let config = parse("[[roots]]\npath = \"/srv/\\\\xFF/*\"\n").unwrap();
        assert_eq!(config.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(config.strong_hash, StrongHash::Blake3);
        assert_eq!(config.threads, 0);
        assert_eq!(config.lookahead, 256 << 20);
        assert_eq!(config.negative_cache, 128 << 20);
        assert_eq!(config.probe_threads, 0);
        assert_eq!(config.candidate_attempts, 128);
        assert_eq!(config.compression_level, 9);
        assert!(config.compression_long);
        assert_eq!(config.compression_threads, 0);
        assert!(config.pin_directory.ends_with(b"/hashsync/pins"));
        assert_eq!(parse("compression_level = 0").unwrap().compression_level, 0);
        assert_eq!(config.roots[0].pattern, b"/srv/\xff/*");
        let config = parse("[[roots]]\npath = '/srv/\\xFF/*'\n").unwrap();
        assert_eq!(config.roots[0].pattern, b"/srv/\xff/*");
        assert!(parse("").unwrap().roots.is_empty());
        assert_eq!(parse("block_size = 8192").unwrap().block_size, 8192);
        assert_eq!(parse("block_size = 512").unwrap().block_size, 512);
    }

    #[test]
    fn rejected_configurations() {
        assert!(
            parse("block_size = 511")
                .unwrap_err()
                .contains("at least 512 bytes")
        );
        assert!(
            parse("block_size = \"8GiB\"")
                .unwrap_err()
                .contains("at most")
        );
        assert!(parse("strong_hash = \"md5\"").is_err());
        assert!(parse("threads = -1").is_err());
        assert!(
            parse("lookahead = \"4095\"")
                .unwrap_err()
                .contains("lookahead must be at least block_size")
        );
        assert!(
            parse("block_size = \"64KiB\"\nlookahead = \"32KiB\"")
                .unwrap_err()
                .contains("at least block_size")
        );
        assert!(
            parse("candidate_attempts = 0")
                .unwrap_err()
                .contains("at least 1")
        );
        assert!(parse("negative_cache = \"lots\"").is_err());
        assert!(parse("probe_threads = -1").is_err());
        assert!(
            parse("pin_directory = '/a\\x00b'")
                .unwrap_err()
                .contains("NUL")
        );
        for level in ["-1", "20"] {
            assert!(
                parse(&format!("compression_level = {level}"))
                    .unwrap_err()
                    .contains("compression_level must be from 0 to 19")
            );
        }
        assert!(parse("compression_threads = -1").is_err());
        assert!(
            parse("[[roots]]\npath = \"relative\"")
                .unwrap_err()
                .contains("absolute")
        );
        assert!(
            parse("[[roots]]\npath = \"/a/../b\"")
                .unwrap_err()
                .contains("..")
        );
        assert!(
            parse("[[roots]]\npath = \"/a\"\nimmutable = true")
                .unwrap_err()
                .contains("immutable")
        );
        assert!(
            parse("[[roots]]\npath = \"/a\"\nidentifier = \"name\"")
                .unwrap_err()
                .contains("immutable")
        );
        assert!(parse("[[roots]]\npath = \"/a\"\nbogus = 1").is_err());
        assert!(
            parse("[[roots]]\npath = '/a\\x00b'")
                .unwrap_err()
                .contains("NUL")
        );
        assert!(
            parse("[[roots]]\npath = '/a'\nexclude = ['']")
                .unwrap_err()
                .contains("empty")
        );
        assert!(
            parse("[[roots]]\npath = '/a\\'")
                .unwrap_err()
                .contains("trailing")
        );
    }

    #[test]
    fn root_slash() {
        let config = parse("[[roots]]\npath = \"/\"").unwrap();
        assert_eq!(config.roots[0].pattern, b"/");
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90d").unwrap(), 90 * 24 * 3600);
        assert_eq!(parse_duration("12h").unwrap(), 12 * 3600);
        assert_eq!(parse_duration("30 m").unwrap(), 30 * 60);
        assert_eq!(parse_duration("45s").unwrap(), 45);
        assert_eq!(parse_duration("3600").unwrap(), 3600);
        assert!(parse_duration("d").is_err());
        assert!(parse_duration("5w").is_err());
        assert!(parse_duration("99999999999999999999d").is_err());
    }

    #[test]
    fn transfer_defaults() {
        let config = parse("").unwrap();
        assert_eq!(config.hashlist_max_age, DEFAULT_HASHLIST_MAX_AGE);
        let options = config.transfer_options(b"anyhost", None);
        assert_eq!(options.ssh, vec![b"ssh".to_vec()]);
        assert_eq!(options.sender_command, b"hashsync");
        assert!(options.sender_cache);
    }

    #[test]
    fn host_overrides_and_cli_precedence() {
        let config = parse(
            r#"
ssh = "ssh -i '/key file'"
sender_command = 'hashsync'
sender_cache = true
hashlist_max_age = "1d"

[hosts.builder]
ssh = 'ssh -p 2222'
sender_command = '/opt/bin\x20dir/hashsync'
sender_cache = false
"#,
        )
        .unwrap();
        assert_eq!(config.hashlist_max_age, 24 * 3600);

        let global = config.transfer_options(b"other", None);
        assert_eq!(
            global.ssh,
            vec![b"ssh".to_vec(), b"-i".to_vec(), b"/key file".to_vec()]
        );
        assert_eq!(global.sender_command, b"hashsync");
        assert!(global.sender_cache);

        // The override table is keyed by the HOST argument as given.
        let host = config.transfer_options(b"builder", None);
        assert_eq!(
            host.ssh,
            vec![b"ssh".to_vec(), b"-p".to_vec(), b"2222".to_vec()]
        );
        assert_eq!(host.sender_command, b"/opt/bin dir/hashsync");
        assert!(!host.sender_cache);
        assert!(
            config.transfer_options(b"user@builder", None).sender_cache,
            "the key must match the whole HOST argument"
        );

        // `--ssh` wins over the override, without byte escapes.
        let cli = config.transfer_options(b"builder", Some(br"ssh -o 'X\x41'"));
        assert_eq!(
            cli.ssh,
            vec![b"ssh".to_vec(), b"-o".to_vec(), br"X\x41".to_vec()]
        );
        assert!(!cli.sender_cache, "--ssh replaces only the SSH command");
    }

    #[test]
    fn rejected_transfer_configurations() {
        assert!(parse("ssh = \"\"").unwrap_err().contains("empty"));
        assert!(parse("ssh = \"ssh 'x\"").unwrap_err().contains("quote"));
        assert!(parse("sender_command = ''").unwrap_err().contains("empty"));
        assert!(
            parse("sender_command = 'a\\x00b'")
                .unwrap_err()
                .contains("NUL")
        );
        assert!(
            parse("hashlist_max_age = \"soon\"")
                .unwrap_err()
                .contains("hashlist_max_age")
        );
        assert!(parse("[hosts.h]\nbogus = 1").is_err());
        // Inside shell double quotes the backslash survives splitting
        // and reaches the byte-escape decoder, which rejects `\q`.
        assert!(
            parse("[hosts.h]\nssh = 'x \"\\q\"'")
                .unwrap_err()
                .contains("hosts.h.ssh")
        );
    }
}
