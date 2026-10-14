//! Configuration (DESIGN.md §7).
//!
//! A configuration file describes only its own host: its roots and its
//! index, and where it keeps temporary data and cached lists. Everything
//! about a run is an option of `hashsync ssh` (`settings.rs`). Keys of
//! later milestones are unknown fields and are rejected.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use bstr::ByteSlice;
use serde::Deserialize;

use crate::die::die;
use crate::fsops;
use crate::glob;
use crate::hash::StrongHash;
use crate::names;

pub const MIN_BLOCK_SIZE: u64 = 512;
pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024;

/// 30 days (DESIGN.md §5.6), in seconds.
pub const DEFAULT_HASHLIST_MAX_AGE: u64 = 30 * 24 * 3600;

const DEFAULT_CANDIDATE_ATTEMPTS: u32 = 128;

#[derive(Debug)]
pub struct Config {
    pub block_size: u32,
    pub strong_hash: StrongHash,
    /// Path bytes of the index database.
    pub index_path: Vec<u8>,
    /// Scan threads; 0 selects the default count (DESIGN.md §4.6).
    pub threads: u32,
    /// The age after which this host's list cache drops a list, in
    /// seconds (DESIGN.md §5.6).
    pub hashlist_max_age: u64,
    /// Candidates tried per query; at least 1 (DESIGN.md §6.4).
    pub candidate_attempts: u32,
    /// Path bytes of this host's tmp_data directory, which holds the
    /// segments of the pins and of the spools (DESIGN.md §6.5).
    pub tmp_data_dir: Vec<u8>,
    pub roots: Vec<RootConfig>,
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
    candidate_attempts: Option<u32>,
    tmp_data_dir: Option<String>,
    #[serde(default)]
    roots: Vec<RawRoot>,
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

/// Load the configuration file, `path` or the default one, or die with a
/// message. The commands that use the index need the file, for the roots
/// and the index.
pub fn load(path: Option<&OsStr>) -> Config {
    load_from(path, false)
}

/// Load the configuration of one host of `hashsync ssh` (DESIGN.md §7), as
/// `load` does, unless the host `receives` no deduplicated stream: such a
/// host uses only `tmp_data_dir` and `hashlist_max_age`, which have
/// defaults, so a missing default file gives the default configuration. A
/// file that `--config` names must exist.
pub fn load_for_run(path: Option<&OsStr>, receives: bool) -> Config {
    load_from(path, !receives)
}

fn load_from(path: Option<&OsStr>, optional: bool) -> Config {
    let (path, default) = match path {
        Some(p) => (p.as_bytes().to_vec(), false),
        None => (default_config_path(), true),
    };
    let shown = path.as_bstr();
    let text = match fsops::retry_interrupted(|| std::fs::read(OsStr::from_bytes(&path))) {
        Ok(bytes) => bytes,
        Err(e) if optional && default && e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
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
    let candidate_attempts = raw.candidate_attempts.unwrap_or(DEFAULT_CANDIDATE_ATTEMPTS);
    if candidate_attempts == 0 {
        return Err("candidate_attempts must be at least 1".into());
    }
    let tmp_data_dir = match raw.tmp_data_dir {
        None => default_tmp_data_dir(),
        Some(text) => parse_path(&text).map_err(|e| format!("tmp_data_dir: {e}"))?,
    };
    let hashlist_max_age = match raw.hashlist_max_age {
        None => DEFAULT_HASHLIST_MAX_AGE,
        Some(text) => parse_duration(&text).map_err(|e| format!("hashlist_max_age: {e}"))?,
    };
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
        candidate_attempts,
        tmp_data_dir,
        roots,
    })
}

/// `requested` threads, or half the cores of this host, at least one, for
/// 0: the count of probe threads, and of the worker threads of a zstd
/// stream (DESIGN.md §7).
pub fn thread_count(requested: u32) -> u32 {
    match requested {
        0 => std::thread::available_parallelism()
            .map_or(1, |n| u32::try_from(n.get() / 2).unwrap_or(u32::MAX).max(1)),
        n => n,
    }
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
                let [value] = bytes
                    .get(i + 2..i + 4)
                    .and_then(names::parse_hex)
                    .ok_or_else(|| {
                        "malformed \\x escape: expected two hexadecimal digits".to_string()
                    })?;
                i += 4;
                value
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

/// `$XDG_CACHE_HOME/hashsync/tmp-data`: the default tmp_data directory
/// (DESIGN.md §6.5).
fn default_tmp_data_dir() -> Vec<u8> {
    let mut path = xdg_dir("XDG_CACHE_HOME", ".cache");
    path.extend_from_slice(b"/hashsync/tmp-data");
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
hashlist_max_age = "1d"
candidate_attempts = 3
tmp_data_dir = '/var/\xFE/tmp'

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
        assert_eq!(config.hashlist_max_age, 24 * 3600);
        assert_eq!(config.candidate_attempts, 3);
        assert_eq!(config.tmp_data_dir, b"/var/\xfe/tmp");
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
        assert_eq!(config.hashlist_max_age, DEFAULT_HASHLIST_MAX_AGE);
        assert_eq!(config.candidate_attempts, 128);
        assert!(config.tmp_data_dir.ends_with(b"/hashsync/tmp-data"));
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
            parse("candidate_attempts = 0")
                .unwrap_err()
                .contains("at least 1")
        );
        assert!(
            parse("tmp_data_dir = '/a\\x00b'")
                .unwrap_err()
                .contains("tmp_data_dir: NUL byte is not allowed")
        );
        assert!(
            parse("hashlist_max_age = \"soon\"")
                .unwrap_err()
                .contains("hashlist_max_age")
        );
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
        assert!(parse("bogus = 1").unwrap_err().contains("unknown field"));
        // Every setting of a run is an option of `hashsync ssh`, never a key
        // (DESIGN.md §7), so a file that holds one fails like any other
        // unknown key.
        for setting in ["compression_level = 3", "lookahead = \"1MiB\""] {
            assert!(
                parse(setting).unwrap_err().contains("unknown field"),
                "{setting}"
            );
        }
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
}
