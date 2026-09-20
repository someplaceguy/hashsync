//! Configuration (DESIGN.md §7).
//!
//! This milestone accepts the options that control indexing. Options of
//! later milestones are unknown fields and are rejected.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use bstr::ByteSlice;
use serde::Deserialize;

use crate::die::{EXIT_FAILURE, die};
use crate::fsops;
use crate::glob;
use crate::hash::StrongHash;

pub const MIN_BLOCK_SIZE: u64 = 4096;
pub const DEFAULT_BLOCK_SIZE: u32 = 32 * 1024;

#[derive(Debug)]
pub struct Config {
    pub block_size: u32,
    pub strong_hash: StrongHash,
    /// Path bytes of the index database.
    pub index_path: Vec<u8>,
    /// Scan threads; 0 selects the default count (DESIGN.md §4.6).
    pub threads: u32,
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

/// Load the configuration file, or die with a message.
pub fn load(path: Option<&OsStr>) -> Config {
    let path = match path {
        Some(p) => p.as_bytes().to_vec(),
        None => default_config_path(),
    };
    let shown = path.as_bstr();
    let text = match fsops::retry_interrupted(|| std::fs::read(OsStr::from_bytes(&path))) {
        Ok(bytes) => bytes,
        Err(e) => die(
            EXIT_FAILURE,
            format!("cannot read configuration file {shown}: {e}"),
        ),
    };
    let text = match String::from_utf8(text) {
        Ok(text) => text,
        Err(_) => die(
            EXIT_FAILURE,
            format!("configuration file {shown} is not UTF-8"),
        ),
    };
    match parse(&text) {
        Ok(config) => config,
        Err(message) => die(EXIT_FAILURE, format!("{shown}: {message}")),
    }
}

/// Parse and validate configuration text.
pub fn parse(text: &str) -> Result<Config, String> {
    let raw: RawConfig = toml::from_str(text).map_err(|e| e.to_string())?;
    let block_size = match raw.block_size {
        None => DEFAULT_BLOCK_SIZE,
        Some(size) => {
            let bytes = match size {
                Size::Bytes(n) => n,
                Size::Text(t) => parse_size(&t).map_err(|e| format!("block_size: {e}"))?,
            };
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
        Some(text) => {
            let decoded =
                decode_escapes(&text, Escapes::Plain).map_err(|e| format!("index_path: {e}"))?;
            check_no_nul(&decoded).map_err(|e| format!("index_path: {e}"))?;
            expand_tilde(decoded)
        }
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
        roots,
    })
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

/// Bytes that a backslash may quote in a glob pattern.
const GLOB_QUOTABLE: &[u8] = b"*?[]-!^\\";

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
        match next {
            b'x' => {
                let digits = bytes
                    .get(i + 2..i + 4)
                    .filter(|d| d.iter().all(u8::is_ascii_hexdigit))
                    .ok_or_else(|| {
                        "malformed \\x escape: expected two hexadecimal digits".to_string()
                    })?;
                let value = u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap();
                if mode == Escapes::Glob && GLOB_QUOTABLE.contains(&value) {
                    out.push(b'\\');
                }
                out.push(value);
                i += 4;
            }
            b'\\' => {
                if mode == Escapes::Glob {
                    out.push(b'\\');
                }
                out.push(b'\\');
                i += 2;
            }
            c if mode == Escapes::Glob && GLOB_QUOTABLE.contains(&c) => {
                out.push(b'\\');
                out.push(c);
                i += 2;
            }
            _ => {
                let shown = text[i + 1..].chars().next().unwrap();
                return Err(format!("unsupported escape \\{shown}"));
            }
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
        _ => die(EXIT_FAILURE, "HOME is not set"),
    }
}

/// `$VAR` when it is set to an absolute path, else `$HOME/<fallback>`.
fn xdg_dir(var: &str, fallback: &str) -> Vec<u8> {
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
    path.extend_from_slice(b"/hashsync/index.sqlite");
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
index_path = '/tmp/\xFF/index.sqlite'
threads = 24

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
        assert_eq!(config.index_path, b"/tmp/\xff/index.sqlite");
        assert_eq!(config.threads, 24);
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
        assert_eq!(config.roots[0].pattern, b"/srv/\xff/*");
        let config = parse("[[roots]]\npath = '/srv/\\xFF/*'\n").unwrap();
        assert_eq!(config.roots[0].pattern, b"/srv/\xff/*");
        assert!(parse("").unwrap().roots.is_empty());
        assert_eq!(parse("block_size = 8192").unwrap().block_size, 8192);
    }

    #[test]
    fn rejected_configurations() {
        assert!(
            parse("block_size = \"2KiB\"")
                .unwrap_err()
                .contains("at least")
        );
        assert!(
            parse("block_size = \"8GiB\"")
                .unwrap_err()
                .contains("at most")
        );
        assert!(parse("strong_hash = \"md5\"").is_err());
        assert!(parse("threads = -1").is_err());
        assert!(parse("lookahead = \"1MiB\"").is_err());
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
}
