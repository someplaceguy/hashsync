//! Configuration (DESIGN.md §7).
//!
//! Each command reads one configuration file: the file of a profile, the
//! one that `--profile` names or `default`, or a file anywhere, which
//! `--config` names. The server of a run reads the file that the client
//! names in REQUEST, by default its own file of the profile `default`.
//! Unless the file sets `index_path`, its index is named after it.
//! A configuration file describes only its own host: its roots and its
//! index, and where it keeps temporary data and cached lists. Everything
//! about a run is an option of `hashsync ssh` (`settings.rs`). Keys of
//! later milestones are unknown fields and are rejected.

use std::ffi::OsStr;
use std::fmt::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use bstr::ByteSlice;
use serde::Deserialize;

use crate::die::die;
use crate::fsops;
use crate::glob;
use crate::hash::StrongHash;
use crate::names;
use crate::report::{note, warn};

pub const MIN_BLOCK_SIZE: u64 = 512;
pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024;

/// 30 days (DESIGN.md §5.6), in seconds.
pub const DEFAULT_HASHLIST_MAX_AGE: u64 = 30 * 24 * 3600;

const DEFAULT_CANDIDATE_ATTEMPTS: u32 = 128;

/// The profile of a command without `--profile` or `--config` (DESIGN.md
/// §7).
const DEFAULT_PROFILE: &[u8] = b"default";

#[derive(Debug)]
pub struct Config {
    /// The option that selected this file, `--profile NAME` or `--config
    /// PATH`, for the messages that tell the user what to run; `None` for
    /// the default profile.
    selected_by: Option<String>,
    /// True when the file sets no `index_path`, so that its index is named
    /// after the file.
    index_named_after_file: bool,
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

/// The configuration file of a host, as the options of the run or the
/// command select it (DESIGN.md §7). A profile name is one nonempty file
/// name, as `check_profile_name` requires: the options that name profiles
/// check it, and so does the server's decoder of REQUEST.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// The profile `default`, which a host uses when no option selects its
    /// file: a command without `--profile` or `--config`, and the server of
    /// a run without `--server-profile` or `--server-config`. A host that
    /// needs no file can do without it.
    Default,
    /// The profile that `--profile` or `--server-profile` names.
    Profile(Vec<u8>),
    /// A file anywhere, which `--config` or `--server-config` names.
    File(Vec<u8>),
}

impl Source {
    /// The name of the profile, `default` without an option, or `None` for
    /// a file anywhere.
    fn profile(&self) -> Option<&[u8]> {
        match self {
            Source::Default => Some(DEFAULT_PROFILE),
            Source::Profile(name) => Some(name),
            Source::File(_) => None,
        }
    }

    /// The path of the file: `$XDG_CONFIG_HOME/hashsync/NAME.toml` for the
    /// profile NAME.
    fn path(&self) -> Vec<u8> {
        let profile: &[u8] = match self {
            Source::Default => DEFAULT_PROFILE,
            Source::Profile(name) => name,
            Source::File(path) => return path.clone(),
        };
        let mut path = xdg_dir("XDG_CONFIG_HOME", ".config");
        path.extend_from_slice(b"/hashsync/");
        path.extend_from_slice(profile);
        path.extend_from_slice(b".toml");
        path
    }

    /// The option that selects the file, for the commands that messages
    /// tell the user to run on this host; none for the profile `default`.
    /// The name or path follows `=`, so that one that starts with `-` stays
    /// the value of the option, as one shell word that gives back its bytes.
    fn option(&self) -> Option<String> {
        let (option, value) = match self {
            Source::Default => return None,
            Source::Profile(name) if name == DEFAULT_PROFILE => return None,
            Source::Profile(name) => ("--profile", name),
            Source::File(path) => ("--config", path),
        };
        Some(format!("{option}={}", shell_word(value)))
    }
}

/// Check that `name` is a profile name: one nonempty file name, which
/// becomes part of the path of the profile's file (DESIGN.md §7).
pub fn check_profile_name(name: &[u8]) -> Result<(), String> {
    if name.is_empty() || name.contains(&b'/') {
        return Err("a profile name must not be empty or contain '/'".into());
    }
    Ok(())
}

/// Load the configuration file that `source` selects, or die with a
/// message. A host that `uses_index` needs the file, for its roots and its
/// index. Otherwise the host uses only `tmp_data_dir` and
/// `hashlist_max_age`, which have defaults, so it can do without the file
/// of the profile `default`, which it uses when no option selects one. A
/// file that an option names must exist, so that a mistyped name fails
/// instead of quietly giving the defaults. A note then names the file, and
/// the index if the host uses it (`note_files`).
pub fn load(source: &Source, uses_index: bool) -> Config {
    let path = source.path();
    let shown = path.as_bstr();
    let (text, found) = match fsops::retry_interrupted(|| std::fs::read(OsStr::from_bytes(&path))) {
        Ok(bytes) => (bytes, true),
        Err(e)
            if !uses_index
                && matches!(source, Source::Default)
                && e.kind() == std::io::ErrorKind::NotFound =>
        {
            (Vec::new(), false)
        }
        Err(e) => die(format!("cannot read configuration file {shown}: {e}")),
    };
    let text = match String::from_utf8(text) {
        Ok(text) => text,
        Err(_) => die(format!("configuration file {shown} is not UTF-8")),
    };
    let config = match parse(&text, source) {
        Ok(config) => config,
        Err(message) => die(format!("{shown}: {message}")),
    };
    let index = uses_index.then_some(config.index_path.as_slice());
    note_files(source, &path, found, index);
    // The files of profiles share one directory, so their names are
    // distinct, and so are the names of their indexes. A file anywhere can
    // have the name of another one, such as the file of a profile, and then
    // shares its index.
    if uses_index && config.index_named_after_file && matches!(source, Source::File(_)) {
        warn(format!(
            "{shown} sets no index_path, so its index is {}, as for any other \
             configuration file of the same name; set index_path to avoid sharing it by accident",
            config.index_path.as_bstr()
        ));
    }
    config
}

/// Say in a note which configuration file a command uses, the one at `path`
/// that `source` selects, and the `index` that it uses, if any. Every
/// command says so when it loads its file, on both hosts of a run, so that
/// the user sees which files the options selected (DESIGN.md §7). A file
/// that is not `found` is one of `default` that a host without an index
/// does without. Paths in the home directory start with `~`, which keeps
/// the line short.
fn note_files(source: &Source, path: &[u8], found: bool, index: Option<&[u8]>) {
    let home = std::env::var_os("HOME").unwrap_or_default();
    let shown = |path: &[u8]| with_tilde(path, home.as_bytes());
    let path = shown(path);
    let file = match source.profile() {
        None => format!("config {path}"),
        Some(name) if found => format!("profile `{}` [{path}]", name.as_bstr()),
        Some(name) => format!("profile `{}` [{path} not found]", name.as_bstr()),
    };
    match index {
        Some(index) => note(format_args!("using {file} and index {}", shown(index))),
        None => note(format_args!("using {file}")),
    }
}

impl Config {
    /// The command that updates the index of this configuration, for the
    /// messages that tell the user to run it.
    pub fn update_command(&self) -> String {
        match &self.selected_by {
            None => "hashsync update".into(),
            Some(option) => format!("hashsync {option} update"),
        }
    }
}

/// `bytes` as one POSIX shell word that gives back exactly these bytes, for
/// the commands that messages tell the user to run. Bytes that need no
/// quoting stay as they are, and printable text goes in single quotes.
/// Anything else goes in `$'...'` quotes, with octal escapes for control
/// characters and for bytes that are not UTF-8: a message is text, which
/// cannot hold such bytes, and it stays one line that a copy loses nothing
/// of.
fn shell_word(bytes: &[u8]) -> String {
    let plain = |b: &u8| b.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(b);
    if !bytes.is_empty() && bytes.iter().all(plain) {
        return bytes.iter().copied().map(char::from).collect();
    }
    if let Ok(text) = std::str::from_utf8(bytes)
        && !text.contains(char::is_control)
    {
        return format!("'{}'", text.replace('\'', r"'\''"));
    }
    let mut word = String::from("$'");
    let escape = |word: &mut String, bytes: &[u8]| {
        for byte in bytes {
            write!(word, "\\{byte:03o}").unwrap();
        }
    };
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '\\' | '\'' => {
                    word.push('\\');
                    word.push(c);
                }
                c if c.is_control() => escape(&mut word, c.encode_utf8(&mut [0; 4]).as_bytes()),
                c => word.push(c),
            }
        }
        escape(&mut word, chunk.invalid());
    }
    word.push('\'');
    word
}

/// Parse and validate the text of the configuration file that `source`
/// selects.
pub fn parse(text: &str, source: &Source) -> Result<Config, String> {
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
    let index_named_after_file = raw.index_path.is_none();
    let index_path = match raw.index_path {
        None => default_index_path(source)?,
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
        selected_by: source.option(),
        index_named_after_file,
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

/// `path` with `~` in place of `home`, the home directory, when the path
/// is in it. With a home of `/`, paths stay as they are, or every absolute
/// path would start with `~`.
fn with_tilde(path: &[u8], home: &[u8]) -> String {
    let home = home.trim_end_with(|c| c == '/');
    if home.first() == Some(&b'/')
        && let Some(rest) = path.strip_prefix(home)
        && (rest.is_empty() || rest.first() == Some(&b'/'))
    {
        return format!("~{}", rest.as_bstr());
    }
    path.as_bstr().to_string()
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

/// `$XDG_CACHE_HOME/hashsync/STEM.db`: the index of a configuration file
/// that sets no `index_path`, named after the file without its extension,
/// so `NAME.db` for the profile NAME. The index holds only hints, which an
/// update rebuilds from the roots, so it goes in the cache with the other
/// files that hashsync creates (DESIGN.md §10).
fn default_index_path(source: &Source) -> Result<Vec<u8>, String> {
    let file = source.path();
    let Some(stem) = Path::new(OsStr::from_bytes(&file)).file_stem() else {
        return Err("the file has no name to give its index; set index_path".into());
    };
    let mut path = xdg_dir("XDG_CACHE_HOME", ".cache");
    path.extend_from_slice(b"/hashsync/");
    path.extend_from_slice(stem.as_bytes());
    path.extend_from_slice(b".db");
    Ok(path)
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

    /// The configuration of `text` as the file of the default profile.
    fn parse(text: &str) -> Result<Config, String> {
        super::parse(text, &Source::Default)
    }

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

    fn profile(name: &str) -> Source {
        Source::Profile(name.as_bytes().to_vec())
    }

    /// A profile names its configuration file. Unless a file sets
    /// `index_path`, its index is named after it, without its extension,
    /// and the messages that ask for an update name the option that selects
    /// the file, none for the profile `default` (DESIGN.md §7).
    #[test]
    fn indexes_are_named_after_their_files() {
        assert!(profile("work").path().ends_with(b"/hashsync/work.toml"));
        assert!(Source::Default.path().ends_with(b"/hashsync/default.toml"));
        let default = parse("").unwrap();
        assert!(default.index_path.ends_with(b"/hashsync/default.db"));
        assert_eq!(default.update_command(), "hashsync update");
        let named_default = super::parse("", &profile("default")).unwrap();
        assert_eq!(named_default.update_command(), "hashsync update");
        let work = super::parse("", &profile("work")).unwrap();
        assert!(work.index_path.ends_with(b"/hashsync/work.db"));
        assert_eq!(work.update_command(), "hashsync --profile=work update");
        // A name with dots keeps them.
        let dotted = super::parse("", &profile(".a.b")).unwrap();
        assert!(dotted.index_path.ends_with(b"/hashsync/.a.b.db"));

        let file = Source::File(b"/etc/hashsync/nas.conf".to_vec());
        let nas = super::parse("", &file).unwrap();
        assert!(nas.index_path.ends_with(b"/hashsync/nas.db"));
        assert!(nas.index_named_after_file);
        assert_eq!(
            nas.update_command(),
            "hashsync --config=/etc/hashsync/nas.conf update"
        );
        let set = super::parse("index_path = '/srv/nas.db'", &file).unwrap();
        assert_eq!(set.index_path, b"/srv/nas.db");
        assert!(!set.index_named_after_file);
    }

    /// The note of a run shows a path in the home directory with `~`, but
    /// not one in another directory whose name starts with the same bytes.
    #[test]
    fn notes_show_the_home_directory_as_a_tilde() {
        let home = b"/home/u";
        let cache = b"/home/u/.cache/hashsync/a.db";
        assert_eq!(with_tilde(cache, home), "~/.cache/hashsync/a.db");
        assert_eq!(with_tilde(b"/home/u", home), "~");
        assert_eq!(with_tilde(b"/home/u/x", b"/home/u//"), "~/x");
        assert_eq!(with_tilde(b"/home/user/x", home), "/home/user/x");
        assert_eq!(with_tilde(b"x.toml", home), "x.toml");
        assert_eq!(with_tilde(b"/etc/x", b"/"), "/etc/x");
        assert_eq!(with_tilde(b"/etc/x", b""), "/etc/x");
        assert_eq!(with_tilde(b"/home/u/\xff", home), "~/\u{fffd}");
    }

    /// A profile name becomes part of the path of its file, so it is one
    /// nonempty file name (DESIGN.md §7).
    #[test]
    fn profile_names_are_file_names() {
        for name in [&b"work"[..], b"a.b", b"..", b"\xff", b"-x"] {
            assert_eq!(check_profile_name(name), Ok(()), "{:?}", name.as_bstr());
        }
        for name in [&b""[..], b"a/b", b"/", b"../x"] {
            assert!(check_profile_name(name).is_err(), "{:?}", name.as_bstr());
        }
    }

    /// The command of a message is one that the user can copy into a
    /// shell: the name or path follows `=`, so that a leading `-` stays its
    /// value, in one shell word that gives back every byte of it.
    #[test]
    fn update_commands_give_back_names_and_paths() {
        for (name, word) in [
            (&b"work"[..], "work"),
            (b"work files", "'work files'"),
            (b"-x", "-x"),
            (b"it's", r"'it'\''s'"),
            ("café".as_bytes(), "'café'"),
            (b"a\xffb", r"$'a\377b'"),
            (b"tab\there\\'", r"$'tab\011here\\\''"),
        ] {
            let profile = Source::Profile(name.to_vec());
            let config = super::parse("index_path = '/i'", &profile).unwrap();
            let command = format!("hashsync --profile={word} update");
            assert_eq!(config.update_command(), command);
        }
        let file = Source::File(b"my dir/nas.conf".to_vec());
        let config = super::parse("index_path = '/i'", &file).unwrap();
        let command = "hashsync --config='my dir/nas.conf' update";
        assert_eq!(config.update_command(), command);
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
