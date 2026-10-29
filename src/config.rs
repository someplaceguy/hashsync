// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

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

use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;

use serde::Deserialize;

use crate::die::die;
use crate::escapes::{self, Syntax, quoted, quoted_from_home, quoted_pattern};
use crate::fsops;
use crate::glob;
use crate::hash::StrongHash;
use crate::report::{note, warn};

pub const MIN_BLOCK_SIZE: u64 = 512;
/// The smallest block size that an update does not warn about. Smaller
/// blocks work, but they reduce performance a lot.
pub const MIN_RECOMMENDED_BLOCK_SIZE: u32 = 4 * 1024;
pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024;

/// 30 days (DESIGN.md §5.6), in seconds.
pub const DEFAULT_HASHLIST_MAX_AGE: u64 = 30 * 24 * 3600;
/// The smallest `hashlist_max_age` that gets no warning, an hour. Expiry
/// cannot tell a temporary that a failed run left from one that a run in
/// progress is still writing, and a shorter age can remove the latter
/// (DESIGN.md §7).
const MIN_RECOMMENDED_HASHLIST_MAX_AGE: u64 = 3600;

const DEFAULT_CANDIDATE_ATTEMPTS: u32 = 128;

/// The profile of a command without `--profile` or `--config` (DESIGN.md
/// §7).
const DEFAULT_PROFILE: &[u8] = b"default";

#[derive(Debug)]
pub struct Config {
    /// How the options selected this file: for the messages that tell the
    /// user what to run, and for a child process that reads it again.
    pub source: Source,
    /// True when the file sets no `index_path`, so that its index is named
    /// after the file.
    index_named_after_file: bool,
    pub block_size: u32,
    pub strong_hash: StrongHash,
    /// Path bytes of the index database. Messages show it with `~` for the
    /// home directory (`escapes::quoted_from_home`), as the note of `load`
    /// that names the index does, so that the same index does not appear
    /// as two different paths.
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
    /// How an immutable root names the group of each of its matches;
    /// `None` for a mutable root (DESIGN.md §4.2).
    pub identifier: Option<Identifier>,
    /// A root that does not exist is normal: it gets no warning, and when
    /// it vanishes after root expansion, the scan of its group takes the
    /// next copy instead (DESIGN.md §4.2). It is not part of the indexing
    /// policy, so copies can differ in it.
    pub may_vanish: bool,
}

impl RootConfig {
    /// What differs between the indexing policies of this root and
    /// `other`: their exclusions, compared as sets of patterns, or their
    /// `one_filesystem` values. This function exists because copies of an
    /// immutable group must index the same content, whichever copy an
    /// update scans (DESIGN.md §4.2). The patterns compare as bytes after
    /// byte-escape decoding and normalization (`parse_exclusions`), with
    /// their glob quoting: two globs that match the same names but differ
    /// in their bytes still differ.
    pub fn policy_difference(&self, other: &RootConfig) -> Option<&'static str> {
        if self.exclude != other.exclude {
            Some("exclusions")
        } else if self.one_filesystem != other.one_filesystem {
            Some("one_filesystem values")
        } else {
            None
        }
    }
}

/// How an immutable root names the group of each match of its pattern
/// (DESIGN.md §4.2). Matches with the same identifier are copies of one
/// group, which the index holds once.
#[derive(Debug, PartialEq, Eq)]
pub enum Identifier {
    /// The absolute path of the match, the default.
    Path,
    /// The last component of the path, so that copies under different
    /// parent directories share it.
    Name,
    /// The path of the match without this prefix, a normalized absolute
    /// path, and still absolute: with the prefix `/mnt/disk`, the match
    /// `/mnt/disk/nix/store/x` gives `/nix/store/x`, the identifier that
    /// `Path` gives the copy at `/nix/store/x`. The prefix ends at a
    /// component, and the prefix itself gives `/`.
    StripPrefix(Vec<u8>),
    /// The same bytes for every match, which stay when the tree moves.
    Literal(Vec<u8>),
}

impl Identifier {
    /// The identifier of the match at `path`, a normalized absolute path.
    /// Identifiers are never empty, so a match that cannot have one is a
    /// configuration error; the error says why, as the end of a sentence
    /// about the match.
    pub fn of(&self, path: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Identifier::Path => Ok(path.to_vec()),
            Identifier::Name => match path.rsplit(|&b| b == b'/').next() {
                Some(name) if !name.is_empty() => Ok(name.to_vec()),
                _ => Err("has no name for its identifier".into()),
            },
            Identifier::StripPrefix(prefix) => {
                // Every path is below `/`, and stays as it is.
                let below = match prefix.as_slice() {
                    b"/" => Some(path),
                    prefix => path
                        .strip_prefix(prefix)
                        .filter(|rest| matches!(rest.first(), None | Some(b'/'))),
                };
                match below {
                    Some(b"") => Ok(b"/".to_vec()),
                    Some(rest) => Ok(rest.to_vec()),
                    None => Err(format!(
                        "is not below the strip_prefix {} of its identifier",
                        quoted(prefix)
                    )),
                }
            }
            Identifier::Literal(bytes) => Ok(bytes.clone()),
        }
    }
}

/// The exclusion patterns of one root, normalized (`parse_exclusions`).
/// Patterns beginning with `/` match absolute paths; others match
/// basenames. Each kind is sorted and without duplicates, so that equal
/// sets of patterns compare equal (`RootConfig::policy_difference`).
#[derive(Default, Debug, PartialEq, Eq)]
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
    block_size: Option<Amount>,
    strong_hash: Option<String>,
    index_path: Option<String>,
    threads: Option<u32>,
    hashlist_max_age: Option<Amount>,
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
    #[serde(default)]
    may_vanish: bool,
}

/// A size or a duration in the file: an integer, of bytes or seconds, or a
/// string, which can give a unit (`parse_size`, `parse_duration`). Without
/// `expecting`, a value of another type, such as a negative integer, would
/// get serde's message for an untagged enum, which names this type.
#[derive(Deserialize)]
#[serde(untagged, expecting = "expected a nonnegative integer or a string")]
enum Amount {
    Number(u64),
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
        Some(format!("{option}={}", escapes::shell_word(value)))
    }

    /// The argument that selects the file in a child `hashsync`, which
    /// resolves it as this process did, in the same environment and working
    /// directory; none for the profile `default`. The name or path follows
    /// `=`, as in `option`.
    pub fn arg(&self) -> Option<OsString> {
        let (option, value) = match self {
            Source::Default => return None,
            Source::Profile(name) => ("--profile=", name),
            Source::File(path) => ("--config=", path),
        };
        Some(OsString::from_vec([option.as_bytes(), value].concat()))
    }
}

/// Check that `name` is a profile name: one nonempty file name, which
/// becomes part of the path of the profile's file (DESIGN.md §7). No
/// command-line argument holds a NUL byte, but REQUEST could.
pub fn check_profile_name(name: &[u8]) -> Result<(), String> {
    if name.is_empty() || name.contains(&b'/') {
        return Err("a profile name must not be empty or contain '/'".into());
    }
    check_no_nul(name)
}

/// Load the configuration file that `source` selects, or die with a
/// message. A host that `uses_index` needs the file, for its roots and its
/// index. Otherwise the host uses only `tmp_data_dir` and
/// `hashlist_max_age`, which have defaults, so it can do without the file
/// of the profile `default`, which it uses when no option selects one. A
/// file that an option names must exist, so that a mistyped name fails
/// instead of quietly giving the defaults. A note then names the file, and
/// the index if the host uses it (`note_files`), and a warning follows if
/// the file sets a `hashlist_max_age` below an hour.
pub fn load(source: &Source, uses_index: bool) -> Config {
    let path = source.path();
    let (config, found) = read(source, &path, uses_index);
    let index = uses_index.then_some(config.index_path.as_slice());
    note_files(source, &path, found, index);
    if config.hashlist_max_age < MIN_RECOMMENDED_HASHLIST_MAX_AGE {
        warn(format!(
            "{} sets hashlist_max_age below an hour, so expiry can remove the temporary \
             files of a `hashsync ssh` that is still running",
            quoted_from_home(&path)
        ));
    }
    // The files of profiles share one directory, so their names are
    // distinct, and so are the names of their indexes. A file anywhere can
    // have the name of another one, such as the file of a profile, and then
    // shares its index. The warning names the file and the index as the
    // note before it does.
    if uses_index && config.index_named_after_file && matches!(source, Source::File(_)) {
        warn(format!(
            "{} sets no index_path, so its index is {}, as for any other \
             configuration file of the same name; set index_path to avoid sharing it by accident",
            quoted_from_home(&path),
            quoted_from_home(&config.index_path)
        ));
    }
    config
}

/// Load the file that `source` selects again, for the index of a run's
/// receiver, in the child that updates it after the command (DESIGN.md
/// §3): as `load` does, but without the note and the warnings, which the
/// run gave when it loaded the file at its start.
pub fn reload(source: &Source) -> Config {
    read(source, &source.path(), true).0
}

/// Read and parse the file at `path`, which `source` selects, or die with a
/// message; also return whether the file was found. Only a host that does
/// not use an index can do without the file of the profile `default`.
fn read(source: &Source, path: &[u8], uses_index: bool) -> (Config, bool) {
    let shown = quoted(path);
    let (text, found) = match fsops::retry_interrupted(|| std::fs::read(OsStr::from_bytes(path))) {
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
    match parse(&text, source) {
        Ok(config) => (config, found),
        Err(message) => die(format!("{shown}: {message}")),
    }
}

/// Say in a note which configuration file a command uses, the one at `path`
/// that `source` selects, and the `index` that it uses, if any. Every
/// command says so when it loads its file, on both hosts of a run, so that
/// the user sees which files the options selected (DESIGN.md §7). A file
/// that is not `found` is one of `default` that a host without an index
/// does without. Paths in the home directory start with `~`, which keeps
/// the line short.
fn note_files(source: &Source, path: &[u8], found: bool, index: Option<&[u8]>) {
    let path = quoted_from_home(path);
    let file = match source.profile() {
        None => format!("config {path}"),
        Some(name) if found => format!("profile {} [{path}]", quoted(name)),
        Some(name) => format!("profile {} [{path} not found]", quoted(name)),
    };
    match index {
        Some(index) => note(format_args!(
            "using {file} and index {}",
            quoted_from_home(index)
        )),
        None => note(format_args!("using {file}")),
    }
}

impl Config {
    /// The command that updates the index of this configuration, for the
    /// messages that tell the user to run it.
    pub fn update_command(&self) -> String {
        match self.source.option() {
            None => "hashsync update".into(),
            Some(option) => format!("hashsync {option} update"),
        }
    }
}

/// Parse and validate the text of the configuration file that `source`
/// selects.
pub fn parse(text: &str, source: &Source) -> Result<Config, String> {
    let raw: RawConfig = toml::from_str(text).map_err(|e| toml_error(text, &e))?;
    let block_size = match raw.block_size {
        None => DEFAULT_BLOCK_SIZE,
        Some(size) => {
            let bytes = size
                .value(parse_size)
                .map_err(|e| format!("block_size: {e}"))?;
            block_size(bytes).map_err(|e| format!("block_size {e}"))?
        }
    };
    // The name is parsed here and not by serde, so that the error shows it
    // as messages show every value of the file (DESIGN.md §8).
    let strong_hash = match raw.strong_hash {
        None => StrongHash::DEFAULT,
        Some(name) => StrongHash::from_id(&name).ok_or_else(|| {
            let names: Vec<String> = StrongHash::ALL
                .iter()
                .map(|hash| format!("`{}`", hash.id()))
                .collect();
            format!(
                "strong_hash: unknown hash {}, expected {}",
                quoted(name.as_bytes()),
                names.join(" or ")
            )
        })?,
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
        Some(age) => age
            .value(parse_duration)
            .map_err(|e| format!("hashlist_max_age: {e}"))?,
    };
    // Expiry compares whole seconds, so an age of 0 would remove every file
    // older than the current second: the temporaries of runs in progress,
    // even ones only milliseconds old, and at the end of each exchange,
    // all lists but those that the exchange renewed within that second.
    // What a user who sets 0 most likely wants, a whole list every time,
    // is what `--no-sender-cache` does (DESIGN.md §7).
    if hashlist_max_age == 0 {
        let instead =
            "to send the whole hash list every time, use `hashsync ssh --no-sender-cache`";
        return Err(format!(
            "hashlist_max_age must be at least 1 second; {instead}"
        ));
    }
    let mut roots = Vec::with_capacity(raw.roots.len());
    for (i, root) in raw.roots.into_iter().enumerate() {
        roots.push(parse_root(root).map_err(|e| format!("roots[{i}]: {e}"))?);
    }
    Ok(Config {
        source: source.clone(),
        index_named_after_file,
        block_size,
        strong_hash,
        index_path,
        threads: raw.threads.unwrap_or(0),
        hashlist_max_age,
        candidate_attempts,
        tmp_data_dir,
        roots,
    })
}

/// `requested` threads, or one per core of this host for 0: the count of
/// probe threads, and of the worker threads of a zstd stream (DESIGN.md
/// §8).
pub fn thread_count(requested: u32) -> u32 {
    match requested {
        0 => std::thread::available_parallelism()
            .map_or(1, |n| u32::try_from(n.get()).unwrap_or(u32::MAX)),
        n => n,
    }
}

impl Amount {
    /// The number of bytes or seconds, which `parse_text` reads from a
    /// string.
    fn value(self, parse_text: fn(&str) -> Result<u64, String>) -> Result<u64, String> {
        match self {
            Amount::Number(n) => Ok(n),
            Amount::Text(text) => parse_text(&text),
        }
    }
}

/// A TOML error in `text` as one line: its line and column, then what it
/// is. Every report is one line (`report`), and the error's own text
/// quotes the line of the file on lines of their own.
fn toml_error(text: &str, e: &toml::de::Error) -> String {
    let Some(span) = e.span() else {
        return e.message().to_string();
    };
    let before = &text.as_bytes()[..span.start.min(text.len())];
    let line = before.iter().filter(|&&b| b == b'\n').count() + 1;
    let start = before
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |i| i + 1);
    // The column counts characters: a byte that continues a UTF-8
    // character starts none.
    let column = before[start..]
        .iter()
        .filter(|&&b| b & 0xC0 != 0x80)
        .count()
        + 1;
    format!("line {line}, column {column}: {}", e.message())
}

/// A configured path: byte escapes and no NUL. A path that is `~` or that
/// starts with `~/` starts at the home directory. Only a `~` as it is does:
/// `\x7E` is a literal `~`, as every escape gives a literal byte, so that a
/// relative path whose first directory is named `~` can be written as
/// messages show it (DESIGN.md §7 and §8).
fn parse_path(text: &str) -> Result<Vec<u8>, String> {
    let (mut path, rest) = match escapes::starts_at_home(text.as_bytes()) {
        true => (home_dir(), &text[1..]),
        false => (Vec::new(), text),
    };
    path.extend(escapes::decode(rest, Syntax::Plain)?);
    check_no_nul(&path)?;
    Ok(path)
}

/// Parse a duration such as `90d`, `12h`, `30m`, or `3600` (seconds).
pub fn parse_duration(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let digits_end = text
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(text.len());
    if digits_end == 0 {
        return Err(format!("expected a number: {}", quoted(text.as_bytes())));
    }
    let number: u64 = text[..digits_end]
        .parse()
        .map_err(|_| format!("number too large: {}", quoted(text.as_bytes())))?;
    let multiplier: u64 = match text[digits_end..].trim() {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 24 * 3600,
        unit => return Err(format!("unknown unit {}", quoted(unit.as_bytes()))),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("duration too large: {}", quoted(text.as_bytes())))
}

fn parse_root(raw: RawRoot) -> Result<RootConfig, String> {
    let identifier = match (raw.immutable, raw.identifier) {
        (false, None) => None,
        (false, Some(_)) => {
            return Err("identifier: only immutable roots have identifiers".into());
        }
        (true, None) => Some(Identifier::Path),
        (true, Some(value)) => {
            Some(parse_identifier(value).map_err(|e| format!("identifier: {e}"))?)
        }
    };
    let pattern = escapes::decode(&raw.path, Syntax::Glob).map_err(|e| format!("path: {e}"))?;
    check_no_nul(&pattern).map_err(|e| format!("path: {e}"))?;
    let normalized = normalize(&pattern).map_err(|e| format!("path: {e}"))?;
    let exclude = parse_exclusions(&raw.exclude).map_err(|e| format!("exclude: {e}"))?;
    Ok(RootConfig {
        pattern: normalized,
        exclude,
        one_filesystem: raw.one_filesystem,
        identifier,
        may_vanish: raw.may_vanish,
    })
}

/// The `exclude` patterns of a root (DESIGN.md §4.2), normalized to the
/// form of what they are matched against. A pattern that begins with `/`
/// matches absolute paths, which have no empty, `.` or `..` components and
/// no trailing `/`, so it is normalized as a root pattern is. Any other
/// pattern matches basenames, which have no `/`, so it must be one
/// component, and a trailing `/` is dropped. A pattern written as a path
/// below the root, such as `*/.links`, is then an error instead of an
/// exclusion that matches nothing.
fn parse_exclusions(texts: &[String]) -> Result<Exclusions, String> {
    let mut exclude = Exclusions::default();
    for text in texts {
        let pattern = escapes::decode(text, Syntax::Glob)?;
        check_no_nul(&pattern)?;
        if pattern.is_empty() {
            return Err("empty pattern".into());
        }
        let quoted = quoted_pattern(&pattern);
        if pattern[0] == b'/' {
            let path = normalize(&pattern).map_err(|e| format!("{quoted}: {e}"))?;
            exclude.paths.push(path);
            continue;
        }
        let components: Vec<&[u8]> = glob::components(&pattern).collect();
        match components[..] {
            [b"." | b".."] => {
                return Err(format!("{quoted}: `.` and `..` components are not allowed"));
            }
            [name] => exclude.names.push(name.to_vec()),
            _ => {
                return Err(format!(
                    "{quoted}: basenames have no `/`; \
                     start the pattern with `/` to match absolute paths"
                ));
            }
        }
    }
    for patterns in [&mut exclude.names, &mut exclude.paths] {
        patterns.sort_unstable();
        patterns.dedup();
    }
    Ok(exclude)
}

/// `path`, an absolute path or a pattern of absolute paths, without empty
/// components and without a trailing `/`, and `/` alone for the root
/// directory. `.` and `..` components are not allowed: the root checks
/// compare paths as bytes (DESIGN.md §4.2), and the paths that exclusion
/// patterns match have none.
fn normalize(path: &[u8]) -> Result<Vec<u8>, String> {
    if path.first() != Some(&b'/') {
        return Err("must be an absolute path".into());
    }
    let mut normalized = Vec::with_capacity(path.len());
    for component in glob::components(path) {
        if component == b"." || component == b".." {
            return Err("`.` and `..` components are not allowed".into());
        }
        normalized.push(b'/');
        normalized.extend_from_slice(component);
    }
    if normalized.is_empty() {
        normalized.push(b'/');
    }
    Ok(normalized)
}

/// The `identifier` of an immutable root (DESIGN.md §4.2): `"path"`,
/// `"name"`, `{ strip_prefix = "..." }`, or `{ literal = "..." }`. Both
/// strings take byte escapes (DESIGN.md §7), and a literal can hold any
/// bytes, NUL included, but not none.
fn parse_identifier(value: toml::Value) -> Result<Identifier, String> {
    const EXPECTED: &str = "expected `\"path\"`, `\"name\"`, `{ strip_prefix = \"...\" }`, \
                            or `{ literal = \"...\" }`";
    let table = match value {
        toml::Value::String(form) => {
            return match form.as_str() {
                "path" => Ok(Identifier::Path),
                "name" => Ok(Identifier::Name),
                _ => Err(format!(
                    "unknown form {}, {EXPECTED}",
                    quoted(form.as_bytes())
                )),
            };
        }
        toml::Value::Table(table) if table.len() == 1 => table,
        _ => return Err(EXPECTED.into()),
    };
    let (key, value) = table.into_iter().next().unwrap();
    let toml::Value::String(text) = value else {
        return Err(format!("{}: expected a string", quoted(key.as_bytes())));
    };
    match key.as_str() {
        "strip_prefix" => {
            let prefix =
                escapes::decode(&text, Syntax::Plain).map_err(|e| format!("strip_prefix: {e}"))?;
            check_no_nul(&prefix).map_err(|e| format!("strip_prefix: {e}"))?;
            let prefix = normalize(&prefix).map_err(|e| format!("strip_prefix: {e}"))?;
            Ok(Identifier::StripPrefix(prefix))
        }
        "literal" => {
            let literal =
                escapes::decode(&text, Syntax::Plain).map_err(|e| format!("literal: {e}"))?;
            if literal.is_empty() {
                return Err("literal: an identifier must not be empty".into());
            }
            Ok(Identifier::Literal(literal))
        }
        _ => Err(format!(
            "unknown key {}, {EXPECTED}",
            quoted(key.as_bytes())
        )),
    }
}

fn check_no_nul(bytes: &[u8]) -> Result<(), String> {
    if bytes.contains(&0) {
        return Err("NUL byte is not allowed".into());
    }
    Ok(())
}

/// Parse a block size given on the command line, which takes the same
/// values as `block_size` in a configuration file.
pub fn parse_block_size(text: &str) -> Result<u32, String> {
    block_size(parse_size(text)?)
}

/// `bytes` as a block size. The index and HELLO store a block size as a
/// u32, so a larger one is an error too.
fn block_size(bytes: u64) -> Result<u32, String> {
    if bytes < MIN_BLOCK_SIZE {
        return Err(format!("must be at least {MIN_BLOCK_SIZE} bytes"));
    }
    u32::try_from(bytes).map_err(|_| format!("must be at most {} bytes", u32::MAX))
}

/// Parse a size such as `32KiB`, `1MB`, or `4096`.
pub fn parse_size(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let digits_end = text
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(text.len());
    if digits_end == 0 {
        return Err(format!("expected a number: {}", quoted(text.as_bytes())));
    }
    let number: u64 = text[..digits_end]
        .parse()
        .map_err(|_| format!("number too large: {}", quoted(text.as_bytes())))?;
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
        _ => return Err(format!("unknown unit {}", quoted(unit.as_bytes()))),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size too large: {}", quoted(text.as_bytes())))
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

#[cfg(test)]
mod tests {
    use bstr::ByteSlice;

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
exclude = ['//nix/store//.links/', '.cache/']
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
        // Exclusions lose empty components and trailing slashes, as root
        // patterns do.
        let exclude = &config.roots[1].exclude;
        assert_eq!(exclude.names, [b".cache"]);
        assert_eq!(exclude.paths, [b"/nix/store/.links"]);
        assert!(exclude.excludes(b".cache", never));
        assert!(exclude.excludes(b".links", || b"/nix/store/.links".to_vec()));
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
        // Sizes and durations can be integers, of bytes and seconds.
        assert_eq!(parse("block_size = 8192").unwrap().block_size, 8192);
        assert_eq!(parse("block_size = 512").unwrap().block_size, 512);
        let max_age = |text: &str| parse(text).unwrap().hashlist_max_age;
        assert_eq!(max_age("hashlist_max_age = 1"), 1);
        assert_eq!(max_age("hashlist_max_age = 3600"), 3600);
        assert_eq!(max_age("hashlist_max_age = \"3600\""), 3600);
        assert_eq!(max_age("hashlist_max_age = \"2h\""), 7200);
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
        assert_eq!(
            parse("strong_hash = \"md5\\u001b\"").unwrap_err(),
            r"strong_hash: unknown hash 'md5\x1B', expected `blake3` or `sha256`"
        );
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
        // An age of 0 points to the option that does what it would.
        for text in ["0", "\"0\"", "\"0d\""] {
            assert_eq!(
                parse(&format!("hashlist_max_age = {text}")).unwrap_err(),
                "hashlist_max_age must be at least 1 second; to send the whole hash list \
                 every time, use `hashsync ssh --no-sender-cache`",
            );
        }
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
        assert!(parse("[[roots]]\npath = \"/a\"\nbogus = 1").is_err());
        assert!(parse("bogus = 1").unwrap_err().contains("unknown field"));
        assert!(
            parse("[[roots]]\npath = '/a\\x00b'")
                .unwrap_err()
                .contains("NUL")
        );
        assert!(
            parse("[[roots]]\npath = '/a\\'")
                .unwrap_err()
                .contains("trailing")
        );
    }

    /// An exclusion must have the form of what it is matched against, so a
    /// pattern written as a path below the root, or with `.` or `..`
    /// components, is an error instead of an exclusion that matches
    /// nothing (DESIGN.md §4.2).
    #[test]
    fn rejected_exclusions() {
        let dots = "`.` and `..` components are not allowed";
        let slash = "basenames have no `/`; start the pattern with `/` to match absolute paths";
        for (pattern, expected) in [
            ("", "empty pattern".to_string()),
            ("/a/../b", format!("'/a/../b': {dots}")),
            ("..", format!("'..': {dots}")),
            ("./", format!("'./': {dots}")),
            ("*/.links", format!("'*/.links': {slash}")),
            ("**/x", format!("'**/x': {slash}")),
        ] {
            let text = format!("[[roots]]\npath = '/a'\nexclude = ['{pattern}']");
            let error = parse(&text).unwrap_err();
            assert_eq!(error, format!("roots[0]: exclude: {expected}"));
        }
    }

    /// A size or a duration that is neither a nonnegative integer nor a
    /// string, such as a negative or fractional number, gets a message that
    /// says what the key takes, not one that names a type of the program.
    #[test]
    fn mistyped_sizes_and_durations_say_what_they_take() {
        for text in [
            "block_size = -4096",
            "block_size = true",
            "hashlist_max_age = -1",
            "hashlist_max_age = 1.5",
        ] {
            let error = parse(text).unwrap_err();
            let expected = "expected a nonnegative integer or a string";
            assert!(error.ends_with(&format!(": {expected}")), "{text}: {error}");
        }
    }

    /// A TOML error is one line, which says where the error is, so that it
    /// stays one report (DESIGN.md §8).
    #[test]
    fn toml_errors_are_one_line() {
        for (text, expected) in [
            ("bogus = 1", "line 1, column 1: unknown field `bogus`"),
            (
                "block_size = 512\n\n[[roots]]\npath = 5",
                "line 4, column 8: invalid type: integer `5`, expected a string",
            ),
            // The column counts characters, not bytes.
            ("index_path = 'é' x", "line 1, column 18: "),
        ] {
            let error = parse(text).unwrap_err();
            assert!(error.starts_with(expected), "{text:?}: {error}");
            assert!(!error.contains('\n'), "{text:?}: {error}");
        }
    }

    /// A path that is `~` or that starts with `~/` starts at the home
    /// directory, but not one that spells its `~` as `\x7E`, which is the
    /// literal byte, as messages show it (DESIGN.md §7 and §8).
    #[test]
    fn only_a_tilde_as_it_is_starts_at_home() {
        let home = std::env::var_os("HOME").unwrap().as_bytes().to_vec();
        let index = |text: &str| parse(&format!("index_path = '{text}'")).unwrap().index_path;
        assert_eq!(index("~"), home);
        assert_eq!(index("~/x"), [&home[..], b"/x"].concat());
        assert_eq!(index(r"\x7E/x"), b"~/x");
        assert_eq!(index(r"\x7E"), b"~");
        assert_eq!(index("~x/y"), b"~x/y");
        assert_eq!(index("/a/~/b"), b"/a/~/b");
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

    /// A profile name becomes part of the path of its file, so it is one
    /// nonempty file name (DESIGN.md §7).
    #[test]
    fn profile_names_are_file_names() {
        for name in [&b"work"[..], b"a.b", b"..", b"\xff", b"-x", b"a\nb"] {
            assert_eq!(check_profile_name(name), Ok(()), "{:?}", name.as_bstr());
        }
        for name in [&b""[..], b"a/b", b"/", b"../x", b"a\0b"] {
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
            (b"a\xffb", r"'a'$'\xFF''b'"),
            (b"tab\there\\'", r"'tab'$'\x09''here\'\'"),
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

    /// The identifier of the root of `extra`, an immutable root.
    fn identifier(extra: &str) -> Result<Identifier, String> {
        let text = format!("[[roots]]\npath = '/a/*'\nimmutable = true\n{extra}");
        let mut config = parse(&text)?;
        Ok(config.roots.remove(0).identifier.unwrap())
    }

    /// Each form of `identifier`: an immutable root without one names its
    /// group by its path. Literals and prefixes take byte escapes, and a
    /// literal can hold NUL (DESIGN.md §4.2 and §7).
    #[test]
    fn identifier_forms() {
        let mutable = parse("[[roots]]\npath = '/a'").unwrap();
        assert_eq!(mutable.roots[0].identifier, None);
        assert_eq!(identifier(""), Ok(Identifier::Path));
        assert_eq!(identifier("identifier = 'path'"), Ok(Identifier::Path));
        assert_eq!(identifier("identifier = 'name'"), Ok(Identifier::Name));
        assert_eq!(
            identifier(r"identifier = { strip_prefix = '/mnt//\xFF/' }"),
            Ok(Identifier::StripPrefix(b"/mnt/\xff".to_vec()))
        );
        assert_eq!(
            identifier("identifier = { strip_prefix = '/' }"),
            Ok(Identifier::StripPrefix(b"/".to_vec()))
        );
        assert_eq!(
            identifier(r"identifier = { literal = 'photos-\xFF\x00\\' }"),
            Ok(Identifier::Literal(b"photos-\xff\0\\".to_vec()))
        );
        assert_eq!(
            identifier("identifier.literal = \"double\\\\x41\""),
            Ok(Identifier::Literal(b"doubleA".to_vec()))
        );
    }

    #[test]
    fn rejected_identifiers() {
        let error = parse("[[roots]]\npath = '/a'\nidentifier = 'name'").unwrap_err();
        assert_eq!(
            error,
            "roots[0]: identifier: only immutable roots have identifiers"
        );
        for (extra, expected) in [
            (
                "identifier = 'Name'",
                "identifier: unknown form 'Name', expected",
            ),
            ("identifier = 5", "identifier: expected `\"path\"`"),
            ("identifier = {}", "identifier: expected"),
            (
                "identifier = { literal = 'a', strip_prefix = '/' }",
                "identifier: expected",
            ),
            (
                "identifier = { prefix = '/' }",
                "identifier: unknown key 'prefix'",
            ),
            (
                "identifier = { literal = 1 }",
                "identifier: 'literal': expected a string",
            ),
            (
                "identifier = { literal = '' }",
                "identifier: literal: an identifier must not be empty",
            ),
            (
                r"identifier = { literal = '\q' }",
                "identifier: literal: unsupported escape",
            ),
            (
                "identifier = { strip_prefix = 'mnt' }",
                "identifier: strip_prefix: must be an absolute path",
            ),
            (
                "identifier = { strip_prefix = '/mnt/../x' }",
                "identifier: strip_prefix: `.` and `..`",
            ),
            (
                r"identifier = { strip_prefix = '/a\x00' }",
                "identifier: strip_prefix: NUL byte is not allowed",
            ),
        ] {
            let error = identifier(extra).unwrap_err();
            assert!(
                error.starts_with(&format!("roots[0]: {expected}")),
                "{extra}: {error}"
            );
        }
    }

    /// `may_vanish` is off by default, and any root can have it. It is not
    /// part of the indexing policy, so copies can differ in it (DESIGN.md
    /// §4.2).
    #[test]
    fn roots_may_vanish() {
        let root = |extra: &str| {
            let mut config = parse(&format!("[[roots]]\npath = '/a'\n{extra}")).unwrap();
            config.roots.remove(0)
        };
        assert!(!root("").may_vanish);
        assert!(root("may_vanish = true").may_vanish);
        let copy = root("immutable = true\nmay_vanish = true");
        assert!(copy.may_vanish);
        assert_eq!(copy.policy_difference(&root("immutable = true")), None);
        assert!(parse("[[roots]]\npath = '/a'\nmay_vanish = 1").is_err());
    }

    /// The identifier of each match (DESIGN.md §4.2). `strip_prefix` takes
    /// whole components, and gives an absolute path, `/` for the prefix
    /// itself, so that a copy below it shares the identifier that `path`
    /// gives the copy at that path.
    #[test]
    fn identifiers_of_matches() {
        let of = |form: Identifier, path: &[u8]| form.of(path);
        assert_eq!(of(Identifier::Path, b"/a/b"), Ok(b"/a/b".to_vec()));
        assert_eq!(of(Identifier::Name, b"/a/b\xff"), Ok(b"b\xff".to_vec()));
        assert_eq!(of(Identifier::Name, b"/a"), Ok(b"a".to_vec()));
        assert!(of(Identifier::Name, b"/").unwrap_err().contains("no name"));
        let strip = |prefix: &[u8]| Identifier::StripPrefix(prefix.to_vec());
        assert_eq!(
            of(strip(b"/mnt/disk"), b"/mnt/disk/nix/x"),
            Ok(b"/nix/x".to_vec())
        );
        assert_eq!(of(strip(b"/mnt/disk"), b"/mnt/disk"), Ok(b"/".to_vec()));
        assert_eq!(of(strip(b"/"), b"/nix/x"), Ok(b"/nix/x".to_vec()));
        assert_eq!(of(strip(b"/"), b"/"), Ok(b"/".to_vec()));
        for path in [&b"/mnt/disk2/x"[..], b"/mnt", b"/srv/x"] {
            let error = of(strip(b"/mnt/disk"), path).unwrap_err();
            assert_eq!(
                error,
                "is not below the strip_prefix '/mnt/disk' of its identifier"
            );
        }
        let literal = Identifier::Literal(b"x\0".to_vec());
        assert_eq!(of(literal, b"/anything"), Ok(b"x\0".to_vec()));
    }

    /// Copies must have the same exclusions, as sets of patterns after
    /// byte-escape decoding and normalization, and the same
    /// `one_filesystem` (DESIGN.md §4.2).
    #[test]
    fn indexing_policies_compare_as_sets() {
        let root = |extra: &str| {
            let mut config = parse(&format!("[[roots]]\npath = '/a'\n{extra}")).unwrap();
            config.roots.remove(0)
        };
        let base = root(r"exclude = ['*.tmp', '/a/x', 'literal\*']");
        for same in [
            r"exclude = ['/a/x', 'literal\x2A', '*.tmp', '*.tmp']",
            r"exclude = ['literal\*', '/a/x', '*.tmp']",
            r"exclude = ['//a/x/', 'literal\*/', '*.tmp']",
        ] {
            assert_eq!(base.policy_difference(&root(same)), None, "{same}");
        }
        for (other, difference) in [
            (r"exclude = ['*.tmp', '/a/x']", "exclusions"),
            (r"exclude = ['*.tmp', '/a/x', 'literal*']", "exclusions"),
            (r"exclude = ['*.tmp', 'x', 'literal\*']", "exclusions"),
            (
                "exclude = ['*.tmp', '/a/x', 'literal\\*']\none_filesystem = true",
                "one_filesystem values",
            ),
        ] {
            let found = base.policy_difference(&root(other));
            assert_eq!(found, Some(difference), "{other}");
        }
    }

    /// The man page, `docs/hashsync.1`, describes the keys of the
    /// configuration file in the two lists of its CONFIGURATION section,
    /// those of the file and those of each root, with an item for each key
    /// and for no other, so that a new key cannot go undocumented and a
    /// removed one cannot stay. Its example files are valid configurations.
    #[test]
    fn the_man_page_documents_every_key() {
        let page = include_str!("../docs/hashsync.1");
        let section = page
            .lines()
            .skip_while(|line| *line != ".Sh CONFIGURATION")
            .skip(1)
            .take_while(|line| !line.starts_with(".Sh "));
        let mut lists: Vec<Vec<&str>> = Vec::new();
        for line in section {
            if line.starts_with(".Bl ") {
                lists.push(Vec::new());
            } else if let Some(key) = line.strip_prefix(".It Cm ") {
                lists.last_mut().expect("an item in a list").push(key);
            }
        }
        let sorted = |mut keys: Vec<&'static str>| {
            keys.sort_unstable();
            keys
        };
        assert_eq!(
            lists.into_iter().map(sorted).collect::<Vec<_>>(),
            [
                sorted(serde_fields::<RawConfig>().to_vec()),
                sorted(serde_fields::<RawRoot>().to_vec()),
            ],
            "the keys of the file and of each root"
        );
        // The literal blocks of mdoc, in which `\e` is a backslash.
        let mut examples = 0;
        let mut lines = page.lines();
        while lines.any(|line| line.starts_with(".Bd -literal")) {
            let block: Vec<&str> = lines.by_ref().take_while(|line| *line != ".Ed").collect();
            let text = block.join("\n").replace(r"\e", r"\");
            if text.contains("[[roots]]") {
                parse(&text).unwrap_or_else(|e| panic!("{e}:\n{text}"));
                examples += 1;
            }
        }
        assert_eq!(examples, 3, "the examples of configuration files");
    }

    /// The fields of `T`, which are the keys of its table in the
    /// configuration file: serde gives them to the deserializer.
    fn serde_fields<T: for<'de> Deserialize<'de>>() -> &'static [&'static str] {
        /// A deserializer that keeps the fields of the struct that asks for
        /// them, and fails.
        struct Fields(Option<&'static [&'static str]>);

        impl<'de> serde::Deserializer<'de> for &mut Fields {
            type Error = serde::de::value::Error;

            fn deserialize_any<V: serde::de::Visitor<'de>>(
                self,
                _: V,
            ) -> Result<V::Value, Self::Error> {
                Err(serde::de::Error::custom("not a struct"))
            }

            fn deserialize_struct<V: serde::de::Visitor<'de>>(
                self,
                _name: &'static str,
                fields: &'static [&'static str],
                _: V,
            ) -> Result<V::Value, Self::Error> {
                self.0 = Some(fields);
                Err(serde::de::Error::custom("no values"))
            }

            serde::forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map enum identifier ignored_any
            }
        }

        let mut fields = Fields(None);
        let _ = T::deserialize(&mut fields);
        fields.0.expect("a struct")
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
