// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! Byte strings in text (DESIGN.md §7 and §8).
//!
//! Paths, file names and profile names can hold any byte but NUL, while
//! configuration files and messages are text. The configuration file
//! spells a byte that text cannot hold as `\xHH` and a backslash as `\\`
//! (`decode`). Messages show each byte string between single quotes in the
//! same syntax (`quoted`), so that a program can tell where it ends and get
//! its bytes back, and a person can copy it into a configuration file. A
//! command that a message tells the user to run is in shell syntax instead
//! (`shell_word`), since it goes to a shell. Command-line arguments have no
//! escapes at all: the shell already passes any bytes.

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::glob;
use crate::names;

/// How to decode a byte-string field of the configuration file.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Syntax {
    /// `\xHH` and `\\` only.
    Plain,
    /// Also `\*`, `\?`, `\[`, `\]`, `\-`, `\!`, and `\^`, kept as glob
    /// quoting. Decoded bytes that are glob metacharacters are quoted.
    Glob,
}

/// Decode the byte escapes of DESIGN.md §7 after TOML parsing.
pub fn decode(text: &str, syntax: Syntax) -> Result<Vec<u8>, String> {
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
            c if syntax == Syntax::Glob && glob::QUOTABLE.contains(&c) => {
                i += 2;
                c
            }
            _ => {
                let next = text[i + 1..].chars().next().unwrap();
                let escape = format!("\\{next}");
                return Err(format!("unsupported escape {}", quoted(escape.as_bytes())));
            }
        };
        match syntax {
            Syntax::Glob => glob::push_quoted(&mut out, value),
            Syntax::Plain => out.push(value),
        }
    }
    Ok(out)
}

/// `bytes` as messages show a path, a file name, a profile name, or
/// another byte string that comes from outside the program (DESIGN.md §8):
/// between single quotes, in the syntax of the configuration file. Each
/// byte of a control character or of a line separator (`must_escape`), each
/// byte that is not part of a UTF-8 character, and a single quote are
/// `\xHH`, and a backslash is `\\`; every other character is as it is. So
/// the string has no single quote before its end, no control character, and
/// no line end. A `~` that starts the string, alone or before a `/`, is
/// `\x7E` too, since there it would stand for the home directory, as in the
/// configuration file (`quoted_from_home`).
pub fn quoted(bytes: &[u8]) -> Quoted<'_> {
    Quoted {
        bytes,
        form: Form::Plain,
    }
}

/// A path as `quoted` shows it.
pub fn quoted_path(path: &Path) -> Quoted<'_> {
    quoted(path.as_os_str().as_bytes())
}

/// `path` as `quoted` shows it, but with `~` in place of the home
/// directory, `$HOME`, when the path is in it, which keeps a message short.
/// Without `$HOME`, paths stay as they are.
pub fn quoted_from_home(path: &[u8]) -> Quoted<'_> {
    let home = std::env::var_os("HOME").unwrap_or_default();
    from_home(path, home.as_bytes())
}

/// `path` as `quoted_from_home` shows it when the home directory is
/// `home`. This function exists so that the tests can choose the home
/// directory without changing the environment of the test process. With a
/// home of `/`, paths stay as they are, or every absolute path would start
/// with `~`.
fn from_home<'a>(path: &'a [u8], home: &[u8]) -> Quoted<'a> {
    let mut home = home;
    while let [rest @ .., b'/'] = home {
        home = rest;
    }
    if home.first() == Some(&b'/')
        && let Some(rest) = path.strip_prefix(home)
        && matches!(rest.first(), None | Some(b'/'))
    {
        return Quoted {
            bytes: rest,
            form: Form::Home,
        };
    }
    quoted(path)
}

/// A glob pattern of the configuration, such as a root pattern, as
/// messages show it: as `quoted` shows a byte string, but with the quoting
/// of its metacharacters as the configuration file spells it, such as `\*`
/// for a literal `*`, so that the pattern can be copied into the file. The
/// patterns that `decode` makes have a quoted byte after every backslash.
pub fn quoted_pattern(pattern: &[u8]) -> Quoted<'_> {
    Quoted {
        bytes: pattern,
        form: Form::Pattern,
    }
}

/// A byte string that a message shows, from `quoted` and the functions
/// next to it.
pub struct Quoted<'a> {
    bytes: &'a [u8],
    form: Form,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    /// A byte string.
    Plain,
    /// The rest of a path after the home directory, which shows as `~`.
    Home,
    /// A glob pattern with its quoting.
    Pattern,
}

impl Display for Quoted<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('\'')?;
        let mut bytes = self.bytes;
        match self.form {
            Form::Home => f.write_char('~')?,
            Form::Plain if starts_at_home(bytes) => {
                f.write_str(r"\x7E")?;
                bytes = &bytes[1..];
            }
            Form::Plain | Form::Pattern => {}
        }
        for chunk in bytes.utf8_chunks() {
            for c in chunk.valid().chars() {
                match c {
                    // Every backslash of a pattern quotes the byte after
                    // it, as the configuration file spells it.
                    '\\' if self.form == Form::Pattern => f.write_char('\\')?,
                    '\\' => f.write_str(r"\\")?,
                    '\'' => f.write_str(r"\x27")?,
                    c if must_escape(c) => write_escapes(f, c.encode_utf8(&mut [0; 4]).as_bytes())?,
                    c => f.write_char(c)?,
                }
            }
            write_escapes(f, chunk.invalid())?;
        }
        f.write_char('\'')
    }
}

/// True for a path that starts at the home directory in the configuration
/// file: `~` alone, or followed by `/` (DESIGN.md §7).
pub fn starts_at_home(path: &[u8]) -> bool {
    path.first() == Some(&b'~') && matches!(path.get(1), None | Some(b'/'))
}

/// True for the characters that messages never show as they are: the
/// control characters, which can end a line or drive a terminal, and the
/// line and paragraph separators U+2028 and U+2029, which some programs,
/// such as Python's `splitlines`, take for line ends too.
fn must_escape(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}')
}

/// Write each of `bytes` as `\xHH`, with uppercase digits, as the examples
/// of DESIGN.md §7 spell bytes.
fn write_escapes(out: &mut impl Write, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(out, "\\x{byte:02X}")?;
    }
    Ok(())
}

/// `text` with each character that `must_escape` as `\xHH` for each of its
/// bytes, as `quoted` shows it. This function exists so that every report
/// of this program is one line without terminal controls, whatever text
/// it carries, such as the error text of a library, which can hold a path
/// as it is (DESIGN.md §8).
pub fn one_line(text: &str) -> Cow<'_, str> {
    if !text.contains(must_escape) {
        return Cow::Borrowed(text);
    }
    let mut line = String::with_capacity(text.len() + 16);
    for c in text.chars() {
        if must_escape(c) {
            write_escapes(&mut line, c.encode_utf8(&mut [0; 4]).as_bytes()).unwrap();
        } else {
            line.push(c);
        }
    }
    Cow::Owned(line)
}

/// `bytes` as one POSIX shell word that gives back exactly these bytes, for
/// the commands that messages tell the user to run (DESIGN.md §8). A word
/// of letters, digits, and `_@%+=:,./-` stays as it is. Any other word is
/// made of parts: its printable characters go in single quotes, a single
/// quote is `\'`, and each byte of a control character, of a backquote,
/// which would end the command in the message, and each byte that is not
/// UTF-8 is `\xHH` in `$'...'`, as messages spell bytes elsewhere. A
/// `$'...'` part holds only such escapes, so no hexadecimal digit follows
/// one: POSIX leaves the length of an escape that more digits follow
/// unspecified, and shells read it differently.
pub fn shell_word(bytes: &[u8]) -> String {
    let plain = |b: &u8| b.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(b);
    if !bytes.is_empty() && bytes.iter().all(plain) {
        return bytes.iter().copied().map(char::from).collect();
    }
    let mut word = ShellWord::default();
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '\'' => {
                    word.enter(Part::Bare);
                    word.text.push_str(r"\'");
                }
                c if c == '`' || must_escape(c) => {
                    word.escape(c.encode_utf8(&mut [0; 4]).as_bytes());
                }
                c => {
                    word.enter(Part::Quoted);
                    word.text.push(c);
                }
            }
        }
        if !chunk.invalid().is_empty() {
            word.escape(chunk.invalid());
        }
    }
    word.enter(Part::Bare);
    if word.text.is_empty() {
        word.text.push_str("''");
    }
    word.text
}

/// A shell word that `shell_word` builds, part after part.
#[derive(Default)]
struct ShellWord {
    text: String,
    part: Part,
}

/// The kind of the part of a shell word that `shell_word` writes.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Part {
    /// Outside quotes.
    #[default]
    Bare,
    /// In `'...'`.
    Quoted,
    /// In `$'...'`.
    Escaped,
}

impl ShellWord {
    /// End the current part and start one of the kind `part`, unless the
    /// current part has that kind.
    fn enter(&mut self, part: Part) {
        if self.part == part {
            return;
        }
        if self.part != Part::Bare {
            self.text.push('\'');
        }
        match part {
            Part::Bare => {}
            Part::Quoted => self.text.push('\''),
            Part::Escaped => self.text.push_str("$'"),
        }
        self.part = part;
    }

    fn escape(&mut self, bytes: &[u8]) {
        self.enter(Part::Escaped);
        write_escapes(&mut self.text, bytes).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use bstr::ByteSlice;

    use super::*;

    #[test]
    fn plain_escapes() {
        assert_eq!(decode("abc", Syntax::Plain).unwrap(), b"abc");
        assert_eq!(decode(r"a\xFFb", Syntax::Plain).unwrap(), b"a\xffb");
        assert_eq!(decode(r"a\xffb", Syntax::Plain).unwrap(), b"a\xffb");
        assert_eq!(decode(r"a\\b", Syntax::Plain).unwrap(), b"a\\b");
        assert_eq!(decode("é", Syntax::Plain).unwrap(), "é".as_bytes());
        assert!(decode(r"a\", Syntax::Plain).is_err());
        assert!(decode(r"a\xF", Syntax::Plain).is_err());
        assert!(decode(r"a\xGG", Syntax::Plain).is_err());
        assert!(decode(r"a\*", Syntax::Plain).is_err());
        assert!(decode(r"a\n", Syntax::Plain).is_err());
        assert!(decode(r"\101", Syntax::Plain).is_err());
        // Decoded bytes are never decoded again.
        assert_eq!(decode(r"\x5Cx41", Syntax::Plain).unwrap(), b"\\x41");
    }

    #[test]
    fn glob_escapes_keep_quoting() {
        assert_eq!(decode(r"\x2A", Syntax::Glob).unwrap(), b"\\*");
        assert_eq!(decode(r"\*", Syntax::Glob).unwrap(), b"\\*");
        assert_eq!(
            decode(r"\?\[\]\-\!\^", Syntax::Glob).unwrap(),
            b"\\?\\[\\]\\-\\!\\^"
        );
        assert_eq!(
            decode(r"back\\slash", Syntax::Glob).unwrap(),
            b"back\\\\slash"
        );
        assert_eq!(decode(r"\x5C", Syntax::Glob).unwrap(), b"\\\\");
        assert_eq!(decode(r"\x2F", Syntax::Glob).unwrap(), b"/");
        assert_eq!(decode(r"\xFF*", Syntax::Glob).unwrap(), b"\xff*");
        assert!(decode(r"a\n", Syntax::Glob).is_err());
        // The quoted forms match literally.
        let pattern = decode(r"literal\x2Aname", Syntax::Glob).unwrap();
        assert!(glob::matches(&pattern, b"literal*name"));
        assert!(!glob::matches(&pattern, b"literalXname"));
        let pattern = decode(r"back\\slash", Syntax::Glob).unwrap();
        assert!(glob::matches(&pattern, b"back\\slash"));
        assert!(glob::matches(
            &decode(r"[\x2D]", Syntax::Glob).unwrap(),
            b"-"
        ));
    }

    /// Messages show a byte string between single quotes, in the syntax of
    /// the configuration file, with every printable character as it is
    /// (DESIGN.md §8).
    #[test]
    fn messages_show_byte_strings_as_the_configuration_spells_them() {
        for (bytes, shown) in [
            (&b"/srv/data"[..], "'/srv/data'"),
            (b"", "''"),
            (
                "/srv/café ünïcode 日本".as_bytes(),
                "'/srv/café ünïcode 日本'",
            ),
            (b"it's", r"'it\x27s'"),
            (br#"say "hi""#, r#"'say "hi"'"#),
            (b"back\\slash", r"'back\\slash'"),
            (b"a\xffb\xc3", r"'a\xFFb\xC3'"),
            (
                b"line\nend\r\tab\x1b[0m\x7f",
                r"'line\x0Aend\x0D\x09ab\x1B[0m\x7F'",
            ),
            // A C1 control and the Unicode line separators, by their bytes.
            (
                "a\u{85}b\u{2028}c\u{2029}".as_bytes(),
                r"'a\xC2\x85b\xE2\x80\xA8c\xE2\x80\xA9'",
            ),
            // A `~` that would start at the home directory, and others.
            (b"~", r"'\x7E'"),
            (b"~/x", r"'\x7E/x'"),
            (b"~x/~/y", "'~x/~/y'"),
            (b"/a/~", "'/a/~'"),
            // Bytes that look like escapes stay text.
            (br"\xFF", r"'\\xFF'"),
        ] {
            assert_eq!(quoted(bytes).to_string(), shown, "{:?}", bytes.as_bstr());
        }
    }

    /// What a message shows decodes, with the syntax of the configuration
    /// file, to the bytes that it shows, whatever they are; and the string
    /// ends at the first single quote after its start, so a program can
    /// find it in a line (DESIGN.md §8).
    #[test]
    fn shown_byte_strings_decode_back() {
        let mut all: Vec<u8> = (0..=255).collect();
        all.extend_from_slice("é\u{85}\u{2028}日".as_bytes());
        let mut cases = vec![all.clone(), b"~".to_vec(), b"~/a".to_vec()];
        for byte in 0..=255u8 {
            cases.push(vec![byte, b'x', byte]);
        }
        for bytes in cases {
            let shown = quoted(&bytes).to_string();
            let inner = shown
                .strip_prefix('\'')
                .unwrap()
                .strip_suffix('\'')
                .unwrap();
            assert!(!inner.contains('\''), "{shown}");
            assert!(!inner.contains(must_escape), "{shown}");
            assert_eq!(decode(inner, Syntax::Plain).unwrap(), bytes, "{shown}");
        }
    }

    /// A path in the home directory shows with `~`, but not a path in
    /// another directory whose name starts with the same bytes.
    #[test]
    fn messages_show_the_home_directory_as_a_tilde() {
        let home = b"/home/u";
        let shown = |path: &[u8], home: &[u8]| from_home(path, home).to_string();
        assert_eq!(
            shown(b"/home/u/.cache/hashsync/a.db", home),
            "'~/.cache/hashsync/a.db'"
        );
        assert_eq!(shown(b"/home/u", home), "'~'");
        assert_eq!(shown(b"/home/u/x", b"/home/u//"), "'~/x'");
        assert_eq!(shown(b"/home/user/x", home), "'/home/user/x'");
        assert_eq!(shown(b"x.toml", home), "'x.toml'");
        assert_eq!(shown(b"/etc/x", b"/"), "'/etc/x'");
        assert_eq!(shown(b"/etc/x", b""), "'/etc/x'");
        assert_eq!(shown(b"/home/u/\xff'", home), r"'~/\xFF\x27'");
        // A relative path whose first directory is named `~`.
        assert_eq!(shown(b"~/x", home), r"'\x7E/x'");
    }

    /// A root pattern shows its quoting as the configuration file spells
    /// it, so that the pattern decodes back to itself.
    #[test]
    fn patterns_keep_their_quoting() {
        for text in [
            r"/srv/\*",
            r"/srv/*/\x2A?\[x\]",
            r"/a/back\\slash/*",
            r"/srv/archive-\xFF/it\x27s\x0A",
            "/srv/日本/*",
        ] {
            let pattern = decode(text, Syntax::Glob).unwrap();
            let shown = quoted_pattern(&pattern).to_string();
            let inner = shown
                .strip_prefix('\'')
                .unwrap()
                .strip_suffix('\'')
                .unwrap();
            assert!(!inner.contains('\''), "{shown}");
            assert_eq!(decode(inner, Syntax::Glob).unwrap(), pattern, "{shown}");
        }
        let shown = |text| quoted_pattern(&decode(text, Syntax::Glob).unwrap()).to_string();
        assert_eq!(shown(r"/srv/\x2A"), r"'/srv/\*'");
        assert_eq!(shown(r"/a/\\/\xFF*"), r"'/a/\\/\xFF*'");
        // A pattern has no home directory.
        assert_eq!(quoted_pattern(b"~/x").to_string(), "'~/x'");
    }

    #[test]
    fn reports_stay_one_line() {
        assert!(matches!(one_line("no controls: 'x' \\"), Cow::Borrowed(_)));
        assert_eq!(
            one_line("a\nb\tc\u{1b}\u{2028}é"),
            r"a\x0Ab\x09c\x1B\xE2\x80\xA8é"
        );
    }

    /// The command of a message is one that the user can copy into a shell,
    /// with every byte as `\xHH` that is not printable text, and without a
    /// backquote, which ends the command in the message.
    #[test]
    fn shell_words_give_back_their_bytes() {
        for (bytes, word) in [
            (&b"work"[..], "work"),
            (b"-x", "-x"),
            (b"/etc/hashsync/nas.conf", "/etc/hashsync/nas.conf"),
            (b"", "''"),
            (b"work files", "'work files'"),
            (b"it's", r"'it'\''s'"),
            (b"'", r"\'"),
            ("café".as_bytes(), "'café'"),
            (b"a\xffb", r"'a'$'\xFF''b'"),
            (b"\xff", r"$'\xFF'"),
            (b"\xffab", r"$'\xFF''ab'"),
            (b"tab\there\\'", r"'tab'$'\x09''here\'\'"),
            (b"a`b", r"'a'$'\x60''b'"),
            (b"$HOME !x", "'$HOME !x'"),
        ] {
            assert_eq!(shell_word(bytes), word, "{:?}", bytes.as_bstr());
        }
    }
}
