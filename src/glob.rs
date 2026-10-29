// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! Glob matching on bytes with `gix-glob`'s raw `wildmatch` (DESIGN.md §4.2).
//!
//! Patterns are matched in pathname mode, case-sensitively, with leading
//! dots included: `*`, `?`, and bracket classes cannot match `/`, and `**`
//! spans path components. A backslash quotes the next byte.

use bstr::ByteSlice;
use gix_glob::wildmatch::Mode;

pub const MODE: Mode = Mode::NO_MATCH_SLASH_LITERAL;

pub fn matches(pattern: &[u8], value: &[u8]) -> bool {
    gix_glob::wildmatch(pattern.as_bstr(), value.as_bstr(), MODE)
}

/// Bytes that a backslash may quote in a pattern.
pub const QUOTABLE: &[u8] = b"*?[]-!^\\";

/// Append `byte` to `pattern` so that the pattern matches it literally:
/// a quotable byte gets a backslash, every other byte matches itself.
pub fn push_quoted(pattern: &mut Vec<u8>, byte: u8) {
    if QUOTABLE.contains(&byte) {
        pattern.push(b'\\');
    }
    pattern.push(byte);
}

/// One `/`-separated component of a root pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Component {
    /// No wildcards; the bytes are unquoted and name one entry.
    Literal(Vec<u8>),
    /// A pattern to match against entry names.
    Wild(Vec<u8>),
    /// `**`: any number of directories.
    AnyDepth,
}

/// Split a pattern on `/` into non-empty components.
pub fn components(pattern: &[u8]) -> impl Iterator<Item = &[u8]> {
    pattern.split(|&b| b == b'/').filter(|c| !c.is_empty())
}

/// Classify one component. A quoted metacharacter is literal.
pub fn classify(component: &[u8]) -> Component {
    if component == b"**" {
        return Component::AnyDepth;
    }
    let mut literal = Vec::with_capacity(component.len());
    let mut i = 0;
    while i < component.len() {
        match component[i] {
            b'\\' => match component.get(i + 1) {
                Some(&quoted) => {
                    literal.push(quoted);
                    i += 2;
                }
                // A trailing backslash matches nothing; keep it a pattern.
                None => return Component::Wild(component.to_vec()),
            },
            b'*' | b'?' | b'[' => return Component::Wild(component.to_vec()),
            b => {
                literal.push(b);
                i += 1;
            }
        }
    }
    Component::Literal(literal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pathname_mode_and_dot_names() {
        assert!(matches(b"*", b".links"));
        assert!(!matches(b"*", b"a/b"));
        assert!(!matches(b"?", b"/"));
        assert!(!matches(b"[/a]", b"/"));
        assert!(matches(b"/nix/store/*", b"/nix/store/abc-foo"));
        assert!(!matches(b"/nix/store/*", b"/nix/store/abc/bin"));
        assert!(matches(b"/srv/**/data", b"/srv/a/b/data"));
        assert!(matches(b"/srv/**/data", b"/srv/data"));
        assert!(matches(b"*.tmp", b"x.tmp"));
        assert!(!matches(b"*.TMP", b"x.tmp"));
    }

    #[test]
    fn bytes_and_quoting() {
        assert!(matches(b"\xff*", b"\xff\xfe"));
        assert!(matches(b"?", b"\xff"));
        assert!(matches(b"[\xf0-\xff]", b"\xf8"));
        assert!(matches(b"\\*", b"*"));
        assert!(!matches(b"\\*", b"x"));
        assert!(matches(b"\\?", b"?"));
        assert!(matches(b"\\[a]", b"[a]"));
        assert!(matches(b"a\\\\b", b"a\\b"));
        assert!(matches(b"[\\!x]", b"!"));
        assert!(!matches(b"[\\!x]", b"y"));
        assert!(matches(b"[a\\-c]", b"-"));
        assert!(!matches(b"[a\\-c]", b"b"));
    }

    #[test]
    fn component_classification() {
        assert_eq!(classify(b"store"), Component::Literal(b"store".to_vec()));
        assert_eq!(classify(b"\\*x"), Component::Literal(b"*x".to_vec()));
        assert_eq!(classify(b"a\\\\b"), Component::Literal(b"a\\b".to_vec()));
        assert_eq!(classify(b"*"), Component::Wild(b"*".to_vec()));
        assert_eq!(classify(b"a[bc]"), Component::Wild(b"a[bc]".to_vec()));
        assert_eq!(classify(b"a?"), Component::Wild(b"a?".to_vec()));
        assert_eq!(classify(b"**"), Component::AnyDepth);
        assert_eq!(classify(b"a**"), Component::Wild(b"a**".to_vec()));
        let comps: Vec<&[u8]> = components(b"//srv//data/").collect();
        assert_eq!(comps, vec![&b"srv"[..], &b"data"[..]]);
    }

    #[test]
    fn pushed_bytes_match_literally() {
        let mut pattern = Vec::new();
        for &byte in b"a*b?\\\xff" {
            push_quoted(&mut pattern, byte);
        }
        assert_eq!(pattern, b"a\\*b\\?\\\\\xff");
        assert!(matches(&pattern, b"a*b?\\\xff"));
        assert_eq!(
            classify(&pattern),
            Component::Literal(b"a*b?\\\xff".to_vec())
        );
    }
}
