//! The two SSH sessions and their command lines (DESIGN.md §6.1).
//!
//! The configured `ssh` command is shell-split; the first word is the
//! executable and the rest are its options. Leading `-o` options inserted
//! by hashsync take precedence under SSH's first-value rule, and the
//! trailing `-T -x` disable PTYs and X11 forwarding even if the configured
//! options requested them.
//!
//! SSH runs its remote command through the remote login shell, so the
//! bootstrap single-quotes every argument for POSIX shells, preserving
//! all bytes, including non-UTF-8 bytes.

use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use bstr::ByteSlice;
use rustix::process::Pid;

use crate::die::{self, die};

/// Split a command string like a POSIX shell splits words, without any
/// expansion: whitespace separates words; `\` quotes the next byte;
/// `'...'` is literal; inside `"..."`, `\` quotes only `$`, backquote,
/// `"`, `\`, and a newline. A quoted newline continues the line.
pub fn shell_split(text: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut words = Vec::new();
    let mut word: Option<Vec<u8>> = None;
    let mut bytes = text.iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        match byte {
            b' ' | b'\t' | b'\n' => {
                if let Some(word) = word.take() {
                    words.push(word);
                }
            }
            b'\\' => match bytes.next() {
                None => return Err("trailing backslash".into()),
                Some(b'\n') => {}
                Some(quoted) => word.get_or_insert_default().push(quoted),
            },
            b'\'' => {
                let word = word.get_or_insert_default();
                loop {
                    match bytes.next() {
                        None => return Err("unterminated single quote".into()),
                        Some(b'\'') => break,
                        Some(byte) => word.push(byte),
                    }
                }
            }
            b'"' => {
                let word = word.get_or_insert_default();
                loop {
                    match bytes.next() {
                        None => return Err("unterminated double quote".into()),
                        Some(b'"') => break,
                        Some(b'\\') => match bytes.next() {
                            None => return Err("unterminated double quote".into()),
                            Some(b'\n') => {}
                            Some(quoted @ (b'$' | b'`' | b'"' | b'\\')) => word.push(quoted),
                            Some(other) => word.extend([b'\\', other]),
                        },
                        Some(byte) => word.push(byte),
                    }
                }
            }
            byte => word.get_or_insert_default().push(byte),
        }
    }
    if let Some(word) = word {
        words.push(word);
    }
    Ok(words)
}

/// The remote bootstrap: `exec` and each argument single-quoted for the
/// remote POSIX shell, with embedded single quotes as `'\''`.
pub fn remote_command(argv: &[&[u8]]) -> Vec<u8> {
    let mut out = b"exec".to_vec();
    for arg in argv {
        out.extend_from_slice(b" '");
        for &byte in *arg {
            match byte {
                b'\'' => out.extend_from_slice(b"'\\''"),
                byte => out.push(byte),
            }
        }
        out.push(b'\'');
    }
    out
}

/// One session's local argv: the split `ssh` command with the options,
/// HOST, and remote command that DESIGN.md §6.1 specifies.
pub fn session_argv(ssh: &[Vec<u8>], host: &[u8], remote: &[u8]) -> Vec<OsString> {
    let mut argv: Vec<Vec<u8>> = Vec::with_capacity(ssh.len() + 10);
    argv.push(ssh[0].clone());
    for option in ["-o", "ClearAllForwardings=yes", "-o", "RemoteCommand=none"] {
        argv.push(option.into());
    }
    argv.extend(ssh[1..].iter().cloned());
    for option in ["-T", "-x", "-o", "Compression=no"] {
        argv.push(option.into());
    }
    argv.push(host.to_vec());
    argv.push(remote.to_vec());
    argv.into_iter().map(OsString::from_vec).collect()
}

/// One running SSH session. The child stays unreaped so its PID stays
/// reserved (DESIGN.md §6.6); dropping `stdin` closes the pipe, and the
/// sender takes that EOF as the receiver's disappearance until it has sent
/// DONE, so the receiver holds `stdin` until DONE (DESIGN.md §6.7).
pub struct Session {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

impl Session {
    pub fn pid(&self) -> Pid {
        Pid::from_child(&self.child)
    }
}

/// Start the data and control sessions together. Each SSH child is
/// SIGKILLed on a fatal exit; stderr is inherited, so the sender's
/// relayed bytes and SSH's own errors reach the receiver's stderr.
pub fn start_sessions(
    ssh: &[Vec<u8>],
    sender_command: &[u8],
    host: &[u8],
    token: &str,
) -> (Session, Session) {
    let data = spawn(ssh, host, sender_command, b"serve", token);
    let control = spawn(ssh, host, sender_command, b"attach", token);
    (data, control)
}

fn spawn(ssh: &[Vec<u8>], host: &[u8], sender_command: &[u8], role: &[u8], token: &str) -> Session {
    let remote = remote_command(&[sender_command, role, b"--token", token.as_bytes()]);
    let argv = session_argv(ssh, host, &remote);
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    let mut child = match die::spawn_guarded(die::KillGuard::Process, &mut command) {
        Ok(child) => child,
        Err(e) => die(format!(
            "cannot start {}: {e}",
            argv[0].as_encoded_bytes().as_bstr()
        )),
    };
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    Session {
        child,
        stdin,
        stdout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(text: &str) -> Vec<Vec<u8>> {
        shell_split(text.as_bytes()).unwrap()
    }

    #[test]
    fn words_split_like_a_shell() {
        assert_eq!(split("ssh"), vec![b"ssh".to_vec()]);
        assert_eq!(
            split("  ssh  -p   2222 "),
            vec![b"ssh".to_vec(), b"-p".to_vec(), b"2222".to_vec()]
        );
        assert_eq!(
            split(r"ssh -i 'my key'"),
            vec![b"ssh".to_vec(), b"-i".to_vec(), b"my key".to_vec()]
        );
        assert_eq!(split(r#"a"b c"d"#), vec![b"ab cd".to_vec()]);
        assert_eq!(split(r"a\ b"), vec![b"a b".to_vec()]);
        assert_eq!(split(r#""a\$\x""#), vec![b"a$\\x".to_vec()]);
        assert_eq!(split(r#""a\\b""#), vec![b"a\\b".to_vec()]);
        assert_eq!(split("'don'\\''t'"), vec![b"don't".to_vec()]);
        assert_eq!(split("a\\\nb"), vec![b"ab".to_vec()]);
        assert_eq!(split("\"a\\\nb\""), vec![b"ab".to_vec()]);
        assert_eq!(split(""), Vec::<Vec<u8>>::new());
        assert!(shell_split(b"'open").is_err());
        assert!(shell_split(b"\"open").is_err());
        assert!(shell_split(b"end\\").is_err());
    }

    #[test]
    fn remote_commands_quote_every_argument() {
        assert_eq!(
            remote_command(&[b"hashsync", b"serve", b"--token", b"abc"]),
            b"exec 'hashsync' 'serve' '--token' 'abc'".to_vec()
        );
        assert_eq!(
            remote_command(&[b"/odd path/it's \"here\"/$HOME `x` \xff!"]),
            b"exec '/odd path/it'\\''s \"here\"/$HOME `x` \xff!'".to_vec()
        );
    }

    #[test]
    fn session_argv_places_the_specified_options() {
        let ssh = vec![
            b"ssh".to_vec(),
            b"-C".to_vec(),
            b"-p".to_vec(),
            b"22".to_vec(),
        ];
        let argv = session_argv(&ssh, b"user@host", b"exec 'hashsync'");
        let words: Vec<&[u8]> = argv.iter().map(|w| w.as_encoded_bytes()).collect();
        assert_eq!(
            words,
            vec![
                &b"ssh"[..],
                b"-o",
                b"ClearAllForwardings=yes",
                b"-o",
                b"RemoteCommand=none",
                b"-C",
                b"-p",
                b"22",
                b"-T",
                b"-x",
                b"-o",
                b"Compression=no",
                b"user@host",
                b"exec 'hashsync'",
            ]
        );
    }
}
