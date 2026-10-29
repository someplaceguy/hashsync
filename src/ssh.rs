// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The two SSH sessions and their command lines (DESIGN.md §6.1).
//!
//! The `--ssh` command is shell-split; the first word is the executable
//! and the rest are its options. Leading `-o` options inserted by hashsync
//! take precedence under SSH's first-value rule, and the trailing `-T -x`
//! disable PTYs and X11 forwarding even if the `--ssh` options requested
//! them.
//!
//! SSH runs its remote command through the remote login shell, so the
//! bootstrap single-quotes every argument for POSIX shells, preserving
//! all bytes, including non-UTF-8 bytes.

use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use rustix::process::Pid;

use crate::die::{self, die};
use crate::escapes::quoted;

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

/// The SSH program of `--ssh`: the executable and its options (DESIGN.md
/// §6.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Program {
    pub executable: Vec<u8>,
    pub options: Vec<Vec<u8>>,
}

impl Program {
    /// The program of the `--ssh` command `text`, which is shell-split
    /// without byte escapes, as no CLI argument has them. The first word is
    /// the executable, so a command without words is an error.
    pub fn parse(text: &[u8]) -> Result<Program, String> {
        let mut words = shell_split(text)?.into_iter();
        let Some(executable) = words.next() else {
            return Err("empty command".into());
        };
        Ok(Program {
            executable,
            options: words.collect(),
        })
    }
}

/// How the client reaches the server (DESIGN.md §6.1 and §8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remote {
    /// `--ssh`, `ssh` by default.
    pub ssh: Program,
    /// `-l USER`, which goes to SSH as it is.
    pub login: Option<Vec<u8>>,
    /// The SSH destination, `[USER@]HOST`, which goes to SSH unchanged.
    pub host: Vec<u8>,
    /// `--hashsync-path`, `hashsync` by default: one executable name or
    /// path, not a shell fragment, which the bootstrap quotes as one word.
    pub hashsync_path: Vec<u8>,
}

/// One session's local argv: the `--ssh` program with the options,
/// `-l USER`, HOST, and remote command that DESIGN.md §6.1 specifies.
pub fn session_argv(remote: &Remote, command: &[u8]) -> Vec<OsString> {
    let ssh = &remote.ssh;
    let mut argv: Vec<Vec<u8>> = Vec::with_capacity(ssh.options.len() + 13);
    argv.push(ssh.executable.clone());
    for option in ["-o", "ClearAllForwardings=yes", "-o", "RemoteCommand=none"] {
        argv.push(option.into());
    }
    argv.extend(ssh.options.iter().cloned());
    for option in ["-T", "-x", "-o", "Compression=no"] {
        argv.push(option.into());
    }
    if let Some(login) = &remote.login {
        argv.push(b"-l".to_vec());
        argv.push(login.clone());
    }
    argv.push(remote.host.clone());
    argv.push(command.to_vec());
    argv.into_iter().map(OsString::from_vec).collect()
}

/// One running SSH session. The child stays unreaped so its PID stays
/// reserved (DESIGN.md §6.8); dropping `stdin` closes the pipe, and the
/// server takes that EOF as the client's disappearance until it has sent
/// END, so the client holds `stdin` until END (DESIGN.md §6.9).
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
/// SIGKILLed on a fatal exit; stderr is inherited, so the server's
/// relayed bytes and SSH's own errors reach the client's stderr.
pub fn start_sessions(remote: &Remote, token: &str) -> (Session, Session) {
    let data = spawn(remote, b"serve", token);
    let control = spawn(remote, b"attach", token);
    (data, control)
}

fn spawn(remote: &Remote, role: &[u8], token: &str) -> Session {
    let command = remote_command(&[&remote.hashsync_path, role, b"--token", token.as_bytes()]);
    let argv = session_argv(remote, &command);
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    let mut child = match die::spawn_guarded(die::KillGuard::Process, &mut command) {
        Ok(child) => child,
        Err(e) => die(format!(
            "cannot start {}: {e}",
            quoted(argv[0].as_encoded_bytes())
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
        let mut remote = Remote {
            ssh: Program::parse(b"ssh -C -p 22").unwrap(),
            login: None,
            host: b"user@host".to_vec(),
            hashsync_path: b"hashsync".to_vec(),
        };
        let words = |remote: &Remote| -> Vec<Vec<u8>> {
            let argv = session_argv(remote, b"exec 'hashsync'");
            argv.iter().map(|w| w.as_encoded_bytes().to_vec()).collect()
        };
        let expected: Vec<&[u8]> = vec![
            b"ssh",
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
        ];
        assert_eq!(words(&remote), expected);
        // `-l USER` goes right before HOST.
        remote.login = Some(b"backup".to_vec());
        let mut with_login = expected.clone();
        with_login.splice(12..12, [&b"-l"[..], b"backup"]);
        assert_eq!(words(&remote), with_login);
    }

    /// The first word of `--ssh` is the executable and the rest are its
    /// options, so a command needs a word. `--ssh` gets no byte escapes: a
    /// CLI argument has none.
    #[test]
    fn the_ssh_command_is_an_executable_and_its_options() {
        let program = |executable: &[u8], options: &[&[u8]]| Program {
            executable: executable.to_vec(),
            options: options.iter().map(|option| option.to_vec()).collect(),
        };
        assert_eq!(Program::parse(b"ssh"), Ok(program(b"ssh", &[])));
        assert_eq!(
            Program::parse(br"ssh -o 'X\x41'"),
            Ok(program(b"ssh", &[b"-o", br"X\x41"]))
        );
        for (text, error) in [
            (&b""[..], "empty command"),
            (b"  ", "empty command"),
            (b"ssh 'open", "unterminated single quote"),
        ] {
            assert_eq!(Program::parse(text), Err(error.into()));
        }
    }
}
