//! `hashsync attach` driven directly through its standard streams, with
//! the test in the place of `serve` on the rendezvous socket
//! (DESIGN.md §6.1). No wait has a deadline: a regression hangs the test,
//! so that a debugger can examine it.

mod common;

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};

use common::KillOnDrop;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

#[test]
fn after_socket_closure_attach_discards_stdin_until_eof() {
    // `serve` closes the socket first. Then the stdin relay's next write
    // fails, and `attach` reads and discards the rest of its stdin, more
    // than a pipe holds, so that the client's control writer does not
    // block. At socket EOF, `attach` waits for that relay, and exits zero
    // only at stdin EOF, which the client causes after END
    // (DESIGN.md §6.1 and §6.9).
    let home = tempfile::Builder::new()
        .prefix("hashsync-attach-")
        .tempdir()
        .unwrap();
    let dir = home.path().join("runtime/hashsync");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .unwrap();
    // Listen through the directory's descriptor, as `serve` does: a
    // temporary path can exceed the `sun_path` limit.
    let dir_fd = std::fs::File::open(&dir).unwrap();
    let address = format!("/proc/self/fd/{}/{TOKEN}.sock", dir_fd.as_raw_fd());
    let listener = UnixListener::bind(address).unwrap();
    let mut attach = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_hashsync"))
            .args(["attach", "--token", TOKEN])
            .env("XDG_RUNTIME_DIR", home.path().join("runtime"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stdin = attach.0.stdin.take().unwrap();
    let mut stdout = attach.0.stdout.take().unwrap();
    let (mut socket, _) = listener.accept().unwrap();

    // Both directions relay.
    stdin.write_all(b"answers").unwrap();
    let mut relayed = [0u8; 7];
    socket.read_exact(&mut relayed).unwrap();
    assert_eq!(&relayed, b"answers");
    socket.write_all(b"queries").unwrap();
    stdout.read_exact(&mut relayed).unwrap();
    assert_eq!(&relayed, b"queries");

    drop(socket);
    stdin.write_all(&vec![0; 4 << 20]).unwrap();
    assert!(
        attach.0.try_wait().unwrap().is_none(),
        "attach ended before stdin EOF"
    );
    drop(stdin);
    let mut rest = Vec::new();
    stdout.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "bytes after the socket's");
    let mut stderr = String::new();
    let mut pipe = attach.0.stderr.take().unwrap();
    pipe.read_to_string(&mut stderr).unwrap();
    let status = attach.0.wait().unwrap();
    assert_eq!(status.code(), Some(0), "stderr:\n{stderr}");
}

#[test]
fn attach_marks_its_lines_as_the_servers() {
    // `attach` runs on the server, and SSH carries its stderr to the
    // client's, so its lines carry the server's marker (DESIGN.md §8). An
    // invalid token is the error that it reports before anything else.
    let output = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .args(["attach", "--token", "bad"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stderr, "hashsync (server): invalid token \"bad\"\n");
}

#[test]
fn a_usage_error_on_the_server_has_no_marker() {
    // A usage error of the command line of `serve` or `attach` comes before
    // either runs, so its lines carry no server marker (DESIGN.md §8).
    for subcommand in ["serve", "attach"] {
        let output = Command::new(env!("CARGO_BIN_EXE_hashsync"))
            .args([subcommand, "--no-such-option"])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{subcommand}:\n{stderr}");
        assert!(
            stderr.starts_with("error: unexpected argument '--no-such-option'"),
            "{subcommand}:\n{stderr}"
        );
        assert!(!stderr.contains("(server)"), "{subcommand}:\n{stderr}");
    }
}
