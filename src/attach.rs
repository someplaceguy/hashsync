// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! `hashsync attach --token T` (DESIGN.md §6.1): the control-session
//! relay between SSH and `serve`'s rendezvous socket.
//!
//! Stdin is relayed to the socket and the socket to stdout. Stdin EOF
//! half-closes the socket. Socket EOF or a peer-closed socket error ends
//! the relay from the socket; the main thread then waits until stdin EOF
//! ends the relay from stdin, joins that thread, and exits successfully.
//! After a write finds the socket closed by `serve`, the relay from stdin
//! reads stdin to EOF and discards it: the client closes the session's
//! stdin only after its control writer has ended, and that writer must
//! not block on a full pipe. Each host decides from END whether a closure
//! was expected. Either direction may observe closure first. Nothing is
//! buffered: both directions write straight to their descriptors, so the
//! successful end has nothing to flush.

use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;

use crate::die::{self, die};
use crate::fsops::{self, retry_interrupted};
use crate::rendezvous;
use crate::report;

/// True when the peer closed the socket: the normal end of the relay.
fn peer_closed(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
    )
}

pub fn run(token: &str) -> ! {
    // SSH carries this process's stderr to the client's: mark its lines as
    // the server's before anything can report one (DESIGN.md §8).
    report::mark_as_server();
    let socket = rendezvous::connect(token);
    let socket_in = match socket.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the socket: {e}")),
    };

    let stdin_relay = std::thread::Builder::new()
        .name("stdin-relay".into())
        .spawn(move || relay_stdin(socket))
        .expect("cannot start the relay thread");

    // Socket to stdout, until the socket closes.
    let mut socket_in = socket_in;
    let mut stdout = fsops::stdout();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match retry_interrupted(|| socket_in.read(&mut buf)) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if peer_closed(&e) => break,
            Err(e) => die(format!("control relay: socket: {e}")),
        };
        if let Err(e) = stdout.write_all(&buf[..n]) {
            die(format!("control relay: stdout: {e}"));
        }
    }
    stdin_relay.join().expect("the stdin relay panicked");
    die::finish(0)
}

/// Stdin to socket, until stdin EOF. On stdin EOF, half-close the socket
/// so `serve` sees the end of control input while its output still
/// flows. After `serve` has closed the socket, discard the rest of stdin.
fn relay_stdin(mut socket: UnixStream) {
    let mut stdin = fsops::stdin();
    let mut buf = vec![0u8; 64 * 1024];
    // False once a write found the socket closed by `serve`.
    let mut relaying = true;
    loop {
        let n = match retry_interrupted(|| stdin.read(&mut buf)) {
            Ok(0) => {
                let _ = socket.shutdown(Shutdown::Write);
                return;
            }
            Ok(n) => n,
            Err(e) => die(format!("control relay: stdin: {e}")),
        };
        if relaying {
            match socket.write_all(&buf[..n]) {
                Ok(()) => {}
                Err(e) if peer_closed(&e) => relaying = false,
                Err(e) => die(format!("control relay: socket: {e}")),
            }
        }
    }
}
