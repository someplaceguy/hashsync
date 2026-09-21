//! `hashsync attach --token T` (DESIGN.md §6.1): the control-session
//! relay between SSH and `serve`'s rendezvous socket.
//!
//! Stdin is relayed to the socket and the socket to stdout. Stdin EOF
//! half-closes the socket. Socket EOF or a peer-closed socket error ends
//! the relay successfully after flushing stdout, without waiting for
//! stdin EOF or joining the other relay thread; the receiver decides
//! whether that closure was expected. Either direction may observe
//! closure first. Nothing is buffered: both directions write straight to
//! their descriptors, so the successful end has nothing to flush.

use std::io::{Read, Write};
use std::net::Shutdown;

use crate::die::{self, die};
use crate::fsops::{self, retry_interrupted};
use crate::rendezvous;

/// True when the peer closed the socket: the normal end of the relay.
fn peer_closed(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
    )
}

pub fn run(token: &str) -> ! {
    let socket = rendezvous::connect(token);
    let socket_in = match socket.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the socket: {e}")),
    };

    // Stdin to socket: on stdin EOF, half-close the socket so `serve`
    // sees the end of control input while its output still flows.
    std::thread::Builder::new()
        .name("stdin-relay".into())
        .spawn(move || {
            let mut socket = socket;
            let mut stdin = fsops::stdin();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match retry_interrupted(|| stdin.read(&mut buf)) {
                    Ok(0) => {
                        let _ = socket.shutdown(Shutdown::Write);
                        return;
                    }
                    Ok(n) => n,
                    Err(e) => die(format!("control relay: stdin: {e}")),
                };
                match socket.write_all(&buf[..n]) {
                    Ok(()) => {}
                    Err(e) if peer_closed(&e) => die::finish(0),
                    Err(e) => die(format!("control relay: socket: {e}")),
                }
            }
        })
        .expect("cannot start the relay thread");

    // Socket to stdout.
    let mut socket_in = socket_in;
    let mut stdout = fsops::stdout();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match retry_interrupted(|| socket_in.read(&mut buf)) {
            Ok(0) => die::finish(0),
            Ok(n) => n,
            Err(e) if peer_closed(&e) => die::finish(0),
            Err(e) => die(format!("control relay: socket: {e}")),
        };
        if let Err(e) = stdout.write_all(&buf[..n]) {
            die(format!("control relay: stdout: {e}"));
        }
    }
}
