//! Who owns a socket path, and how a second Nexus finds out.
//!
//! A Unix socket file left behind by a Nexus that died is indistinguishable,
//! as a file, from one a live Nexus is listening on. Probing it by connecting
//! answers the question for the instant of the probe and leaves a window
//! between the probe and the bind in which the answer can change. A lock held
//! on a file beside the socket, for the life of the process, has no such
//! window: either this process holds it or another one does.

use std::{os::unix::net::UnixStream as StandardUnixStream, path::PathBuf};

use nexus::SocketAuthority;

use orchestrate_nexus::transport::{
    TransportError,
    socket::{Claiming, SocketClaim},
};

trait Claims {
    fn socket(&self, name: &str) -> PathBuf;
}

impl Claims for tempfile::TempDir {
    fn socket(&self, name: &str) -> PathBuf {
        self.path().join(name)
    }
}

#[test]
fn a_free_socket_path_is_claimed() {
    let directory = tempfile::tempdir().expect("isolated socket directory");
    let path = directory.socket("free.sock");
    let claim = SocketClaim::claim(&path).expect("claim a path nobody holds");
    assert_eq!(claim.socket_path(), path);
}

#[test]
fn a_path_another_claim_still_holds_is_refused_by_name() {
    let directory = tempfile::tempdir().expect("isolated socket directory");
    let path = directory.socket("held.sock");
    let held = SocketClaim::claim(&path).expect("the first claim succeeds");

    let error = SocketClaim::claim(&path)
        .err()
        .expect("a second claim on a held path is refused");
    let TransportError::SocketAlreadyActive(refused) = error else {
        panic!("expected a socket-already-active refusal, found {error:?}");
    };
    assert_eq!(refused, path);
    drop(held);
}

#[test]
fn a_released_claim_leaves_the_path_free() {
    let directory = tempfile::tempdir().expect("isolated socket directory");
    let path = directory.socket("released.sock");
    drop(SocketClaim::claim(&path).expect("claim it once"));
    SocketClaim::claim(&path).expect("and again once the first claim is gone");
}

/// The whole point of the change: a socket file left behind by a Nexus that
/// is no longer running is taken, and one a live Nexus is listening on is
/// not — and neither answer depends on connecting to it.
#[tokio::test]
async fn a_stale_socket_file_with_no_holder_is_taken() {
    let directory = tempfile::tempdir().expect("isolated socket directory");
    let path = directory.socket("stale.sock");
    let abandoned = std::os::unix::net::UnixListener::bind(&path).expect("bind a socket");
    drop(abandoned);
    assert!(
        path.exists(),
        "a dropped listener leaves its socket file behind, which is the \
         situation a restart finds"
    );

    let claim = SocketClaim::claim(&path).expect("nobody holds it, so it is ours");
    let listener = claim
        .bind_listener(SocketAuthority::Ordinary)
        .expect("and binding over the stale file succeeds");
    StandardUnixStream::connect(&path).expect("the new socket answers");
    drop(listener);
}

#[tokio::test]
async fn a_socket_a_live_holder_is_listening_on_is_not_taken() {
    let directory = tempfile::tempdir().expect("isolated socket directory");
    let path = directory.socket("live.sock");
    let claim = SocketClaim::claim(&path).expect("the live holder claims it");
    let listener = claim
        .bind_listener(SocketAuthority::Ordinary)
        .expect("and binds it");

    let error = SocketClaim::claim(&path)
        .err()
        .expect("a second Nexus is refused while the first is listening");
    assert!(matches!(error, TransportError::SocketAlreadyActive(_)));
    assert!(
        StandardUnixStream::connect(&path).is_ok(),
        "and the first Nexus's socket is untouched by the refusal"
    );
    drop(listener);
}
