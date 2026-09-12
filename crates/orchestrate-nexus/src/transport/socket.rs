//! Claiming a Nexus socket path, binding it, and reading the peer on it.
//!
//! The access a socket grants — the mode it is bound with and the peers it
//! answers — is the `nexus` library's `SocketAuthority`, because it is the
//! same in every Nexus. What is here is the part that is not: taking a socket
//! path, on this filesystem, without taking one that belongs to somebody else.
//!
//! The question is "does this path belong to somebody else", and connecting
//! to the socket answers a different one: "is anything listening at this
//! inode right now". The two come apart in both directions. Remove the socket
//! file from under a serving Nexus — a cleanup script, a `tmpfiles` rule —
//! and the probe finds nothing, calls the path free, and lets a second Nexus
//! bind it while the first still runs on the unlinked inode. And whatever the
//! probe answers, it answers for the instant it ran: between the answer and
//! the bind, the path can change hands.
//!
//! An advisory lock on a file beside the socket, taken before the socket is
//! touched and held for the life of the process, answers the question that
//! was asked. It is held by a process rather than by an inode, so unlinking
//! the socket tells it nothing, and it cannot change hands underneath the
//! bind: either this process holds the path or another one does.

use std::{
    fs::{self, File, OpenOptions},
    io::ErrorKind,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use nexus::{Permissive, SocketAuthority};
use rustix::fs::{FlockOperation, flock};
use tokio::net::{UnixListener, UnixStream};

use super::TransportError;

/// The exclusive claim on one socket path, held for as long as the value
/// lives — which, for a serving Nexus, is the life of the process.
pub struct SocketClaim {
    socket_path: PathBuf,
    /// Held open only so that the advisory lock on it is held. Closing the
    /// file is how the claim is released, so this field is the claim, and
    /// nothing ever reads it.
    #[allow(dead_code)]
    claim: File,
}

/// A socket path is claimed before it is bound, and bound only by whoever
/// holds the claim.
pub trait Claiming: Sized {
    fn claim(socket_path: &Path) -> Result<Self, TransportError>;
    fn socket_path(&self) -> &Path;
    fn bind_listener(&self, authority: SocketAuthority) -> Result<UnixListener, TransportError>;
}

impl Claiming for SocketClaim {
    fn claim(socket_path: &Path) -> Result<Self, TransportError> {
        let parent = socket_path
            .parent()
            .ok_or_else(|| TransportError::MissingSocketParent(socket_path.to_path_buf()))?;
        fs::create_dir_all(parent)?;
        let claim = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(Self::claim_path(socket_path))?;
        match flock(&claim, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Self {
                socket_path: socket_path.to_path_buf(),
                claim,
            }),
            Err(refusal) if refusal == rustix::io::Errno::WOULDBLOCK => Err(
                TransportError::SocketAlreadyActive(socket_path.to_path_buf()),
            ),
            Err(refusal) => Err(TransportError::Io(refusal.into())),
        }
    }

    fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Whatever is at the path is this claim's to remove: nothing else holds
    /// the claim, so nothing else is listening there. A live Nexus's socket is
    /// never reached by this line, because its holder would have been refused
    /// at `claim`.
    fn bind_listener(&self, authority: SocketAuthority) -> Result<UnixListener, TransportError> {
        match fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(absent) if absent.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(&self.socket_path)?;
        // The mode is set after bind: a Unix socket takes its permissions
        // from the umask at bind time, which the Nexus does not own.
        fs::set_permissions(
            &self.socket_path,
            fs::Permissions::from_mode(authority.mode()),
        )?;
        Ok(listener)
    }
}

/// The file the claim is held on, beside the socket it claims.
trait Claimable {
    fn claim_path(socket_path: &Path) -> PathBuf;
}

impl Claimable for SocketClaim {
    fn claim_path(socket_path: &Path) -> PathBuf {
        let mut claim = socket_path.as_os_str().to_owned();
        claim.push(".claim");
        PathBuf::from(claim)
    }
}

/// A connected stream can name the user on the other end.
pub trait Attributable {
    fn peer_user(&self) -> Result<u32, TransportError>;
}

impl Attributable for UnixStream {
    fn peer_user(&self) -> Result<u32, TransportError> {
        Ok(self.peer_cred()?.uid())
    }
}

/// The user a bound socket belongs to.
///
/// Taken from the socket file the Nexus has just created, so it is the
/// process's own user by construction — read rather than assumed, and with no
/// second source of truth to drift from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SocketOwner {
    user: u32,
}

pub trait OwnsSocket: Sized {
    fn of(socket_path: &Path) -> Result<Self, TransportError>;
    /// A named owner, for a test that must express a socket belonging to a
    /// user this process is not. A single-user test host has no second user
    /// id to borrow, and the rule under test is about the two numbers being
    /// different, not about where either came from.
    #[cfg(test)]
    fn named(user: u32) -> Self;
    fn user(&self) -> u32;
}

impl OwnsSocket for SocketOwner {
    fn of(socket_path: &Path) -> Result<Self, TransportError> {
        Ok(Self {
            user: std::os::unix::fs::MetadataExt::uid(&fs::metadata(socket_path)?),
        })
    }

    #[cfg(test)]
    fn named(user: u32) -> Self {
        Self { user }
    }

    fn user(&self) -> u32 {
        self.user
    }
}
