//! Binding a Nexus socket, and the authority the socket itself carries.
//!
//! The meta socket is the root of the Nexus, so it is not merely a second
//! filename: it is bound for the owning user alone, and a connection on it is
//! answered only when the kernel says the peer is that user. The ordinary
//! socket serves any peer the filesystem admits.

use std::{
    fs,
    io::ErrorKind,
    os::unix::{fs::PermissionsExt, net::UnixStream as StandardUnixStream},
    path::Path,
};

use tokio::net::{UnixListener, UnixStream};

use super::TransportError;

/// The access one socket grants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketAuthority {
    /// Readable and writable by the owning user and group.
    Ordinary,
    /// Readable and writable by the owning user alone, and answered only for
    /// that user.
    Privileged,
}

/// An authority states the file mode that expresses it.
pub trait Permissive {
    fn mode(&self) -> u32;
    fn admits(&self, peer_user: u32, owner: u32) -> bool;
}

impl Permissive for SocketAuthority {
    fn mode(&self) -> u32 {
        match self {
            Self::Ordinary => 0o660,
            Self::Privileged => 0o600,
        }
    }

    fn admits(&self, peer_user: u32, owner: u32) -> bool {
        match self {
            Self::Ordinary => true,
            Self::Privileged => peer_user == owner,
        }
    }
}

/// A path can become a bound Nexus socket carrying one authority.
pub trait Bindable {
    fn bind_socket(&self, authority: SocketAuthority) -> Result<UnixListener, TransportError>;
    fn prepare_socket(&self) -> Result<(), TransportError>;
}

impl Bindable for Path {
    fn bind_socket(&self, authority: SocketAuthority) -> Result<UnixListener, TransportError> {
        self.prepare_socket()?;
        let listener = UnixListener::bind(self)?;
        // The mode is set after bind: a Unix socket takes its permissions
        // from the umask at bind time, which the Nexus does not own.
        fs::set_permissions(self, fs::Permissions::from_mode(authority.mode()))?;
        Ok(listener)
    }

    fn prepare_socket(&self) -> Result<(), TransportError> {
        let parent = self
            .parent()
            .ok_or_else(|| TransportError::MissingSocketParent(self.to_path_buf()))?;
        fs::create_dir_all(parent)?;
        match StandardUnixStream::connect(self) {
            Ok(_) => Err(TransportError::SocketAlreadyActive(self.to_path_buf())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                fs::remove_file(self)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
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
    fn user(&self) -> u32;
}

impl OwnsSocket for SocketOwner {
    fn of(socket_path: &Path) -> Result<Self, TransportError> {
        Ok(Self {
            user: std::os::unix::fs::MetadataExt::uid(&fs::metadata(socket_path)?),
        })
    }

    fn user(&self) -> u32 {
        self.user
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_privileged_authority_admits_its_owner_and_nobody_else() {
        let owner = 1001;
        assert!(SocketAuthority::Privileged.admits(owner, owner));
        for peer in [0, 1, 1000, 1002, u32::MAX] {
            assert!(
                !SocketAuthority::Privileged.admits(peer, owner),
                "user {peer} is not the Nexus's own user {owner}, root included"
            );
            assert!(
                SocketAuthority::Ordinary.admits(peer, owner),
                "the ordinary socket admits whoever the filesystem let through"
            );
        }
    }

    #[test]
    fn the_privileged_socket_mode_grants_nothing_beyond_its_owner() {
        assert_eq!(SocketAuthority::Privileged.mode() & 0o077, 0);
        assert_eq!(SocketAuthority::Privileged.mode(), 0o600);
        assert_eq!(SocketAuthority::Ordinary.mode(), 0o660);
        assert_eq!(
            SocketAuthority::Ordinary.mode() & 0o007,
            0,
            "neither socket is world-reachable"
        );
    }
}
