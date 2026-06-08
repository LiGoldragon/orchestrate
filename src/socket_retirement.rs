//! The public-socket retirement step of the version-handover protocol.
//!
//! When a handover finalizes, the retiring orchestrate instance stops
//! accepting public (working + meta) traffic by removing those socket files —
//! the upgrade socket stays bound so the handover exchange can complete. The
//! engine owns this step because it owns the handover state machine that
//! decides when finalization happened; the paths to retire are captured from
//! the daemon configuration at startup.

use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// The ordinary and meta socket paths the engine retires on handover
/// finalization. An empty value (`none`) retires nothing — the shape tests open
/// the engine directly without a bound daemon and never finalize a handover
/// against live sockets.
#[derive(Clone, Debug, Default)]
pub struct PublicSocketRetirement {
    public_socket_paths: Vec<PathBuf>,
}

impl PublicSocketRetirement {
    /// The retirement set carrying the daemon's public (working + meta) socket
    /// paths.
    pub fn new(ordinary_socket_path: PathBuf, meta_socket_path: PathBuf) -> Self {
        Self {
            public_socket_paths: std::vec![ordinary_socket_path, meta_socket_path],
        }
    }

    /// The empty retirement set — retiring it removes nothing.
    pub fn none() -> Self {
        Self {
            public_socket_paths: Vec::new(),
        }
    }

    /// Remove each registered public socket file. A path that is absent is a
    /// no-op; a path that exists but is not a socket is an error (a foreign file
    /// at a socket path is a misconfiguration the daemon must not silently
    /// delete).
    pub fn retire(&self) -> Result<()> {
        for path in &self.public_socket_paths {
            Self::remove_socket_path(path)?;
        }
        Ok(())
    }

    fn remove_socket_path(path: &Path) -> Result<()> {
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                std::fs::remove_file(path)?;
                Ok(())
            }
            Ok(_) => Err(Error::SocketPathIsNotSocket(path.display().to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
