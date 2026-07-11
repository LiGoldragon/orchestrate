use std::path::Path;

use nota::{NotaDecode, NotaEncode};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_orchestrate::WirePath;
use triad_runtime::{RequestConcurrencyLimit, SocketMode};

const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

#[derive(
    NotaEncode, NotaDecode, Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq,
)]
pub struct DaemonConfiguration {
    pub store_path: WirePath,
    pub ordinary_socket_path: WirePath,
    pub meta_socket_path: WirePath,
    pub upgrade_socket_path: WirePath,
    pub workspace_root: WirePath,
    pub git_index_root: WirePath,
    /// The co-resident router's working socket, when configured. On a successful
    /// agent registration with discovered reachability, orchestrate propagates
    /// the minted identity to the router over this socket so it becomes a live
    /// delivery target. `None` (the default, and the shape an older deployment
    /// writes) leaves registration landing without router propagation.
    pub router_working_socket_path: Option<WirePath>,
}

impl DaemonConfiguration {
    pub fn new(
        store_path: WirePath,
        ordinary_socket_path: WirePath,
        meta_socket_path: WirePath,
        upgrade_socket_path: WirePath,
        workspace_root: WirePath,
        git_index_root: WirePath,
    ) -> Self {
        Self {
            store_path,
            ordinary_socket_path,
            meta_socket_path,
            upgrade_socket_path,
            workspace_root,
            git_index_root,
            router_working_socket_path: None,
        }
    }

    /// Set the co-resident router working socket the daemon propagates
    /// registrations to. The write-configuration boundary calls this when a
    /// router socket path is supplied; absent it, registration lands without
    /// router propagation.
    pub fn with_router_working_socket_path(mut self, router_working_socket_path: WirePath) -> Self {
        self.router_working_socket_path = Some(router_working_socket_path);
        self
    }

    pub fn router_working_socket_path(&self) -> Option<&WirePath> {
        self.router_working_socket_path.as_ref()
    }

    /// Encode the configuration to the binary rkyv form the daemon accepts as
    /// its single startup argument (daemons never parse NOTA — hard override).
    pub fn to_signal_bytes(&self) -> Result<Vec<u8>, ConfigurationError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ConfigurationError::ArchiveEncode)
    }

    /// Decode the configuration from the binary rkyv startup file.
    pub fn from_signal_bytes(bytes: &[u8]) -> Result<Self, ConfigurationError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|_| ConfigurationError::ArchiveDecode)
    }

    /// Read and decode the binary rkyv configuration from the daemon's single
    /// startup-argument file path.
    pub fn from_signal_file(path: &Path) -> Result<Self, ConfigurationError> {
        let bytes = std::fs::read(path).map_err(ConfigurationError::Read)?;
        Self::from_signal_bytes(&bytes)
    }
}

impl triad_runtime::BindingSurface for DaemonConfiguration {
    fn socket_path(&self) -> &Path {
        Path::new(self.ordinary_socket_path.as_str())
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(OWNER_ONLY_SOCKET_MODE))
    }

    fn request_concurrency_limit(&self) -> RequestConcurrencyLimit {
        RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(Path::new(self.meta_socket_path.as_str()))
    }

    fn upgrade_socket_path(&self) -> Option<&Path> {
        Some(Path::new(self.upgrade_socket_path.as_str()))
    }

    fn database_path(&self) -> &Path {
        Path::new(self.store_path.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("read daemon configuration file: {0}")]
    Read(std::io::Error),

    #[error("daemon configuration rkyv encode failed")]
    ArchiveEncode,

    #[error("daemon configuration rkyv decode failed")]
    ArchiveDecode,
}
