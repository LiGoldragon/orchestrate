use std::path::Path;

use nota_next::{NotaDecode, NotaEncode};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_orchestrate::WirePath;
use triad_runtime::{
    AbsoluteRuntimePath, RequestConcurrencyLimit, RuntimePathError, SocketMode, SocketPathSource,
};

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
        }
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
        let configuration = rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|_| ConfigurationError::ArchiveDecode)?;
        configuration.validate()?;
        Ok(configuration)
    }

    pub fn validate(&self) -> Result<(), ConfigurationError> {
        ConfigurationRuntimePaths::new(self).validate()
    }

    /// Read and decode the binary rkyv configuration from the daemon's single
    /// startup-argument file path.
    pub fn from_signal_file(path: &Path) -> Result<Self, ConfigurationError> {
        let bytes = std::fs::read(path).map_err(ConfigurationError::Read)?;
        Self::from_signal_bytes(&bytes)
    }
}

struct ConfigurationRuntimePaths<'configuration> {
    configuration: &'configuration DaemonConfiguration,
}

impl<'configuration> ConfigurationRuntimePaths<'configuration> {
    fn new(configuration: &'configuration DaemonConfiguration) -> Self {
        Self { configuration }
    }

    fn validate(&self) -> Result<(), ConfigurationError> {
        self.validate_path("store_path", self.configuration.store_path.as_str())?;
        self.validate_path(
            "ordinary_socket_path",
            self.configuration.ordinary_socket_path.as_str(),
        )?;
        self.validate_path(
            "meta_socket_path",
            self.configuration.meta_socket_path.as_str(),
        )?;
        self.validate_path(
            "upgrade_socket_path",
            self.configuration.upgrade_socket_path.as_str(),
        )?;
        self.validate_path("workspace_root", self.configuration.workspace_root.as_str())?;
        self.validate_path("git_index_root", self.configuration.git_index_root.as_str())
    }

    fn validate_path(&self, field: &str, value: &str) -> Result<(), ConfigurationError> {
        AbsoluteRuntimePath::try_new(SocketPathSource::configuration_field(field), value)
            .map(|_| ())
            .map_err(ConfigurationError::RuntimePath)
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

    #[error("daemon configuration runtime path validation failed: {0}")]
    RuntimePath(#[from] RuntimePathError),
}
