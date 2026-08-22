//! Explicit daemon paths. The service owns no production defaults.

use std::{
    env,
    ffi::OsString,
    path::{Component, Path, PathBuf},
};

use thiserror::Error;
use triad_runtime::{BindingSurface, RequestConcurrencyLimit, SocketMode};

use crate::StoreLocation;

const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfiguration {
    pub store: StoreLocation,
    pub ordinary_socket: PathBuf,
    pub meta_socket: PathBuf,
    pub upgrade_socket: PathBuf,
}

impl DaemonConfiguration {
    pub fn from_process_arguments() -> Result<Self, ConfigurationError> {
        Self::from_arguments(env::args_os().skip(1))
    }

    pub fn from_arguments<Arguments, Argument>(
        arguments: Arguments,
    ) -> Result<Self, ConfigurationError>
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<OsString>,
    {
        let paths = arguments
            .into_iter()
            .map(|argument| PathBuf::from(argument.into()))
            .collect::<Vec<_>>();
        let [store, ordinary_socket, meta_socket, upgrade_socket]: [PathBuf; 4] = paths
            .try_into()
            .map_err(|paths: Vec<PathBuf>| ConfigurationError::ArgumentCount {
                actual: paths.len(),
            })?;
        for (field, path) in [
            ("store", &store),
            ("ordinary socket", &ordinary_socket),
            ("meta socket", &meta_socket),
            ("upgrade socket", &upgrade_socket),
        ] {
            Self::validate_absolute_path(field, path)?;
        }
        Ok(Self {
            store: StoreLocation::new(store.to_string_lossy()),
            ordinary_socket,
            meta_socket,
            upgrade_socket,
        })
    }

    fn validate_absolute_path(field: &'static str, path: &Path) -> Result<(), ConfigurationError> {
        if path.as_os_str().is_empty() || !path.is_absolute() {
            return Err(ConfigurationError::InvalidPath {
                field,
                path: path.to_path_buf(),
            });
        }
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(ConfigurationError::InvalidPath {
                field,
                path: path.to_path_buf(),
            });
        }
        Ok(())
    }
}

impl BindingSurface for DaemonConfiguration {
    fn socket_path(&self) -> &Path {
        &self.ordinary_socket
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(OWNER_ONLY_SOCKET_MODE))
    }

    fn request_concurrency_limit(&self) -> RequestConcurrencyLimit {
        RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(&self.meta_socket)
    }

    fn upgrade_socket_path(&self) -> Option<&Path> {
        Some(&self.upgrade_socket)
    }

    fn database_path(&self) -> &Path {
        self.store.as_path()
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error(
        "expected exactly four daemon paths (store, ordinary socket, meta socket, upgrade socket), received {actual}"
    )]
    ArgumentCount { actual: usize },

    #[error("{field} path must be a normalized absolute path: {}", path.display())]
    InvalidPath { field: &'static str, path: PathBuf },
}
