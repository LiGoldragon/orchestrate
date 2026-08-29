//! Executable-owned per-user locations for Orchestrate Nexus state and sockets.

use std::{
    env,
    path::{Path, PathBuf},
};

use meta_signal_orchestrate::Configure;
use thiserror::Error;

const STATE_DIRECTORY: &str = "orchestrate-nexus";
const STORE_FILE: &str = "orchestrate-nexus.sema";
const ORDINARY_SOCKET_FILE: &str = "orchestrate.sock";
const META_SOCKET_FILE: &str = "meta-orchestrate.sock";

/// The default durable store and socket configuration derived by the executable.
pub struct DefaultConfiguration {
    store_path: PathBuf,
    configuration: Configure,
}

impl DefaultConfiguration {
    /// Reads the per-user XDG roots and rejects every startup argument.
    pub fn from_process() -> Result<Self, DefaultConfigurationError> {
        if env::args_os().nth(1).is_some() {
            return Err(DefaultConfigurationError::StartupArguments);
        }
        let state_home = Self::state_home()?;
        let runtime_directory = Self::runtime_directory()?;
        let socket_directory = runtime_directory.join(STATE_DIRECTORY);
        let ordinary_socket_path = socket_directory
            .join(ORDINARY_SOCKET_FILE)
            .display()
            .to_string()
            .try_into()
            .map_err(|_| DefaultConfigurationError::InvalidSocketPath)?;
        let meta_socket_path = socket_directory
            .join(META_SOCKET_FILE)
            .display()
            .to_string()
            .try_into()
            .map_err(|_| DefaultConfigurationError::InvalidSocketPath)?;
        Ok(Self {
            store_path: state_home.join(STATE_DIRECTORY).join(STORE_FILE),
            configuration: Configure {
                ordinary_socket_path,
                meta_socket_path,
            },
        })
    }

    pub fn store_path(&self) -> &Path {
        &self.store_path
    }

    pub fn configuration(&self) -> Configure {
        self.configuration.clone()
    }

    fn state_home() -> Result<PathBuf, DefaultConfigurationError> {
        match env::var_os("XDG_STATE_HOME") {
            Some(path) => Self::absolute_path("XDG_STATE_HOME", PathBuf::from(path)),
            None => Ok(Self::home_directory()?.join(".local/state")),
        }
    }

    fn runtime_directory() -> Result<PathBuf, DefaultConfigurationError> {
        match env::var_os("XDG_RUNTIME_DIR") {
            Some(path) => Self::absolute_path("XDG_RUNTIME_DIR", PathBuf::from(path)),
            None => Err(DefaultConfigurationError::MissingRuntimeDirectory),
        }
    }

    fn home_directory() -> Result<PathBuf, DefaultConfigurationError> {
        match env::var_os("HOME") {
            Some(path) => Self::absolute_path("HOME", PathBuf::from(path)),
            None => Err(DefaultConfigurationError::MissingHomeDirectory),
        }
    }

    fn absolute_path(
        variable: &'static str,
        path: PathBuf,
    ) -> Result<PathBuf, DefaultConfigurationError> {
        if path.is_absolute() {
            Ok(path)
        } else {
            Err(DefaultConfigurationError::RelativePath { variable, path })
        }
    }
}

#[derive(Debug, Error)]
pub enum DefaultConfigurationError {
    #[error("accepts zero arguments")]
    StartupArguments,
    #[error("XDG_RUNTIME_DIR is required for the per-user Nexus runtime directory")]
    MissingRuntimeDirectory,
    #[error("HOME is required when XDG_STATE_HOME is unset")]
    MissingHomeDirectory,
    #[error("{variable} must be an absolute path: {path:?}")]
    RelativePath {
        variable: &'static str,
        path: PathBuf,
    },
    #[error("derived socket path is not representable as Datomic text")]
    InvalidSocketPath,
}
