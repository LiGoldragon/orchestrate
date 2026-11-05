//! Executable-owned per-user locations for Orchestrate Nexus state and sockets.

use std::{
    env, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use signal_orchestrate::OrchestrateNexusConfiguration;
use thiserror::Error;

const STATE_DIRECTORY: &str = "orchestrate-nexus";
const STORE_FILE: &str = "orchestrate-nexus.memory";
const PRE_MEMORY_STORE_FILE: &str = "orchestrate-nexus.sema";
const ORDINARY_SOCKET_FILE: &str = "orchestrate.sock";
const META_SOCKET_FILE: &str = "orchestrate-meta.sock";

/// The default durable store and socket configuration derived by the executable.
pub struct DefaultConfiguration {
    store_path: PathBuf,
    configuration: OrchestrateNexusConfiguration,
}

pub trait ReadsDefaultConfiguration: Sized {
    fn from_process() -> Result<Self, DefaultConfigurationError>;
    fn store_path(&self) -> &Path;
    fn configuration(&self) -> OrchestrateNexusConfiguration;
}

impl ReadsDefaultConfiguration for DefaultConfiguration {
    /// Reads the per-user XDG roots and rejects every startup argument.
    fn from_process() -> Result<Self, DefaultConfigurationError> {
        if env::args_os().nth(1).is_some() {
            return Err(DefaultConfigurationError::StartupArguments);
        }
        let state_home = Self::state_home()?;
        let runtime_directory = Self::runtime_directory()?;
        let socket_directory = runtime_directory.join(STATE_DIRECTORY);
        let ordinary_socket_path = socket_directory
            .join(ORDINARY_SOCKET_FILE)
            .display()
            .to_string();
        let meta_socket_path = socket_directory
            .join(META_SOCKET_FILE)
            .display()
            .to_string();
        Ok(Self {
            store_path: Self::upgraded_store_path(&state_home)?,
            configuration: OrchestrateNexusConfiguration {
                ordinary_socket_path,
                meta_socket_path,
            },
        })
    }

    fn store_path(&self) -> &Path {
        &self.store_path
    }

    fn configuration(&self) -> OrchestrateNexusConfiguration {
        self.configuration.clone()
    }
}

trait UpgradesDefaultStore {
    fn upgraded_store_path(state_home: &Path) -> Result<PathBuf, DefaultConfigurationError>;
    fn path_exists(path: &Path) -> Result<bool, DefaultConfigurationError>;
}

impl UpgradesDefaultStore for DefaultConfiguration {
    fn upgraded_store_path(state_home: &Path) -> Result<PathBuf, DefaultConfigurationError> {
        let state_directory = state_home.join(STATE_DIRECTORY);
        let memory_path = state_directory.join(STORE_FILE);
        let previous_path = state_directory.join(PRE_MEMORY_STORE_FILE);
        match (
            Self::path_exists(&memory_path)?,
            Self::path_exists(&previous_path)?,
        ) {
            (true, true) => Err(DefaultConfigurationError::ConflictingStores {
                memory_path,
                previous_path,
            }),
            (true, false) | (false, false) => Ok(memory_path),
            (false, true) => {
                // Creating the new name without replacement makes concurrent
                // starts converge without either one overwriting a store.
                fs::hard_link(&previous_path, &memory_path).map_err(|source| {
                    DefaultConfigurationError::StorePathUpgrade {
                        previous_path: previous_path.clone(),
                        memory_path: memory_path.clone(),
                        source,
                    }
                })?;
                fs::remove_file(&previous_path).map_err(|source| {
                    DefaultConfigurationError::StorePathUpgrade {
                        previous_path,
                        memory_path: memory_path.clone(),
                        source,
                    }
                })?;
                Ok(memory_path)
            }
        }
    }

    fn path_exists(path: &Path) -> Result<bool, DefaultConfigurationError> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(false),
            Err(source) => Err(DefaultConfigurationError::InspectStorePath {
                path: path.to_path_buf(),
                source,
            }),
        }
    }
}

trait LocatesXdgRoots {
    fn state_home() -> Result<PathBuf, DefaultConfigurationError>;
    fn runtime_directory() -> Result<PathBuf, DefaultConfigurationError>;
    fn home_directory() -> Result<PathBuf, DefaultConfigurationError>;
    fn absolute_path(
        variable: &'static str,
        path: PathBuf,
    ) -> Result<PathBuf, DefaultConfigurationError>;
}

impl LocatesXdgRoots for DefaultConfiguration {
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
    #[error("both the Memory store {memory_path:?} and its previous path {previous_path:?} exist")]
    ConflictingStores {
        memory_path: PathBuf,
        previous_path: PathBuf,
    },
    #[error("cannot inspect default store path {path:?}: {source}")]
    InspectStorePath {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "cannot upgrade default store path from {previous_path:?} to {memory_path:?}: {source}"
    )]
    StorePathUpgrade {
        previous_path: PathBuf,
        memory_path: PathBuf,
        source: std::io::Error,
    },
}
