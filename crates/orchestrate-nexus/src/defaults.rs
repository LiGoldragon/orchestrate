//! Executable-owned per-user locations for Orchestrate Nexus state and sockets.

use std::{
    env, fs,
    io::ErrorKind,
    os::unix::fs::MetadataExt,
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
            store_path: Self::default_store_path(&state_home)?,
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

/// Moves a discovered previous default store to the Memory path at Nexus startup.
pub trait UpgradesDefaultStore: Sized {
    fn upgrade_store_path(self) -> Result<Self, DefaultConfigurationError>;
}

trait ResolvesDefaultStore {
    fn default_store_path(state_home: &Path) -> Result<PathBuf, DefaultConfigurationError>;
    fn path_exists(path: &Path) -> Result<bool, DefaultConfigurationError>;
    fn same_file(first: &Path, second: &Path) -> Result<Option<bool>, DefaultConfigurationError>;
    fn store_paths(store_path: &Path) -> (PathBuf, PathBuf);
    fn store_path_upgrade_error(
        previous_path: PathBuf,
        memory_path: PathBuf,
        source: std::io::Error,
    ) -> DefaultConfigurationError;
}

impl ResolvesDefaultStore for DefaultConfiguration {
    fn default_store_path(state_home: &Path) -> Result<PathBuf, DefaultConfigurationError> {
        let state_directory = state_home.join(STATE_DIRECTORY);
        let memory_path = state_directory.join(STORE_FILE);
        let previous_path = state_directory.join(PRE_MEMORY_STORE_FILE);
        loop {
            match (
                Self::path_exists(&memory_path)?,
                Self::path_exists(&previous_path)?,
            ) {
                (true, true) => match Self::same_file(&memory_path, &previous_path)? {
                    Some(true) => return Ok(memory_path),
                    Some(false) => {
                        return Err(DefaultConfigurationError::ConflictingStores {
                            memory_path,
                            previous_path,
                        });
                    }
                    None => {}
                },
                (true, false) | (false, false) => return Ok(memory_path),
                (false, true) => return Ok(previous_path),
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

    fn same_file(first: &Path, second: &Path) -> Result<Option<bool>, DefaultConfigurationError> {
        let first_metadata = match fs::symlink_metadata(first) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(DefaultConfigurationError::InspectStorePath {
                    path: first.to_path_buf(),
                    source,
                });
            }
        };
        let second_metadata = match fs::symlink_metadata(second) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(DefaultConfigurationError::InspectStorePath {
                    path: second.to_path_buf(),
                    source,
                });
            }
        };
        Ok(Some(
            first_metadata.dev() == second_metadata.dev()
                && first_metadata.ino() == second_metadata.ino(),
        ))
    }

    fn store_paths(store_path: &Path) -> (PathBuf, PathBuf) {
        let state_directory = store_path
            .parent()
            .expect("a derived default store path has a state directory");
        (
            state_directory.join(STORE_FILE),
            state_directory.join(PRE_MEMORY_STORE_FILE),
        )
    }

    fn store_path_upgrade_error(
        previous_path: PathBuf,
        memory_path: PathBuf,
        source: std::io::Error,
    ) -> DefaultConfigurationError {
        DefaultConfigurationError::StorePathUpgrade {
            previous_path,
            memory_path,
            source,
        }
    }
}

impl UpgradesDefaultStore for DefaultConfiguration {
    fn upgrade_store_path(mut self) -> Result<Self, DefaultConfigurationError> {
        let (memory_path, previous_path) = Self::store_paths(&self.store_path);
        loop {
            match (
                Self::path_exists(&memory_path)?,
                Self::path_exists(&previous_path)?,
            ) {
                (true, true) => match Self::same_file(&memory_path, &previous_path)? {
                    Some(true) => match fs::remove_file(&previous_path) {
                        Ok(()) => {}
                        Err(source) if source.kind() == ErrorKind::NotFound => {}
                        Err(source) => {
                            return Err(Self::store_path_upgrade_error(
                                previous_path,
                                memory_path,
                                source,
                            ));
                        }
                    },
                    Some(false) => {
                        return Err(DefaultConfigurationError::ConflictingStores {
                            memory_path,
                            previous_path,
                        });
                    }
                    None => {}
                },
                (true, false) | (false, false) => {
                    self.store_path = memory_path;
                    return Ok(self);
                }
                (false, true) => {
                    // Creating the new name without replacement makes concurrent
                    // starts converge without either one overwriting a store.
                    match fs::hard_link(&previous_path, &memory_path) {
                        Ok(()) => {}
                        Err(source)
                            if matches!(
                                source.kind(),
                                ErrorKind::AlreadyExists | ErrorKind::NotFound
                            ) => {}
                        Err(source) => {
                            return Err(Self::store_path_upgrade_error(
                                previous_path,
                                memory_path,
                                source,
                            ));
                        }
                    }
                }
            }
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
