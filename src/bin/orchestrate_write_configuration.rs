//! Write the binary daemon configuration consumed by `orchestrate-daemon`.
//!
//! The daemon deliberately accepts one signal-encoded configuration file rather
//! than parsing text at startup. This small boundary program lets declarative
//! service managers materialize that file from ordinary absolute paths.

use std::{
    env,
    ffi::OsString,
    fmt::{Display, Formatter},
    path::{Component, Path, PathBuf},
    process::ExitCode,
};

use orchestrate::{
    ConfigurationError, DaemonConfiguration, Error as OrchestrateError, layout::wire_path,
};
use thiserror::Error;

const REQUIRED_ARGUMENT_COUNT: usize = 7;

fn main() -> ExitCode {
    match DaemonConfigurationWriter::from_environment().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-write-configuration: {error}");
            ExitCode::FAILURE
        }
    }
}

struct DaemonConfigurationWriter {
    arguments: Vec<OsString>,
}

impl DaemonConfigurationWriter {
    fn from_environment() -> Self {
        Self {
            arguments: env::args_os().skip(1).collect(),
        }
    }

    fn run(self) -> Result<(), DaemonConfigurationWriterError> {
        let arguments = DaemonConfigurationArguments::try_from(self.arguments)?;
        arguments.write()
    }
}

struct DaemonConfigurationArguments {
    signal_path: RuntimePath,
    store_path: RuntimePath,
    ordinary_socket_path: RuntimePath,
    meta_socket_path: RuntimePath,
    upgrade_socket_path: RuntimePath,
    workspace_root: RuntimePath,
    git_index_root: RuntimePath,
}

impl TryFrom<Vec<OsString>> for DaemonConfigurationArguments {
    type Error = DaemonConfigurationWriterError;

    fn try_from(arguments: Vec<OsString>) -> Result<Self, Self::Error> {
        if arguments.len() != REQUIRED_ARGUMENT_COUNT {
            return Err(DaemonConfigurationWriterError::ArgumentCount {
                expected: REQUIRED_ARGUMENT_COUNT,
                actual: arguments.len(),
            });
        }
        let mut paths = arguments.into_iter();
        Ok(Self {
            signal_path: ArgumentPath::required("signal_path", &mut paths)?,
            store_path: ArgumentPath::required("store_path", &mut paths)?,
            ordinary_socket_path: ArgumentPath::required("ordinary_socket_path", &mut paths)?,
            meta_socket_path: ArgumentPath::required("meta_socket_path", &mut paths)?,
            upgrade_socket_path: ArgumentPath::required("upgrade_socket_path", &mut paths)?,
            workspace_root: ArgumentPath::required("workspace_root", &mut paths)?,
            git_index_root: ArgumentPath::required("git_index_root", &mut paths)?,
        })
    }
}

impl DaemonConfigurationArguments {
    fn write(self) -> Result<(), DaemonConfigurationWriterError> {
        let bytes = self.configuration()?.to_signal_bytes()?;
        self.create_runtime_directories()?;
        let signal_path = self.signal_path.into_path_buf();
        std::fs::write(&signal_path, bytes).map_err(|source| {
            DaemonConfigurationWriterError::WriteSignalFile {
                path: signal_path,
                source,
            }
        })
    }

    fn configuration(&self) -> Result<DaemonConfiguration, DaemonConfigurationWriterError> {
        Ok(DaemonConfiguration::new(
            wire_path(self.store_path.as_path())?,
            wire_path(self.ordinary_socket_path.as_path())?,
            wire_path(self.meta_socket_path.as_path())?,
            wire_path(self.upgrade_socket_path.as_path())?,
            wire_path(self.workspace_root.as_path())?,
            wire_path(self.git_index_root.as_path())?,
        ))
    }

    fn create_runtime_directories(&self) -> Result<(), DaemonConfigurationWriterError> {
        PathPreparation::new(&self.signal_path).create_parent()?;
        PathPreparation::new(&self.store_path).create_parent()?;
        PathPreparation::new(&self.ordinary_socket_path).create_parent()?;
        PathPreparation::new(&self.meta_socket_path).create_parent()?;
        PathPreparation::new(&self.upgrade_socket_path).create_parent()
    }
}

struct ArgumentPath;

impl ArgumentPath {
    fn required(
        field: &'static str,
        paths: &mut impl Iterator<Item = OsString>,
    ) -> Result<RuntimePath, DaemonConfigurationWriterError> {
        let path = paths
            .next()
            .ok_or(DaemonConfigurationWriterError::MissingArgument)?;
        RuntimePath::try_new(field, PathBuf::from(path))
            .map_err(DaemonConfigurationWriterError::RuntimePath)
    }
}

struct RuntimePath {
    path: PathBuf,
}

impl RuntimePath {
    fn try_new(field: &'static str, path: PathBuf) -> Result<Self, RuntimePathError> {
        if path.as_os_str().is_empty() {
            return Err(RuntimePathError::new(
                field,
                path,
                RuntimePathErrorKind::Empty,
            ));
        }
        if !path.is_absolute() {
            return Err(RuntimePathError::new(
                field,
                path,
                RuntimePathErrorKind::Relative,
            ));
        }
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(RuntimePathError::new(
                field,
                path,
                RuntimePathErrorKind::ParentDirectory,
            ));
        }
        Ok(Self { path })
    }

    fn as_path(&self) -> &Path {
        &self.path
    }

    fn into_path_buf(self) -> PathBuf {
        self.path
    }
}

#[derive(Debug)]
struct RuntimePathError {
    field: &'static str,
    path: PathBuf,
    kind: RuntimePathErrorKind,
}

impl RuntimePathError {
    fn new(field: &'static str, path: PathBuf, kind: RuntimePathErrorKind) -> Self {
        Self { field, path, kind }
    }
}

impl Display for RuntimePathError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "configuration field {} path {} is {}",
            self.field,
            self.path.display(),
            self.kind
        )
    }
}

impl std::error::Error for RuntimePathError {}

#[derive(Clone, Copy, Debug)]
enum RuntimePathErrorKind {
    Empty,
    Relative,
    ParentDirectory,
}

impl Display for RuntimePathErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("empty"),
            Self::Relative => formatter.write_str("relative"),
            Self::ParentDirectory => formatter.write_str("using a parent directory component"),
        }
    }
}

struct PathPreparation<'path> {
    path: &'path RuntimePath,
}

impl<'path> PathPreparation<'path> {
    fn new(path: &'path RuntimePath) -> Self {
        Self { path }
    }

    fn create_parent(&self) -> Result<(), DaemonConfigurationWriterError> {
        match self.path.as_path().parent() {
            Some(parent) => std::fs::create_dir_all(parent).map_err(|source| {
                DaemonConfigurationWriterError::CreateDirectory {
                    path: parent.to_path_buf(),
                    source,
                }
            }),
            None => Err(DaemonConfigurationWriterError::ParentlessPath {
                path: self.path.as_path().to_path_buf(),
            }),
        }
    }
}

#[derive(Debug, Error)]
enum DaemonConfigurationWriterError {
    #[error("expected {expected} path arguments, received {actual}")]
    ArgumentCount { expected: usize, actual: usize },

    #[error("missing required path argument")]
    MissingArgument,

    #[error("invalid orchestrate path: {0}")]
    Path(#[from] OrchestrateError),

    #[error("invalid runtime path: {0}")]
    RuntimePath(#[from] RuntimePathError),

    #[error("configuration encode failed: {0}")]
    Configuration(#[from] ConfigurationError),

    #[error("create directory {}: {source}", path.display())]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("path has no parent directory: {}", path.display())]
    ParentlessPath { path: PathBuf },

    #[error("write signal file {}: {source}", path.display())]
    WriteSignalFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
