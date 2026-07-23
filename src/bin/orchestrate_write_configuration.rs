//! Write the binary daemon configuration consumed by `orchestrate-daemon`.
//!
//! The daemon deliberately accepts one signal-encoded configuration file rather
//! than parsing text at startup. This small boundary program lets declarative
//! service managers materialize that file from ordinary absolute paths.

use std::{
    env,
    ffi::OsString,
    fmt::{Display, Formatter},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Component, Path, PathBuf},
    process::ExitCode,
};

use orchestrate::{
    ConfigurationError, DaemonConfiguration, Error as OrchestrateError, layout::wire_path,
};
use thiserror::Error;

/// The seven required path arguments (signal, store, three sockets, two roots).
///
/// Trailing arguments name optional co-resident downstream sockets by label —
/// `router=<absolute-path>` propagates discovered registrations to the router,
/// `messenger=<absolute-path>` pushes minted identities and discovered
/// endpoints to the messenger. Labels take any order and any subset; each may
/// appear at most once. An absent label leaves that propagation leg off.
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
    downstream_sockets: DownstreamSocketArguments,
}

impl TryFrom<Vec<OsString>> for DaemonConfigurationArguments {
    type Error = DaemonConfigurationWriterError;

    fn try_from(arguments: Vec<OsString>) -> Result<Self, Self::Error> {
        if arguments.len() < REQUIRED_ARGUMENT_COUNT {
            return Err(DaemonConfigurationWriterError::ArgumentCount {
                expected: REQUIRED_ARGUMENT_COUNT,
                actual: arguments.len(),
            });
        }
        let mut paths = ArgumentQueue::new(arguments);
        Ok(Self {
            signal_path: paths.required("signal_path")?,
            store_path: paths.required("store_path")?,
            ordinary_socket_path: paths.required("ordinary_socket_path")?,
            meta_socket_path: paths.required("meta_socket_path")?,
            upgrade_socket_path: paths.required("upgrade_socket_path")?,
            workspace_root: paths.required("workspace_root")?,
            git_index_root: paths.required("git_index_root")?,
            downstream_sockets: paths.downstream_sockets()?,
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
        let configuration = DaemonConfiguration::new(
            wire_path(self.store_path.as_path())?,
            wire_path(self.ordinary_socket_path.as_path())?,
            wire_path(self.meta_socket_path.as_path())?,
            wire_path(self.upgrade_socket_path.as_path())?,
            wire_path(self.workspace_root.as_path())?,
            wire_path(self.git_index_root.as_path())?,
        );
        let configuration = match &self.downstream_sockets.router_working_socket_path {
            Some(router_working_socket_path) => configuration
                .with_router_working_socket_path(wire_path(router_working_socket_path.as_path())?),
            None => configuration,
        };
        Ok(
            match &self.downstream_sockets.messenger_working_socket_path {
                Some(messenger_working_socket_path) => configuration
                    .with_messenger_working_socket_path(wire_path(
                        messenger_working_socket_path.as_path(),
                    )?),
                None => configuration,
            },
        )
    }

    fn create_runtime_directories(&self) -> Result<(), DaemonConfigurationWriterError> {
        PathPreparation::new(&self.signal_path).create_parent()?;
        PathPreparation::new(&self.store_path).create_parent()?;
        PathPreparation::new(&self.ordinary_socket_path).create_parent()?;
        PathPreparation::new(&self.meta_socket_path).create_parent()?;
        PathPreparation::new(&self.upgrade_socket_path).create_parent()
    }
}

struct ArgumentQueue {
    paths: std::vec::IntoIter<OsString>,
}

impl ArgumentQueue {
    fn new(arguments: Vec<OsString>) -> Self {
        Self {
            paths: arguments.into_iter(),
        }
    }

    fn required(
        &mut self,
        field: &'static str,
    ) -> Result<RuntimePath, DaemonConfigurationWriterError> {
        let path = self
            .paths
            .next()
            .ok_or(DaemonConfigurationWriterError::MissingArgument)?;
        RuntimePath::try_new(field, PathBuf::from(path))
            .map_err(DaemonConfigurationWriterError::RuntimePath)
    }

    /// Consume every trailing argument as a labeled downstream-socket
    /// assignment. Labels take any order and any subset, each at most once.
    fn downstream_sockets(
        self,
    ) -> Result<DownstreamSocketArguments, DaemonConfigurationWriterError> {
        let mut downstream_sockets = DownstreamSocketArguments::default();
        for argument in self.paths {
            downstream_sockets.assign(DownstreamSocketAssignment::parse(argument)?)?;
        }
        Ok(downstream_sockets)
    }
}

/// The optional co-resident downstream sockets, as parsed from labeled
/// trailing arguments. Each leg is independently expressible so any subset —
/// messenger without router included — is a truthful configuration.
#[derive(Default)]
struct DownstreamSocketArguments {
    router_working_socket_path: Option<RuntimePath>,
    messenger_working_socket_path: Option<RuntimePath>,
}

impl DownstreamSocketArguments {
    fn assign(
        &mut self,
        assignment: DownstreamSocketAssignment,
    ) -> Result<(), DaemonConfigurationWriterError> {
        let slot = match assignment.component {
            DownstreamSocketComponent::Router => &mut self.router_working_socket_path,
            DownstreamSocketComponent::Messenger => &mut self.messenger_working_socket_path,
        };
        if slot.is_some() {
            return Err(DaemonConfigurationWriterError::DuplicateDownstreamSocket {
                label: assignment.component.label(),
            });
        }
        *slot = Some(assignment.path);
        Ok(())
    }
}

/// One `label=<absolute-path>` trailing argument, parsed.
struct DownstreamSocketAssignment {
    component: DownstreamSocketComponent,
    path: RuntimePath,
}

impl DownstreamSocketAssignment {
    fn parse(argument: OsString) -> Result<Self, DaemonConfigurationWriterError> {
        let bytes = argument.as_bytes();
        let separator = bytes.iter().position(|byte| *byte == b'=').ok_or_else(|| {
            DaemonConfigurationWriterError::UnlabeledDownstreamSocket {
                argument: argument.to_string_lossy().into_owned(),
            }
        })?;
        let component = DownstreamSocketComponent::from_label_bytes(&bytes[..separator])?;
        let path = RuntimePath::try_new(
            component.field(),
            PathBuf::from(OsString::from_vec(bytes[separator + 1..].to_vec())),
        )?;
        Ok(Self { component, path })
    }
}

/// The closed set of co-resident components a downstream socket may name.
#[derive(Clone, Copy, Debug)]
enum DownstreamSocketComponent {
    Router,
    Messenger,
}

impl DownstreamSocketComponent {
    fn from_label_bytes(label: &[u8]) -> Result<Self, DaemonConfigurationWriterError> {
        match label {
            b"router" => Ok(Self::Router),
            b"messenger" => Ok(Self::Messenger),
            other => Err(
                DaemonConfigurationWriterError::UnknownDownstreamSocketLabel {
                    label: String::from_utf8_lossy(other).into_owned(),
                },
            ),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Router => "router",
            Self::Messenger => "messenger",
        }
    }

    const fn field(self) -> &'static str {
        match self {
            Self::Router => "router_working_socket_path",
            Self::Messenger => "messenger_working_socket_path",
        }
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
    #[error("expected at least {expected} path arguments, received {actual}")]
    ArgumentCount { expected: usize, actual: usize },

    #[error("missing required path argument")]
    MissingArgument,

    #[error(
        "trailing argument {argument} is not a labeled downstream socket; \
         expected router=<absolute-path> or messenger=<absolute-path>"
    )]
    UnlabeledDownstreamSocket { argument: String },

    #[error("unknown downstream socket label {label}; accepted labels are router and messenger")]
    UnknownDownstreamSocketLabel { label: String },

    #[error("downstream socket label {label} appears more than once")]
    DuplicateDownstreamSocket { label: &'static str },

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
