//! Typed daemon startup arguments.
//!
//! The daemon receives its configuration directly from its service manager as
//! argv.  There is deliberately no materialized configuration file: the only
//! files the daemon owns are its Sema store and any pre-migration preserve
//! beside that store.  The service manager owns the parent directories for the
//! store and Unix sockets.

use std::{
    env,
    ffi::OsString,
    fmt::{Display, Formatter},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Component, Path, PathBuf},
};

use signal_orchestrate::WirePath;
use triad_runtime::{RequestConcurrencyLimit, SocketMode};

use crate::{Error, layout::wire_path};

const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;
const REQUIRED_ARGUMENT_COUNT: usize = 6;

/// The daemon's typed runtime configuration, assembled directly from its
/// startup arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfiguration {
    pub store_path: WirePath,
    pub ordinary_socket_path: WirePath,
    pub meta_socket_path: WirePath,
    pub upgrade_socket_path: WirePath,
    pub workspace_root: WirePath,
    pub git_index_root: WirePath,
    /// The co-resident router's working socket, when configured. On a
    /// successful agent registration with discovered reachability,
    /// orchestrate propagates the minted identity to the router over this
    /// socket so it becomes a live delivery target.
    pub router_working_socket_path: Option<WirePath>,
    /// The co-resident messenger's working socket, when configured. The
    /// orchestrator pushes minted identities and discovered endpoints to its
    /// durable registry over this socket.
    pub messenger_working_socket_path: Option<WirePath>,
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
            messenger_working_socket_path: None,
        }
    }

    /// Parse the daemon's explicit argv contract. The six required arguments
    /// are, in order: Sema store, ordinary socket, meta socket, upgrade socket,
    /// workspace root, and git-index root. Optional downstream sockets are
    /// labeled `router=<absolute-path>` and `messenger=<absolute-path>`.
    pub fn from_process_arguments() -> std::result::Result<Self, ConfigurationError> {
        Self::from_arguments(env::args_os().skip(1))
    }

    pub fn from_arguments<Arguments, Argument>(
        arguments: Arguments,
    ) -> std::result::Result<Self, ConfigurationError>
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<OsString>,
    {
        DaemonConfigurationArguments::try_from(
            arguments.into_iter().map(Into::into).collect::<Vec<_>>(),
        )?
        .configuration()
    }

    pub fn with_router_working_socket_path(mut self, router_working_socket_path: WirePath) -> Self {
        self.router_working_socket_path = Some(router_working_socket_path);
        self
    }

    pub fn router_working_socket_path(&self) -> Option<&WirePath> {
        self.router_working_socket_path.as_ref()
    }

    pub fn with_messenger_working_socket_path(
        mut self,
        messenger_working_socket_path: WirePath,
    ) -> Self {
        self.messenger_working_socket_path = Some(messenger_working_socket_path);
        self
    }

    pub fn messenger_working_socket_path(&self) -> Option<&WirePath> {
        self.messenger_working_socket_path.as_ref()
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

struct DaemonConfigurationArguments {
    store_path: RuntimePath,
    ordinary_socket_path: RuntimePath,
    meta_socket_path: RuntimePath,
    upgrade_socket_path: RuntimePath,
    workspace_root: RuntimePath,
    git_index_root: RuntimePath,
    downstream_sockets: DownstreamSocketArguments,
}

impl TryFrom<Vec<OsString>> for DaemonConfigurationArguments {
    type Error = ConfigurationError;

    fn try_from(arguments: Vec<OsString>) -> std::result::Result<Self, Self::Error> {
        if arguments.len() < REQUIRED_ARGUMENT_COUNT {
            return Err(ConfigurationError::ArgumentCount {
                expected: REQUIRED_ARGUMENT_COUNT,
                actual: arguments.len(),
            });
        }
        let mut arguments = ArgumentQueue::new(arguments);
        Ok(Self {
            store_path: arguments.required("store_path")?,
            ordinary_socket_path: arguments.required("ordinary_socket_path")?,
            meta_socket_path: arguments.required("meta_socket_path")?,
            upgrade_socket_path: arguments.required("upgrade_socket_path")?,
            workspace_root: arguments.required("workspace_root")?,
            git_index_root: arguments.required("git_index_root")?,
            downstream_sockets: arguments.downstream_sockets()?,
        })
    }
}

impl DaemonConfigurationArguments {
    fn configuration(self) -> std::result::Result<DaemonConfiguration, ConfigurationError> {
        let configuration = DaemonConfiguration::new(
            wire_path(self.store_path.as_path())?,
            wire_path(self.ordinary_socket_path.as_path())?,
            wire_path(self.meta_socket_path.as_path())?,
            wire_path(self.upgrade_socket_path.as_path())?,
            wire_path(self.workspace_root.as_path())?,
            wire_path(self.git_index_root.as_path())?,
        );
        let configuration = match self.downstream_sockets.router_working_socket_path {
            Some(router_working_socket_path) => configuration
                .with_router_working_socket_path(wire_path(router_working_socket_path.as_path())?),
            None => configuration,
        };
        match self.downstream_sockets.messenger_working_socket_path {
            Some(messenger_working_socket_path) => Ok(configuration
                .with_messenger_working_socket_path(wire_path(
                    messenger_working_socket_path.as_path(),
                )?)),
            None => Ok(configuration),
        }
    }
}

struct ArgumentQueue {
    arguments: std::vec::IntoIter<OsString>,
}

impl ArgumentQueue {
    fn new(arguments: Vec<OsString>) -> Self {
        Self {
            arguments: arguments.into_iter(),
        }
    }

    fn required(
        &mut self,
        field: &'static str,
    ) -> std::result::Result<RuntimePath, ConfigurationError> {
        let path = self
            .arguments
            .next()
            .ok_or(ConfigurationError::MissingArgument)?;
        RuntimePath::try_new(field, PathBuf::from(path))
    }

    fn downstream_sockets(
        self,
    ) -> std::result::Result<DownstreamSocketArguments, ConfigurationError> {
        let mut downstream_sockets = DownstreamSocketArguments::default();
        for argument in self.arguments {
            downstream_sockets.assign(DownstreamSocketAssignment::parse(argument)?)?;
        }
        Ok(downstream_sockets)
    }
}

#[derive(Default)]
struct DownstreamSocketArguments {
    router_working_socket_path: Option<RuntimePath>,
    messenger_working_socket_path: Option<RuntimePath>,
}

impl DownstreamSocketArguments {
    fn assign(
        &mut self,
        assignment: DownstreamSocketAssignment,
    ) -> std::result::Result<(), ConfigurationError> {
        let slot = match assignment.component {
            DownstreamSocketComponent::Router => &mut self.router_working_socket_path,
            DownstreamSocketComponent::Messenger => &mut self.messenger_working_socket_path,
        };
        if slot.is_some() {
            return Err(ConfigurationError::DuplicateDownstreamSocket {
                label: assignment.component.label(),
            });
        }
        *slot = Some(assignment.path);
        Ok(())
    }
}

struct DownstreamSocketAssignment {
    component: DownstreamSocketComponent,
    path: RuntimePath,
}

impl DownstreamSocketAssignment {
    fn parse(argument: OsString) -> std::result::Result<Self, ConfigurationError> {
        let bytes = argument.as_bytes();
        let separator = bytes.iter().position(|byte| *byte == b'=').ok_or_else(|| {
            ConfigurationError::UnlabeledDownstreamSocket {
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

#[derive(Clone, Copy, Debug)]
enum DownstreamSocketComponent {
    Router,
    Messenger,
}

impl DownstreamSocketComponent {
    fn from_label_bytes(label: &[u8]) -> std::result::Result<Self, ConfigurationError> {
        match label {
            b"router" => Ok(Self::Router),
            b"messenger" => Ok(Self::Messenger),
            other => Err(ConfigurationError::UnknownDownstreamSocketLabel {
                label: String::from_utf8_lossy(other).into_owned(),
            }),
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
    fn try_new(
        field: &'static str,
        path: PathBuf,
    ) -> std::result::Result<Self, ConfigurationError> {
        if path.as_os_str().is_empty() {
            return Err(ConfigurationError::InvalidRuntimePath {
                field,
                path,
                kind: RuntimePathErrorKind::Empty,
            });
        }
        if !path.is_absolute() {
            return Err(ConfigurationError::InvalidRuntimePath {
                field,
                path,
                kind: RuntimePathErrorKind::Relative,
            });
        }
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(ConfigurationError::InvalidRuntimePath {
                field,
                path,
                kind: RuntimePathErrorKind::ParentDirectory,
            });
        }
        Ok(Self { path })
    }

    fn as_path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RuntimePathErrorKind {
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

#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("expected at least {expected} daemon startup arguments, received {actual}")]
    ArgumentCount { expected: usize, actual: usize },

    #[error("missing required daemon startup path")]
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

    #[error("invalid {field} path {}: {kind}", path.display())]
    InvalidRuntimePath {
        field: &'static str,
        path: PathBuf,
        kind: RuntimePathErrorKind,
    },

    #[error("invalid orchestrate path: {0}")]
    Path(#[from] Error),

    #[error(
        "the configuration-file startup boundary has been removed; pass typed daemon arguments"
    )]
    ConfigurationFileBoundaryRemoved,
}

#[cfg(test)]
mod tests {
    use super::DaemonConfiguration;

    #[test]
    fn parses_explicit_startup_paths_and_optional_downstream_sockets() {
        let configuration = DaemonConfiguration::from_arguments([
            "/state/orchestrate/orchestrate.sema",
            "/run/user/1000/orchestrate/orchestrate.sock",
            "/run/user/1000/orchestrate/orchestrate-owner.sock",
            "/run/user/1000/orchestrate/orchestrate-upgrade.sock",
            "/home/li/primary",
            "/git/github.com/LiGoldragon",
            "router=/run/user/1000/router/router.sock",
            "messenger=/run/user/1000/message/message.sock",
        ])
        .expect("parse explicit daemon startup contract");

        assert_eq!(
            configuration.store_path.as_str(),
            "/state/orchestrate/orchestrate.sema"
        );
        assert_eq!(
            configuration
                .router_working_socket_path()
                .expect("router socket")
                .as_str(),
            "/run/user/1000/router/router.sock"
        );
        assert_eq!(
            configuration
                .messenger_working_socket_path()
                .expect("messenger socket")
                .as_str(),
            "/run/user/1000/message/message.sock"
        );
    }

    #[test]
    fn rejects_file_writer_shape_and_non_absolute_paths() {
        let file_shape = DaemonConfiguration::from_arguments([
            "/state/orchestrate/daemon.signal",
            "/state/orchestrate/orchestrate.sema",
            "/run/user/1000/orchestrate/orchestrate.sock",
            "/run/user/1000/orchestrate/orchestrate-owner.sock",
            "/run/user/1000/orchestrate/orchestrate-upgrade.sock",
            "/home/li/primary",
            "/git/github.com/LiGoldragon",
        ]);
        assert!(
            file_shape.is_err(),
            "the removed signal-file argument is not accepted"
        );

        let relative = DaemonConfiguration::from_arguments([
            "state/orchestrate/orchestrate.sema",
            "/run/user/1000/orchestrate/orchestrate.sock",
            "/run/user/1000/orchestrate/orchestrate-owner.sock",
            "/run/user/1000/orchestrate/orchestrate-upgrade.sock",
            "/home/li/primary",
            "/git/github.com/LiGoldragon",
        ]);
        assert!(relative.is_err(), "relative startup paths are rejected");
    }
}
