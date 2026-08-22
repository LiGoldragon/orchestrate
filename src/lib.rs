//! Durable, metadata-only registration of native Datom path locks.

pub mod configuration;
pub mod daemon;
pub mod error;
pub mod location;
pub mod service;
pub mod signal_transport;
pub mod tables;

pub use configuration::{ConfigurationError, DaemonConfiguration};
pub use daemon::{OrchestrateDaemon, OrchestrateDaemonError};
pub use error::{Error, Result};
pub use location::StoreLocation;
pub use service::OrchestrateService;
pub use signal_orchestrate::{
    NativePathLock, NativePathLockRegistered, NativePathLockRegistrationRejected, OrchestrateReply,
    OrchestrateRequest, PathLock, PathLockRegistered, PathLockRegistrationRejected,
    PathLockRegistrationRejection,
};
pub use signal_transport::{OrdinarySignalTransport, TransportError};
pub use tables::{OrchestrateTables, StoredPathLock};
