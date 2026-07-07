use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota::NotaDecodeError),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("harness transport frame: {0}")]
    HarnessTransportFrame(triad_runtime::FrameError),

    #[error("harness reply rejected request: {reason}")]
    HarnessReplyRejected {
        reason: signal_frame::RequestRejectionReason,
    },

    #[error("harness reply did not commit: {outcome}")]
    HarnessReplyNotCommitted { outcome: String },

    #[error("unexpected harness frame: {got}")]
    UnexpectedHarnessFrame { got: String },

    #[error("unexpected harness reply: {got}")]
    UnexpectedHarnessReply { got: String },

    #[error("harness model resolver is not configured")]
    HarnessResolverNotConfigured,

    #[error("harness model resolution operation was unimplemented: {operation}")]
    HarnessResolutionUnimplemented { operation: String },

    #[error("workflow model resolution archive encode failed: {message}")]
    WorkflowResolutionArchiveEncode { message: String },

    #[error("operation dispatch: {0}")]
    OperationDispatch(#[from] signal_frame::OperationDispatchError),

    #[error("handover mirror component mismatch: expected {expected}, got {actual}")]
    MirrorComponentMismatch {
        expected: &'static str,
        actual: String,
    },

    #[error("handover mirror record kind mismatch: expected {expected}, got {actual}")]
    MirrorKindMismatch {
        expected: &'static str,
        actual: String,
    },

    #[error("handover mirror target version mismatch: expected {expected:?}, got {actual:?}")]
    MirrorTargetVersionMismatch {
        expected: version_projection::ContractVersion,
        actual: version_projection::ContractVersion,
    },

    #[error("handover mirror archive encode failed: {message}")]
    MirrorArchiveEncode { message: String },

    #[error("handover mirror archive decode failed: {message}")]
    MirrorArchiveDecode { message: String },

    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("signal-orchestrate: {0}")]
    SignalOrchestrate(#[from] signal_orchestrate::Error),

    #[error("sema storage kernel: {0}")]
    SemaStorageKernel(#[from] sema_engine::StorageKernelError),

    #[error("sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("orchestrate service sequence lock was poisoned")]
    ServiceSequencePoisoned,

    #[error("path is not valid UTF-8")]
    PathIsNotUtf8,

    #[error("socket path exists and is not a socket: {0}")]
    SocketPathIsNotSocket(String),

    #[error("invalid legacy lock line in {path}:{line_number}: {line}")]
    InvalidLegacyLockLine {
        path: String,
        line_number: usize,
        line: String,
    },

    #[error("daemon socket handler expected a request frame")]
    SocketExpectedRequestFrame,

    #[error("daemon listener: {0}")]
    DaemonListener(#[from] triad_runtime::ListenerError),

    #[error("daemon socket thread panicked")]
    DaemonThreadPanicked,

    #[error("signal frame is too large: {length} bytes")]
    FrameTooLarge { length: usize },

    #[error("lane role vector must contain at least one token")]
    EmptyLaneRole,

    #[error("lane ordinal {ordinal} is unsupported")]
    UnsupportedLaneOrdinal { ordinal: usize },

    #[error("lane is not registered: {lane}")]
    LaneNotRegistered { lane: String },

    #[error("worktree scan failed for {path}: {message}")]
    WorktreeScan { path: String, message: String },

    #[error(
        "atomic batch has {operation_count} operations; orchestrate supports one operation per execution batch today"
    )]
    UnsupportedAtomicBatch { operation_count: usize },

    #[error(
        "operation plan has {command_count} commands; orchestrate supports one command per operation today"
    )]
    UnsupportedAtomicOperationPlan { command_count: usize },

    #[error("executor rejected the request before execution: {reason}")]
    ExecutorReplyRejected {
        reason: signal_frame::RequestRejectionReason,
    },

    #[error("executor did not commit the single operation")]
    ExecutorReplyNotCommitted,

    #[error("schema bridge: {message}")]
    SchemaBridge { message: String },

    #[error("nexus replied on the {actual} tier while {expected} was expected")]
    NexusReplyTierMismatch {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("nexus did not produce a signal reply; action route: {route}")]
    NexusDidNotReply { route: String },

    #[error("worktree not found for archive transition: {path}")]
    WorktreeNotFound { path: String },
}
