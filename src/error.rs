use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("signal-orchestrate: {0}")]
    SignalOrchestrate(#[from] signal_orchestrate::Error),

    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("orchestrate service sequence lock was poisoned")]
    ServiceSequencePoisoned,

    #[error("path is not valid UTF-8")]
    PathIsNotUtf8,

    #[error("socket path exists and is not a socket: {0}")]
    SocketPathIsNotSocket(String),

    #[error("daemon socket handler expected a request frame")]
    SocketExpectedRequestFrame,

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
}
