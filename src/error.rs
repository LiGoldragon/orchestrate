use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("dotos: {0}")]
    Dotos(#[from] dotos::DotosDecodeError),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("signal wire route: {0}")]
    WireRoute(#[from] signal_frame::WireRouteError),

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

    #[error("harness reply exchange mismatch: expected {expected:?}, got {actual:?}")]
    HarnessReplyExchangeMismatch {
        expected: signal_frame::ExchangeIdentifier,
        actual: signal_frame::ExchangeIdentifier,
    },

    #[error("harness reply route mismatch: expected {expected:?}, got {actual:?}")]
    HarnessReplyRouteMismatch {
        expected: signal_frame::WireRoute,
        actual: signal_frame::WireRoute,
    },

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

    #[error("path is not valid UTF-8")]
    PathIsNotUtf8,

    #[error("socket path exists and is not a socket: {0}")]
    SocketPathIsNotSocket(String),

    #[error("daemon socket handler expected a request frame")]
    SocketExpectedRequestFrame,

    #[error("lane role vector must contain at least one token")]
    EmptyLaneRole,

    #[error("lane ordinal {ordinal} is unsupported")]
    UnsupportedLaneOrdinal { ordinal: usize },

    #[error("lane is not registered: {lane}")]
    LaneNotRegistered { lane: String },

    #[error("no worktree is registered for owning lane {lane}")]
    WorktreeLaneNotFound { lane: String },

    #[error(
        "owning lane {lane} identifies multiple non-recycled worktrees: {worktrees}; refusing destructive conclusion"
    )]
    WorktreeLaneAmbiguous { lane: String, worktrees: String },

    #[error(
        "atomic batch has {operation_count} operations; orchestrate supports one operation per execution batch today"
    )]
    UnsupportedAtomicBatch { operation_count: usize },

    #[error("orchestration request was rejected: {reason}")]
    OrchestrationRequestRejected {
        reason: signal_frame::RequestRejectionReason,
    },

    #[error("orchestration request did not commit its single operation")]
    OrchestrationRequestNotCommitted,

    #[error("orchestrator topic is not registered: {path}")]
    OrchestratorTopicNotFound { path: String },

    #[error("worktree not found for archive transition: {path}")]
    WorktreeNotFound { path: String },

    #[error(
        "orchestrator agent identifier space is exhausted between {minimum} and {maximum} characters"
    )]
    OrchestratorAgentIdentifierExhausted { minimum: usize, maximum: usize },

    #[error("orchestrator agent identifier randomness failed: {message}")]
    OrchestratorAgentIdentifierRandomness { message: String },

    #[error(
        "pre-minted agent identity {identifier} is not in the registry; \
         mint it with MintAgentIdentity before registering with it"
    )]
    UnknownPreMintedAgentIdentity { identifier: String },
}

impl Error {
    /// Whether this error is the engine's rejection of a well-formed request the
    /// caller can act on — an invalid domain value (e.g. a session identifier
    /// that is not CamelCase alphanumeric) or a claim against an unregistered
    /// lane — as opposed to an infrastructure failure.
    ///
    /// The signal wire boundary routes caller rejections through the typed reply
    /// channel so the reason is diagnosable at the call site. Other execution
    /// errors abort the request batch; malformed or wrong-tier frames fail before
    /// service execution.
    pub fn is_caller_rejection(&self) -> bool {
        matches!(
            self,
            Error::SignalOrchestrate(_)
                | Error::LaneNotRegistered { .. }
                | Error::WorktreeLaneNotFound { .. }
                | Error::WorktreeLaneAmbiguous { .. }
                | Error::UnknownPreMintedAgentIdentity { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn ambiguous_worktree_lane_is_a_caller_rejection() {
        let error = Error::WorktreeLaneAmbiguous {
            lane: "MultiRepositoryLane".to_owned(),
            worktrees: "orchestrate/feature, message/feature".to_owned(),
        };
        assert!(error.is_caller_rejection());
    }

    #[test]
    fn missing_worktree_lane_is_a_caller_rejection() {
        let error = Error::WorktreeLaneNotFound {
            lane: "MissingLane".to_owned(),
        };
        assert!(error.is_caller_rejection());
    }
}
