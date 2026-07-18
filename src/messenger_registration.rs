//! The client seam that pushes agent identity into the messenger's durable
//! registry.
//!
//! The orchestrator is the mint (psyche-ruled 2026-07-17): every minted or
//! registered identity is seated in the messenger's registry — the durable
//! consumer view of identity — and a discovered reachability follows as an
//! endpoint binding carrying the pid + start-time pin. The messenger is a
//! co-resident peer, not orchestrate's own daemon, so both pushes are
//! best-effort side effects: an unreachable or refusing messenger is a NAMED,
//! non-fatal degradation the caller records as a divergence; the mint or
//! registration itself still succeeds.
//!
//! Wire note: the push speaks the published `signal-message` contract. The
//! messenger daemon's ingress decodes its own generated mirror of the same
//! vocabulary; the two schemas emit identical frame headers by construction
//! (same operation order and record shapes). The messenger-promotion packets
//! make that convergence structural by moving the daemon ingress onto the
//! contract crate. A daemon-local reply outside the contract's vocabulary
//! (e.g. its `Error` report) decodes here as an unknown header and degrades
//! as `Unreachable` with the decode detail.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use signal_message::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentifier,
    AgentIdentityAssignment, EndpointPath, HarnessPid, HarnessStartTime, Input, MessageBody,
    MessageKind, MessageRecipient, MessageSubmission, Output, ProcessPinSelection,
    ResumeSelection, ThreadSelection,
};
use signal_orchestrate::OrchestratorAgentIdentifier;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

use crate::{StoredAgentEndpointKind, StoredAgentReachability};

/// Pushes identity and endpoint facts to the messenger over its working socket.
pub struct MessengerRegistryPush {
    socket_path: PathBuf,
}

impl MessengerRegistryPush {
    /// The bound on a single messenger exchange. The messenger is co-resident,
    /// so a healthy round-trip is sub-millisecond; this only guards against a
    /// wedged messenger, which degrades rather than blocking the caller.
    const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(2);

    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// Seat `agent` in the messenger's registry. The process pin is `None`:
    /// seating happens at mint or registration, ahead of (or independent of)
    /// reachability discovery, and the endpoint binding that follows discovery
    /// carries the pin. The resume identity is not plumbed yet (cold-delivery
    /// packet 4.1 owns it), so the selection is `None`.
    pub fn seat_identity(
        &self,
        agent: &OrchestratorAgentIdentifier,
    ) -> Result<(), MessengerRegistrationDegradation> {
        let assignment = AgentIdentityAssignment {
            agent_identifier: AgentIdentifier::new(agent.as_str().to_string()),
            process_pin_selection: ProcessPinSelection::None,
            resume_selection: ResumeSelection::None,
        };
        match self.exchange(Input::assign_agent_identity(assignment))? {
            Output::AgentIdentityAssigned(_) => Ok(()),
            Output::AgentRegistryRejected(rejection) => Err(
                MessengerRegistrationDegradation::Rejected(format!("{rejection:?}")),
            ),
            other => Err(MessengerRegistrationDegradation::Unreachable(format!(
                "unexpected messenger reply to AssignAgentIdentity: {other:?}"
            ))),
        }
    }

    /// Bind `agent`'s discovered reachability as its live delivery endpoint,
    /// pinning the harness process generation (pid + start time).
    pub fn bind_endpoint(
        &self,
        agent: &OrchestratorAgentIdentifier,
        reachability: &StoredAgentReachability,
    ) -> Result<(), MessengerRegistrationDegradation> {
        let binding = AgentEndpointBinding {
            agent_identifier: AgentIdentifier::new(agent.as_str().to_string()),
            agent_endpoint: AgentEndpoint {
                agent_endpoint_kind: Self::endpoint_kind(reachability.endpoint_kind),
                endpoint_path: EndpointPath::new(reachability.target.clone().into()),
            },
            harness_pid: HarnessPid::new(u64::from(reachability.harness_pid)),
            harness_start_time: HarnessStartTime::new(reachability.harness_start_time),
        };
        match self.exchange(Input::bind_agent_endpoint(binding))? {
            Output::AgentEndpointBound(_) => Ok(()),
            Output::AgentRegistryRejected(rejection) => Err(
                MessengerRegistrationDegradation::Rejected(format!("{rejection:?}")),
            ),
            other => Err(MessengerRegistrationDegradation::Unreachable(format!(
                "unexpected messenger reply to BindAgentEndpoint: {other:?}"
            ))),
        }
    }

    /// Submit a routed orchestrator message into the messenger's local
    /// ledger for delivery to `recipient`'s bound endpoint (or inbox
    /// parking). The messenger stamps its own transport-level provenance at
    /// ingress; the semantic sender rides inside `body`, which the caller
    /// composes as the NOTA delivery note.
    pub fn submit_message(
        &self,
        recipient: &OrchestratorAgentIdentifier,
        body: String,
    ) -> Result<(), MessengerRegistrationDegradation> {
        let submission = MessageSubmission {
            message_recipient: MessageRecipient::new(recipient.as_str().to_string()),
            message_kind: MessageKind::Send,
            message_body: MessageBody::new(body),
            thread_selection: ThreadSelection::None,
        };
        match self.exchange(Input::submit(submission))? {
            Output::SubmissionAccepted(_) => Ok(()),
            Output::SubmissionRejected(rejection) => Err(
                MessengerRegistrationDegradation::Rejected(format!("{rejection:?}")),
            ),
            other => Err(MessengerRegistrationDegradation::Unreachable(format!(
                "unexpected messenger reply to Submit: {other:?}"
            ))),
        }
    }

    /// Map orchestrate's discovered endpoint kind onto the messenger's endpoint
    /// vocabulary. A terminal-cell reachability is the terminal/PTY transport
    /// plane; a harness-process reachability is the harness signal socket.
    fn endpoint_kind(kind: StoredAgentEndpointKind) -> AgentEndpointKind {
        match kind {
            StoredAgentEndpointKind::TerminalCell => AgentEndpointKind::PtySocket,
            StoredAgentEndpointKind::HarnessProcess => AgentEndpointKind::HarnessSocket,
        }
    }

    /// One blocking request/reply exchange over the messenger working socket,
    /// framed exactly as the messenger daemon serves it: a `LengthPrefixedCodec`
    /// body wrapping an encoded `signal-message` signal frame. Every transport
    /// or codec failure becomes an `Unreachable` degradation.
    fn exchange(&self, input: Input) -> Result<Output, MessengerRegistrationDegradation> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|error| {
            MessengerRegistrationDegradation::unreachable(&self.socket_path, error)
        })?;
        stream
            .set_read_timeout(Some(Self::EXCHANGE_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(Self::EXCHANGE_TIMEOUT)))
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        let codec = LengthPrefixedCodec::default();
        let request_bytes = input
            .encode_signal_frame()
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        codec
            .write_body(&mut stream, &LengthPrefixedFrameBody::new(request_bytes))
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        stream
            .flush()
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        let body = codec
            .read_body(&mut stream)
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        let (_route, output) = Output::decode_signal_frame(&body.into_bytes())
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        Ok(output)
    }
}

/// A named, non-fatal messenger-push degradation: the messenger leg of a mint
/// or registration did not apply. `Unreachable` is a transport or codec
/// failure (messenger down, socket missing, malformed exchange); `Rejected`
/// is a messenger registry refusal carrying the typed reason's rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessengerRegistrationDegradation {
    Unreachable(String),
    Rejected(String),
}

impl MessengerRegistrationDegradation {
    fn unreachable(socket_path: &Path, error: std::io::Error) -> Self {
        Self::Unreachable(format!(
            "connect to messenger working socket {}: {error}",
            socket_path.display()
        ))
    }
}
