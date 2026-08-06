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
//! Wire note: the push speaks the published `signal-message` contract directly.
//! A daemon-local reply outside the contract's vocabulary
//! (e.g. its `Error` report) decodes here as an unknown header and degrades
//! as `Unreachable` with the decode detail.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply, WireRoute,
};
use signal_message::{
    Frame, FrameBody, Input, Output, z2VMBf, z2VNPW, z2VNcG, z2VPEW, z2VQY5, z2VRqE, z2VTiK,
    z2VUs6, z2VVAD, z2VXMQ, z2VY2v, z2VYrY, z2Vari, z2Vcfd, z2VdsV, z2VevD,
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
        let assignment = z2VevD {
            field_0: z2VNPW::new(agent.as_str().to_string()),
            field_1: z2Vcfd::z2VRLv,
            field_2: z2VXMQ::z2VNZi,
        };
        match self.exchange(Input::AssignAgentIdentity(assignment))? {
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
        let binding = z2VVAD {
            field_0: z2VNPW::new(agent.as_str().to_string()),
            field_1: z2VMBf {
                field_0: Self::endpoint_kind(reachability.endpoint_kind),
                field_1: z2VRqE::new(z2VQY5::new(reachability.target.clone())),
            },
            field_2: z2VPEW::new(u64::from(reachability.harness_pid)),
            field_3: z2VYrY::new(reachability.harness_start_time),
        };
        match self.exchange(Input::BindAgentEndpoint(binding))? {
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
    /// composes as the Dotos delivery note.
    pub fn submit_message(
        &self,
        recipient: &OrchestratorAgentIdentifier,
        body: String,
    ) -> Result<(), MessengerRegistrationDegradation> {
        let submission = z2VY2v {
            field_0: z2Vari::new(recipient.as_str().to_string()),
            field_1: z2VdsV::z2VXeo,
            field_2: z2VNcG::new(body),
            field_3: z2VTiK::z2VR2m,
        };
        match self.exchange(Input::Submit(submission))? {
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
    fn endpoint_kind(kind: StoredAgentEndpointKind) -> z2VUs6 {
        match kind {
            StoredAgentEndpointKind::TerminalCell => z2VUs6::z2VZk6,
            StoredAgentEndpointKind::HarnessProcess => z2VUs6::z2VTin,
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
        let exchange = Self::exchange_identifier();
        let request = input.into_frame(exchange);
        let route = request.short_header().route();
        let request_bytes = request
            .encode()
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
        let frame = Frame::decode(&body.into_bytes())
            .map_err(|error| MessengerRegistrationDegradation::Unreachable(error.to_string()))?;
        Self::output_from_reply(frame, exchange, route)
    }

    fn output_from_reply(
        frame: Frame,
        expected_exchange: ExchangeIdentifier,
        expected_route: WireRoute,
    ) -> Result<Output, MessengerRegistrationDegradation> {
        let actual_route = frame.short_header().route();
        let FrameBody::Reply { exchange, reply } = frame.into_body() else {
            return Err(MessengerRegistrationDegradation::Unreachable(
                "messenger reply frame was not a reply body".to_string(),
            ));
        };
        if exchange != expected_exchange {
            return Err(MessengerRegistrationDegradation::Unreachable(
                "messenger reply carried a different exchange identifier".to_string(),
            ));
        }
        if actual_route != expected_route {
            return Err(MessengerRegistrationDegradation::Unreachable(format!(
                "messenger reply carried route {actual_route:?}, expected {expected_route:?}"
            )));
        }
        let Reply::Accepted { per_operation, .. } = reply else {
            return Err(MessengerRegistrationDegradation::Unreachable(
                "messenger rejected the request frame".to_string(),
            ));
        };
        match per_operation.into_head() {
            SubReply::Ok(output) => Ok(output),
            other => Err(MessengerRegistrationDegradation::Unreachable(format!(
                "messenger sub-reply was not Ok: {other:?}"
            ))),
        }
    }

    fn exchange_identifier() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
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
