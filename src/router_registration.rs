//! The client seam that registers a discovered agent with the router.
//!
//! On a successful agent registration WITH discovered reachability, orchestrate
//! hands the router the minted identity and its endpoint over the router working
//! socket, so the canonical address (orchestrate identifier = router
//! `ActorIdentifier`) becomes a live delivery target. This is the runtime
//! `RegisterActor` operation the router exposes on its ordinary working tier.
//!
//! The router is a co-resident peer, not orchestrate's own daemon, so this is a
//! best-effort side effect: a router that is down, unreachable, or that refuses
//! the registration is a NAMED, non-fatal degradation. Agent registration itself
//! still succeeds — the identity and topics are valid regardless — and the
//! caller records the router outcome as a divergence rather than failing.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use signal_orchestrate::OrchestratorAgentIdentifier;
use signal_router::{
    Actor, ActorIdentifier, ActorRegistrationDisposition, ActorRegistrationRefusalReason,
    EndpointKind, EndpointTransport, Frame, FrameBody, Input, Output,
};
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

use crate::{StoredAgentEndpointKind, StoredAgentReachability};

/// Registers a discovered agent with the router over its working socket.
pub struct RouterActorRegistration {
    socket_path: PathBuf,
}

impl RouterActorRegistration {
    /// The bound on a single router exchange. The router is co-resident, so a
    /// healthy round-trip is sub-millisecond; this only guards against a wedged
    /// router, which degrades rather than blocking registration.
    const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(2);

    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// Register `agent` at its discovered `reachability`. On success the router's
    /// disposition names whether the actor row was created or its endpoint
    /// replaced (last-wins). A transport failure or a router refusal is returned
    /// as a typed degradation for the caller to record — never an error that
    /// fails the agent's own registration.
    pub fn register(
        &self,
        agent: &OrchestratorAgentIdentifier,
        reachability: &StoredAgentReachability,
    ) -> Result<ActorRegistrationDisposition, RouterRegistrationDegradation> {
        let actor = Self::actor_for(agent, reachability);
        match self.exchange(Input::RegisterActor(actor))? {
            Output::ActorRegistered(registered) => Ok(registered.disposition()),
            Output::ActorRegistrationRefused(refused) => {
                Err(RouterRegistrationDegradation::Rejected(refused.reason()))
            }
            other => Err(RouterRegistrationDegradation::Unreachable(format!(
                "unexpected router reply to RegisterActor: {other:?}"
            ))),
        }
    }

    /// Build the wire `Actor`: the minted identity is the router
    /// `ActorIdentifier`, the discovered harness pid is the process, and the
    /// discovered endpoint maps onto the router's endpoint vocabulary.
    fn actor_for(
        agent: &OrchestratorAgentIdentifier,
        reachability: &StoredAgentReachability,
    ) -> Actor {
        Actor::new(
            ActorIdentifier::new(agent.as_str()),
            u64::from(reachability.harness_pid),
            Some(EndpointTransport::new(
                Self::endpoint_kind(reachability.endpoint_kind),
                reachability.target.clone(),
                None,
            )),
        )
    }

    /// Map orchestrate's discovered endpoint kind onto the router's endpoint
    /// vocabulary. A terminal-cell reachability is the terminal/PTY transport
    /// plane; a harness-process reachability is the harness signal socket.
    fn endpoint_kind(kind: StoredAgentEndpointKind) -> EndpointKind {
        match kind {
            StoredAgentEndpointKind::TerminalCell => EndpointKind::PtySocket,
            StoredAgentEndpointKind::HarnessProcess => EndpointKind::HarnessSocket,
        }
    }

    /// One blocking request/reply exchange over the router working socket, framed
    /// exactly as the router daemon serves it: a `LengthPrefixedCodec` body
    /// wrapping an encoded `signal-router` `Frame`. Every transport or codec
    /// failure becomes an `Unreachable` degradation.
    fn exchange(&self, input: Input) -> Result<Output, RouterRegistrationDegradation> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|error| {
            RouterRegistrationDegradation::unreachable(&self.socket_path, error)
        })?;
        // A wedged router (connection accepted, reply never sent) must degrade,
        // never block the agent's own registration on the actor mailbox. A
        // healthy co-resident exchange is sub-millisecond, so a short bound turns
        // a hang into an `Unreachable` degradation.
        stream
            .set_read_timeout(Some(Self::EXCHANGE_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(Self::EXCHANGE_TIMEOUT)))
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        let codec = LengthPrefixedCodec::default();
        let request = Frame::new(FrameBody::Request {
            exchange: Self::exchange_identifier(),
            request: input.into_request(),
        });
        let request_bytes = request
            .encode()
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        codec
            .write_body(&mut stream, &LengthPrefixedFrameBody::new(request_bytes))
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        stream
            .flush()
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        let body = codec
            .read_body(&mut stream)
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        let frame = Frame::decode(&body.into_bytes())
            .map_err(|error| RouterRegistrationDegradation::Unreachable(error.to_string()))?;
        Self::output_from_reply(frame)
    }

    fn output_from_reply(frame: Frame) -> Result<Output, RouterRegistrationDegradation> {
        let FrameBody::Reply { reply, .. } = frame.into_body() else {
            return Err(RouterRegistrationDegradation::Unreachable(
                "router reply frame was not a reply body".to_string(),
            ));
        };
        let Reply::Accepted { per_operation, .. } = reply else {
            return Err(RouterRegistrationDegradation::Unreachable(
                "router rejected the RegisterActor frame".to_string(),
            ));
        };
        match per_operation.into_head() {
            SubReply::Ok(output) => Ok(output),
            other => Err(RouterRegistrationDegradation::Unreachable(format!(
                "router sub-reply was not Ok: {other:?}"
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

/// A named, non-fatal router-registration degradation: the router leg of a
/// registration did not apply. `Unreachable` is a transport or codec failure
/// (router down, socket missing, malformed exchange); `Rejected` is a router
/// admission refusal carrying the typed reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterRegistrationDegradation {
    Unreachable(String),
    Rejected(ActorRegistrationRefusalReason),
}

impl RouterRegistrationDegradation {
    fn unreachable(socket_path: &Path, error: std::io::Error) -> Self {
        Self::Unreachable(format!(
            "connect to router working socket {}: {error}",
            socket_path.display()
        ))
    }
}
