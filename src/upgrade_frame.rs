//! The version-handover upgrade frame the daemon's upgrade tier exchanges.
//!
//! The upgrade tier speaks the version-handover *contract* wire (not a
//! schema-emitted frame): the handover protocol is shared across components and
//! is not part of orchestrate's own signal schema. This type owns one decoded
//! upgrade request — its exchange identifier, short header, and operation
//! request — and validates that the header names the same operation root the
//! payload carries before the request reaches the engine. The contract
//! `UpgradeFrame::decode` does not cross-check the header against the payload,
//! so this guard is the upgrade tier's pre-dispatch check (mirroring the
//! built-in header validation the schema `decode_signal_frame` performs on the
//! working and meta tiers).

use signal_frame::{ExchangeIdentifier, OperationDispatchError, Reply, Request, ShortHeader};
use signal_version_handover::{
    Frame as ContractFrame, FrameBody as ContractFrameBody, Operation as UpgradeOperation,
    Reply as UpgradeReply,
};

use crate::Error;

/// One decoded, header-validated upgrade request awaiting execution.
pub struct UpgradeRequestFrame {
    exchange: ExchangeIdentifier,
    request: Request<UpgradeOperation>,
}

impl UpgradeRequestFrame {
    /// Decode and validate one upgrade request from the contract frame body off
    /// the wire. Rejects a non-request frame and a frame whose short header
    /// disagrees with the operation root the payload carries.
    pub fn decode(body: &[u8]) -> Result<Self, Error> {
        let frame = ContractFrame::decode(body).map_err(Error::SignalFrame)?;
        let short_header = frame.short_header();
        let ContractFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(Error::SocketExpectedRequestFrame);
        };
        Self::validate_header(short_header, &request)?;
        Ok(Self { exchange, request })
    }

    /// Split the frame into its exchange identifier (carried into the reply) and
    /// the validated request that drives the engine.
    pub fn into_parts(self) -> (ExchangeIdentifier, Request<UpgradeOperation>) {
        (self.exchange, self.request)
    }

    /// Re-pair an exchange identifier with the engine's reply into the contract
    /// reply frame bytes to write back to the caller.
    pub fn encode_reply(
        exchange: ExchangeIdentifier,
        reply: Reply<UpgradeReply>,
    ) -> Result<Vec<u8>, Error> {
        ContractFrame::new(ContractFrameBody::Reply { exchange, reply })
            .encode()
            .map_err(Error::SignalFrame)
    }

    fn validate_header(
        short_header: ShortHeader,
        request: &Request<UpgradeOperation>,
    ) -> Result<(), Error> {
        let expected = short_header.to_le_bytes()[0];
        let expected_kind = UpgradeOperation::kind_from_short_header(short_header)
            .ok_or(OperationDispatchError::UnknownOperationRoot { root: expected })?;
        let actual_kind = request.payloads().head().kind();
        if actual_kind != expected_kind {
            return Err(OperationDispatchError::HeaderOperationMismatch {
                expected,
                actual: actual_kind as u8,
            }
            .into());
        }
        Ok(())
    }
}
