//! The version-handover upgrade frame the daemon's upgrade tier exchanges.
//!
//! The upgrade tier speaks the version-handover contract wire. The handover
//! protocol is shared across components rather than being part of either
//! orchestrate contract. This type owns one decoded
//! upgrade request — its exchange identifier, short header, and operation
//! request — and validates that the header names the same operation root the
//! payload carries before the request reaches the engine. The contract
//! `UpgradeFrame::decode` does not cross-check the header against the payload,
//! so this guard is the upgrade tier's pre-dispatch check. The ordinary and
//! meta listeners perform the same validation through their contract frames.

use signal_frame::{
    ExchangeIdentifier, OperationDispatchError, Reply, Request, ShortHeader, WireRoute,
};
use signal_version_handover::{
    Frame as ContractFrame, FrameBody as ContractFrameBody, Operation as UpgradeOperation,
    Reply as UpgradeReply,
};

use crate::Error;

/// One decoded, header-validated upgrade request awaiting execution.
#[derive(Debug)]
pub struct UpgradeRequestFrame {
    route: WireRoute,
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
        let route = Self::validate_header(short_header, &request)?;
        Ok(Self {
            route,
            exchange,
            request,
        })
    }

    /// Split the frame into its exchange identifier (carried into the reply) and
    /// the validated request that drives the engine.
    pub fn into_parts(self) -> (WireRoute, ExchangeIdentifier, Request<UpgradeOperation>) {
        (self.route, self.exchange, self.request)
    }

    /// Re-pair an exchange identifier with the engine's reply into the contract
    /// reply frame bytes to write back to the caller.
    pub fn encode_reply(
        route: WireRoute,
        exchange: ExchangeIdentifier,
        reply: Reply<UpgradeReply>,
    ) -> Result<Vec<u8>, Error> {
        ContractFrame::new(route, ContractFrameBody::Reply { exchange, reply })
            .encode()
            .map_err(Error::SignalFrame)
    }

    fn validate_header(
        short_header: ShortHeader,
        request: &Request<UpgradeOperation>,
    ) -> Result<WireRoute, Error> {
        let expected = request.route()?;
        let actual = short_header.route();
        if expected != actual {
            return Err(OperationDispatchError::HeaderRouteMismatch { expected, actual }.into());
        }
        Ok(expected)
    }
}

#[cfg(test)]
mod tests {
    use signal_frame::{
        ExchangeLane, LaneSequence, Request, RequestRejectionReason, RootCode, SessionEpoch,
    };
    use signal_version_handover::MarkerRequest;
    use version_projection::ComponentName;

    use super::*;

    fn exchange() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(7),
            ExchangeLane::Connector,
            LaneSequence::new(11),
        )
    }

    fn marker_request() -> UpgradeOperation {
        UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: ComponentName::new("orchestrate"),
        })
    }

    #[test]
    fn rejects_a_header_route_that_disagrees_with_the_request_body() {
        let exchange = exchange();
        let request = Request::from_payload(marker_request());
        let expected = request.route().expect("request route");
        let wrong = WireRoute::new(
            RootCode::new(expected.root().value().wrapping_add(1)),
            expected.variant(),
        );
        let bytes = ContractFrame::new(wrong, ContractFrameBody::Request { exchange, request })
            .encode()
            .expect("encode forged frame");

        let error = UpgradeRequestFrame::decode(&bytes).expect_err("route mismatch must fail");
        assert!(matches!(
            error,
            Error::OperationDispatch(OperationDispatchError::HeaderRouteMismatch {
                expected: actual_expected,
                actual,
            }) if actual_expected == expected && actual == wrong
        ));
    }

    #[test]
    fn rejects_an_empty_short_header_before_body_dispatch() {
        let mut bytes = marker_request()
            .into_frame(exchange())
            .expect("request route")
            .encode()
            .expect("encode request frame");
        bytes[..signal_frame::SHORT_HEADER_BYTE_COUNT].fill(0);

        let error = UpgradeRequestFrame::decode(&bytes).expect_err("unbound header must fail");
        assert!(matches!(
            error,
            Error::SignalFrame(signal_frame::FrameError::UnboundHeader)
        ));
    }

    #[test]
    fn reply_echoes_the_validated_request_route_and_exchange() {
        let exchange = exchange();
        let request_frame = marker_request()
            .into_frame(exchange)
            .expect("request route");
        let route = request_frame.short_header().route();
        let decoded_request =
            UpgradeRequestFrame::decode(&request_frame.encode().expect("encode request"))
                .expect("decode request");
        let (validated_route, validated_exchange, _) = decoded_request.into_parts();
        assert_eq!(validated_route, route);
        assert_eq!(validated_exchange, exchange);

        let reply = Reply::Rejected {
            reason: RequestRejectionReason::Internal,
        };
        let reply_bytes =
            UpgradeRequestFrame::encode_reply(validated_route, validated_exchange, reply)
                .expect("encode reply");
        let reply_frame = ContractFrame::decode(&reply_bytes).expect("decode reply");
        assert_eq!(reply_frame.short_header().route(), route);
        assert!(matches!(
            reply_frame.into_body(),
            ContractFrameBody::Reply {
                exchange: reply_exchange,
                ..
            } if reply_exchange == exchange
        ));
    }
}
