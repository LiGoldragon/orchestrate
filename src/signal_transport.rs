//! The ordinary CLI's binary Signal transport.

use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
};

use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
    WireRoute,
};
use thiserror::Error;
use triad_runtime::{FrameBody as TransportBody, LengthPrefixedCodec};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("signal frame error: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("signal wire route error: {0}")]
    WireRoute(#[from] signal_frame::WireRouteError),
    #[error("length-prefixed frame error: {0}")]
    TransportFrame(#[from] triad_runtime::FrameError),
    #[error("daemon returned a non-reply frame")]
    UnexpectedFrame,
    #[error("daemon reply carried a different exchange identifier")]
    ExchangeMismatch,
    #[error("daemon reply route mismatch: expected {expected:?}, got {actual:?}")]
    RouteMismatch {
        expected: WireRoute,
        actual: WireRoute,
    },
    #[error("daemon rejected the request: {0}")]
    RequestRejected(signal_frame::RequestRejectionReason),
    #[error("daemon accepted the request without a typed reply")]
    MissingTypedReply,
}

pub struct OrdinarySignalTransport {
    stream: UnixStream,
    next_sequence: LaneSequence,
}

impl OrdinarySignalTransport {
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self, TransportError> {
        Ok(Self {
            stream: UnixStream::connect(socket_path)?,
            next_sequence: LaneSequence::first(),
        })
    }

    pub fn exchange(
        &mut self,
        operation: &signal_orchestrate::OrchestrateRequest,
    ) -> Result<signal_orchestrate::OrchestrateReply, TransportError> {
        let exchange = self.mint_exchange();
        let frame = operation.clone().into_frame(exchange)?;
        let route = frame.short_header().route();
        FrameExchange::new(&mut self.stream).write_frame(frame.encode()?)?;
        let frame = signal_orchestrate::OrchestrateFrame::decode(
            &FrameExchange::new(&mut self.stream).read_frame()?,
        )?;
        let reply_route = frame.short_header().route();
        let signal_orchestrate::OrchestrateFrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } = frame.into_body()
        else {
            return Err(TransportError::UnexpectedFrame);
        };
        if reply_exchange != exchange {
            return Err(TransportError::ExchangeMismatch);
        }
        if reply_route != route {
            return Err(TransportError::RouteMismatch {
                expected: route,
                actual: reply_route,
            });
        }
        Self::typed_payload(reply)
    }

    fn mint_exchange(&mut self) -> ExchangeIdentifier {
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            self.next_sequence,
        );
        self.next_sequence = self.next_sequence.next();
        exchange
    }

    fn typed_payload<Payload>(reply: Reply<Payload>) -> Result<Payload, TransportError> {
        match reply {
            Reply::Accepted {
                outcome: AcceptedOutcome::Committed,
                per_operation,
            } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                _ => Err(TransportError::MissingTypedReply),
            },
            Reply::Accepted {
                outcome: AcceptedOutcome::OperationAborted { .. },
                per_operation,
            } => match per_operation.into_head() {
                SubReply::Failed {
                    detail: Some(payload),
                    ..
                } => Ok(payload),
                _ => Err(TransportError::MissingTypedReply),
            },
            Reply::Accepted { .. } => Err(TransportError::MissingTypedReply),
            Reply::Rejected { reason } => Err(TransportError::RequestRejected(reason)),
        }
    }
}

struct FrameExchange<'stream, Stream> {
    stream: &'stream mut Stream,
}

impl<Stream> FrameExchange<'_, Stream>
where
    Stream: Read + Write,
{
    fn new(stream: &mut Stream) -> FrameExchange<'_, Stream> {
        FrameExchange { stream }
    }

    fn write_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        LengthPrefixedCodec::default().write_body(self.stream, &TransportBody::new(frame))?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, TransportError> {
        Ok(LengthPrefixedCodec::default()
            .read_body(self.stream)?
            .into_bytes())
    }
}
