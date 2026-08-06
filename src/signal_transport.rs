//! Direct contract-Frame transports for the ordinary and meta CLIs.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

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

    #[error("daemon accepted the request without committing it: {0}")]
    RequestNotCommitted(String),

    #[error("daemon returned no successful operation payload: {0}")]
    OperationFailed(String),
}

/// A connected ordinary socket speaking the canonical signal-orchestrate Frame.
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
        successful_payload(reply)
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
}

/// A connected owner socket speaking the canonical meta-signal-orchestrate Frame.
pub struct MetaSignalTransport {
    stream: UnixStream,
    next_sequence: LaneSequence,
}

impl MetaSignalTransport {
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self, TransportError> {
        Ok(Self {
            stream: UnixStream::connect(socket_path)?,
            next_sequence: LaneSequence::first(),
        })
    }

    pub fn exchange(
        &mut self,
        operation: &meta_signal_orchestrate::MetaOrchestrateRequest,
    ) -> Result<meta_signal_orchestrate::MetaOrchestrateReply, TransportError> {
        let exchange = self.mint_exchange();
        let frame = operation.clone().into_frame(exchange)?;
        let route = frame.short_header().route();
        FrameExchange::new(&mut self.stream).write_frame(frame.encode()?)?;
        let frame = meta_signal_orchestrate::Frame::decode(
            &FrameExchange::new(&mut self.stream).read_frame()?,
        )?;
        let reply_route = frame.short_header().route();
        let meta_signal_orchestrate::FrameBody::Reply {
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
        successful_payload(reply)
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
}

fn successful_payload<Payload>(reply: Reply<Payload>) -> Result<Payload, TransportError>
where
    Payload: std::fmt::Debug,
{
    match reply {
        Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        } => match per_operation.into_head() {
            SubReply::Ok(payload) => Ok(payload),
            other => Err(TransportError::OperationFailed(format!("{other:?}"))),
        },
        Reply::Accepted { outcome, .. } => {
            Err(TransportError::RequestNotCommitted(format!("{outcome:?}")))
        }
        Reply::Rejected { reason } => Err(TransportError::RequestRejected(reason)),
    }
}

struct FrameExchange<'stream, Stream> {
    stream: &'stream mut Stream,
}

impl<'stream, Stream> FrameExchange<'stream, Stream>
where
    Stream: Read + Write,
{
    fn new(stream: &'stream mut Stream) -> Self {
        Self { stream }
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
