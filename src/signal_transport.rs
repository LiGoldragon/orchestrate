//! The schema-frame client transports orchestrate's CLI speaks to the daemon.
//!
//! The actor-shell daemon decodes the schema-emitted `Input` frame on each
//! tier (working over the ordinary socket, meta over the meta socket) — the
//! same `encode_signal_frame` / `decode_signal_frame` short-header wire spirit
//! uses, wrapped in `triad_runtime::LengthPrefixedCodec` length framing. These
//! transports are the client halves: one per tier, each owning a connected
//! stream and exchanging one typed `Input` for one typed `Output`.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use thiserror::Error;
use triad_runtime::{
    FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec, SocketPathSelection,
    SocketPathSource,
};

use meta_signal_orchestrate::schema::lib::{
    Input as MetaInput, Output as MetaOutput, OutputRoute as MetaOutputRoute,
    SignalFrameError as MetaSignalFrameError,
};
use signal_orchestrate::schema::lib::{Input, Output, OutputRoute, SignalFrameError};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connect {tier} signal socket {} from {path_source}: {source}", path.display())]
    Connect {
        tier: TransportTier,
        path: PathBuf,
        path_source: SocketPathSource,
        #[source]
        source: std::io::Error,
    },

    #[error("ordinary signal frame error: {0}")]
    SignalFrame(#[from] SignalFrameError),

    #[error("meta signal frame error: {0}")]
    MetaSignalFrame(#[from] MetaSignalFrameError),

    #[error("transport frame error: {0}")]
    Frame(#[from] FrameError),
}

/// The working-tier client transport: a connected stream exchanging one ordinary
/// schema `Input` for one ordinary schema `Output`.
pub struct OrdinarySignalTransport {
    stream: UnixStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportTier {
    Ordinary,
    Meta,
}

impl std::fmt::Display for TransportTier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
            Self::Meta => formatter.write_str("meta"),
        }
    }
}

impl OrdinarySignalTransport {
    pub fn connect(socket_path: SocketPathSelection) -> Result<Self, TransportError> {
        Ok(Self {
            stream: TransportSocket::new(TransportTier::Ordinary, socket_path).connect()?,
        })
    }

    pub fn exchange(&mut self, input: &Input) -> Result<(OutputRoute, Output), TransportError> {
        FrameExchange::new(&mut self.stream).write_frame(input.encode_signal_frame()?)?;
        Ok(Output::decode_signal_frame(
            &FrameExchange::new(&mut self.stream).read_frame()?,
        )?)
    }
}

/// The meta-tier client transport: a connected stream exchanging one meta schema
/// `Input` for one meta schema `Output`.
pub struct MetaSignalTransport {
    stream: UnixStream,
}

impl MetaSignalTransport {
    pub fn connect(socket_path: SocketPathSelection) -> Result<Self, TransportError> {
        Ok(Self {
            stream: TransportSocket::new(TransportTier::Meta, socket_path).connect()?,
        })
    }

    pub fn exchange(
        &mut self,
        input: &MetaInput,
    ) -> Result<(MetaOutputRoute, MetaOutput), TransportError> {
        FrameExchange::new(&mut self.stream).write_frame(input.encode_signal_frame()?)?;
        Ok(MetaOutput::decode_signal_frame(
            &FrameExchange::new(&mut self.stream).read_frame()?,
        )?)
    }
}

struct TransportSocket {
    tier: TransportTier,
    socket_path: SocketPathSelection,
}

impl TransportSocket {
    fn new(tier: TransportTier, socket_path: SocketPathSelection) -> Self {
        Self { tier, socket_path }
    }

    fn connect(&self) -> Result<UnixStream, TransportError> {
        UnixStream::connect(self.socket_path.as_path()).map_err(|source| TransportError::Connect {
            tier: self.tier,
            path: self.socket_path.as_path().to_path_buf(),
            path_source: self.socket_path.source().clone(),
            source,
        })
    }
}

/// One length-prefixed frame read/write over a borrowed stream — the framing
/// both tier transports share.
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
        LengthPrefixedCodec::default()
            .write_body(self.stream, &LengthPrefixedFrameBody::new(frame))?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, TransportError> {
        Ok(LengthPrefixedCodec::default()
            .read_body(self.stream)?
            .into_bytes())
    }
}
