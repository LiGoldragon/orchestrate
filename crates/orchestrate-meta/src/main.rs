//! Datom edge client for the privileged Orchestrate Signal.

#[path = "generated/client.rs"]
mod generated_client;

use datom_codec::{Actualizing, Budget, Datom, Datomizable, Potential};
use generated_client::{ClientFailure, Unreachable};
use meta_signal_orchestrate::{Query, Response};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use std::{env, os::unix::net::UnixStream, process::ExitCode};

enum Invocation {
    Describe,
    Query(String),
}

trait Invoking: Sized {
    fn from_process() -> Result<Self, String>;
    fn invoke(self) -> ExitCode;
}

impl Invoking for Invocation {
    fn from_process() -> Result<Self, String> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        match arguments.as_slice() {
            [] => Ok(Self::Describe),
            [source] if !source.starts_with("--") => Ok(Self::Query(source.clone())),
            _ => Err("accepts exactly one inline Datom query and no flags".to_owned()),
        }
    }

    fn invoke(self) -> ExitCode {
        match self {
            Self::Describe => {
                println!("{}", meta_signal_orchestrate::ETHOS.trim());
                println!("{}", include_str!("../client.ethos").trim());
                ExitCode::SUCCESS
            }
            Self::Query(source) => match Client::from_process() {
                Ok(client) => client.query_source(source),
                Err(error) => {
                    eprintln!("orchestrate-meta: {error}");
                    ExitCode::FAILURE
                }
            },
        }
    }
}

struct Client {
    socket_path: String,
}

trait ClientConfiguring: Sized {
    fn from_process() -> Result<Self, String>;
}

impl ClientConfiguring for Client {
    fn from_process() -> Result<Self, String> {
        Ok(Self {
            socket_path: env::var("ORCHESTRATE_META_SOCKET")
                .map_err(|_| "ORCHESTRATE_META_SOCKET is required".to_owned())?,
        })
    }
}

trait Querying {
    fn query_source(&self, source: String) -> ExitCode;
    fn query(&self, query: &Query) -> Result<Response, ClientFailure>;
}

impl Querying for Client {
    fn query_source(&self, source: String) -> ExitCode {
        let mut potential = Potential::<Query>::from(source);
        let mut budget = Budget {
            remaining: 10_000,
            reader: ReaderBudget { remaining: 10_000 },
            depth: 0,
            maximum_depth: 256,
        };
        let result = potential
            .actualize(&mut budget)
            .map_err(ClientFailure::Unreadable)
            .and_then(|query| self.query(&query));
        match result {
            Ok(response) => {
                println!("{}", response.datom_text());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{}", error.datom_text());
                ExitCode::FAILURE
            }
        }
    }

    fn query(&self, query: &Query) -> Result<Response, ClientFailure> {
        let exchange = query
            .signalize()
            .map_err(|_| TransportError::Archive)
            .and_then(|signal| SignalConnection::connect(&self.socket_path)?.exchange(&signal))
            .and_then(|bytes| {
                Signal::<Response>::from(bytes)
                    .restore()
                    .map_err(|_| TransportError::Archive)
            });
        exchange.map_err(|error| {
            ClientFailure::Unreachable(Unreachable {
                socket_path: self.socket_path.clone(),
                transport_error: error.to_string(),
            })
        })
    }
}

trait DatomText {
    fn datom_text(&self) -> String;
}

impl<T> DatomText for T
where
    T: Datomizable<Output = Datom>,
{
    fn datom_text(&self) -> String {
        self.datomize(Vec::new()).protosize().textualize()
    }
}

struct SignalConnection {
    stream: UnixStream,
}

trait Connecting: Sized {
    fn connect(socket_path: &str) -> Result<Self, TransportError>;
}

impl Connecting for SignalConnection {
    fn connect(socket_path: &str) -> Result<Self, TransportError> {
        Ok(Self {
            stream: UnixStream::connect(socket_path)?,
        })
    }
}

trait Exchanging {
    fn exchange<T>(&mut self, query: &Signal<T>) -> Result<Vec<u8>, TransportError>;
}

impl Exchanging for SignalConnection {
    /// One framed query out, one framed reply in. Framing is `signal`'s: this
    /// client owns no length prefix of its own.
    ///
    /// Exactly one reply is read even where the Nexus would go on writing —
    /// an Observe opens a subscription there. A CLI that takes one argument
    /// and prints one value ends at the state on open; dropping the
    /// connection is how it unsubscribes.
    fn exchange<T>(&mut self, query: &Signal<T>) -> Result<Vec<u8>, TransportError> {
        let capacity = FrameCapacity::default();
        self.stream.write_frame(query, capacity)?;
        Ok(Vec::from(self.stream.read_frame(capacity)?))
    }
}

#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Signal frame: {0}")]
    Frame(#[from] signal::FrameError),
    #[error("Signal archive validation failed")]
    Archive,
}

fn main() -> ExitCode {
    match Invocation::from_process() {
        Ok(invocation) => invocation.invoke(),
        Err(error) => {
            eprintln!("orchestrate-meta: {error}");
            ExitCode::FAILURE
        }
    }
}
