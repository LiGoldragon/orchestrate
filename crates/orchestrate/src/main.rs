//! Datom edge client for the ordinary Orchestrate Signal.

#[path = "generated/client.rs"]
mod generated_client;

use datom_codec::{Actualizing, Budget, Datom, Datomizable, Potential};
use generated_client::{ClientFailure, Unreachable};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal_orchestrate::{ByteViewable, Query, Response, Restorable, Signal, Signalizable};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

const MAXIMUM_SIGNAL_BYTES: usize = 8 * 1024 * 1024;

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
                println!("{}", signal_orchestrate::ETHOS.trim());
                println!("{}", include_str!("../client.ethos").trim());
                ExitCode::SUCCESS
            }
            Self::Query(source) => match Client::from_process() {
                Ok(client) => client.query_source(source),
                Err(error) => {
                    eprintln!("orchestrate: {error}");
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
            socket_path: env::var("ORCHESTRATE_SOCKET")
                .map_err(|_| "ORCHESTRATE_SOCKET is required".to_owned())?,
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
    fn exchange<T>(&mut self, query: &Signal<T>) -> Result<Vec<u8>, TransportError> {
        let bytes = query.bytes();
        if bytes.len() > MAXIMUM_SIGNAL_BYTES {
            return Err(TransportError::TooLarge(bytes.len()));
        }
        let length =
            u32::try_from(bytes.len()).map_err(|_| TransportError::TooLarge(bytes.len()))?;
        self.stream.write_all(&length.to_le_bytes())?;
        self.stream.write_all(bytes)?;
        self.stream.flush()?;
        let mut prefix = [0; 4];
        self.stream.read_exact(&mut prefix)?;
        let response_length = u32::from_le_bytes(prefix) as usize;
        if response_length > MAXIMUM_SIGNAL_BYTES {
            return Err(TransportError::TooLarge(response_length));
        }
        let mut payload = vec![0; response_length];
        self.stream.read_exact(&mut payload)?;
        Ok(payload)
    }
}

#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Signal archive validation failed")]
    Archive,
    #[error("Signal payload exceeds the 8 MiB limit: {0} bytes")]
    TooLarge(usize),
}

fn main() -> ExitCode {
    match Invocation::from_process() {
        Ok(invocation) => invocation.invoke(),
        Err(error) => {
            eprintln!("orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}
