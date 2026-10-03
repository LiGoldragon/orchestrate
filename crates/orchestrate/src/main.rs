//! Datom edge client for the ordinary Orchestrate Signal.

#[path = "generated/client.rs"]
mod generated_client;

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use generated_client::{ClientFailure, Unreachable};
use protos::{Compactable, Protosizable, ReaderBudget};
use signal::{
    Conclusion, Contracted, Delivery, Dispatch, ExchangeFault, ExchangeLedger, ExchangeMinting,
    Exchanged, FrameCapacity, FrameReading, FrameWriting, Greeted, HandshakeReceipt,
    HandshakeRejection, Opening, Restorable, Signal, Signalizable,
};
use signal_orchestrate::{Query, Response};
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
        SignalConnection::connect(&self.socket_path)
            .and_then(|mut connection| connection.ask(query.clone()))
            .map_err(|error| match error {
                TransportError::Refused(rejection) => ClientFailure::GreetingRefused(rejection),
                TransportError::Faulted(fault) => ClientFailure::ExchangeFaulted(fault),
                error => ClientFailure::Unreachable(Unreachable {
                    socket_path: self.socket_path.clone(),
                    transport_error: error.to_string(),
                }),
            })
    }
}

trait DatomText {
    fn datom_text(&self) -> String;
}

impl<T> DatomText for T
where
    T: Datomizable,
{
    fn datom_text(&self) -> String {
        self.datomize(Vec::new()).protosize().compact()
    }
}

/// One connection, greeted once, carrying the one exchange this CLI opens.
struct SignalConnection {
    stream: UnixStream,
    ledger: ExchangeLedger,
}

trait Connecting: Sized {
    fn connect(socket_path: &str) -> Result<Self, TransportError>;
}

impl Connecting for SignalConnection {
    fn connect(socket_path: &str) -> Result<Self, TransportError> {
        Ok(Self {
            stream: UnixStream::connect(socket_path)?,
            ledger: ExchangeLedger::default(),
        })
    }
}

/// Whole frames of signal's exchange layer. Framing is `signal`'s: this
/// client owns no length prefix of its own.
trait Framing {
    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> Result<(), TransportError>
    where
        Dispatch<Q>: Signalizable;
    fn receive<R>(&mut self) -> Result<Delivery<R>, TransportError>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>;
}

impl Framing for SignalConnection {
    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> Result<(), TransportError>
    where
        Dispatch<Q>: Signalizable,
    {
        let signal = dispatch.signalize().map_err(|_| TransportError::Archive)?;
        self.stream.write_frame(&signal, FrameCapacity::default())?;
        Ok(())
    }

    fn receive<R>(&mut self) -> Result<Delivery<R>, TransportError>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>,
    {
        let body = self.stream.read_frame(FrameCapacity::default())?;
        Signal::<Delivery<R>>::from(Vec::from(body))
            .restore()
            .map_err(|_| TransportError::Archive)
    }
}

trait Asking {
    /// Greet once, open one exchange with the query, and read its answer.
    ///
    /// Exactly one answer is read even where the Nexus would go on writing:
    /// an `Observe` opens a stream there. A CLI that takes one argument and
    /// prints one value ends at the state on open; closing the connection is
    /// how it leaves the stream.
    fn ask<Q, R>(&mut self, query: Q) -> Result<R, TransportError>
    where
        Q: Contracted,
        Dispatch<Q>: Signalizable,
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>;
}

impl Asking for SignalConnection {
    fn ask<Q, R>(&mut self, query: Q) -> Result<R, TransportError>
    where
        Q: Contracted,
        Dispatch<Q>: Signalizable,
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>,
    {
        self.send(&Dispatch::<Q>::Greet(Q::greeting()))?;
        match self.receive::<R>()? {
            Delivery::Greeted(HandshakeReceipt::Greeted(_)) => {
                self.ledger.greet().map_err(TransportError::Faulted)?
            }
            Delivery::Greeted(HandshakeReceipt::GreetingRefused(rejection)) => {
                return Err(TransportError::Refused(rejection));
            }
            // A socket that will not admit this peer says so unprompted, as
            // an answer against the connection itself.
            Delivery::Answer(answer) if answer.is_connection_wide() => {
                return Ok(answer.response);
            }
            Delivery::End(ending) => return Err(ending.conclusion.into()),
            Delivery::Answer(_) => return Err(TransportError::Unexpected),
        }
        let exchange = self.ledger.open().map_err(TransportError::Faulted)?;
        self.send(&Dispatch::Open(Opening { exchange, query }))?;
        match self.receive::<R>()? {
            Delivery::Answer(answer)
                if answer.exchange() == exchange || answer.is_connection_wide() =>
            {
                Ok(answer.response)
            }
            Delivery::End(ending) => Err(ending.conclusion.into()),
            Delivery::Answer(_) | Delivery::Greeted(_) => Err(TransportError::Unexpected),
        }
    }
}

impl From<Conclusion> for TransportError {
    fn from(conclusion: Conclusion) -> Self {
        match conclusion {
            Conclusion::Faulted(fault) => Self::Faulted(fault),
            Conclusion::Completed => Self::Unanswered,
        }
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
    #[error("the Nexus refused the greeting: {0:?}")]
    Refused(HandshakeRejection),
    #[error("the exchange ended with a fault: {0:?}")]
    Faulted(ExchangeFault),
    #[error("the exchange ended without an answer")]
    Unanswered,
    #[error("the Nexus sent a frame for no exchange this client opened")]
    Unexpected,
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
