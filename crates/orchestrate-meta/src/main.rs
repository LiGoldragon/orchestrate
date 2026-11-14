//! Datom edge client for the privileged Orchestrate Signal.

#[path = "generated/client.rs"]
mod generated_client;

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use generated_client::{ClientFailure, SocketPath, TransportError, Unreachable};
use meta_signal_orchestrate::{Input, Output};
use protos::{Compactable, Protosizable, ReaderBudget};
use signal::{
    Conclusion, Contracted, Delivery, Dispatch, ExchangeLedger, ExchangeMinting, Exchanged,
    FrameCapacity, FrameReading, FrameWriting, Greeted, HandshakeReceipt, Opening, Restorable,
    Signal, Signalizable,
};
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
        let arguments = env::args_os().skip(1).collect::<Vec<_>>();
        match arguments.as_slice() {
            [] => Ok(Self::Describe),
            [source] if !source.as_encoded_bytes().starts_with(b"--") => source
                .clone()
                .into_string()
                .map(Self::Query)
                .map_err(|_| "inline Datom query must be UTF-8".to_owned()),
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
            socket_path: env::var("ORCHESTRATE_META_SOCKET").map_err(|error| match error {
                env::VarError::NotPresent => "ORCHESTRATE_META_SOCKET is required".to_owned(),
                env::VarError::NotUnicode(_) => "ORCHESTRATE_META_SOCKET must be UTF-8".to_owned(),
            })?,
        })
    }
}

trait Querying {
    fn query_source(&self, source: String) -> ExitCode;
    fn query(&self, input: &Input) -> Result<Output, ClientFailure>;
}

impl Querying for Client {
    fn query_source(&self, source: String) -> ExitCode {
        let mut potential = Potential::<Input>::from(source);
        let mut budget = Budget {
            remaining: 10_000,
            reader: ReaderBudget::try_from(10_000usize).unwrap(),
            depth: 0,
            maximum_depth: 256,
        };
        let result = potential
            .actualize(&mut budget)
            .map_err(ClientFailure::Unreadable)
            .and_then(|input| self.query(&input));
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

    fn query(&self, input: &Input) -> Result<Output, ClientFailure> {
        SignalConnection::connect(SocketPath(self.socket_path.clone()))
            .and_then(|mut connection| connection.ask(input.clone()))
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
///
/// Every failure on it is the generated `ClientFailure`: a refused greeting
/// and a faulted exchange as themselves, and anything else that kept the
/// answer from arriving as `Unreachable`, the socket beside the
/// `TransportError` text of what failed.
struct SignalConnection {
    socket_path: SocketPath,
    stream: UnixStream,
    ledger: ExchangeLedger,
}

trait Connecting: Sized {
    fn connect(socket_path: SocketPath) -> Result<Self, ClientFailure>;
}

impl Connecting for SignalConnection {
    fn connect(socket_path: SocketPath) -> Result<Self, ClientFailure> {
        match UnixStream::connect(&socket_path.0) {
            Ok(stream) => Ok(Self {
                socket_path,
                stream,
                ledger: ExchangeLedger::default(),
            }),
            Err(error) => Err(socket_path.unreachable(format!("Unix socket I/O failed: {error}"))),
        }
    }
}

/// A socket names where a failure happened when the answer did not arrive.
trait Unreaching {
    fn unreachable(&self, transport_error: String) -> ClientFailure;
}

impl Unreaching for SocketPath {
    fn unreachable(&self, transport_error: String) -> ClientFailure {
        ClientFailure::Unreachable(Unreachable {
            socket_path: self.clone(),
            transport_error: TransportError(transport_error),
        })
    }
}

/// Whole frames of signal's exchange layer. Framing is `signal`'s: this
/// client owns no length prefix of its own.
trait Framing {
    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> Result<(), ClientFailure>
    where
        Dispatch<Q>: Signalizable;
    fn receive<R>(&mut self) -> Result<Delivery<R>, ClientFailure>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>;
    fn unexpected(&self) -> ClientFailure;
    fn concluded(&self, conclusion: Conclusion) -> ClientFailure;
}

impl Framing for SignalConnection {
    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> Result<(), ClientFailure>
    where
        Dispatch<Q>: Signalizable,
    {
        let signal = dispatch.signalize().map_err(|_| {
            self.socket_path
                .unreachable("Signal archive validation failed".to_owned())
        })?;
        self.stream
            .write_frame(&signal, FrameCapacity::default())
            .map_err(|error| {
                self.socket_path
                    .unreachable(format!("Signal frame: {error}"))
            })
    }

    fn receive<R>(&mut self) -> Result<Delivery<R>, ClientFailure>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>,
    {
        let body = self
            .stream
            .read_frame(FrameCapacity::default())
            .map_err(|error| {
                self.socket_path
                    .unreachable(format!("Signal frame: {error}"))
            })?;
        Signal::<Delivery<R>>::from(Vec::from(body))
            .restore()
            .map_err(|_| {
                self.socket_path
                    .unreachable("Signal archive validation failed".to_owned())
            })
    }

    fn unexpected(&self) -> ClientFailure {
        self.socket_path
            .unreachable("the Nexus sent a frame for no exchange this client opened".to_owned())
    }

    fn concluded(&self, conclusion: Conclusion) -> ClientFailure {
        match conclusion {
            Conclusion::Faulted(fault) => ClientFailure::ExchangeFaulted(fault),
            Conclusion::Completed => self
                .socket_path
                .unreachable("the exchange ended without an answer".to_owned()),
        }
    }
}

trait Asking {
    /// Greet once, open one exchange with the input, and read its answer.
    ///
    /// Exactly one answer is read even where the Nexus would go on writing:
    /// an `Observe` opens a stream there. A CLI that takes one argument and
    /// prints one value ends at the state on open; closing the connection is
    /// how it leaves the stream.
    fn ask<Q, R>(&mut self, input: Q) -> Result<R, ClientFailure>
    where
        Q: Contracted,
        Dispatch<Q>: Signalizable,
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>;
}

impl Asking for SignalConnection {
    fn ask<Q, R>(&mut self, input: Q) -> Result<R, ClientFailure>
    where
        Q: Contracted,
        Dispatch<Q>: Signalizable,
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>,
    {
        self.send(&Dispatch::<Q>::Greet(Q::greeting()))?;
        match self.receive::<R>()? {
            Delivery::Greeted(HandshakeReceipt::Greeted(digest))
                if digest == Q::contract_digest() =>
            {
                self.ledger
                    .greet()
                    .map_err(ClientFailure::ExchangeFaulted)?
            }
            Delivery::Greeted(HandshakeReceipt::GreetingRefused(rejection)) => {
                return Err(ClientFailure::GreetingRefused(rejection));
            }
            Delivery::Greeted(HandshakeReceipt::Greeted(_)) => {
                return Err(self.unexpected());
            }
            // A socket that will not admit this peer says so unprompted, as
            // an answer against the connection itself.
            Delivery::Answer(answer) if answer.is_connection_wide() => {
                return Ok(answer.response);
            }
            Delivery::End(ending) if ending.is_connection_wide() => {
                return Err(self.concluded(ending.conclusion));
            }
            Delivery::End(_) => return Err(self.unexpected()),
            Delivery::Answer(_) => return Err(self.unexpected()),
        }
        let exchange = self.ledger.open().map_err(ClientFailure::ExchangeFaulted)?;
        self.send(&Dispatch::Open(Opening {
            exchange,
            query: input,
        }))?;
        match self.receive::<R>()? {
            Delivery::Answer(answer)
                if answer.exchange() == exchange || answer.is_connection_wide() =>
            {
                Ok(answer.response)
            }
            Delivery::End(ending)
                if ending.exchange() == exchange || ending.is_connection_wide() =>
            {
                Err(self.concluded(ending.conclusion))
            }
            Delivery::End(_) => Err(self.unexpected()),
            Delivery::Answer(_) | Delivery::Greeted(_) => Err(self.unexpected()),
        }
    }
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
