//! One accepted connection: one greeting, then any number of exchanges.
//!
//! The wire is `signal`'s exchange layer. The peer greets once with the
//! digest of its contract's source; the greeting gate answers it, and a peer
//! built from another source is refused and closed. After that every query
//! opens an exchange the peer names, and every frame this side sends names
//! the exchange it belongs to — so an `Observe` stream and a `Lock` can run
//! on one connection at once, and the peer tells their frames apart by
//! exchange alone.
//!
//! Framing is `signal`'s and only `signal`'s: this Nexus owns no length
//! prefix of its own.
//!
//! Reading runs as a task of its own because reading a frame is not safe to
//! cancel halfway, and the session must also be ready to write a change to a
//! subscription at any moment. Each subscription is fed by a task of its own,
//! which stops reading the core's change stream while the peer is not taking
//! frames; a subscriber that falls further behind than the core keeps changes
//! for has its exchange ended `Lagged`, and opens `Observe` again to be given
//! the state on open.

mod contract;
mod gate;
mod ledger;
#[cfg(test)]
mod tests;

use std::marker::PhantomData;

use kameo::actor::ActorRef;
use nexus::{Permissive, SocketAuthority};
use signal::{
    Answer, AsyncFrameReading, AsyncFrameWriting, CONNECTION_EXCHANGE, Delivery, Dispatch, Ending,
    ExchangeFault, ExchangeId, Exchanged, FrameCapacity, FrameError, Restorable, Signal,
    Signalizable,
};
use tokio::{
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
    task::AbortHandle,
};

use crate::core::NexusCore;

pub(crate) use contract::SocketContract;
use contract::{Announcements, Followed, Following, Opened, Refusing};
use gate::{Gating, GreetingGate};
use ledger::{Ledgering, SessionLedger};

use super::{
    TransportError,
    socket::{Attributable, OwnsSocket, SocketOwner},
};

/// One accepted connection on one socket, bearing one contract.
pub(crate) struct Session<Q> {
    stream: UnixStream,
    authority: SocketAuthority,
    owner: SocketOwner,
    contract: PhantomData<fn() -> Q>,
}

/// A session is opened around an accepted stream.
pub(crate) trait Opening {
    fn opened(stream: UnixStream, authority: SocketAuthority, owner: SocketOwner) -> Self;
}

impl<Q> Opening for Session<Q> {
    fn opened(stream: UnixStream, authority: SocketAuthority, owner: SocketOwner) -> Self {
        Self {
            stream,
            authority,
            owner,
            contract: PhantomData,
        }
    }
}

/// What the reading task hands the session.
enum Received<Q> {
    Dispatch(Dispatch<Q>),
    /// A frame arrived that is not a dispatch of this contract, or claimed a
    /// length past the frame capacity. Nothing after it can be trusted to
    /// start on a frame boundary.
    Unreadable,
}

/// The reading half of a connection, and where what it reads goes.
struct Inbound<Q> {
    reading: OwnedReadHalf,
    received: mpsc::Sender<Received<Q>>,
}

trait Listening {
    /// Read frames until the peer closes, or sends one that cannot be read.
    async fn listen(self);
}

impl<Q> Listening for Inbound<Q>
where
    Q: rkyv::Archive,
    Signal<Dispatch<Q>>: Restorable<Dispatch<Q>>,
{
    async fn listen(mut self) {
        loop {
            let received = match self.reading.read_frame(FrameCapacity::default()).await {
                Ok(body) => match Signal::<Dispatch<Q>>::from(Vec::from(body)).restore() {
                    Ok(dispatch) => Received::Dispatch(dispatch),
                    Err(_) => Received::Unreadable,
                },
                Err(FrameError::BodyTooLarge { .. }) => Received::Unreadable,
                // The peer closed, or the socket failed: either way the
                // connection is over, and the session learns it by this
                // sender going away.
                Err(FrameError::Io(_)) => return,
            };
            let unreadable = matches!(received, Received::Unreadable);
            if self.received.send(received).await.is_err() || unreadable {
                return;
            }
        }
    }
}

/// The task feeding one subscription: the exchange it answers on, the change
/// stream it follows, and the session it hands framed changes to.
struct Feeder<R> {
    exchange: ExchangeId,
    announcements: Announcements<R>,
    fed: mpsc::Sender<Delivery<R>>,
}

trait Feeding {
    async fn feed(self);
}

impl<R> Feeding for Feeder<R> {
    async fn feed(mut self) {
        loop {
            let delivery = match self.announcements.follow().await {
                Followed::Changed(response) => Delivery::Answer(Answer {
                    exchange: self.exchange,
                    response,
                }),
                Followed::Lagged => {
                    Delivery::End(Ending::faulted(self.exchange, ExchangeFault::Lagged))
                }
                Followed::Closed => Delivery::End(Ending::completed(self.exchange)),
            };
            let ending = matches!(delivery, Delivery::End(_));
            if self.fed.send(delivery).await.is_err() || ending {
                return;
            }
        }
    }
}

/// A connection past admission: the writing half, the greeting gate, the
/// exchange ledger, and the core every exchange is opened against.
struct Exchanges<Q: SocketContract> {
    writing: OwnedWriteHalf,
    gate: GreetingGate<Q>,
    ledger: SessionLedger,
    fed: mpsc::Sender<Delivery<Q::Response>>,
    core: ActorRef<NexusCore>,
    listening: AbortHandle,
}

/// Whether a connection goes on after what it just handled.
enum Continuation {
    Going,
    Ended,
}

/// Writing one frame of the exchange layer.
trait Delivering<R> {
    async fn deliver(&mut self, delivery: &Delivery<R>) -> Result<(), TransportError>;

    /// A fault ends what it names: the exchange, or for a fault against the
    /// connection, the connection.
    async fn faulted(&mut self, ending: Ending) -> Result<Continuation, TransportError> {
        let connection_wide = ending.is_connection_wide();
        self.deliver(&Delivery::End(ending)).await?;
        Ok(if connection_wide {
            Continuation::Ended
        } else {
            Continuation::Going
        })
    }
}

impl<Q> Delivering<Q::Response> for Exchanges<Q>
where
    Q: SocketContract,
    Delivery<Q::Response>: Signalizable,
{
    async fn deliver(&mut self, delivery: &Delivery<Q::Response>) -> Result<(), TransportError> {
        let signal = delivery.signalize().map_err(|_| TransportError::Archive)?;
        self.writing
            .write_frame(&signal, FrameCapacity::default())
            .await?;
        Ok(())
    }
}

/// What a connection does with each frame its peer sends, and with each
/// change a subscription's feeder hands it.
trait Dispatching<Q: SocketContract> {
    async fn dispatched(&mut self, dispatch: Dispatch<Q>) -> Result<Continuation, TransportError>;
    async fn fed(
        &mut self,
        delivery: Delivery<Q::Response>,
    ) -> Result<Continuation, TransportError>;
}

impl<Q> Dispatching<Q> for Exchanges<Q>
where
    Q: SocketContract,
    Delivery<Q::Response>: Signalizable,
{
    async fn dispatched(&mut self, dispatch: Dispatch<Q>) -> Result<Continuation, TransportError> {
        match dispatch {
            Dispatch::Greet(greeting) => match self.gate.greeted(&greeting) {
                Err(fault) => self.faulted(Ending::connection_faulted(fault)).await,
                Ok(receipt) => {
                    self.deliver(&Delivery::Greeted(receipt)).await?;
                    if !self.gate.settled() {
                        // Refused: the peer was told this side's digest, and
                        // two sources do not negotiate.
                        return Ok(Continuation::Ended);
                    }
                    match self.ledger.settle() {
                        Ok(()) => Ok(Continuation::Going),
                        Err(fault) => self.faulted(Ending::connection_faulted(fault)).await,
                    }
                }
            },
            Dispatch::Open(opening) => {
                if let Err(fault) = self.gate.require_settled() {
                    return self.faulted(Ending::connection_faulted(fault)).await;
                }
                let exchange = opening.exchange();
                if exchange == CONNECTION_EXCHANGE {
                    return self
                        .faulted(Ending::connection_faulted(ExchangeFault::UnknownExchange))
                        .await;
                }
                if let Err(fault) = self.ledger.admit(exchange) {
                    return self.faulted(Ending::faulted(exchange, fault)).await;
                }
                match opening.query.opened(&self.core).await? {
                    Opened::Answered(response) => {
                        let _ = self.ledger.conclude(exchange);
                        self.deliver(&Delivery::Answer(Answer { exchange, response }))
                            .await?;
                    }
                    Opened::Streaming {
                        opening,
                        announcements,
                    } => {
                        // The state on open is written before the feeder
                        // exists, so no change can overtake it.
                        self.deliver(&Delivery::Answer(Answer {
                            exchange,
                            response: opening,
                        }))
                        .await?;
                        let feeding = tokio::spawn(
                            Feeder {
                                exchange,
                                announcements,
                                fed: self.fed.clone(),
                            }
                            .feed(),
                        );
                        self.ledger.streaming(exchange, feeding.abort_handle());
                    }
                }
                Ok(Continuation::Going)
            }
            Dispatch::Abandon(exchange) => match self.ledger.abandon(exchange) {
                Ok(_) => Ok(Continuation::Going),
                Err(fault) => self.faulted(Ending::faulted(exchange, fault)).await,
            },
        }
    }

    async fn fed(
        &mut self,
        delivery: Delivery<Q::Response>,
    ) -> Result<Continuation, TransportError> {
        let exchange = match &delivery {
            Delivery::Answer(answer) => answer.exchange(),
            Delivery::End(ending) => ending.exchange(),
            Delivery::Greeted(_) => return Ok(Continuation::Going),
        };
        // A change already on its way when the peer abandoned the exchange is
        // not wanted any more.
        if !self.ledger.bears(exchange) {
            return Ok(Continuation::Going);
        }
        if matches!(delivery, Delivery::End(_)) {
            let _ = self.ledger.conclude(exchange);
        }
        self.deliver(&delivery).await?;
        Ok(Continuation::Going)
    }
}

/// The reading task ends with the connection, whoever ends it.
impl<Q: SocketContract> Drop for Exchanges<Q> {
    fn drop(&mut self) {
        self.listening.abort();
    }
}

/// Every session asks its socket's authority whether the connecting peer is
/// admitted, before a single frame is read.
///
/// The ordinary authority admits whoever the filesystem let through. The
/// privileged one admits only the user the socket belongs to, and says so in
/// its own contract's vocabulary when it does not: a refused peer is sent
/// `PeerRefused.PeerRejection` as an answer against the connection itself,
/// unprompted, and closed — never left to guess at a dropped connection.
trait Admitting {
    async fn admitted(&mut self) -> Result<bool, TransportError>;
}

impl<Q> Admitting for Session<Q>
where
    Q: SocketContract,
    Delivery<Q::Response>: Signalizable,
{
    async fn admitted(&mut self) -> Result<bool, TransportError> {
        let peer_user = self.stream.peer_user()?;
        if self.authority.admits(peer_user, self.owner.user()) {
            return Ok(true);
        }
        if let Some(response) = Q::Response::peer_refusal(peer_user) {
            let refusal = Delivery::Answer(Answer {
                exchange: CONNECTION_EXCHANGE,
                response,
            })
            .signalize()
            .map_err(|_| TransportError::Archive)?;
            self.stream
                .write_frame(&refusal, FrameCapacity::default())
                .await?;
        }
        Ok(false)
    }
}

/// Serves one connection until the peer closes it or breaks the protocol.
pub(crate) trait ServesConnection {
    async fn serve(self, core: ActorRef<NexusCore>) -> Result<(), TransportError>;
}

impl<Q> ServesConnection for Session<Q>
where
    Q: SocketContract + rkyv::Archive,
    Signal<Dispatch<Q>>: Restorable<Dispatch<Q>>,
    Delivery<Q::Response>: Signalizable,
{
    async fn serve(mut self, core: ActorRef<NexusCore>) -> Result<(), TransportError> {
        if !self.admitted().await? {
            return Ok(());
        }
        let (reading, writing) = self.stream.into_split();
        let (received_sender, mut received) = mpsc::channel(1);
        let listening = tokio::spawn(
            Inbound::<Q> {
                reading,
                received: received_sender,
            }
            .listen(),
        )
        .abort_handle();
        // One frame of room: a feeder holds the next change until the peer
        // has taken this one, so a subscriber that is not reading holds the
        // core's change stream back and is found lagging, rather than being
        // buffered for without limit.
        let (fed_sender, mut fed) = mpsc::channel(1);
        let mut exchanges = Exchanges::<Q> {
            writing,
            gate: GreetingGate::default(),
            ledger: SessionLedger::default(),
            fed: fed_sender,
            core,
            listening,
        };
        loop {
            let handled = tokio::select! {
                dispatch = received.recv() => match dispatch {
                    None => Ok(Continuation::Ended),
                    Some(Received::Dispatch(dispatch)) => exchanges.dispatched(dispatch).await,
                    Some(Received::Unreadable) => {
                        exchanges
                            .faulted(Ending::connection_faulted(ExchangeFault::UnreadableQuery))
                            .await
                    }
                },
                Some(delivery) = fed.recv() => exchanges.fed(delivery).await,
            };
            match handled {
                Ok(Continuation::Going) => {}
                Ok(Continuation::Ended) => return Ok(()),
                // The peer closed while a frame was on its way to it. Closing
                // is how a peer leaves, so this is an ordinary end.
                Err(TransportError::Frame(FrameError::Io(_))) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }
}
