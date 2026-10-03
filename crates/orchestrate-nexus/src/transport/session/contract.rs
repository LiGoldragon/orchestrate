//! What each socket's contract does with one opened query.
//!
//! The session is the same machinery on both sockets: one greeting, then any
//! number of exchanges. What differs is the contract — which queries stream,
//! and how a peer the socket will not admit is told so — and that is borne by
//! the contract's own query and response roots, the data-bearing types the
//! socket speaks.

use kameo::actor::ActorRef;
use meta_signal_orchestrate::{PeerRejection, Query as MetaQuery, Response as MetaResponse};
use signal::{Contracted, Signalizable};
use signal_orchestrate::{Observation, Query as OrdinaryQuery, Response as OrdinaryResponse};
use tokio::sync::broadcast::{self, error::RecvError};

use crate::core::{Attending, NexusCore};

use super::super::{Delivered, TransportError};

/// What opening one exchange produced.
pub(crate) enum Opened<R> {
    /// The one answer; the exchange ends with it.
    Answered(R),
    /// The state on open, and the changes that follow it for as long as the
    /// exchange stays open.
    Streaming {
        opening: R,
        announcements: Announcements<R>,
    },
}

/// One subscriber's place in the core's change stream, and how its contract
/// frames each change.
pub(crate) struct Announcements<R> {
    receiver: broadcast::Receiver<Observation>,
    framing: fn(Observation) -> R,
}

/// The next thing a change stream has for its subscriber.
pub(crate) enum Followed<R> {
    /// A change, already framed as the contract's response.
    Changed(R),
    /// The core moved on further than it keeps changes for; the subscriber
    /// cannot be brought current by this stream any more.
    Lagged,
    /// The core has stopped, and with it the stream.
    Closed,
}

pub(crate) trait Following<R> {
    async fn follow(&mut self) -> Followed<R>;
}

impl<R> Following<R> for Announcements<R> {
    async fn follow(&mut self) -> Followed<R> {
        match self.receiver.recv().await {
            Ok(observation) => Followed::Changed((self.framing)(observation)),
            Err(RecvError::Lagged(_)) => Followed::Lagged,
            Err(RecvError::Closed) => Followed::Closed,
        }
    }
}

/// How a contract names a peer its socket will not answer.
///
/// A refusal is a value of the contract the socket bears, never of some other
/// contract that happens to have one: a frame written on a socket must be
/// readable by the peer on it, and a peer on the ordinary socket is speaking
/// the ordinary contract.
///
/// The exception taken here: `None` is silence, and silence is what the
/// ordinary contract can honestly say. The ordinary socket admits whoever the
/// filesystem let through, so it never refuses; a contract that gained a
/// refusing socket would gain the vocabulary for it at the same time.
pub(crate) trait Refusing: Sized {
    fn peer_refusal(peer_user: u32) -> Option<Self>;
}

impl Refusing for MetaResponse {
    fn peer_refusal(peer_user: u32) -> Option<Self> {
        Some(Self::PeerRefused(PeerRejection {
            peer_user_id: i64::from(peer_user),
        }))
    }
}

impl Refusing for OrdinaryResponse {
    fn peer_refusal(_: u32) -> Option<Self> {
        None
    }
}

/// A socket's query root: it settles its contract by the greeting, and opens
/// an exchange against the core.
pub(crate) trait SocketContract: Contracted + Sized + Send + 'static {
    type Response: Refusing + Signalizable + Send + Sync + 'static;

    async fn opened(
        self,
        core: &ActorRef<NexusCore>,
    ) -> Result<Opened<Self::Response>, TransportError>;
}

/// The ordinary contract: `Observe` streams, every other query is answered
/// once.
impl SocketContract for OrdinaryQuery {
    type Response = OrdinaryResponse;

    async fn opened(
        self,
        core: &ActorRef<NexusCore>,
    ) -> Result<Opened<Self::Response>, TransportError> {
        match self {
            // Joining and reading the opening state are one step of the
            // core, so no change can commit between them: a subscriber
            // neither loses a change nor is sent the same one twice.
            Self::Observe(selection) => {
                let attendance = core.ask(Attending { selection }).await.delivered()?;
                Ok(Opened::Streaming {
                    opening: OrdinaryResponse::Observed(attendance.opening),
                    announcements: Announcements {
                        receiver: attendance.announcements,
                        framing: OrdinaryResponse::Observed,
                    },
                })
            }
            query => Ok(Opened::Answered(core.ask(query).await.delivered()?)),
        }
    }
}

/// The privileged contract: every query is answered once.
impl SocketContract for MetaQuery {
    type Response = MetaResponse;

    async fn opened(
        self,
        core: &ActorRef<NexusCore>,
    ) -> Result<Opened<Self::Response>, TransportError> {
        Ok(Opened::Answered(core.ask(self).await.delivered()?))
    }
}
