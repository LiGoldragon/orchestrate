//! One accepted connection, from the first frame to the last.
//!
//! Framing is `signal`'s and only `signal`'s: this Nexus owns no length
//! prefix of its own. Every frame on both sockets is the shared
//! four-byte big-endian prefix and one validated rkyv archive.

use std::sync::Arc;

use meta_signal_orchestrate::{PeerRejection, Query as MetaQuery, Response as MetaResponse};
use signal::{
    AsyncFrameReading, AsyncFrameWriting, FrameCapacity, Restorable, Signal, Signalizable,
};
use signal_orchestrate::{ObserveSelection, Query as OrdinaryQuery, Response as OrdinaryResponse};
use tokio::{net::UnixStream, sync::broadcast::error::RecvError};

use crate::core::{Announcing, Applies, NexusCore};

use super::{
    TransportError,
    socket::{Attributable, OwnsSocket, Permissive, SocketAuthority, SocketOwner},
};

/// One accepted connection on one socket.
pub(crate) struct Session {
    stream: UnixStream,
    authority: SocketAuthority,
    owner: SocketOwner,
}

/// A session is opened around an accepted stream.
pub(crate) trait Opening {
    fn opened(stream: UnixStream, authority: SocketAuthority, owner: SocketOwner) -> Self;
}

impl Opening for Session {
    fn opened(stream: UnixStream, authority: SocketAuthority, owner: SocketOwner) -> Self {
        Self {
            stream,
            authority,
            owner,
        }
    }
}

/// A session reads and writes whole Signal frames of one contract.
trait Exchanging {
    async fn receive<T>(&mut self) -> Result<T, TransportError>
    where
        T: rkyv::Archive,
        Signal<T>: Restorable<T>;

    async fn answer<T>(&mut self, response: &T) -> Result<(), TransportError>
    where
        T: Signalizable;
}

impl Exchanging for Session {
    async fn receive<T>(&mut self) -> Result<T, TransportError>
    where
        T: rkyv::Archive,
        Signal<T>: Restorable<T>,
    {
        let body = self.stream.read_frame(FrameCapacity::default()).await?;
        Signal::<T>::from(Vec::from(body))
            .restore()
            .map_err(|_| TransportError::Archive)
    }

    async fn answer<T>(&mut self, response: &T) -> Result<(), TransportError>
    where
        T: Signalizable,
    {
        let signal = response.signalize().map_err(|_| TransportError::Archive)?;
        self.stream
            .write_frame(&signal, FrameCapacity::default())
            .await?;
        Ok(())
    }
}

/// Every session asks its socket's authority whether the connecting peer is
/// admitted, before a single frame is read.
///
/// The ordinary authority admits whoever the filesystem let through. The
/// privileged one admits only the user the socket belongs to, and says so in
/// vocabulary when it does not: a refused peer is answered
/// `PeerRefused.PeerRejection`, never left to guess at a dropped connection.
/// That reply is on the meta contract, which is the only contract a peer on
/// the privileged socket can be speaking.
trait Admitting {
    async fn admitted(&mut self) -> Result<bool, TransportError>;
}

impl Admitting for Session {
    async fn admitted(&mut self) -> Result<bool, TransportError> {
        let peer_user = self.stream.peer_user()?;
        if self.authority.admits(peer_user, self.owner.user()) {
            return Ok(true);
        }
        self.answer(&MetaResponse::PeerRefused(PeerRejection {
            peer_user_id: i64::from(peer_user),
        }))
        .await?;
        Ok(false)
    }
}

/// The ordinary contract: one query, then either one reply or a subscription.
pub(crate) trait ServingOrdinary {
    async fn serve_ordinary(&mut self, core: Arc<NexusCore>) -> Result<(), TransportError>;
}

impl ServingOrdinary for Session {
    async fn serve_ordinary(&mut self, core: Arc<NexusCore>) -> Result<(), TransportError> {
        if !self.admitted().await? {
            return Ok(());
        }
        match self.receive::<OrdinaryQuery>().await? {
            OrdinaryQuery::Observe(selection) => self.subscribe(core, selection).await,
            query => {
                let response = core.apply(query).await?;
                self.answer(&response).await
            }
        }
    }
}

/// Observation is a subscription: the state on open, then each change.
trait Subscribing {
    async fn subscribe(
        &mut self,
        core: Arc<NexusCore>,
        selection: ObserveSelection,
    ) -> Result<(), TransportError>;
}

impl Subscribing for Session {
    async fn subscribe(
        &mut self,
        core: Arc<NexusCore>,
        selection: ObserveSelection,
    ) -> Result<(), TransportError> {
        // Subscribed before the opening state is read, so a change committed
        // in between is delivered rather than lost. It may then be delivered
        // twice, since the opening read already reflects it — harmless,
        // because an announcement is the whole observation and applying the
        // same one twice leaves the subscriber where it already was. Losing a
        // change would not be harmless, so the order is this way round.
        let mut announcements = core.announcements();
        let opening = core.opening_observation(&selection).await?;
        self.answer(&OrdinaryResponse::Observed(opening)).await?;
        loop {
            let observation = match announcements.recv().await {
                Ok(observation) => observation,
                // A subscriber that fell too far behind is sent the current
                // state: the value it would have converged on anyway.
                Err(RecvError::Lagged(_)) => core.opening_observation(&selection).await?,
                Err(RecvError::Closed) => return Ok(()),
            };
            match self.answer(&OrdinaryResponse::Observed(observation)).await {
                Ok(()) => {}
                // The peer closed the connection. Closing is how a peer
                // unsubscribes, so this is an ordinary end, not a failure.
                Err(TransportError::Frame(signal::FrameError::Io(_))) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }
}

/// The privileged contract: admission, then one query and one reply.
pub(crate) trait ServingMeta {
    async fn serve_meta(&mut self, core: Arc<NexusCore>) -> Result<(), TransportError>;
}

impl ServingMeta for Session {
    async fn serve_meta(&mut self, core: Arc<NexusCore>) -> Result<(), TransportError> {
        if !self.admitted().await? {
            return Ok(());
        }
        let query = self.receive::<MetaQuery>().await?;
        let response: MetaResponse = core.apply(query).await?;
        self.answer(&response).await
    }
}
