//! One accepted connection, from the first frame to the last.
//!
//! Framing is `signal`'s and only `signal`'s: this Nexus owns no length
//! prefix of its own. Every frame on both sockets is the shared
//! four-byte big-endian prefix and one validated rkyv archive.

use kameo::actor::ActorRef;
use meta_signal_orchestrate::{PeerRejection, Query as MetaQuery, Response as MetaResponse};
use nexus::{Permissive, SocketAuthority};
use signal::{
    AsyncFrameReading, AsyncFrameWriting, FrameCapacity, Restorable, Signal, Signalizable,
};
use signal_orchestrate::{ObserveSelection, Query as OrdinaryQuery, Response as OrdinaryResponse};
use tokio::{net::UnixStream, sync::broadcast::error::RecvError};

use crate::core::{Attending, NexusCore, Overtaking};

use super::{
    Delivered, TransportError,
    socket::{Attributable, OwnsSocket, SocketOwner},
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

/// How a contract names a peer its socket will not answer.
///
/// A refusal is a value of the contract the socket bears, never of some other
/// contract that happens to have one: a frame written on a socket must be
/// readable by the peer on it, and a peer on the ordinary socket is speaking
/// the ordinary contract. That is the whole reason this is a trait on the
/// response types rather than one hard-coded reply — and the reason the
/// ordinary response has no `PeerRefused` to return.
///
/// The exception taken here: `None` is silence, and silence is what the
/// ordinary contract can honestly say. The ordinary socket admits whoever the
/// filesystem let through, so it never refuses; a contract that gained a
/// refusing socket would gain the vocabulary for it at the same time.
trait Refusing: Sized {
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

/// Every session asks its socket's authority whether the connecting peer is
/// admitted, before a single frame is read.
///
/// The ordinary authority admits whoever the filesystem let through. The
/// privileged one admits only the user the socket belongs to, and says so in
/// its own contract's vocabulary when it does not: a refused peer is answered
/// `PeerRefused.PeerRejection`, never left to guess at a dropped connection.
trait Admitting {
    async fn admitted<Response>(&mut self) -> Result<bool, TransportError>
    where
        Response: Refusing + Signalizable;
}

impl Admitting for Session {
    async fn admitted<Response>(&mut self) -> Result<bool, TransportError>
    where
        Response: Refusing + Signalizable,
    {
        let peer_user = self.stream.peer_user()?;
        if self.authority.admits(peer_user, self.owner.user()) {
            return Ok(true);
        }
        if let Some(refusal) = Response::peer_refusal(peer_user) {
            self.answer(&refusal).await?;
        }
        Ok(false)
    }
}

/// The ordinary contract: one query, then either one reply or a subscription.
pub(crate) trait ServingOrdinary {
    async fn serve_ordinary(&mut self, core: ActorRef<NexusCore>) -> Result<(), TransportError>;
}

impl ServingOrdinary for Session {
    async fn serve_ordinary(&mut self, core: ActorRef<NexusCore>) -> Result<(), TransportError> {
        if !self.admitted::<OrdinaryResponse>().await? {
            return Ok(());
        }
        match self.receive::<OrdinaryQuery>().await? {
            OrdinaryQuery::Observe(selection) => self.subscribe(core, selection).await,
            query => {
                let response = core.ask(query).await.delivered()?;
                self.answer(&response).await
            }
        }
    }
}

/// Observation is a subscription: the state on open, then each change.
trait Subscribing {
    async fn subscribe(
        &mut self,
        core: ActorRef<NexusCore>,
        selection: ObserveSelection,
    ) -> Result<(), TransportError>;
}

impl Subscribing for Session {
    async fn subscribe(
        &mut self,
        core: ActorRef<NexusCore>,
        selection: ObserveSelection,
    ) -> Result<(), TransportError> {
        // Joining and reading the opening state are one step of the core, so
        // no change can commit between them: a subscriber neither loses a
        // change nor is sent the same one twice. Two separate acquisitions
        // could promise only the first of those.
        let attendance = core
            .ask(Attending {
                selection: selection.clone(),
            })
            .await
            .delivered()?;
        let mut announcements = attendance.announcements;
        self.answer(&OrdinaryResponse::Observed(attendance.opening))
            .await?;
        loop {
            let observation = match announcements.recv().await {
                Ok(observation) => observation,
                // A subscriber that fell too far behind is sent the current
                // state: the value it would have converged on anyway.
                Err(RecvError::Lagged(_)) => core
                    .ask(Overtaking {
                        selection: selection.clone(),
                    })
                    .await
                    .delivered()?,
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
    async fn serve_meta(&mut self, core: ActorRef<NexusCore>) -> Result<(), TransportError>;
}

impl ServingMeta for Session {
    async fn serve_meta(&mut self, core: ActorRef<NexusCore>) -> Result<(), TransportError> {
        if !self.admitted::<MetaResponse>().await? {
            return Ok(());
        }
        let query = self.receive::<MetaQuery>().await?;
        let response: MetaResponse = core.ask(query).await.delivered()?;
        self.answer(&response).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read, os::unix::net::UnixStream as StandardUnixStream, path::PathBuf, time::Duration,
    };

    use kameo::actor::Spawn;
    use signal::{FrameReading, FrameWriting};
    use signal_orchestrate::OrchestrateNexusConfiguration;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        store::{OpensStore, OrchestrateStore},
        transport::socket::{Claiming, SocketClaim},
    };

    /// A temporary directory can host one privileged socket, served for one
    /// connection, belonging to a user this test names rather than to
    /// whoever happens to own the file.
    ///
    /// Naming the owner is what makes the refusing branch reachable on a
    /// single-user host: the rule compares the peer the kernel reports with
    /// the user the socket belongs to, and a test process cannot become a
    /// second user. Everything else on the path is the production one — a
    /// real bound socket, a real accepted connection, real `SO_PEERCRED`, and
    /// the real `Session`.
    #[allow(dead_code)]
    trait ServesOnePrivilegedConnection {
        fn serve_one(&self, owner: u32) -> (PathBuf, JoinHandle<()>);
        /// The same socket, the same refusing rule, and the same real
        /// `SO_PEERCRED` — but served as the ordinary contract, which is what
        /// a third socket bearing that contract would be.
        fn serve_one_ordinary(&self, owner: u32) -> (PathBuf, JoinHandle<()>);
        /// One claimed, bound, privileged socket and one core behind it —
        /// the part both of the above share.
        fn one_socket(
            &self,
            name: &str,
        ) -> (PathBuf, tokio::net::UnixListener, ActorRef<NexusCore>);
        /// The user this test process is, read from a file it has just
        /// created rather than assumed.
        fn own_user(&self) -> u32;
    }

    impl ServesOnePrivilegedConnection for tempfile::TempDir {
        fn serve_one(&self, owner: u32) -> (PathBuf, JoinHandle<()>) {
            let (socket_path, listener, core) = self.one_socket("privileged.sock");
            let handle = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept one connection");
                let mut session = Session::opened(
                    stream,
                    SocketAuthority::Privileged,
                    SocketOwner::named(owner),
                );
                session.serve_meta(core).await.expect("serve one session");
            });
            (socket_path, handle)
        }

        fn serve_one_ordinary(&self, owner: u32) -> (PathBuf, JoinHandle<()>) {
            let (socket_path, listener, core) = self.one_socket("ordinary-privileged.sock");
            let handle = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept one connection");
                let mut session = Session::opened(
                    stream,
                    SocketAuthority::Privileged,
                    SocketOwner::named(owner),
                );
                session
                    .serve_ordinary(core)
                    .await
                    .expect("serve one session");
            });
            (socket_path, handle)
        }

        fn one_socket(
            &self,
            name: &str,
        ) -> (PathBuf, tokio::net::UnixListener, ActorRef<NexusCore>) {
            let socket_path = self.path().join(name);
            let claim = SocketClaim::claim(&socket_path).expect("claim the socket path");
            let listener = claim
                .bind_listener(SocketAuthority::Privileged)
                .expect("bind the privileged socket");
            let (store, _) = OrchestrateStore::open(
                &self.path().join(format!("{name}.sema")),
                OrchestrateNexusConfiguration {
                    ordinary_socket_path: self.path().join("o.sock").display().to_string(),
                    meta_socket_path: socket_path.display().to_string(),
                },
            )
            .expect("open an isolated store");
            // The claim outlives this call because the listener does; leaking
            // it is how a test that never unbinds says so.
            std::mem::forget(claim);
            (socket_path, listener, NexusCore::spawn(store))
        }

        fn own_user(&self) -> u32 {
            let witness = self.path().join("own-user");
            std::fs::write(&witness, []).expect("create a file of our own");
            SocketOwner::of(&witness)
                .expect("read our own user from it")
                .user()
        }
    }

    /// One connection from this process, which never writes a query.
    trait Probes {
        fn refusal(self) -> (MetaResponse, Vec<u8>);
    }

    impl Probes for PathBuf {
        fn refusal(self) -> (MetaResponse, Vec<u8>) {
            let mut stream = StandardUnixStream::connect(&self).expect("connect");
            // Bounded, so a frame the session never sends fails the test
            // rather than hanging the harness.
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("bound the wait for a frame");
            let body = stream
                .read_frame(FrameCapacity::default())
                .expect("read the refusal frame");
            let response = Signal::<MetaResponse>::from(Vec::from(body))
                .restore()
                .expect("restore the refusal");
            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).expect("read to the close");
            (response, rest)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_who_is_not_the_socket_owner_is_refused_in_vocabulary_and_closed() {
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let us = directory.own_user();
        let (socket_path, handle) = directory.serve_one(us.wrapping_add(1));

        let (response, rest) = tokio::task::spawn_blocking(move || socket_path.refusal())
            .await
            .expect("run the peer");
        assert_eq!(
            response,
            MetaResponse::PeerRefused(PeerRejection {
                peer_user_id: i64::from(us),
            }),
            "the peer is named a refusal in vocabulary, not left to guess at a dropped connection"
        );
        assert!(
            rest.is_empty(),
            "and nothing follows it: the connection is closed without a query being read"
        );
        handle.await.expect("the session ended without failing");
    }

    /// The defect this shape closes: a refusal must be a frame the peer can
    /// read. A session bearing the ordinary contract that wrote the meta
    /// contract's `PeerRefused` would be writing a frame its peer cannot
    /// restore — correct today only because the ordinary authority never
    /// refuses, and wrong the moment a third socket bears that contract.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_peer_on_the_ordinary_contract_is_never_sent_a_meta_frame() {
        use std::io::Read;

        let directory = tempfile::tempdir().expect("temporary socket directory");
        let us = directory.own_user();
        let (socket_path, handle) = directory.serve_one_ordinary(us.wrapping_add(1));

        let rest = tokio::task::spawn_blocking(move || {
            let mut stream = StandardUnixStream::connect(&socket_path).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("bound the wait");
            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).expect("read to the close");
            rest
        })
        .await
        .expect("run the peer");
        assert!(
            rest.is_empty(),
            "the ordinary contract has no way to name a refused peer, so the \
             refusal is the close and nothing else: found {rest:?}"
        );
        handle.await.expect("the session ended without failing");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_socket_owner_reaches_the_contract_on_the_same_path() {
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let (socket_path, handle) = directory.serve_one(directory.own_user());

        let response = tokio::task::spawn_blocking(move || {
            let mut stream = StandardUnixStream::connect(&socket_path).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("bound the wait for a frame");
            let query = MetaQuery::ReverseMetaConfiguration
                .signalize()
                .expect("archive a query");
            stream
                .write_frame(&query, FrameCapacity::default())
                .expect("write the query");
            let body = stream
                .read_frame(FrameCapacity::default())
                .expect("read the reply");
            Signal::<MetaResponse>::from(Vec::from(body))
                .restore()
                .expect("restore the reply")
        })
        .await
        .expect("run the peer");
        assert!(
            matches!(response, MetaResponse::OrdinaryConfigurationReopened(_)),
            "the owner is admitted and answered on the contract, found {response:?}"
        );
        handle.await.expect("the session ended without failing");
    }
}
