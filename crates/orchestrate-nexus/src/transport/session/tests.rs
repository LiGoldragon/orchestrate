use std::{
    io::Read, os::unix::net::UnixStream as StandardUnixStream, path::PathBuf, time::Duration,
};

use kameo::actor::Spawn;
use meta_signal_orchestrate::{PeerRejection, Query as MetaQuery, Response as MetaResponse};
use signal::{
    Contracted, FrameReading, FrameWriting, Handshake, HandshakeReceipt, HandshakeRejection,
    Opening as ExchangeOpening,
};
use signal_orchestrate::{OrchestrateNexusConfiguration, Query as OrdinaryQuery};
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
    fn one_socket(&self, name: &str) -> (PathBuf, tokio::net::UnixListener, ActorRef<NexusCore>);
    /// The user this test process is, read from a file it has just
    /// created rather than assumed.
    fn own_user(&self) -> u32;
}

impl ServesOnePrivilegedConnection for tempfile::TempDir {
    fn serve_one(&self, owner: u32) -> (PathBuf, JoinHandle<()>) {
        let (socket_path, listener, core) = self.one_socket("privileged.sock");
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept one connection");
            let session = Session::<MetaQuery>::opened(
                stream,
                SocketAuthority::Privileged,
                SocketOwner::named(owner),
            );
            session.serve(core).await.expect("serve one session");
        });
        (socket_path, handle)
    }

    fn serve_one_ordinary(&self, owner: u32) -> (PathBuf, JoinHandle<()>) {
        let (socket_path, listener, core) = self.one_socket("ordinary-privileged.sock");
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept one connection");
            let session = Session::<OrdinaryQuery>::opened(
                stream,
                SocketAuthority::Privileged,
                SocketOwner::named(owner),
            );
            session.serve(core).await.expect("serve one session");
        });
        (socket_path, handle)
    }

    fn one_socket(&self, name: &str) -> (PathBuf, tokio::net::UnixListener, ActorRef<NexusCore>) {
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
        let delivery = Signal::<Delivery<MetaResponse>>::from(Vec::from(body))
            .restore()
            .expect("restore the refusal");
        let Delivery::Answer(answer) = delivery else {
            panic!("a refusal is an answer, found {delivery:?}");
        };
        assert_eq!(
            answer.exchange(),
            CONNECTION_EXCHANGE,
            "unprompted, so it answers the connection rather than an exchange"
        );
        let response = answer.response;
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

/// One peer connection, speaking the exchange layer the way a client does.
struct Peer {
    stream: StandardUnixStream,
}

trait Speaks: Sized {
    fn connected(socket_path: &std::path::Path) -> Self;
    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> &mut Self
    where
        Dispatch<Q>: Signalizable;
    fn hear<R>(&mut self) -> Delivery<R>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>;
    /// Everything the session writes until it closes the connection.
    fn rest(mut self) -> Vec<u8> {
        let mut rest = Vec::new();
        self.stream_mut()
            .read_to_end(&mut rest)
            .expect("read to the close");
        rest
    }
    fn stream_mut(&mut self) -> &mut StandardUnixStream;
}

impl Speaks for Peer {
    fn connected(socket_path: &std::path::Path) -> Self {
        let stream = StandardUnixStream::connect(socket_path).expect("connect");
        // Bounded, so a frame the session never sends fails the test rather
        // than hanging the harness.
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("bound the wait for a frame");
        Self { stream }
    }

    fn send<Q>(&mut self, dispatch: &Dispatch<Q>) -> &mut Self
    where
        Dispatch<Q>: Signalizable,
    {
        let signal = dispatch.signalize().expect("archive a dispatch");
        self.stream
            .write_frame(&signal, FrameCapacity::default())
            .expect("write the dispatch");
        self
    }

    fn hear<R>(&mut self) -> Delivery<R>
    where
        R: rkyv::Archive,
        Signal<Delivery<R>>: Restorable<Delivery<R>>,
    {
        let body = self
            .stream
            .read_frame(FrameCapacity::default())
            .expect("read a delivery");
        Signal::<Delivery<R>>::from(Vec::from(body))
            .restore()
            .expect("restore a delivery")
    }

    fn stream_mut(&mut self) -> &mut StandardUnixStream {
        &mut self.stream
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_socket_owner_reaches_the_contract_on_the_same_path() {
    let directory = tempfile::tempdir().expect("temporary socket directory");
    let (socket_path, handle) = directory.serve_one(directory.own_user());

    let (greeted, answered) = tokio::task::spawn_blocking(move || {
        let mut peer = Peer::connected(&socket_path);
        let greeted = peer
            .send(&Dispatch::<MetaQuery>::Greet(MetaQuery::greeting()))
            .hear::<MetaResponse>();
        let answered = peer
            .send(&Dispatch::Open(ExchangeOpening {
                exchange: 1,
                query: MetaQuery::ReverseMetaConfiguration,
            }))
            .hear::<MetaResponse>();
        (greeted, answered)
    })
    .await
    .expect("run the peer");
    assert_eq!(
        greeted,
        Delivery::Greeted(HandshakeReceipt::Greeted(MetaQuery::contract_digest()))
    );
    let Delivery::Answer(answer) = answered else {
        panic!("the owner is answered on the exchange it opened, found {answered:?}");
    };
    assert_eq!(answer.exchange(), 1);
    assert!(
        matches!(
            answer.response,
            MetaResponse::OrdinaryConfigurationReopened(_)
        ),
        "the owner is admitted and answered on the contract, found {:?}",
        answer.response
    );
    handle.await.expect("the session ended without failing");
}

/// The greeting gate: an ordinary peer that dials the meta socket greets with
/// the ordinary contract's digest and is refused with the meta one, then
/// closed — before any query of either contract is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_greeting_with_another_contract_is_refused_and_closed() {
    let directory = tempfile::tempdir().expect("temporary socket directory");
    let (socket_path, handle) = directory.serve_one(directory.own_user());

    let (greeted, rest) = tokio::task::spawn_blocking(move || {
        let mut peer = Peer::connected(&socket_path);
        let greeted = peer
            .send(&Dispatch::<OrdinaryQuery>::Greet(OrdinaryQuery::greeting()))
            .hear::<MetaResponse>();
        (greeted, peer.rest())
    })
    .await
    .expect("run the peer");
    assert_eq!(
        greeted,
        Delivery::Greeted(HandshakeReceipt::GreetingRefused(
            HandshakeRejection::ContractMismatch(MetaQuery::contract_digest())
        ))
    );
    assert!(rest.is_empty(), "nothing follows a refused greeting");
    handle.await.expect("the session ended without failing");
}

/// An exchange opened before any greeting is a fault against the connection,
/// and the query in it never reaches the core.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exchange_opened_before_the_greeting_is_a_connection_fault() {
    let directory = tempfile::tempdir().expect("temporary socket directory");
    let (socket_path, handle) = directory.serve_one(directory.own_user());

    let (heard, rest) = tokio::task::spawn_blocking(move || {
        let mut peer = Peer::connected(&socket_path);
        let heard = peer
            .send(&Dispatch::Open(ExchangeOpening {
                exchange: 1,
                query: MetaQuery::ReverseMetaConfiguration,
            }))
            .hear::<MetaResponse>();
        (heard, peer.rest())
    })
    .await
    .expect("run the peer");
    assert_eq!(
        heard,
        Delivery::End(Ending::connection_faulted(ExchangeFault::GreetingExpected))
    );
    assert!(rest.is_empty(), "and the connection is closed");
    handle.await.expect("the session ended without failing");
}

/// A second greeting on a settled connection is a fault: the contract is
/// settled once or not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeated_greeting_is_a_connection_fault() {
    let directory = tempfile::tempdir().expect("temporary socket directory");
    let (socket_path, handle) = directory.serve_one(directory.own_user());

    let heard = tokio::task::spawn_blocking(move || {
        let mut peer = Peer::connected(&socket_path);
        let greeting =
            Dispatch::<MetaQuery>::Greet(Handshake::of_source(meta_signal_orchestrate::ETHOS));
        peer.send(&greeting).hear::<MetaResponse>();
        peer.send(&greeting).hear::<MetaResponse>()
    })
    .await
    .expect("run the peer");
    assert_eq!(
        heard,
        Delivery::End(Ending::connection_faulted(ExchangeFault::GreetingRepeated))
    );
    handle.await.expect("the session ended without failing");
}
