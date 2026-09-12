//! What a stopped Nexus leaves behind for the next one.
//!
//! A restart is the whole deployment story: the new Nexus opens the same
//! store and binds the same two paths. Both are held by the old one — the
//! store inside the core, the paths under an exclusive claim — so stopping
//! has to be something that finishes rather than something that merely
//! returns. This drives the real transport: two real sockets, a real client
//! frame, and the same store file twice.

use std::{os::unix::net::UnixStream, path::Path, time::Duration};

use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    transport::{Binding, Serving, TransportRuntime},
};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{
    LockRequest, Observation, ObserveSelection, OrchestrateNexusConfiguration,
    Query as OrdinaryQuery, Response as OrdinaryResponse,
};
use tokio::sync::oneshot;

trait Defaults {
    fn defaults(&self) -> OrchestrateNexusConfiguration;
    fn store(&self) -> std::path::PathBuf;
}

impl Defaults for Path {
    fn defaults(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("ordinary.sock").display().to_string(),
            meta_socket_path: self.join("meta.sock").display().to_string(),
        }
    }

    fn store(&self) -> std::path::PathBuf {
        self.join("stopping.sema")
    }
}

/// One exchange on the ordinary socket, framed with the crate the Nexus
/// frames with.
trait Asks {
    fn asks(&self, query: &OrdinaryQuery) -> OrdinaryResponse;
}

impl Asks for str {
    fn asks(&self, query: &OrdinaryQuery) -> OrdinaryResponse {
        let mut stream = UnixStream::connect(self).expect("connect the ordinary socket");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("bound the wait for a frame");
        let signal = query.signalize().expect("archive the query");
        stream
            .write_frame(&signal, FrameCapacity::default())
            .expect("write the query");
        let body = stream
            .read_frame(FrameCapacity::default())
            .expect("read the reply");
        Signal::<OrdinaryResponse>::from(Vec::from(body))
            .restore()
            .expect("restore the reply")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_nexus_leaves_its_store_and_its_socket_paths_to_the_next_one() {
    let directory = tempfile::tempdir().expect("isolated Nexus directory");
    let defaults = directory.path().defaults();
    let (store, configuration) =
        OrchestrateStore::open(&directory.path().store(), defaults.clone()).expect("open");
    let transport = TransportRuntime::bind(configuration.clone(), store).expect("bind both");
    let (stop, shutdown) = oneshot::channel();
    let serving = tokio::spawn(transport.serve_until(shutdown));

    let ordinary = configuration.ordinary_socket_path.clone();
    let held = {
        let request = OrdinaryQuery::Lock(LockRequest {
            lock_name: "survives".to_owned(),
            flow_id: "test-flow".to_owned(),
            lock_path_vector: vec![directory.path().join("held").display().to_string()],
            lock_reason: "acquired before the stop".to_owned(),
        });
        let ordinary = ordinary.clone();
        match tokio::task::spawn_blocking(move || ordinary.as_str().asks(&request))
            .await
            .expect("run the client")
        {
            OrdinaryResponse::Locked(lock) => lock,
            other => panic!("expected Locked, found {other:?}"),
        }
    };

    stop.send(()).expect("ask the Nexus to stop");
    serving
        .await
        .expect("the serving task ended")
        .expect("and ended without failing");

    // Both of these fail against a Nexus that has merely stopped answering:
    // the engine refuses a store still open, and the claim refuses a path
    // still held.
    let (store, resumed) =
        OrchestrateStore::open(&directory.path().store(), directory.path().defaults())
            .expect("the stopped Nexus let its store go");
    assert_eq!(resumed, defaults);
    let transport = TransportRuntime::bind(resumed, store).expect("and let its socket paths go");
    let (stop, shutdown) = oneshot::channel();
    let serving = tokio::spawn(transport.serve_until(shutdown));

    let observed = tokio::task::spawn_blocking(move || {
        ordinary
            .as_str()
            .asks(&OrdinaryQuery::Observe(ObserveSelection::Locks))
    })
    .await
    .expect("run the client");
    assert_eq!(
        observed,
        OrdinaryResponse::Observed(Observation::Locks(vec![held])),
        "and the Lock it wrote before it stopped is there to be served"
    );

    stop.send(()).expect("stop the second Nexus");
    serving
        .await
        .expect("the second serving task ended")
        .expect("without failing");
}
