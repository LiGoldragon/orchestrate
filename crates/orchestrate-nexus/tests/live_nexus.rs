//! Live two-socket proof: shared framing, socket authority, durable state,
//! and Observe as a subscription.
//!
//! Every frame here is written and read with `signal`, the crate the Nexus
//! itself frames with. A test that hand-rolled the prefix could agree with a
//! Nexus that hand-rolled the same mistake; this one cannot.

use std::{
    io::{BufRead, BufReader},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use meta_signal_orchestrate::{Query as MetaQuery, Response as MetaResponse};
use orchestrate_nexus::store::record::{
    Familial, SCHEMA_VERSION, StoredAllocator, StoredConfiguration, StoredLock, Storing,
};
use sema_engine::{Assertion, Engine, EngineOpen};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason, Lock, LockRequest,
    Observation, ObserveSelection, OrchestrateNexusConfiguration, Query as OrdinaryQuery,
    Response as OrdinaryResponse,
};

struct IsolatedXdg {
    state_home: PathBuf,
    runtime_directory: PathBuf,
}

trait CreatesIsolatedXdg: Sized {
    fn create(temporary: &tempfile::TempDir) -> Self;
    fn store(&self) -> PathBuf;
    fn ordinary_socket(&self) -> PathBuf;
    fn meta_socket(&self) -> PathBuf;
    fn command(&self, binary: &str) -> Command;
}

impl CreatesIsolatedXdg for IsolatedXdg {
    fn create(temporary: &tempfile::TempDir) -> Self {
        let state_home = temporary.path().join("state");
        let runtime_directory = temporary.path().join("runtime");
        std::fs::create_dir_all(&state_home).expect("create state root");
        std::fs::create_dir_all(&runtime_directory).expect("create runtime root");
        Self {
            state_home,
            runtime_directory,
        }
    }

    fn store(&self) -> PathBuf {
        self.state_home
            .join("orchestrate-nexus/orchestrate-nexus.sema")
    }

    fn ordinary_socket(&self) -> PathBuf {
        self.runtime_directory
            .join("orchestrate-nexus/orchestrate.sock")
    }

    fn meta_socket(&self) -> PathBuf {
        self.runtime_directory
            .join("orchestrate-nexus/meta-orchestrate.sock")
    }

    fn command(&self, binary: &str) -> Command {
        let mut command = Command::new(binary);
        command
            .env("XDG_STATE_HOME", &self.state_home)
            .env("XDG_RUNTIME_DIR", &self.runtime_directory)
            .env("HOME", self.state_home.join("home"));
        command
    }
}

struct LiveNexus {
    child: Child,
    roots: IsolatedXdg,
}

trait StartsNexus: Sized {
    fn start(binary: &str, roots: IsolatedXdg) -> Self;
}

impl StartsNexus for LiveNexus {
    fn start(binary: &str, roots: IsolatedXdg) -> Self {
        let mut child = roots
            .command(binary)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start isolated Nexus");
        let stdout = child.stdout.take().expect("capture Nexus stdout");
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut lines = BufReader::new(stdout).lines();
            let _ = sender.send(lines.next().transpose());
        });
        let line = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("Nexus readiness deadline")
            .expect("read Nexus readiness")
            .expect("Nexus exited before readiness");
        assert_eq!(line, "orchestrate-nexus ready");
        Self { child, roots }
    }
}

impl Drop for LiveNexus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One connection to one socket, framed with the shared Signal frame.
struct Connection {
    stream: UnixStream,
}

trait Connects: Sized {
    fn to(socket_path: &Path) -> Self;
    fn ask<Q: Signalizable>(&mut self, query: &Q) -> &mut Self;
    fn hear<R>(&mut self) -> R
    where
        R: rkyv::Archive,
        Signal<R>: Restorable<R>;
}

impl Connects for Connection {
    fn to(socket_path: &Path) -> Self {
        let stream = UnixStream::connect(socket_path).expect("connect Signal socket");
        // Bounded so that a frame the Nexus never sends fails this test
        // rather than hanging the harness.
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("bound the wait for a frame");
        Self { stream }
    }

    fn ask<Q: Signalizable>(&mut self, query: &Q) -> &mut Self {
        let signal = query.signalize().expect("archive query");
        self.stream
            .write_frame(&signal, FrameCapacity::default())
            .expect("write query frame");
        self
    }

    fn hear<R>(&mut self) -> R
    where
        R: rkyv::Archive,
        Signal<R>: Restorable<R>,
    {
        let body = self
            .stream
            .read_frame(FrameCapacity::default())
            .expect("read response frame");
        Signal::<R>::from(Vec::from(body))
            .restore()
            .expect("restore response")
    }
}

trait ExchangesOrdinary {
    fn ordinary(&self, query: &OrdinaryQuery) -> OrdinaryResponse;
}

impl ExchangesOrdinary for LiveNexus {
    fn ordinary(&self, query: &OrdinaryQuery) -> OrdinaryResponse {
        Connection::to(&self.roots.ordinary_socket())
            .ask(query)
            .hear()
    }
}

trait ExchangesMeta {
    fn meta(&self, query: &MetaQuery) -> MetaResponse;
}

impl ExchangesMeta for LiveNexus {
    fn meta(&self, query: &MetaQuery) -> MetaResponse {
        Connection::to(&self.roots.meta_socket()).ask(query).hear()
    }
}

trait DescribesSocket {
    fn mode(&self) -> u32;
}

impl DescribesSocket for Path {
    fn mode(&self) -> u32 {
        std::fs::metadata(self)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777
    }
}

#[test]
fn nexus_serves_both_typed_sockets_and_resumes_state() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let mut nexus = LiveNexus::start(binary, roots);
    let owned = temporary.path().join("owned").display().to_string();
    let locked = nexus.ordinary(&OrdinaryQuery::Lock(LockRequest {
        lock_name: "live-lock".to_owned(),
        flow_id: "test-flow".to_owned(),
        lock_path_vector: vec![owned.clone()],
        lock_reason: "live two socket proof".to_owned(),
    }));
    let OrdinaryResponse::Locked(lock) = locked else {
        panic!("expected Locked response, found {locked:?}");
    };
    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(vec![lock.clone()])),
    );
    let configured = OrchestrateNexusConfiguration {
        ordinary_socket_path: nexus.roots.ordinary_socket().display().to_string(),
        meta_socket_path: nexus.roots.meta_socket().display().to_string(),
    };
    assert_eq!(
        nexus.meta(&MetaQuery::Configure(configured.clone())),
        MetaResponse::Configured(ConfigurationReceipt {
            orchestrate_nexus_configuration: configured,
            meta_configure_done: true,
        }),
    );

    let roots = IsolatedXdg {
        state_home: nexus.roots.state_home.clone(),
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    nexus.child.kill().expect("stop first Nexus");
    nexus.child.wait().expect("reap first Nexus");
    drop(nexus);
    let resumed = LiveNexus::start(binary, roots);
    assert_eq!(
        resumed.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(vec![lock])),
    );
    assert!(resumed.roots.store().exists());
}

#[test]
fn the_privileged_socket_is_bound_for_its_owner_alone() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);
    assert_eq!(
        nexus.roots.meta_socket().mode(),
        0o600,
        "the meta socket is the root of the Nexus and admits only its owner"
    );
    assert_eq!(nexus.roots.ordinary_socket().mode(), 0o660);
}

#[test]
fn observe_delivers_the_state_on_open_and_every_later_change() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);

    let mut subscriber = Connection::to(&nexus.roots.ordinary_socket());
    subscriber.ask(&OrdinaryQuery::Observe(ObserveSelection::Locks));
    assert_eq!(
        subscriber.hear::<OrdinaryResponse>(),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
        "a subscriber receives the state on open"
    );

    let owned = temporary.path().join("owned").display().to_string();
    let OrdinaryResponse::Locked(lock) = nexus.ordinary(&OrdinaryQuery::Lock(LockRequest {
        lock_name: "watched".to_owned(),
        flow_id: "test-flow".to_owned(),
        lock_path_vector: vec![owned],
        lock_reason: "subscription proof".to_owned(),
    })) else {
        panic!("expected a Lock to be acquired");
    };
    assert_eq!(
        subscriber.hear::<OrdinaryResponse>(),
        OrdinaryResponse::Observed(Observation::Locks(vec![lock.clone()])),
        "an acquisition reaches the open subscription without being asked for"
    );

    assert!(matches!(
        nexus.ordinary(&OrdinaryQuery::Release(lock.lock_id)),
        OrdinaryResponse::Released(_)
    ));
    assert_eq!(
        subscriber.hear::<OrdinaryResponse>(),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
        "a release reaches it too"
    );
}

#[test]
fn a_little_endian_prefix_is_not_the_shared_frame() {
    use std::io::{Read, Write};

    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);
    let query = OrdinaryQuery::Observe(ObserveSelection::Locks)
        .signalize()
        .expect("archive a perfectly good query");
    let body = signal::ByteViewable::bytes(&query);

    let mut stream = UnixStream::connect(nexus.roots.ordinary_socket()).expect("connect socket");
    stream
        .write_all(&(body.len() as u32).to_le_bytes())
        .expect("write the prefix Orchestrate used to write");
    stream.write_all(body).expect("write the query");
    stream.flush().expect("flush");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("finish the query");
    let mut response = Vec::new();
    // The Nexus reads the prefix byte-swapped, finds a body far past the
    // frame capacity, and drops the connection: the read ends empty or is
    // reset outright. Either way no Signal frame comes back.
    let read = stream.read_to_end(&mut response);
    assert!(
        response.is_empty(),
        "the Nexus frames big-endian; a little-endian prefix is refused, not answered: {read:?}"
    );

    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
        "and the same query framed by the shared crate is answered"
    );
}

#[test]
fn nexus_rejects_arguments_without_opening_state() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let output = roots
        .command(env!("CARGO_BIN_EXE_orchestrate-nexus"))
        .arg("unexpected")
        .output()
        .expect("run zero-argument Nexus with invalid input");
    assert!(!output.status.success());
    assert!(!roots.store().exists());
}

#[test]
fn malformed_archive_never_reaches_the_store() {
    use std::io::{Read, Write};

    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);
    let mut stream = UnixStream::connect(nexus.roots.ordinary_socket()).expect("connect socket");
    // Hand-written on purpose: the point is a frame the shared crate would
    // never produce, carrying a body that is not a valid archive.
    stream
        .write_all(&1_u32.to_be_bytes())
        .expect("write frame prefix");
    stream.write_all(&[0]).expect("write invalid archive");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("finish invalid query");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read closed socket");
    assert!(response.is_empty());
    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
    );
}

/// Writes, at the isolated store path, exactly the store deployed 0.30.0 and
/// released 0.31.0 leave behind: the configuration in its own family, a Lock
/// and an allocator in the families this generation still uses, and no
/// metadata tree, because none existed. The shape is the one carried by
/// `5f016531:src/store.rs`, the deployed 0.30.0 — the same three family names,
/// schema labels and record fields.
trait WritesAPreviousGenerationStore {
    fn write_previous_generation_store(&self, held: &Lock);
}

impl WritesAPreviousGenerationStore for IsolatedXdg {
    fn write_previous_generation_store(&self, held: &Lock) {
        let store_path = self.store();
        std::fs::create_dir_all(store_path.parent().expect("store path has a parent"))
            .expect("create the state directory");
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))
        .expect("open a previous-generation store");
        let configurations = engine
            .register_table(StoredConfiguration::descriptor())
            .expect("register the previous configuration family");
        engine
            .assert(Assertion::new(
                configurations,
                StoredConfiguration {
                    ordinary_socket: self.ordinary_socket().display().to_string(),
                    meta_socket: self.meta_socket().display().to_string(),
                },
            ))
            .expect("write the previous configuration");
        let locks = engine
            .register_table(StoredLock::descriptor())
            .expect("register the Lock family");
        engine
            .assert(Assertion::new(locks, StoredLock::from_public(held)))
            .expect("write a held Lock");
        let allocator = engine
            .register_table(StoredAllocator::descriptor())
            .expect("register the allocator family");
        engine
            .assert(Assertion::new(
                allocator,
                StoredAllocator {
                    next_lock_id: held.lock_id + 1,
                },
            ))
            .expect("write the allocator");
    }
}

/// The cutover, driven through the sockets rather than through the store API:
/// a Nexus that resumes a previous generation's store does not reopen the
/// ordinary bootstrap window, so no ordinary peer can repoint its sockets.
#[test]
fn a_resumed_previous_generation_store_keeps_ordinary_configure_shut() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let held = Lock {
        lock_id: 7,
        lock_name: "carried".to_owned(),
        flow_id: "flow-857335".to_owned(),
        lock_path_vector: vec![temporary.path().join("held").display().to_string()],
        lock_reason: "held across the cutover".to_owned(),
    };
    roots.write_previous_generation_store(&held);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);

    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(vec![held])),
        "the carried Lock is served by the resumed Nexus"
    );

    let stranding = OrchestrateNexusConfiguration {
        ordinary_socket_path: temporary.path().join("stranded.sock").display().to_string(),
        meta_socket_path: temporary
            .path()
            .join("stranded-meta.sock")
            .display()
            .to_string(),
    };
    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Configure(stranding.clone())),
        OrdinaryResponse::ConfigurationRefused(ConfigurationRejection {
            configuration_rejection_reason: ConfigurationRejectionReason::MetaConfigureOccurred,
        }),
        "an ordinary peer cannot repoint the sockets of a Nexus carried across the cutover"
    );

    let MetaResponse::OrdinaryConfigurationReopened(receipt) =
        nexus.meta(&MetaQuery::ReverseMetaConfiguration)
    else {
        panic!("the privileged socket reopens ordinary Configure");
    };
    assert!(!receipt.meta_configure_done);
    assert_eq!(
        receipt.orchestrate_nexus_configuration.ordinary_socket_path,
        nexus.roots.ordinary_socket().display().to_string(),
        "the carried configuration is what the Nexus is bound by, and the refusal left it alone"
    );
    assert!(
        matches!(
            nexus.ordinary(&OrdinaryQuery::Configure(stranding)),
            OrdinaryResponse::ConfigurationAccepted(_)
        ),
        "and only after the privileged reversal is the ordinary surface open again"
    );
}
