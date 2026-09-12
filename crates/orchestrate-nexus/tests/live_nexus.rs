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
    fn claim(&self, socket: &Path) -> PathBuf;
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
            .join("orchestrate-nexus/orchestrate-meta.sock")
    }

    fn claim(&self, socket: &Path) -> PathBuf {
        let mut claim = socket.as_os_str().to_owned();
        claim.push(".claim");
        PathBuf::from(claim)
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

/// What became of a second Nexus started against a runtime directory, which
/// is what a stray start or a double-enabled unit produces.
struct SecondNexus {
    exited_by_itself: bool,
    said: String,
}

/// Bounded on purpose: a second Nexus that does not exit is precisely the
/// defect these tests are about, and a harness that waited on it forever
/// would hang rather than report. It is stopped by the process id this helper
/// holds, never by a pattern.
trait StartsASecondNexus {
    fn second_nexus(&self, binary: &str) -> SecondNexus;
}

impl StartsASecondNexus for IsolatedXdg {
    fn second_nexus(&self, binary: &str) -> SecondNexus {
        use std::io::Read;

        let mut child = self
            .command(binary)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start a second Nexus");
        let mut stderr = child.stderr.take().expect("capture its stderr");
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut said = String::new();
            let _ = stderr.read_to_string(&mut said);
            let _ = sender.send(said);
        });
        // The pipe closes when the process ends, so this waits on the exit
        // itself; the duration is only the bound on a Nexus that never ends.
        let (exited_by_itself, said) = match receiver.recv_timeout(Duration::from_secs(10)) {
            Ok(said) => (true, said),
            Err(_) => {
                child
                    .kill()
                    .expect("stop the second Nexus this test started");
                (false, receiver.recv().unwrap_or_default())
            }
        };
        child.wait().expect("reap the second Nexus");
        SecondNexus {
            exited_by_itself,
            said,
        }
    }
}

/// The witnessed hazard, driven through the real executable: a second Nexus
/// carrying a *different* store must not take the sockets of the one already
/// serving.
///
/// This is the case the store's own lock cannot see — two stores, one pair of
/// socket paths, which is what a meta Configure pointing a second Nexus at
/// the live paths produces. It is refused by the claim rather than by a
/// probe, so the refusal does not depend on the first Nexus happening to
/// answer a connection at the instant the second one asks.
#[test]
fn a_second_nexus_with_its_own_store_cannot_take_the_serving_nexus_paths() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let nexus = LiveNexus::start(binary, roots);

    let second_state = temporary.path().join("second-state");
    std::fs::create_dir_all(&second_state).expect("create a second state root");
    let stray = IsolatedXdg {
        state_home: second_state,
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    let second = stray.second_nexus(binary);
    assert!(
        second.exited_by_itself,
        "a second Nexus on a held path exits rather than taking it"
    );
    assert!(
        second.said.contains("already owns socket")
            && second
                .said
                .contains(&nexus.roots.ordinary_socket().display().to_string()),
        "and says which path is held: {:?}",
        second.said
    );
    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
        "while the first Nexus is still serving on the same socket"
    );
}

/// The case a connect-probe cannot see, and the reason the probe is gone.
///
/// A probe answers "is anybody listening at this path" by connecting to the
/// file. Remove the file from under a serving Nexus — a tidy-up script, a
/// stale-socket cleaner, a `tmpfiles` rule — and the probe finds nothing,
/// concludes the path is free, and lets a second Nexus bind it while the
/// first is still running on the unlinked inode. Two Nexuses then each
/// believe they own the Lock table.
///
/// A claim is held on a file beside the socket rather than on the socket, and
/// is answered by the kernel rather than by a connection, so removing the
/// socket tells it nothing.
#[test]
fn removing_the_socket_file_under_a_serving_nexus_does_not_free_its_path() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let nexus = LiveNexus::start(binary, roots);

    std::fs::remove_file(nexus.roots.ordinary_socket()).expect("remove the socket file");
    std::fs::remove_file(nexus.roots.meta_socket()).expect("remove the meta socket file");

    let second_state = temporary.path().join("second-state");
    std::fs::create_dir_all(&second_state).expect("create a second state root");
    let stray = IsolatedXdg {
        state_home: second_state,
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    let second = stray.second_nexus(binary);
    assert!(
        second.exited_by_itself,
        "the path is still held by the Nexus serving on it, socket file or no \
         socket file"
    );
    assert!(
        second.said.contains("already owns socket"),
        "and is refused by name: {:?}",
        second.said
    );
}

/// A socket file left behind by a Nexus that is gone is taken, because no
/// claim is held on it — the case the old connect-probe also handled, kept
/// as a witness that the new rule did not close it.
#[test]
fn a_nexus_takes_the_socket_files_a_dead_nexus_left_behind() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let mut nexus = LiveNexus::start(binary, roots);
    nexus.child.kill().expect("stop the first Nexus");
    nexus.child.wait().expect("reap the first Nexus");
    let roots = IsolatedXdg {
        state_home: nexus.roots.state_home.clone(),
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    drop(nexus);
    assert!(
        roots.ordinary_socket().exists(),
        "a killed Nexus leaves its socket files behind, which is the state a \
         restart finds"
    );

    let resumed = LiveNexus::start(binary, roots);
    assert_eq!(
        resumed.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
    );
    assert!(
        resumed
            .roots
            .claim(&resumed.roots.ordinary_socket())
            .exists(),
        "and holds the claim beside it"
    );
}

/// The witnessed incident: a copy of a real store, opened by the same user on
/// the same machine, carries the production socket paths and would bind them.
#[test]
fn a_nexus_refuses_to_serve_a_store_carried_from_somewhere_else() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let original = IsolatedXdg::create(&temporary);
    let original_store = original.store();
    let nexus = LiveNexus::start(binary, original);
    drop(nexus);

    let carried_root = temporary.path().join("carried");
    std::fs::create_dir_all(&carried_root).expect("create the second root");
    let carried = IsolatedXdg {
        state_home: carried_root.join("state"),
        runtime_directory: carried_root.join("runtime"),
    };
    std::fs::create_dir_all(carried.store().parent().expect("store has a parent"))
        .expect("create the carried state directory");
    std::fs::create_dir_all(&carried.runtime_directory).expect("create the carried runtime root");
    std::fs::copy(&original_store, carried.store()).expect("copy the store the way a backup does");

    let second = carried.second_nexus(binary);
    assert!(
        second.exited_by_itself,
        "a Nexus opening a carried store exits rather than binding the socket \
         paths it carries"
    );
    assert!(
        second.said.contains(&original_store.display().to_string())
            && second.said.contains(&carried.store().display().to_string()),
        "and names where the store says it lives and where it was opened: {:?}",
        second.said
    );
}

/// A Nexus asked to go by its service manager goes, and leaves both paths and
/// its store to whatever starts next.
#[test]
fn a_terminated_nexus_stops_cleanly_and_the_next_one_starts_at_once() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let mut nexus = LiveNexus::start(binary, roots);
    let owned = temporary.path().join("owned").display().to_string();
    let OrdinaryResponse::Locked(held) = nexus.ordinary(&OrdinaryQuery::Lock(LockRequest {
        lock_name: "across-the-stop".to_owned(),
        flow_id: "test-flow".to_owned(),
        lock_path_vector: vec![owned],
        lock_reason: "held across a termination".to_owned(),
    })) else {
        panic!("expected a Lock to be acquired");
    };

    // By the PID this test holds, never by a pattern: a scratch Nexus and a
    // real one are the same executable.
    assert!(
        nexus.terminate(),
        "SIGTERM was delivered to the Nexus this test started"
    );
    let status = nexus.child.wait().expect("reap the terminated Nexus");
    assert!(
        status.success(),
        "a Nexus asked to stop exits successfully, found {status:?}"
    );

    let roots = IsolatedXdg {
        state_home: nexus.roots.state_home.clone(),
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    drop(nexus);
    let resumed = LiveNexus::start(binary, roots);
    assert_eq!(
        resumed.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(vec![held])),
        "and the next Nexus takes the same paths and serves the same state"
    );
}

/// Asking one known process to stop the way a service manager does.
trait Terminable {
    fn terminate(&self) -> bool;
}

impl Terminable for LiveNexus {
    /// By the process id this test's `Child` holds, never by a name or path
    /// pattern: a scratch Nexus and a deployed one are the same executable,
    /// and a pattern would reach both.
    fn terminate(&self) -> bool {
        Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("ask the kernel to terminate one process id")
            .success()
    }
}

// ---------------------------------------------------------------------------
// A store that was moved, driven end to end through the real executables.
//
// The refusal a carried store meets is correct and, on its own, a dead end:
// the record that would have to change lives inside the store, and the meta
// socket a client would ask through is named by that same store. So the way
// out is a second executable that opens the store file with no Nexus running
// — `orchestrate-relocate`. These tests are the whole ritual: what it refuses,
// what it admits, and what the admission does not extend to.
// ---------------------------------------------------------------------------

/// A second pair of XDG roots beside the first, which is what relocating a
/// store to a different state directory looks like from outside.
trait Beside {
    fn beside(temporary: &tempfile::TempDir, name: &str) -> Self;
}

impl Beside for IsolatedXdg {
    fn beside(temporary: &tempfile::TempDir, name: &str) -> Self {
        let root = temporary.path().join(name);
        let roots = Self {
            state_home: root.join("state"),
            runtime_directory: root.join("runtime"),
        };
        std::fs::create_dir_all(roots.store().parent().expect("the store has a parent"))
            .expect("create the second state directory");
        std::fs::create_dir_all(&roots.runtime_directory).expect("create the second runtime root");
        roots
    }
}

/// What `orchestrate-relocate` said, and whether it declared anything.
struct Declaration {
    declared: bool,
    said: String,
}

/// Running the operator's declaration in one pair of roots.
trait Declares {
    fn relocate(&self) -> Declaration;
}

impl Declares for IsolatedXdg {
    fn relocate(&self) -> Declaration {
        let finished = self
            .command(env!("CARGO_BIN_EXE_orchestrate-relocate"))
            .output()
            .expect("run the relocation declaration");
        Declaration {
            declared: finished.status.success(),
            said: format!(
                "{}{}",
                String::from_utf8_lossy(&finished.stdout),
                String::from_utf8_lossy(&finished.stderr)
            ),
        }
    }
}

/// Carrying a store's bytes without its file, which is what a cross-filesystem
/// `mv`, a `tar`, and every snapshot restore do — and the only case that needs
/// declaring, since a move that keeps the file is recognised unaided.
trait Carries {
    fn carry_store_to(&self, destination: &IsolatedXdg);
}

impl Carries for IsolatedXdg {
    fn carry_store_to(&self, destination: &IsolatedXdg) {
        std::fs::create_dir_all(
            destination
                .store()
                .parent()
                .expect("the store has a parent"),
        )
        .expect("create the destination state directory");
        std::fs::copy(self.store(), destination.store()).expect("carry the bytes across");
        std::fs::remove_file(self.store()).expect("and leave nothing at the origin");
    }
}

#[test]
fn a_copied_store_is_refused_and_will_not_be_declared_a_move() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let original = IsolatedXdg::create(&temporary);
    let original_store = original.store();
    drop(LiveNexus::start(binary, original));

    let second = IsolatedXdg::beside(&temporary, "copy");
    std::fs::copy(&original_store, second.store()).expect("copy the store, leaving the original");

    let refused = second.relocate();
    assert!(
        !refused.declared,
        "a copy is not a move, and the original is still there to prove it: {:?}",
        refused.said
    );
    assert!(
        refused.said.contains(&original_store.display().to_string()),
        "and the refusal names what the operator has to deal with first: {:?}",
        refused.said
    );

    let started = second.second_nexus(binary);
    assert!(
        started.exited_by_itself,
        "so the copy is still refused, as it was before anything was asked"
    );
    assert!(
        started.said.contains(&original_store.display().to_string()),
        "found {:?}",
        started.said
    );
}

#[test]
fn a_relocation_is_refused_while_the_nexus_is_still_serving() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let original = IsolatedXdg::create(&temporary);
    let original_store = original.store();
    let nexus = LiveNexus::start(binary, original);

    // The store is unlinked from under a Nexus that goes on serving from the
    // open file, which is exactly the case the origin check cannot see. The
    // socket claim can, and is the reason the tool takes it.
    let second = IsolatedXdg::beside(&temporary, "moved");
    std::fs::copy(&original_store, second.store()).expect("carry the bytes across");
    std::fs::remove_file(&original_store).expect("and unlink the original");

    let refused = second.relocate();
    assert!(
        !refused.declared,
        "something is still serving the sockets this store would bind: {:?}",
        refused.said
    );
    assert!(
        refused
            .said
            .contains(&nexus.roots.ordinary_socket().display().to_string())
            || refused
                .said
                .contains(&nexus.roots.meta_socket().display().to_string()),
        "and it names the socket that is held: {:?}",
        refused.said
    );
    assert_eq!(
        nexus.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(Vec::new())),
        "and the Nexus it refused to displace is still answering"
    );
}

#[test]
fn a_carried_store_serves_again_with_its_state_once_the_move_is_declared() {
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let original = IsolatedXdg::create(&temporary);
    let mut nexus = LiveNexus::start(binary, original);
    let owned = temporary.path().join("owned").display().to_string();
    let OrdinaryResponse::Locked(held) = nexus.ordinary(&OrdinaryQuery::Lock(LockRequest {
        lock_name: "across-the-move".to_owned(),
        flow_id: "test-flow".to_owned(),
        lock_path_vector: vec![owned],
        lock_reason: "held across a relocation".to_owned(),
    })) else {
        panic!("expected a Lock to be acquired");
    };
    let original_roots = IsolatedXdg {
        state_home: nexus.roots.state_home.clone(),
        runtime_directory: nexus.roots.runtime_directory.clone(),
    };
    assert!(nexus.terminate(), "SIGTERM delivered to this test's Nexus");
    let status = nexus.child.wait().expect("reap the terminated Nexus");
    assert!(
        status.success(),
        "a Nexus asked to stop exits successfully, found {status:?}"
    );

    // The state directory moves and the runtime directory does not, which is
    // the shape a relocation actually has: the socket paths live in the
    // store's metadata tree, so a relocated Nexus goes on listening exactly
    // where it listened before. Only the store has gone somewhere else.
    let moved = IsolatedXdg {
        state_home: IsolatedXdg::beside(&temporary, "relocated").state_home,
        runtime_directory: original_roots.runtime_directory.clone(),
    };
    original_roots.carry_store_to(&moved);

    // Without the declaration, this is a copy as far as anything can tell.
    let undeclared = moved.second_nexus(binary);
    assert!(
        undeclared.exited_by_itself,
        "an absent origin is evidence, not consent: {:?}",
        undeclared.said
    );

    let declared = moved.relocate();
    assert!(
        declared.declared,
        "the original is gone and nothing holds the sockets: {:?}",
        declared.said
    );

    let relocated = LiveNexus::start(binary, moved);
    assert_eq!(
        relocated.ordinary(&OrdinaryQuery::Observe(ObserveSelection::Locks)),
        OrdinaryResponse::Observed(Observation::Locks(vec![held])),
        "and it serves the state it was carrying all along"
    );

    // The move is complete, so the declaration that admitted it is spent. A
    // copy taken from the relocated store finds no standing licence, and a
    // second relocation needs a second declaration.
    let relocated_store = relocated.roots.store();
    let third = IsolatedXdg {
        state_home: IsolatedXdg::beside(&temporary, "third").state_home,
        runtime_directory: relocated.roots.runtime_directory.clone(),
    };
    std::fs::create_dir_all(third.store().parent().expect("the store has a parent"))
        .expect("create the third state directory");
    std::fs::copy(&relocated_store, third.store()).expect("copy the relocated store");
    let forged = third.second_nexus(binary);
    assert!(
        forged.exited_by_itself,
        "a declaration admits one move, not a class of them: {:?}",
        forged.said
    );
    assert!(
        forged.said.contains(&relocated_store.display().to_string())
            && forged.said.contains(&third.store().display().to_string()),
        "and it is refused as the copy it is, naming the address the record \
         holds and the one it was opened at, rather than merely failing to \
         bind: {:?}",
        forged.said
    );
    assert!(
        !third.relocate().declared,
        "nor can it be declared, because the store it was copied from is \
         still there"
    );
}
