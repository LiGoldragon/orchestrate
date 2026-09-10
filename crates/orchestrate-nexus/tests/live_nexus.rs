//! Live two-socket proof for typed Signal frames and durable state.

use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use meta_signal_orchestrate::{
    ByteViewable as MetaByteViewable, Configure, Query as MetaQuery, Response as MetaResponse,
    Restorable as MetaRestorable, Signal as MetaSignal, Signalizable as MetaSignalizable,
};
use signal_orchestrate::{
    ByteViewable as OrdinaryByteViewable, LockRequest, Observation, ObserveSelection,
    Query as OrdinaryQuery, Response as OrdinaryResponse, Restorable as OrdinaryRestorable,
    Signal as OrdinarySignal, Signalizable as OrdinarySignalizable,
};

const MAXIMUM_SIGNAL_BYTES: usize = 8 * 1024 * 1024;

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

trait ExchangesOrdinary {
    fn ordinary(&self, query: &OrdinaryQuery) -> OrdinaryResponse;
}

impl ExchangesOrdinary for LiveNexus {
    fn ordinary(&self, query: &OrdinaryQuery) -> OrdinaryResponse {
        let signal = query.signalize().expect("archive ordinary query");
        let bytes = self.exchange(&self.roots.ordinary_socket(), signal.bytes());
        OrdinarySignal::<OrdinaryResponse>::from(bytes)
            .restore()
            .expect("restore ordinary response")
    }
}

trait ExchangesMeta {
    fn meta(&self, query: &MetaQuery) -> MetaResponse;
}

impl ExchangesMeta for LiveNexus {
    fn meta(&self, query: &MetaQuery) -> MetaResponse {
        let signal = query.signalize().expect("archive meta query");
        let bytes = self.exchange(&self.roots.meta_socket(), signal.bytes());
        MetaSignal::<MetaResponse>::from(bytes)
            .restore()
            .expect("restore meta response")
    }
}

trait ExchangesBytes {
    fn exchange(&self, socket_path: &Path, payload: &[u8]) -> Vec<u8>;
}

impl ExchangesBytes for LiveNexus {
    fn exchange(&self, socket_path: &Path, payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() <= MAXIMUM_SIGNAL_BYTES);
        let mut stream = UnixStream::connect(socket_path).expect("connect Signal socket");
        let length = u32::try_from(payload.len()).expect("bounded fixture Signal");
        stream
            .write_all(&length.to_le_bytes())
            .expect("write Signal length");
        stream.write_all(payload).expect("write Signal");
        stream.flush().expect("flush Signal");
        let mut prefix = [0; 4];
        stream
            .read_exact(&mut prefix)
            .expect("read response length");
        let response_length = u32::from_le_bytes(prefix) as usize;
        assert!(response_length <= MAXIMUM_SIGNAL_BYTES);
        let mut response = vec![0; response_length];
        stream.read_exact(&mut response).expect("read response");
        response
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
    let configured = Configure {
        ordinary_socket_path: nexus.roots.ordinary_socket().display().to_string(),
        meta_socket_path: nexus.roots.meta_socket().display().to_string(),
    };
    assert_eq!(
        nexus.meta(&MetaQuery::Configure(configured.clone())),
        MetaResponse::Configured(configured),
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
    let temporary = tempfile::tempdir().expect("isolated Nexus directory");
    let roots = IsolatedXdg::create(&temporary);
    let nexus = LiveNexus::start(env!("CARGO_BIN_EXE_orchestrate-nexus"), roots);
    let mut stream = UnixStream::connect(nexus.roots.ordinary_socket()).expect("connect socket");
    stream
        .write_all(&(1_u32).to_le_bytes())
        .expect("write invalid length");
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
