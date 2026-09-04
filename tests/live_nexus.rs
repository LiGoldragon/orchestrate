//! Live two-socket proof for generated roots, Datomic edges, and persistence.

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
};

struct IsolatedXdg {
    state_home: PathBuf,
    runtime_directory: PathBuf,
}

impl IsolatedXdg {
    fn new(temporary: &tempfile::TempDir) -> Self {
        let state_home = temporary.path().join("state");
        let runtime_directory = temporary.path().join("runtime");
        fs::create_dir_all(&state_home).expect("create isolated XDG state root");
        fs::create_dir_all(&runtime_directory).expect("create isolated XDG runtime root");
        Self {
            state_home,
            runtime_directory,
        }
    }

    fn state_store(&self) -> PathBuf {
        self.state_home
            .join("orchestrate-nexus")
            .join("orchestrate-nexus.sema")
    }

    fn socket_directory(&self) -> PathBuf {
        self.runtime_directory.join("orchestrate-nexus")
    }

    fn ordinary_socket(&self) -> PathBuf {
        self.socket_directory().join("orchestrate.sock")
    }

    fn meta_socket(&self) -> PathBuf {
        self.socket_directory().join("meta-orchestrate.sock")
    }

    fn configure(&self, ordinary: &Path, meta: &Path) -> String {
        format!("Configure.{{ {} {} }}", ordinary.display(), meta.display())
    }

    fn command(&self, binary: &str) -> Command {
        let mut command = Command::new(binary);
        command
            .env("XDG_STATE_HOME", &self.state_home)
            .env("XDG_RUNTIME_DIR", &self.runtime_directory);
        command
    }
}

fn start(nexus: &str, roots: &IsolatedXdg) -> Child {
    roots
        .command(nexus)
        .stdout(Stdio::piped())
        .spawn()
        .expect("start zero-argument Orchestrate Nexus")
}

fn wait_until_ready(nexus: &mut Child) {
    let stdout = nexus.stdout.take().expect("Orchestrate Nexus stdout");
    let mut stdout = BufReader::new(stdout);
    let mut ready = String::new();
    stdout
        .read_line(&mut ready)
        .expect("wait for Nexus ready event");
    assert_eq!(ready, "orchestrate-nexus ready\n");
}

fn invoke(
    roots: &IsolatedXdg,
    binary: &str,
    socket_variable: &str,
    socket: &Path,
    request: &str,
) -> Output {
    roots
        .command(binary)
        .env(socket_variable, socket)
        .arg(request)
        .output()
        .expect("run client")
}

fn reply(output: Output, binary: &str, request: &str) -> String {
    assert!(
        output.status.success(),
        "{binary} rejected {request:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("client reply is UTF-8")
        .trim()
        .to_owned()
}

fn stop(nexus: &mut Child) {
    nexus.kill().expect("stop Orchestrate Nexus");
    nexus.wait().expect("reap Orchestrate Nexus");
}

#[test]
fn zero_argument_startup_initializes_default_store_and_rejects_extras() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let nexus_binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let mut nexus = start(nexus_binary, &roots);
    wait_until_ready(&mut nexus);
    assert!(
        roots.state_store().is_file(),
        "first start creates the default Sema store"
    );
    let default_configure = roots.configure(&roots.ordinary_socket(), &roots.meta_socket());
    assert_eq!(
        reply(
            invoke(
                &roots,
                env!("CARGO_BIN_EXE_meta-orchestrate"),
                "ORCHESTRATE_META_SOCKET",
                &roots.meta_socket(),
                &default_configure
            ),
            "meta-orchestrate",
            "default Configure",
        ),
        format!(
            "Configured.{{ {} {} }}",
            roots.ordinary_socket().display(),
            roots.meta_socket().display()
        )
    );
    stop(&mut nexus);

    let extra = roots
        .command(nexus_binary)
        .arg("unexpected")
        .output()
        .expect("run Nexus with extra argument");
    assert!(!extra.status.success(), "Nexus rejects startup arguments");
    assert!(String::from_utf8_lossy(&extra.stderr).contains("accepts zero arguments"));
}

#[test]
fn meta_configuration_persists_and_a_restart_resumes_it() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let nexus_binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let mut nexus = start(nexus_binary, &roots);
    wait_until_ready(&mut nexus);

    let changed_ordinary = roots.socket_directory().join("changed-ordinary.sock");
    let changed_meta = roots.socket_directory().join("changed-meta.sock");
    let changed_configure = roots.configure(&changed_ordinary, &changed_meta);
    let expected = format!(
        "Configured.{{ {} {} }}",
        changed_ordinary.display(),
        changed_meta.display()
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                env!("CARGO_BIN_EXE_meta-orchestrate"),
                "ORCHESTRATE_META_SOCKET",
                &roots.meta_socket(),
                &changed_configure
            ),
            "meta-orchestrate",
            &changed_configure,
        ),
        expected,
    );
    stop(&mut nexus);

    let mut resumed = start(nexus_binary, &roots);
    wait_until_ready(&mut resumed);
    assert_eq!(
        reply(
            invoke(
                &roots,
                env!("CARGO_BIN_EXE_meta-orchestrate"),
                "ORCHESTRATE_META_SOCKET",
                &changed_meta,
                &changed_configure
            ),
            "meta-orchestrate",
            &changed_configure,
        ),
        format!(
            "Configured.{{ {} {} }}",
            changed_ordinary.display(),
            changed_meta.display()
        ),
    );
    stop(&mut resumed);
}

#[test]
fn ordinary_cli_uses_datomic_request_reply_and_refusal_roots_against_a_live_nexus() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let ordinary_binary = env!("CARGO_BIN_EXE_orchestrate");
    let mut nexus = start(env!("CARGO_BIN_EXE_orchestrate-nexus"), &roots);
    wait_until_ready(&mut nexus);

    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                "Observe.Locks"
            ),
            "orchestrate",
            "Observe.Locks"
        ),
        "Observed.Locks.[]",
    );
    let lock_path = temporary.path().join("cli-owned");
    let lock_request = format!(
        "Lock.{{ cli-lock 01a03eda [ {} ] cli-reason }}",
        lock_path.display()
    );
    let locked = format!(
        "Locked.{{ 1 cli-lock 01a03eda [ {} ] cli-reason }}",
        lock_path.display()
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                &lock_request
            ),
            "orchestrate",
            &lock_request
        ),
        locked,
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                &lock_request
            ),
            "orchestrate",
            &lock_request
        ),
        format!(
            "LockRejected.DuplicateName.{{ 1 cli-lock 01a03eda [ {} ] cli-reason }}",
            lock_path.display()
        ),
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                "Release.1"
            ),
            "orchestrate",
            "Release.1"
        ),
        format!(
            "Released.{{ 1 cli-lock 01a03eda [ {} ] cli-reason }}",
            lock_path.display()
        ),
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                "Release.1"
            ),
            "orchestrate",
            "Release.1"
        ),
        "ReleaseRejected.UnknownLockId",
    );
    let obsolete = invoke(
        &roots,
        ordinary_binary,
        "ORCHESTRATE_SOCKET",
        &roots.ordinary_socket(),
        "Observe.{Locks.{Current}}",
    );
    assert!(
        !obsolete.status.success(),
        "the old nested Observe request is not accepted"
    );
    stop(&mut nexus);
}

#[test]
fn no_argument_prints_ethos_source() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let ordinary_binary = env!("CARGO_BIN_EXE_orchestrate");
    let no_arg = roots
        .command(ordinary_binary)
        .env("ORCHESTRATE_SOCKET", roots.ordinary_socket())
        .output()
        .expect("no-arg run");
    assert!(no_arg.status.success());
    let stdout = String::from_utf8(no_arg.stdout).unwrap();
    assert!(
        stdout.contains("Signal.{ 1 0 0 }"),
        "ethos source should contain Signal version"
    );
}

#[test]
fn invalid_request_fails_on_stderr() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let ordinary_binary = env!("CARGO_BIN_EXE_orchestrate");
    let mut nexus = start(env!("CARGO_BIN_EXE_orchestrate-nexus"), &roots);
    wait_until_ready(&mut nexus);
    let bad = invoke(
        &roots,
        ordinary_binary,
        "ORCHESTRATE_SOCKET",
        &roots.ordinary_socket(),
        "NotARequest.{ bad }",
    );
    assert!(!bad.status.success());
    let stderr = String::from_utf8(bad.stderr).unwrap();
    assert!(!stderr.is_empty());
    stop(&mut nexus);
}

#[test]
fn malformed_frame_is_refused_before_it_reaches_the_store() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let mut nexus = start(env!("CARGO_BIN_EXE_orchestrate-nexus"), &roots);
    wait_until_ready(&mut nexus);
    let mut socket = UnixStream::connect(roots.ordinary_socket()).expect("connect ordinary socket");
    socket
        .write_all(&[1, 0, 0, 0, 0])
        .expect("write malformed envelope");
    socket
        .shutdown(std::net::Shutdown::Write)
        .expect("finish malformed envelope");
    let mut reply = Vec::new();
    socket
        .read_to_end(&mut reply)
        .expect("read malformed response");
    assert!(
        reply.is_empty(),
        "invalid envelope gets no typed store response"
    );
    stop(&mut nexus);
}
