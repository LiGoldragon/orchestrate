use std::{
    fs,
    io::{BufRead, BufReader},
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
        format!("Configure.{{{} {}}}", ordinary.display(), meta.display())
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
        .expect("wait for Orchestrate Nexus ready event");
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
fn zero_argument_startup_initializes_the_default_store_and_rejects_extras() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let nexus_binary = env!("CARGO_BIN_EXE_orchestrate-nexus");

    let mut nexus = start(nexus_binary, &roots);
    wait_until_ready(&mut nexus);
    assert!(
        roots.state_store().is_file(),
        "first start creates the default Sema store"
    );
    assert_eq!(
        reply(
            invoke(
                &roots,
                env!("CARGO_BIN_EXE_meta-orchestrate"),
                "ORCHESTRATE_META_SOCKET",
                &roots.meta_socket(),
                &roots.configure(&roots.ordinary_socket(), &roots.meta_socket()),
            ),
            "meta-orchestrate",
            "default Configure",
        ),
        format!(
            "Configured.{{{} {}}}",
            roots.ordinary_socket().display(),
            roots.meta_socket().display(),
        )
    );
    stop(&mut nexus);

    let extra = roots
        .command(nexus_binary)
        .arg("unexpected")
        .output()
        .expect("run Nexus with extra argument");
    assert!(!extra.status.success(), "Nexus rejects startup arguments");
    assert!(
        String::from_utf8_lossy(&extra.stderr).contains("accepts zero arguments"),
        "extra-argument refusal names the zero-argument boundary"
    );
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
    assert_eq!(
        reply(
            invoke(
                &roots,
                env!("CARGO_BIN_EXE_meta-orchestrate"),
                "ORCHESTRATE_META_SOCKET",
                &roots.meta_socket(),
                &changed_configure,
            ),
            "meta-orchestrate",
            &changed_configure,
        ),
        format!(
            "Configured.{{{} {}}}",
            changed_ordinary.display(),
            changed_meta.display()
        )
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
                &changed_configure,
            ),
            "meta-orchestrate",
            &changed_configure,
        ),
        format!(
            "Configured.{{{} {}}}",
            changed_ordinary.display(),
            changed_meta.display()
        )
    );
    stop(&mut resumed);
}

#[test]
fn ordinary_cli_uses_the_generated_datom_roots_against_a_live_nexus() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let roots = IsolatedXdg::new(&temporary);
    let nexus_binary = env!("CARGO_BIN_EXE_orchestrate-nexus");
    let ordinary_binary = env!("CARGO_BIN_EXE_orchestrate");
    let mut nexus = start(nexus_binary, &roots);
    wait_until_ready(&mut nexus);

    assert_eq!(
        reply(
            invoke(
                &roots,
                ordinary_binary,
                "ORCHESTRATE_SOCKET",
                &roots.ordinary_socket(),
                "Observe.Locks",
            ),
            "orchestrate",
            "Observe.Locks",
        ),
        "Observed(Locks(LockSnapshot { locks: Locks([]) }))"
    );

    let lock_path = temporary.path().join("cli-owned");
    let lock_request = format!(
        "Lock.{{cli-lock 01a03eda [{}] cli-reason}}",
        lock_path.display()
    );
    let locked = reply(
        invoke(
            &roots,
            ordinary_binary,
            "ORCHESTRATE_SOCKET",
            &roots.ordinary_socket(),
            &lock_request,
        ),
        "orchestrate",
        &lock_request,
    );
    assert!(locked.starts_with("Locked(Lock { lock_id: LockId(1),"));

    let released = reply(
        invoke(
            &roots,
            ordinary_binary,
            "ORCHESTRATE_SOCKET",
            &roots.ordinary_socket(),
            "Release.{1}",
        ),
        "orchestrate",
        "Release.{1}",
    );
    assert!(released.starts_with("Released(Lock { lock_id: LockId(1),"));

    let obsolete = invoke(
        &roots,
        ordinary_binary,
        "ORCHESTRATE_SOCKET",
        &roots.ordinary_socket(),
        "Observe.{Locks.{Current}}",
    );
    assert!(
        !obsolete.status.success(),
        "the old nested Observe request must not be accepted"
    );
    stop(&mut nexus);
}
