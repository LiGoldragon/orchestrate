//! The refusing branch of the meta socket's peer check, driven by a peer the
//! kernel reports as a genuinely different user.
//!
//! `transport::session::tests` witnesses the same branch end to end over a
//! real socket by naming the socket's owner, because a test process cannot
//! become a second user by asking. This test closes the remaining step — that
//! the *kernel's* answer for a real second user reaches the rule — using the
//! subordinate user id range the host grants an unprivileged user. Inside a
//! user namespace that maps inner 0 to us and inner 1 to the first
//! subordinate id, `setpriv --reuid=1` yields a process whose credential in
//! the Nexus's own namespace is that subordinate id.
//!
//! ## What this test requires of its host
//!
//! A subordinate uid range for the current user in `/etc/subuid`, the
//! `newuidmap` helper that range is written through, and `unshare` and
//! `setpriv`. A Nix build sandbox has none of these: it runs as one
//! unprivileged user with no subordinate range, and no unprivileged process
//! can invent one. Where the requirement is unmet the test says so and ends;
//! it never reports the refusal it did not see. The `nix flake check` gate
//! therefore covers the named-owner witness, and this test covers the
//! kernel's own translation wherever a developer or host runs it.
//!
//! It also departs from production in one way, which is the only way it can:
//! the meta socket is bound `0600`, so `connect(2)` refuses a second user
//! before the check is ever reached. The test relaxes the socket to `0666`
//! for the one exchange. In production the peer check is the defence against
//! a peer the file mode alone cannot stop.

use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use meta_signal_orchestrate::{PeerRejection, Response as MetaResponse};
use signal::{FrameCapacity, FrameReading, Restorable, Signal};

/// The subordinate user id range a host grants the current user.
struct SubordinateUsers {
    first: u32,
}

/// The range is read from the host, never assumed.
trait Subordinated: Sized {
    fn granted() -> Option<Self>;
    fn first(&self) -> u32;
    fn mapped_command(&self, program: &Path) -> Command;
    fn usable(&self) -> bool;
}

impl Subordinated for SubordinateUsers {
    fn granted() -> Option<Self> {
        let user = Command::new("id").arg("-un").output().ok()?;
        if !user.status.success() {
            return None;
        }
        let user = String::from_utf8(user.stdout).ok()?.trim().to_owned();
        let ranges = std::fs::read_to_string("/etc/subuid").ok()?;
        ranges.lines().find_map(|line| {
            let mut fields = line.split(':');
            (fields.next()? == user).then_some(())?;
            let first = fields.next()?.parse().ok()?;
            let count: u64 = fields.next()?.parse().ok()?;
            (count >= 1).then_some(Self { first })
        })
    }

    fn first(&self) -> u32 {
        self.first
    }

    /// Inner 0 is us, so the namespace can change user at all; inner 1 is the
    /// first subordinate id, which is what the process becomes. Both maps are
    /// written by `newuidmap`, which is what makes the range usable without
    /// privilege.
    fn mapped_command(&self, program: &Path) -> Command {
        let mut command = Command::new("unshare");
        command
            .arg("--map-user=0")
            .arg(format!("--map-users=1:{}:1", self.first))
            .arg("--map-group=0")
            .arg(format!("--map-groups=1:{}:1", self.first))
            .arg("--")
            .arg("setpriv")
            .arg("--reuid=1")
            .arg("--regid=1")
            .arg("--clear-groups")
            .arg(program);
        command
    }

    fn usable(&self) -> bool {
        self.mapped_command(Path::new("/bin/sh"))
            .arg("-c")
            .arg("exit 0")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}

/// One isolated Nexus, reachable by a peer that is not its owner.
struct ReachableNexus {
    child: Child,
    meta_socket: PathBuf,
}

trait StartsReachable: Sized {
    fn start(root: &Path) -> Self;
    fn meta_socket(&self) -> &Path;
}

impl StartsReachable for ReachableNexus {
    fn start(root: &Path) -> Self {
        let state_home = root.join("state");
        let runtime_directory = root.join("runtime");
        for directory in [&state_home, &runtime_directory] {
            std::fs::create_dir_all(directory).expect("create an isolated root");
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_orchestrate-nexus"))
            .env("XDG_STATE_HOME", &state_home)
            .env("XDG_RUNTIME_DIR", &runtime_directory)
            .env("HOME", state_home.join("home"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start an isolated Nexus");
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
        let meta_socket = runtime_directory.join("orchestrate-nexus/orchestrate-meta.sock");
        // The one departure from production, and the reason for it is in this
        // file's header: 0600 refuses the connection before the rule is
        // reached, and the rule is what is under test.
        std::fs::set_permissions(&meta_socket, std::fs::Permissions::from_mode(0o666))
            .expect("relax the meta socket for one exchange");
        std::fs::set_permissions(
            runtime_directory.join("orchestrate-nexus"),
            std::fs::Permissions::from_mode(0o711),
        )
        .expect("let a second user traverse the runtime directory");
        Self { child, meta_socket }
    }

    fn meta_socket(&self) -> &Path {
        &self.meta_socket
    }
}

impl Drop for ReachableNexus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The probe: a copy of this test binary, run as the subordinate user, which
/// connects and writes down the first frame the Nexus sends it. It writes no
/// query: the refusal must arrive unprompted.
const PROBE_SOCKET: &str = "ORCHESTRATE_PEER_PROBE_SOCKET";
const PROBE_FRAME: &str = "ORCHESTRATE_PEER_PROBE_FRAME";
const PROBE_TEST: &str = "a_second_user_probe_records_the_frame_it_is_sent";

#[test]
#[ignore = "the probe half of the second-user witness; run by the test below"]
fn a_second_user_probe_records_the_frame_it_is_sent() {
    let socket = std::env::var(PROBE_SOCKET).expect("the probe is told which socket to open");
    let frame_path = std::env::var(PROBE_FRAME).expect("the probe is told where to write");
    let mut stream =
        std::os::unix::net::UnixStream::connect(socket).expect("connect as a second user");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("bound the wait for a frame");
    let body = stream
        .read_frame(FrameCapacity::default())
        .expect("read the frame sent to an unadmitted peer");
    std::fs::write(frame_path, Vec::from(body)).expect("write the frame down");
}

#[test]
fn a_peer_the_kernel_reports_as_another_user_is_refused_on_the_meta_socket() {
    let Some(subordinate) = SubordinateUsers::granted().filter(Subordinated::usable) else {
        println!(
            "no subordinate user id range is usable here, so no second user exists to refuse: \
             this witness needs /etc/subuid, newuidmap, unshare and setpriv, and a Nix build \
             sandbox has none of them. The named-owner witness in transport::session::tests \
             covers the rule and the wire path; this one covers the kernel's translation."
        );
        return;
    };

    // Under a public root and relaxed, because the probe runs as a user that
    // can traverse neither this test's private directories nor the target
    // directory the binary is built in.
    let temporary = tempfile::tempdir().expect("isolated directory");
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o711))
        .expect("let a second user traverse the isolated root");
    let probe = temporary.path().join("peer-probe");
    std::fs::copy(std::env::current_exe().expect("this test binary"), &probe)
        .expect("copy the probe where a second user can run it");
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755))
        .expect("let a second user run the probe");
    let written = temporary.path().join("written");
    std::fs::create_dir(&written).expect("create the probe's output directory");
    std::fs::set_permissions(&written, std::fs::Permissions::from_mode(0o777))
        .expect("let a second user write its result");
    let frame_path = written.join("frame");

    let nexus = ReachableNexus::start(temporary.path());
    let probe_run = subordinate
        .mapped_command(&probe)
        .args(["--exact", PROBE_TEST, "--ignored", "--nocapture"])
        .env(PROBE_SOCKET, nexus.meta_socket())
        .env(PROBE_FRAME, &frame_path)
        .output()
        .expect("run the probe as the subordinate user");
    assert!(
        probe_run.status.success(),
        "the probe failed: {}{}",
        String::from_utf8_lossy(&probe_run.stdout),
        String::from_utf8_lossy(&probe_run.stderr)
    );

    let frame = std::fs::read(&frame_path).expect("the probe wrote the frame it was sent");
    let response: MetaResponse = Signal::<MetaResponse>::from(frame)
        .restore()
        .expect("restore the frame as the meta contract");
    assert_eq!(
        response,
        MetaResponse::PeerRefused(PeerRejection {
            peer_user_id: i64::from(subordinate.first()),
        }),
        "the kernel's own answer for a second user reaches the rule, and the refusal names that user"
    );
}
