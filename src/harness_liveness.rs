//! Kernel exit-push liveness for registered agents' harness processes.
//!
//! Layer 1 of the liveliness design (`coordination-liveliness-messenger`
//! design §3, psyche-ruled 2026-07-17): the daemon watches each registered
//! agent's harness process through a `pidfd` and, when the kernel reports the
//! process exited, the owning agent is marked with the typed
//! `OrchestratorAgentStatus::Dead` — no longer indistinguishable from idle.
//! Push, not poll: the kernel makes the pidfd readable on exit; nothing scans
//! on an interval.
//!
//! Two cooperating pieces, mirroring the lane-reclamation split:
//!
//! - [`HarnessLivenessWatch`] is the daemon-lifecycle IO worker. It holds the
//!   pidfds and blocks in `poll(2)` until the kernel pushes an exit (or the
//!   engine pushes a new watch set). It has no store access: an exit re-enters
//!   the daemon through the ordinary Signal path, exactly like the
//!   `LaneReclaimer` deadline worker.
//! - [`HarnessLivenessReconciliation`] is the engine-side truth read, run at
//!   the head of every ordinary turn: for each `Active` agent with discovered
//!   reachability it reads `/proc/<pid>/stat` and compares the start time
//!   against the stored generation pin. A missing process or a recycled pid
//!   (same pid, different start time) means the watched generation is gone and
//!   the agent is marked `Dead`. The watcher only wakes the turn; the store
//!   transition is always derived from `/proc` truth by the single durable
//!   -state writer, so a spurious or stale wake can never kill a live agent.
//!
//! The recycled-pid pin: terminal-cell records only the pid, so reachability
//! discovery captured the process start time (`/proc/<pid>/stat` field 22)
//! alongside it. Both the watcher (before trusting a freshly opened pidfd) and
//! the reconciliation (before marking dead) compare against that pin, so a pid
//! reused by an unrelated process is never mistaken for the agent's harness —
//! in either direction.

use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread;

use rustix::event::{EventfdFlags, PollFd, PollFlags, eventfd};
use rustix::process::{Pid, PidfdFlags, pidfd_open};
use signal_orchestrate::OrchestratorAgentStatus;
use signal_orchestrate::schema::lib::{Input, Observation};

use crate::agent_reachability::ProcessStat;
use crate::{OrchestrateTables, OrdinarySignalTransport, Result, StoredAgentReachability};

/// The default `/proc` mount liveness truth is read from. Configurable so a
/// test can point the read at a fixture `/proc`-shaped tree.
const DEFAULT_PROCESS_ROOT: &str = "/proc";

impl StoredAgentReachability {
    /// Whether the exact harness process generation this reachability pinned —
    /// the pid AND its recorded start time — is still alive under
    /// `process_root`. A missing `stat` record means the process is gone; a
    /// present record with a different start time means the pid was recycled
    /// by an unrelated process, so the pinned generation is equally gone.
    pub fn process_generation_alive(&self, process_root: &Path) -> bool {
        match ProcessStat::read(process_root, self.harness_pid) {
            Some(stat) => stat.start_time_ticks == self.harness_start_time,
            None => false,
        }
    }
}

/// The engine-side liveness truth read, run at the head of every ordinary
/// turn beside the bounded-table reaper. Marks `Dead` every `Active` agent
/// whose pinned harness process generation no longer exists.
pub struct HarnessLivenessReconciliation {
    process_root: PathBuf,
}

impl HarnessLivenessReconciliation {
    pub fn new(process_root: impl Into<PathBuf>) -> Self {
        Self {
            process_root: process_root.into(),
        }
    }

    /// The reconciliation bound to the live host's real `/proc`.
    pub fn from_process_environment() -> Self {
        Self::new(DEFAULT_PROCESS_ROOT)
    }

    /// Mark `Dead` every `Active` agent with discovered reachability whose
    /// process generation is gone, returning how many were marked. Agents
    /// without reachability have no pid to read and stay on the idle-age
    /// backstop (the activity-read layer covers them separately).
    pub fn reconcile(&self, tables: &OrchestrateTables) -> Result<u32> {
        let mut marked = 0;
        for agent in tables.orchestrator_agent_records()? {
            if agent.status != OrchestratorAgentStatus::Active {
                continue;
            }
            let Some(reachability) = &agent.reachability else {
                continue;
            };
            if !reachability.process_generation_alive(&self.process_root)
                && tables
                    .mark_orchestrator_agent_dead(&agent.agent_identifier)?
                    .is_some()
            {
                marked += 1;
            }
        }
        Ok(marked)
    }
}

/// One harness process generation the watcher keeps a pidfd on: the pid and
/// the start-time pin that distinguishes it from a later recycled pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchedHarnessProcess {
    pub pid: u32,
    pub start_time_ticks: u64,
}

impl WatchedHarnessProcess {
    /// The desired watch set derived from durable state: every `Active` agent's
    /// discovered reachability pin, deduplicated (several agents may share one
    /// harness process).
    pub fn desired_set(tables: &OrchestrateTables) -> Result<Vec<Self>> {
        let mut wanted: Vec<Self> = Vec::new();
        for agent in tables.orchestrator_agent_records()? {
            if agent.status != OrchestratorAgentStatus::Active {
                continue;
            }
            let Some(reachability) = &agent.reachability else {
                continue;
            };
            let watched = Self {
                pid: reachability.harness_pid,
                start_time_ticks: reachability.harness_start_time,
            };
            if !wanted.contains(&watched) {
                wanted.push(watched);
            }
        }
        Ok(wanted)
    }
}

enum HarnessLivenessSignal {
    Reconcile(Vec<WatchedHarnessProcess>),
    Shutdown,
}

/// The daemon-lifecycle exit watcher. The engine pushes the desired watch set
/// after every ordinary turn; the worker blocks in `poll(2)` over its pidfds
/// and an eventfd control wake. A kernel exit push re-enters the daemon
/// through the normal ordinary Signal path (the `LaneReclaimer` pattern),
/// where the truth read marks the owning agent dead. The worker itself never
/// touches the store.
pub struct HarnessLivenessWatch {
    sender: Sender<HarnessLivenessSignal>,
    wake: OwnedFd,
}

impl HarnessLivenessWatch {
    pub fn spawn(socket_path: PathBuf, process_root: impl Into<PathBuf>) -> Result<Self> {
        let (sender, receiver) = channel();
        let wake = eventfd(0, EventfdFlags::CLOEXEC)
            .map_err(|errno| crate::Error::Io(errno.into()))?;
        let worker_wake = wake.try_clone().map_err(crate::Error::from)?;
        let process_root = process_root.into();
        thread::Builder::new()
            .name("orchestrate-harness-liveness-watch".to_string())
            .spawn(move || {
                HarnessLivenessWatchWorker::new(socket_path, process_root, receiver, worker_wake)
                    .run();
            })
            .map_err(crate::Error::from)?;
        Ok(Self { sender, wake })
    }

    /// Push the desired watch set. The worker opens pidfds for newly watched
    /// generations, drops fds no longer wanted, and immediately reports any
    /// wanted generation that is already gone.
    pub fn reconcile(&self, watched: Vec<WatchedHarnessProcess>) {
        if self
            .sender
            .send(HarnessLivenessSignal::Reconcile(watched))
            .is_ok()
        {
            self.wake_worker();
        }
    }

    fn wake_worker(&self) {
        let _ = rustix::io::write(&self.wake, &1u64.to_ne_bytes());
    }
}

impl Drop for HarnessLivenessWatch {
    fn drop(&mut self) {
        let _ = self.sender.send(HarnessLivenessSignal::Shutdown);
        self.wake_worker();
    }
}

/// One pidfd the worker holds open, tagged with the generation pin it was
/// verified against when opened.
struct WatchedProcessDescriptor {
    watched: WatchedHarnessProcess,
    pidfd: OwnedFd,
}

/// What opening a watch on one wanted generation produced: a live pidfd, or
/// the discovery that the generation is already gone (exited before the watch
/// opened, or its pid recycled by an unrelated process).
enum WatchOpening {
    Watching(WatchedProcessDescriptor),
    GenerationGone,
}

impl WatchedHarnessProcess {
    /// Open a pidfd on this generation and verify the pin. The pidfd is opened
    /// first and the pin checked after, so a recycle between check and open
    /// cannot slip through: whatever process the fd actually tracks, the pin
    /// read decides whether it is the wanted generation.
    fn open_watch(self, process_root: &Path) -> WatchOpening {
        let Some(pid) = Pid::from_raw(self.pid as i32) else {
            return WatchOpening::GenerationGone;
        };
        let Ok(pidfd) = pidfd_open(pid, PidfdFlags::empty()) else {
            return WatchOpening::GenerationGone;
        };
        match ProcessStat::read(process_root, self.pid) {
            Some(stat) if stat.start_time_ticks == self.start_time_ticks => {
                WatchOpening::Watching(WatchedProcessDescriptor {
                    watched: self,
                    pidfd,
                })
            }
            // Present but a different start time: the pid was recycled and the
            // fd tracks a stranger — the wanted generation is gone.
            Some(_) => WatchOpening::GenerationGone,
            // Vanished between open and read: the fd tracks the (now exited)
            // wanted process; keep it, poll reports it readable immediately.
            None => WatchOpening::Watching(WatchedProcessDescriptor {
                watched: self,
                pidfd,
            }),
        }
    }
}

struct HarnessLivenessWatchWorker {
    socket_path: PathBuf,
    process_root: PathBuf,
    receiver: Receiver<HarnessLivenessSignal>,
    wake: OwnedFd,
    watched: Vec<WatchedProcessDescriptor>,
}

impl HarnessLivenessWatchWorker {
    fn new(
        socket_path: PathBuf,
        process_root: PathBuf,
        receiver: Receiver<HarnessLivenessSignal>,
        wake: OwnedFd,
    ) -> Self {
        Self {
            socket_path,
            process_root,
            receiver,
            wake,
            watched: Vec::new(),
        }
    }

    fn run(mut self) {
        loop {
            match self.drain_control_signals() {
                ControlOutcome::Continue => {}
                ControlOutcome::Shutdown => return,
            }
            let (wake_fired, exited) = self.poll_once();
            if wake_fired {
                self.clear_wake();
            }
            if !exited.is_empty() {
                self.watched
                    .retain(|descriptor| !exited.contains(&descriptor.watched));
                // One re-entry covers every exit observed in this poll: the
                // turn's truth read marks all gone generations' agents dead.
                self.submit_exit_event();
            }
        }
    }

    /// Block in `poll(2)` until the control eventfd or any pidfd is readable.
    /// Returns whether the control wake fired and which watched generations'
    /// pidfds reported their process exited.
    fn poll_once(&mut self) -> (bool, Vec<WatchedHarnessProcess>) {
        let mut poll_fds = Vec::with_capacity(1 + self.watched.len());
        poll_fds.push(PollFd::new(&self.wake, PollFlags::IN));
        for descriptor in &self.watched {
            poll_fds.push(PollFd::new(&descriptor.pidfd, PollFlags::IN));
        }
        if rustix::event::poll(&mut poll_fds, None).is_err() {
            // EINTR or transient poll failure: report nothing; the loop
            // re-enters and polls again.
            return (false, Vec::new());
        }
        let wake_fired = !poll_fds[0].revents().is_empty();
        let exited = self
            .watched
            .iter()
            .zip(poll_fds[1..].iter())
            .filter(|(_, poll_fd)| !poll_fd.revents().is_empty())
            .map(|(descriptor, _)| descriptor.watched)
            .collect();
        (wake_fired, exited)
    }

    fn drain_control_signals(&mut self) -> ControlOutcome {
        loop {
            match self.receiver.try_recv() {
                Ok(HarnessLivenessSignal::Reconcile(wanted)) => self.apply_watch_set(wanted),
                Ok(HarnessLivenessSignal::Shutdown) | Err(TryRecvError::Disconnected) => {
                    return ControlOutcome::Shutdown;
                }
                Err(TryRecvError::Empty) => return ControlOutcome::Continue,
            }
        }
    }

    fn apply_watch_set(&mut self, wanted: Vec<WatchedHarnessProcess>) {
        self.watched
            .retain(|descriptor| wanted.contains(&descriptor.watched));
        let mut generation_gone = false;
        for watch in wanted {
            if self
                .watched
                .iter()
                .any(|descriptor| descriptor.watched == watch)
            {
                continue;
            }
            match watch.open_watch(&self.process_root) {
                WatchOpening::Watching(descriptor) => self.watched.push(descriptor),
                WatchOpening::GenerationGone => generation_gone = true,
            }
        }
        if generation_gone {
            // A wanted generation was gone before its watch opened; wake the
            // engine so the truth read marks its agent dead now rather than at
            // the idle backstop.
            self.submit_exit_event();
        }
    }

    fn clear_wake(&self) {
        let mut counter = [0u8; 8];
        let _ = rustix::io::read(&self.wake, &mut counter);
    }

    /// Re-enter the daemon through the ordinary Signal path. The observation
    /// itself is incidental — the head of the ordinary turn runs the liveness
    /// truth read, which derives the death transitions from `/proc`.
    fn submit_exit_event(&self) {
        let input = Input::observe(Observation::Agents);
        let _ = OrdinarySignalTransport::connect(&self.socket_path)
            .and_then(|mut transport| transport.exchange(&input).map(|_| ()));
    }
}

enum ControlOutcome {
    Continue,
    Shutdown,
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::{Child, Command};

    use signal_orchestrate::{
        HarnessKind, MissionDescription, Observation as ContractObservation, OrchestrateReply,
        OrchestrateRequest, SessionIdentifier,
    };

    use super::*;
    use crate::agent_reachability::ProcessStat;
    use crate::{
        OrchestrateLayout, OrchestrateService, OrchestrateTables, StoreLocation,
        StoredAgentEndpointKind, StoredAgentReachability,
    };

    /// Write a fixture `/proc/<pid>/stat` whose field 4 (parent) and field 22
    /// (start time) land where the parser reads them.
    fn write_stat(process_root: &Path, pid: u32, start_time_ticks: u64) {
        let directory = process_root.join(pid.to_string());
        std::fs::create_dir_all(&directory).expect("proc pid directory");
        let mut fields = vec!["R".to_string(), "1".to_string()];
        fields.extend(std::iter::repeat_n("0".to_string(), 17));
        fields.push(start_time_ticks.to_string());
        let stat = format!("{pid} (harness) {}", fields.join(" "));
        std::fs::write(directory.join("stat"), stat).expect("write stat");
    }

    fn reachability_at(pid: u32, start_time_ticks: u64) -> StoredAgentReachability {
        StoredAgentReachability {
            endpoint_kind: StoredAgentEndpointKind::TerminalCell,
            target: "/tmp/liveness-test/data.sock".to_string(),
            harness_pid: pid,
            harness_start_time: start_time_ticks,
        }
    }

    fn spawn_harness_stand_in() -> (Child, WatchedHarnessProcess) {
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn stand-in harness process");
        let pid = child.id();
        let stat =
            ProcessStat::read(Path::new("/proc"), pid).expect("read stand-in process stat");
        (
            child,
            WatchedHarnessProcess {
                pid,
                start_time_ticks: stat.start_time_ticks,
            },
        )
    }

    #[test]
    fn generation_pin_reads_alive_gone_and_recycled() {
        let process_root = tempfile::TempDir::new().expect("proc root");
        write_stat(process_root.path(), 50, 7);

        assert!(reachability_at(50, 7).process_generation_alive(process_root.path()));
        // Same pid, different start time: recycled, the pinned generation is gone.
        assert!(!reachability_at(50, 8).process_generation_alive(process_root.path()));
        // No stat record at all: the process is gone.
        assert!(!reachability_at(51, 7).process_generation_alive(process_root.path()));
    }

    #[test]
    fn reconciliation_marks_gone_and_recycled_generations_dead_only() {
        let temporary = tempfile::TempDir::new().expect("store dir");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let tables = OrchestrateTables::open(&store).expect("store opens");
        let process_root = tempfile::TempDir::new().expect("proc root");
        write_stat(process_root.path(), 60, 5);
        write_stat(process_root.path(), 61, 9);

        let register = |session: &str| {
            tables
                .register_orchestrator_agent(
                    SessionIdentifier::from_camel_case_name(session).expect("session"),
                    MissionDescription::from_text("liveness fixture").expect("mission"),
                    HarnessKind::Codex,
                )
                .expect("register agent")
        };
        let alive = register("AliveHarness");
        tables
            .attach_agent_reachability(&alive.agent_identifier, reachability_at(60, 5))
            .expect("attach alive");
        let recycled = register("RecycledPid");
        tables
            .attach_agent_reachability(&recycled.agent_identifier, reachability_at(61, 4))
            .expect("attach recycled");
        let gone = register("GoneHarness");
        tables
            .attach_agent_reachability(&gone.agent_identifier, reachability_at(62, 3))
            .expect("attach gone");
        let unreachable = register("NoReachability");

        let marked = HarnessLivenessReconciliation::new(process_root.path())
            .reconcile(&tables)
            .expect("reconcile");
        assert_eq!(marked, 2, "recycled and gone are marked, alive is not");

        let status_of = |identifier| {
            tables
                .orchestrator_agent_record(identifier)
                .expect("record readable")
                .expect("record present")
                .status
        };
        assert_eq!(
            status_of(&alive.agent_identifier),
            OrchestratorAgentStatus::Active,
            "a live pinned generation is never marked"
        );
        assert_eq!(
            status_of(&recycled.agent_identifier),
            OrchestratorAgentStatus::Dead,
            "a recycled pid means the pinned generation is gone"
        );
        assert_eq!(
            status_of(&gone.agent_identifier),
            OrchestratorAgentStatus::Dead
        );
        assert_eq!(
            status_of(&unreachable.agent_identifier),
            OrchestratorAgentStatus::Active,
            "an agent without reachability stays on the idle backstop"
        );
    }

    #[test]
    fn pidfd_watch_opens_on_a_live_generation_and_fires_on_exit() {
        let (mut child, watched) = spawn_harness_stand_in();

        let WatchOpening::Watching(descriptor) = watched.open_watch(Path::new("/proc")) else {
            panic!("a live generation opens a watch");
        };

        // Alive: a zero-timeout poll reports no exit.
        let mut poll_fds = [PollFd::new(&descriptor.pidfd, PollFlags::IN)];
        let ready = rustix::event::poll(&mut poll_fds, Some(&rustix::event::Timespec::default()))
            .expect("zero-timeout poll");
        assert_eq!(ready, 0, "a live process's pidfd is not readable");

        child.kill().expect("kill stand-in");
        child.wait().expect("reap stand-in");

        // Exited: the kernel pushes readability; a blocking poll returns at once.
        let mut poll_fds = [PollFd::new(&descriptor.pidfd, PollFlags::IN)];
        let ready = rustix::event::poll(&mut poll_fds, None).expect("blocking poll");
        assert_eq!(ready, 1, "an exited process's pidfd is readable");
    }

    #[test]
    fn pidfd_watch_refuses_a_recycled_generation() {
        let (mut child, watched) = spawn_harness_stand_in();
        let wrong_generation = WatchedHarnessProcess {
            pid: watched.pid,
            start_time_ticks: watched.start_time_ticks + 1,
        };
        assert!(
            matches!(
                wrong_generation.open_watch(Path::new("/proc")),
                WatchOpening::GenerationGone
            ),
            "a start-time mismatch means the wanted generation is gone, whatever owns the pid now"
        );
        child.kill().expect("kill stand-in");
        child.wait().expect("reap stand-in");
    }

    #[test]
    fn killed_harness_shows_typed_dead_through_observe() {
        let temporary = tempfile::TempDir::new().expect("service dir");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let mut service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace, git_index),
        )
        .expect("service opens");

        let (mut child, watched) = spawn_harness_stand_in();
        let agent = service
            .tables()
            .register_orchestrator_agent(
                SessionIdentifier::from_camel_case_name("ObservedDeath").expect("session"),
                MissionDescription::from_text("killed mid-test").expect("mission"),
                HarnessKind::Codex,
            )
            .expect("register agent");
        service
            .tables()
            .attach_agent_reachability(
                &agent.agent_identifier,
                reachability_at(watched.pid, watched.start_time_ticks),
            )
            .expect("attach real reachability");

        let observe = |service: &mut OrchestrateService| {
            let reply = block_on(
                service.handle(OrchestrateRequest::Observe(ContractObservation::Agents)),
            )
            .expect("observe agents");
            let OrchestrateReply::AgentDirectory(directory) = reply else {
                panic!("expected the agent directory");
            };
            directory
                .agents
                .into_iter()
                .find(|summary| summary.agent_identifier == agent.agent_identifier)
                .expect("registered agent listed")
                .status
        };

        // Alive: the ordinary turn's truth read keeps the agent Active.
        assert_eq!(observe(&mut service), OrchestratorAgentStatus::Active);

        child.kill().expect("kill stand-in");
        child.wait().expect("reap stand-in");

        // Dead process: the next ordinary turn derives the typed transition
        // from /proc — witnessed on the wire through Observe.
        assert_eq!(observe(&mut service), OrchestratorAgentStatus::Dead);
    }

    fn block_on<Future: std::future::Future>(future: Future) -> Future::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
    }
}
