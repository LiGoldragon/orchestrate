//! Observational activity reads for registered agents — layer 3 of the
//! coordination-liveliness design. A long-running command can produce no
//! orchestration traffic for hours, so this module can inspect operating-system
//! evidence without treating output silence as a conclusion.
//!
//! It reports only what it observes:
//!
//! - the harness process's live descendant tree, including a running command;
//! - the terminal-cell session directory's artifact recency.
//!
//! An observation, its timestamp, and the absence of either are inspection
//! metadata. They never authorize warning, recovery, retirement, claim release,
//! abandonment, or any other lifecycle transition for an active agent, session,
//! or lane.
//!
//! Everything here is an operating-system-boundary read (`/proc` text and
//! directory modification times), parsed in this module and never on a wire
//! path, mirroring `agent_reachability`. The read is best-effort in the same
//! way discovery is: an unreadable surface is simply no observation, never an
//! error.

use std::path::{Path, PathBuf};

use signal_orchestrate::TimestampNanos;

use crate::agent_reachability::ProcessStat;
use crate::{StoredAgentEndpointKind, StoredAgentReachability, StoredOrchestratorAgent};

/// The default `/proc` mount activity truth is read from. Configurable so a
/// test can point the read at a fixture `/proc`-shaped tree.
const DEFAULT_PROCESS_ROOT: &str = "/proc";

/// A self-parenting/cycle guard for the upward walk from a candidate
/// descendant towards the watched harness process, mirroring the ancestry
/// walk's bound.
const MAXIMUM_ANCESTRY_DEPTH: usize = 64;

impl StoredAgentReachability {
    /// The terminal-cell session directory holding this reachability's data
    /// socket — the artifact surface the activity read inspects. `None` when
    /// the endpoint is not a terminal-cell session (a `HarnessProcess` target
    /// is not a filesystem path) or the target has no parent directory.
    pub fn session_directory(&self) -> Option<PathBuf> {
        match self.endpoint_kind {
            StoredAgentEndpointKind::TerminalCell => {
                Path::new(&self.target).parent().map(Path::to_path_buf)
            }
            StoredAgentEndpointKind::HarnessProcess => None,
        }
    }
}

/// What one observational activity read established about an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentActivityAssessment {
    /// The agent's real latest activity was observed. This is inspection
    /// metadata and does not change lifecycle ownership.
    ActivityObserved(ObservedAgentActivity),
    /// No observation: no live command process under the harness and no
    /// session artifact written after the stored stamp. This is not evidence of
    /// abandonment and does not authorize a lifecycle transition.
    NoActivityObserved,
}

/// The evidence one positive activity read rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedAgentActivity {
    /// A live process still runs under the agent's harness — a command in
    /// flight, however long it has been running.
    LiveCommandProcess { process_identifier: u32 },
    /// A file under the terminal-cell session directory was written after the
    /// agent's stored activity stamp.
    SessionArtifactWrite { written_at: TimestampNanos },
}

/// Reads an agent's real latest activity from operating-system truth: the
/// harness process's live descendant tree under `process_root`, and the
/// terminal-cell session directory named by the agent's discovered
/// reachability.
pub struct AgentActivityRead {
    process_root: PathBuf,
}

impl AgentActivityRead {
    pub fn new(process_root: impl Into<PathBuf>) -> Self {
        Self {
            process_root: process_root.into(),
        }
    }

    /// The read bound to the live host's real `/proc`.
    pub fn from_process_environment() -> Self {
        Self::new(DEFAULT_PROCESS_ROOT)
    }

    /// Read one agent's observed activity. An agent without discovered
    /// reachability has no process pin and no session directory to inspect, so
    /// the read reports no observation and leaves ownership unchanged.
    ///
    /// The descendant scan only runs while the pinned harness generation is
    /// still alive: children of a recycled pid belong to a stranger and are
    /// never attributed to the agent.
    pub fn assess(&self, agent: &StoredOrchestratorAgent) -> AgentActivityAssessment {
        let Some(reachability) = &agent.reachability else {
            return AgentActivityAssessment::NoActivityObserved;
        };
        if reachability.process_generation_alive(&self.process_root) {
            let scan = ProcessDescendantScan::new(&self.process_root);
            if let Some(process_identifier) = scan.live_descendant_of(reachability.harness_pid) {
                return AgentActivityAssessment::ActivityObserved(
                    ObservedAgentActivity::LiveCommandProcess { process_identifier },
                );
            }
        }
        if let Some(directory) = reachability.session_directory()
            && let Some(written_at) = SessionArtifactSurface::new(directory).newest_write()
            && written_at.value() > agent.last_activity.value()
        {
            return AgentActivityAssessment::ActivityObserved(
                ObservedAgentActivity::SessionArtifactWrite { written_at },
            );
        }
        AgentActivityAssessment::NoActivityObserved
    }
}

/// Scans the process table under one `/proc` root for live descendants of a
/// watched process — the inverse direction of `ProcessAncestryWalk`: instead
/// of walking one pid's chain upward, it asks whether any live process's chain
/// passes through the watched pid.
pub struct ProcessDescendantScan {
    process_root: PathBuf,
}

impl ProcessDescendantScan {
    pub fn new(process_root: impl Into<PathBuf>) -> Self {
        Self {
            process_root: process_root.into(),
        }
    }

    /// The smallest-pid live descendant of `ancestor_pid`, or `None` when no
    /// live process descends from it. Deterministic (smallest pid) so a
    /// caller's evidence is stable across repeated reads of the same tree.
    pub fn live_descendant_of(&self, ancestor_pid: u32) -> Option<u32> {
        let parents = self.parent_links();
        let mut descendants: Vec<u32> = parents
            .keys()
            .copied()
            .filter(|pid| *pid != ancestor_pid && self.descends_from(&parents, *pid, ancestor_pid))
            .collect();
        descendants.sort_unstable();
        descendants.first().copied()
    }

    /// Every live process's parent link, read in one pass over the process
    /// root. Unreadable or vanishing entries are skipped: the scan reads a
    /// moving surface and only claims what it saw.
    fn parent_links(&self) -> std::collections::BTreeMap<u32, u32> {
        let mut links = std::collections::BTreeMap::new();
        let Ok(entries) = std::fs::read_dir(&self.process_root) else {
            return links;
        };
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            if let Some(stat) = ProcessStat::read(&self.process_root, pid) {
                links.insert(pid, stat.parent_pid);
            }
        }
        links
    }

    /// Whether `pid`'s parent chain reaches `ancestor_pid`, following links in
    /// the snapshot only, bounded against self-parenting loops.
    fn descends_from(
        &self,
        parents: &std::collections::BTreeMap<u32, u32>,
        pid: u32,
        ancestor_pid: u32,
    ) -> bool {
        let mut current = pid;
        for _ in 0..MAXIMUM_ANCESTRY_DEPTH {
            let Some(parent) = parents.get(&current).copied() else {
                return false;
            };
            if parent == ancestor_pid {
                return true;
            }
            if parent == current || parent == 0 {
                return false;
            }
            current = parent;
        }
        false
    }
}

/// The artifact surface of one terminal-cell session directory: the files a
/// live harness writes beside its sockets. Its newest write instant is the
/// session's last observable output activity.
pub struct SessionArtifactSurface {
    directory: PathBuf,
}

impl SessionArtifactSurface {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// The newest modification instant across the directory's entries, as
    /// nanoseconds since the UNIX epoch — the same epoch the store clock
    /// stamps `last_activity` with. `None` when the directory is unreadable,
    /// empty, or its timestamps predate the epoch.
    pub fn newest_write(&self) -> Option<TimestampNanos> {
        let entries = std::fs::read_dir(&self.directory).ok()?;
        let mut newest: Option<u64> = None;
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            let Ok(modified) = metadata.modified() else {
                continue;
            };
            let Ok(since_epoch) = modified.duration_since(std::time::UNIX_EPOCH) else {
                continue;
            };
            let nanos = since_epoch.as_nanos().min(u64::MAX as u128) as u64;
            newest = Some(newest.map_or(nanos, |current| current.max(nanos)));
        }
        newest.map(TimestampNanos::new)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::{Child, Command};

    use signal_orchestrate::{
        HarnessKind, MissionDescription, OrchestratorAgentStatus, SessionIdentifier,
    };

    use super::*;
    use crate::OrchestratorAgentIdentifier;

    /// Write a fixture `/proc/<pid>/stat` whose parent pid (field 4) and start
    /// time (field 22) land where the parser reads them.
    fn write_stat(process_root: &Path, pid: u32, parent_pid: u32, start_time_ticks: u64) {
        let directory = process_root.join(pid.to_string());
        std::fs::create_dir_all(&directory).expect("proc pid directory");
        let mut fields = vec!["R".to_string(), parent_pid.to_string()];
        fields.extend(std::iter::repeat_n("0".to_string(), 17));
        fields.push(start_time_ticks.to_string());
        let stat = format!("{pid} (harness) {}", fields.join(" "));
        std::fs::write(directory.join("stat"), stat).expect("write stat");
    }

    fn agent_with_reachability(
        target: &Path,
        harness_pid: u32,
        harness_start_time: u64,
        last_activity: TimestampNanos,
    ) -> StoredOrchestratorAgent {
        StoredOrchestratorAgent {
            agent_identifier: OrchestratorAgentIdentifier::from_wire_token("t3st")
                .expect("identifier"),
            session: SessionIdentifier::from_camel_case_name("ActivityRead").expect("session"),
            mission: MissionDescription::from_text("activity fixture").expect("mission"),
            harness: HarnessKind::Codex,
            reachability: Some(StoredAgentReachability {
                endpoint_kind: StoredAgentEndpointKind::TerminalCell,
                target: target.to_string_lossy().into_owned(),
                harness_pid,
                harness_start_time,
            }),
            registered_at: TimestampNanos::new(1),
            last_activity,
            status: OrchestratorAgentStatus::Active,
        }
    }

    /// A stand-in harness that really has a live child: a shell holding a
    /// `sleep` command, the way a real harness holds a running build.
    fn spawn_harness_with_command() -> Child {
        Command::new("sh")
            .arg("-c")
            .arg("sleep 30; :")
            .spawn()
            .expect("spawn harness stand-in with command child")
    }

    #[test]
    fn descendant_scan_finds_transitive_children_and_ignores_strangers() {
        let process_root = tempfile::TempDir::new().expect("proc root");
        write_stat(process_root.path(), 100, 1, 5);
        write_stat(process_root.path(), 101, 100, 6);
        write_stat(process_root.path(), 102, 101, 7);
        write_stat(process_root.path(), 200, 1, 8);

        let scan = ProcessDescendantScan::new(process_root.path());
        assert_eq!(
            scan.live_descendant_of(100),
            Some(101),
            "the smallest live descendant is reported, transitively"
        );
        assert_eq!(
            scan.live_descendant_of(101),
            Some(102),
            "a grandchild is a child's descendant"
        );
        assert_eq!(
            scan.live_descendant_of(200),
            None,
            "a childless process has no descendants"
        );
    }

    #[test]
    fn live_command_child_reads_as_positive_liveness_under_output_silence() {
        // The plan's long-running-child witness, against the real /proc: a
        // harness stand-in holding a running command reads alive although its
        // session surface is silent (an empty directory, nothing written).
        let session = tempfile::TempDir::new().expect("session dir");
        let mut harness = spawn_harness_with_command();
        let stat = ProcessStat::read(Path::new("/proc"), harness.id()).expect("harness stat");
        let agent = agent_with_reachability(
            &session.path().join("data.sock"),
            harness.id(),
            stat.start_time_ticks,
            TimestampNanos::new(u64::MAX - 1),
        );

        // The shell forks its command child asynchronously; wait for the real
        // process tree to show it before asserting.
        let read = AgentActivityRead::new("/proc");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let assessment = loop {
            let assessment = read.assess(&agent);
            if assessment != AgentActivityAssessment::NoActivityObserved
                || std::time::Instant::now() > deadline
            {
                break assessment;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };

        assert!(
            matches!(
                assessment,
                AgentActivityAssessment::ActivityObserved(
                    ObservedAgentActivity::LiveCommandProcess { .. }
                )
            ),
            "a live command child is positive liveness, artifacts and stamp regardless"
        );

        harness.kill().expect("kill harness stand-in");
        harness.wait().expect("reap harness stand-in");
    }

    #[test]
    fn recent_session_artifact_write_reads_as_positive_liveness() {
        let process_root = tempfile::TempDir::new().expect("proc root");
        // The harness is alive but childless: no command in flight.
        write_stat(process_root.path(), 300, 1, 5);
        let session = tempfile::TempDir::new().expect("session dir");
        std::fs::write(session.path().join("transcript.log"), "output").expect("artifact");
        let written = SessionArtifactSurface::new(session.path())
            .newest_write()
            .expect("artifact instant");

        // Stamped before the artifact write: the write is fresh evidence.
        let agent = agent_with_reachability(
            &session.path().join("data.sock"),
            300,
            5,
            TimestampNanos::new(written.value() - 1_000),
        );
        assert_eq!(
            AgentActivityRead::new(process_root.path()).assess(&agent),
            AgentActivityAssessment::ActivityObserved(
                ObservedAgentActivity::SessionArtifactWrite {
                    written_at: written
                }
            ),
        );
    }

    #[test]
    fn childless_harness_with_stale_artifacts_reads_as_no_activity() {
        // The plan's childless-stale witness: the harness is alive, but no
        // command runs and nothing was written after the stored stamp.
        let process_root = tempfile::TempDir::new().expect("proc root");
        write_stat(process_root.path(), 300, 1, 5);
        let session = tempfile::TempDir::new().expect("session dir");
        std::fs::write(session.path().join("transcript.log"), "old output").expect("artifact");
        let written = SessionArtifactSurface::new(session.path())
            .newest_write()
            .expect("artifact instant");

        // Stamped at (and hence after) the newest write: the artifacts are stale.
        let agent = agent_with_reachability(&session.path().join("data.sock"), 300, 5, written);
        assert_eq!(
            AgentActivityRead::new(process_root.path()).assess(&agent),
            AgentActivityAssessment::NoActivityObserved,
        );
    }

    #[test]
    fn recycled_harness_pid_never_attributes_a_strangers_children() {
        let process_root = tempfile::TempDir::new().expect("proc root");
        // Pid 400 exists with a child, but at a different start time than the
        // pin: the pid was recycled by a stranger whose children prove nothing.
        write_stat(process_root.path(), 400, 1, 9);
        write_stat(process_root.path(), 401, 400, 10);
        let session = tempfile::TempDir::new().expect("session dir");

        let agent = agent_with_reachability(
            &session.path().join("data.sock"),
            400,
            5,
            TimestampNanos::new(u64::MAX - 1),
        );
        assert_eq!(
            AgentActivityRead::new(process_root.path()).assess(&agent),
            AgentActivityAssessment::NoActivityObserved,
        );
    }

    #[test]
    fn agent_without_reachability_reads_as_no_activity() {
        let mut agent = agent_with_reachability(
            Path::new("/nonexistent/data.sock"),
            1,
            1,
            TimestampNanos::new(1),
        );
        agent.reachability = None;
        assert_eq!(
            AgentActivityRead::new("/nonexistent-proc").assess(&agent),
            AgentActivityAssessment::NoActivityObserved,
            "no reachability produces no observation and leaves ownership unchanged"
        );
    }
}
