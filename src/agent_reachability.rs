//! Reachability discovery for a registering agent.
//!
//! Reachability is discovered, never caller-declared. At the ordinary-socket
//! boundary orchestrate receives the peer's kernel-vouched process identifier
//! (`SO_PEERCRED`, carried by triad-runtime's `UnixCredentials`). From that pid
//! this module walks the `/proc` ancestry chain and matches each ancestor
//! against the terminal-cell session directories, so a registering agent is
//! bound to the terminal cell whose harness it runs inside.
//!
//! Everything here is an operating-system-boundary read: `/proc/<pid>/stat`
//! text and the on-disk session directory layout are OS surfaces, parsed in
//! this dedicated module and never on any wire path (daemons don't speak
//! string formats). Discovery is best-effort: a missing `/proc` entry, an
//! unreadable session directory, or no match at all is not an error — the agent
//! registers WITHOUT reachability, its identity and topics valid regardless,
//! and delivery parks until an endpoint exists.
//!
//! The terminal-cell session layout this matches against is authored by
//! `terminal-cell/src/session.rs` and `terminal-cell/src/lifecycle_cli.rs`:
//! each session directory is named `session-<stem>-<millis>` and holds
//! `daemon.pid` (the terminal-cell daemon pid) and, once the daemon forks its
//! PTY child, `child.pid` (the harness process the agent runs inside) beside
//! the `control.sock` / `data.sock` endpoints. The pid files carry a bare
//! decimal integer.

use std::path::{Path, PathBuf};

use crate::{StoredAgentEndpointKind, StoredAgentReachability};

/// The default `/proc` mount every ancestry walk reads from. Configurable so a
/// unit test can point the walk at a fixture `/proc`-shaped tree.
const DEFAULT_PROC_ROOT: &str = "/proc";

/// The environment variable terminal-cell reads first for its runtime root
/// (`terminal-cell/src/lifecycle_cli.rs`), mirrored here so discovery finds the
/// same session directories the live terminal cells write to.
const TERMINAL_CELL_RUNTIME_DIRECTORY_VARIABLE: &str = "TERMINAL_CELL_RUNTIME_DIR";

/// The secondary runtime-directory variable terminal-cell falls back to.
const XDG_RUNTIME_DIRECTORY_VARIABLE: &str = "XDG_RUNTIME_DIR";

/// The fixed subdirectory terminal-cell appends to its runtime root.
const TERMINAL_CELL_SUBDIRECTORY: &str = "terminal-cell";

/// The marker prefix of a terminal-cell session directory.
const SESSION_DIRECTORY_PREFIX: &str = "session-";

/// The pid file naming the terminal-cell daemon process.
const DAEMON_PID_FILE: &str = "daemon.pid";

/// The pid file naming the PTY child (the harness the agent runs inside),
/// written asynchronously by the daemon and therefore sometimes absent.
const CHILD_PID_FILE: &str = "child.pid";

/// The data-plane socket a registered agent is reached on.
const DATA_SOCKET_FILE: &str = "data.sock";

/// A cycle/self-parenting guard: `/proc` ancestry never exceeds this depth in
/// practice, so a longer chain means malformed data and the walk stops.
const MAXIMUM_ANCESTRY_DEPTH: usize = 64;

/// Discovers where a registering agent is reached by walking the caller's
/// `/proc` ancestry and matching it against the terminal-cell session index.
/// Both roots are configurable so the discovery is hermetically testable.
pub struct AgentReachabilityDiscovery {
    ancestry: ProcessAncestryWalk,
    sessions: TerminalCellSessionIndex,
}

impl AgentReachabilityDiscovery {
    pub fn new(proc_root: impl Into<PathBuf>, session_root: impl Into<PathBuf>) -> Self {
        Self {
            ancestry: ProcessAncestryWalk::new(proc_root),
            sessions: TerminalCellSessionIndex::new(session_root),
        }
    }

    /// Build a discovery bound to the live host: the real `/proc` and the same
    /// terminal-cell runtime root the live cells write to.
    pub fn from_process_environment() -> Self {
        Self {
            ancestry: ProcessAncestryWalk::new(DEFAULT_PROC_ROOT),
            sessions: TerminalCellSessionIndex::from_process_environment(),
        }
    }

    /// Discover the caller's reachability, or `None` when no ancestor of the
    /// caller belongs to a known terminal-cell session. The matched ancestor's
    /// `/proc` start time is captured as the generation pin: terminal-cell
    /// records only the pid, so the start time read here disambiguates a later
    /// recycled pid.
    pub fn discover(&self, caller_pid: u32) -> Option<StoredAgentReachability> {
        let ancestors = self.ancestry.ancestors_of(caller_pid);
        let sessions = self.sessions.sessions();
        for ancestor in ancestors {
            for session in &sessions {
                if session.owns_process(ancestor.pid) {
                    return Some(StoredAgentReachability {
                        endpoint_kind: StoredAgentEndpointKind::TerminalCell,
                        target: session.data_socket.to_string_lossy().into_owned(),
                        harness_pid: ancestor.pid,
                        harness_start_time: ancestor.start_time_ticks,
                    });
                }
            }
        }
        None
    }
}

/// One process on the caller's ancestry chain: its pid and its `/proc` start
/// time in clock ticks after boot (field 22 of `/proc/<pid>/stat`), the pin
/// that distinguishes this process generation from a later recycled pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AncestorProcess {
    pub pid: u32,
    pub start_time_ticks: u64,
}

/// Walks the `/proc` process ancestry from a starting pid up to pid 1,
/// following the parent-pid link in each process's `stat` record.
pub struct ProcessAncestryWalk {
    proc_root: PathBuf,
}

impl ProcessAncestryWalk {
    pub fn new(proc_root: impl Into<PathBuf>) -> Self {
        Self {
            proc_root: proc_root.into(),
        }
    }

    /// The chain of processes from `caller_pid` up towards pid 1, the caller
    /// first. The walk stops at the init process, at an unreadable or missing
    /// `stat` record, at a non-advancing parent link (a self-parent or a
    /// backward jump that would loop), or at the depth guard.
    pub fn ancestors_of(&self, caller_pid: u32) -> Vec<AncestorProcess> {
        let mut ancestors = Vec::new();
        let mut current = caller_pid;
        for _ in 0..MAXIMUM_ANCESTRY_DEPTH {
            if current == 0 {
                break;
            }
            let Some(stat) = ProcessStat::read(&self.proc_root, current) else {
                break;
            };
            ancestors.push(AncestorProcess {
                pid: current,
                start_time_ticks: stat.start_time_ticks,
            });
            if current == 1 || stat.parent_pid == current {
                break;
            }
            current = stat.parent_pid;
        }
        ancestors
    }
}

/// The two fields of a `/proc/<pid>/stat` record this module reads: the parent
/// pid (field 4) and the start time in clock ticks after boot (field 22).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessStat {
    pub parent_pid: u32,
    pub start_time_ticks: u64,
}

impl ProcessStat {
    /// Read and parse `<proc_root>/<pid>/stat`, or `None` when the process is
    /// gone or the record cannot be read or parsed.
    pub fn read(proc_root: &Path, pid: u32) -> Option<Self> {
        let stat = std::fs::read_to_string(proc_root.join(pid.to_string()).join("stat")).ok()?;
        Self::parse(&stat)
    }

    /// Parse a `stat` record. The comm field (field 2) is parenthesized and may
    /// itself contain spaces and parentheses, so the fixed fields are read
    /// after the final `") "`: index 0 is state (field 3), index 1 is the
    /// parent pid (field 4), index 19 is the start time (field 22).
    pub fn parse(stat: &str) -> Option<Self> {
        let after_comm = &stat[stat.rfind(") ")? + 2..];
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        Some(Self {
            parent_pid: fields.get(1)?.parse().ok()?,
            start_time_ticks: fields.get(19)?.parse().ok()?,
        })
    }
}

/// The terminal-cell session directories under one runtime root.
pub struct TerminalCellSessionIndex {
    root: PathBuf,
}

impl TerminalCellSessionIndex {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve the runtime root the same way terminal-cell does:
    /// `$TERMINAL_CELL_RUNTIME_DIR/terminal-cell`, else
    /// `$XDG_RUNTIME_DIR/terminal-cell`, else `<temp>/terminal-cell`.
    pub fn from_process_environment() -> Self {
        let base = std::env::var_os(TERMINAL_CELL_RUNTIME_DIRECTORY_VARIABLE)
            .map(PathBuf::from)
            .or_else(|| std::env::var_os(XDG_RUNTIME_DIRECTORY_VARIABLE).map(PathBuf::from))
            .unwrap_or_else(std::env::temp_dir);
        Self::new(base.join(TERMINAL_CELL_SUBDIRECTORY))
    }

    /// Every readable session directory under the root. An unreadable root
    /// (terminal-cell never ran) yields an empty index rather than an error.
    pub fn sessions(&self) -> Vec<TerminalCellSessionRecord> {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|entry| TerminalCellSessionRecord::read(&entry.path()))
            .collect()
    }
}

/// One terminal-cell session directory: the pid(s) it owns and the data socket
/// a member agent is reached on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalCellSessionRecord {
    pub directory: PathBuf,
    pub child_pid: Option<u32>,
    pub daemon_pid: Option<u32>,
    pub data_socket: PathBuf,
}

impl TerminalCellSessionRecord {
    /// Read a session directory, or `None` when the path is not a
    /// `session-`-prefixed directory. A session with neither pid file is still
    /// read — it simply owns no process to match.
    pub fn read(directory: &Path) -> Option<Self> {
        if !directory.is_dir() {
            return None;
        }
        let named_session = directory
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(SESSION_DIRECTORY_PREFIX));
        if !named_session {
            return None;
        }
        Some(Self {
            child_pid: Self::read_pid_file(directory, CHILD_PID_FILE),
            daemon_pid: Self::read_pid_file(directory, DAEMON_PID_FILE),
            data_socket: directory.join(DATA_SOCKET_FILE),
            directory: directory.to_path_buf(),
        })
    }

    /// Whether this session owns `pid` — either its PTY child (the harness the
    /// agent runs inside) or its daemon. The child is checked first: it is the
    /// harness process a registering agent descends from.
    pub fn owns_process(&self, pid: u32) -> bool {
        self.child_pid == Some(pid) || self.daemon_pid == Some(pid)
    }

    fn read_pid_file(directory: &Path, file: &str) -> Option<u32> {
        std::fs::read_to_string(directory.join(file))
            .ok()?
            .trim()
            .parse()
            .ok()
    }
}
