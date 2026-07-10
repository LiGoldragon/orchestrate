//! Reachability-discovery witnesses for the orchestrator seat.
//!
//! These drive the `/proc` ancestry walk and the terminal-cell session matcher
//! against fixture `/proc`-shaped trees and fixture session directories, plus a
//! hermetic check against the real `/proc` of the test process itself.

use std::fs;
use std::path::Path;

use orchestrate::{
    AgentReachabilityDiscovery, ProcessAncestryWalk, ProcessStat, StoredAgentEndpointKind,
    TerminalCellSessionRecord,
};
use tempfile::TempDir;

/// Write a fixture `/proc/<pid>/stat` with the given parent pid and start time,
/// padding the intervening fixed fields so field 4 (parent) and field 22
/// (start time) land where the parser reads them. The comm field carries a
/// space and a parenthesis to prove the parser splits on the final `") "`.
fn write_stat(proc_root: &Path, pid: u32, parent_pid: u32, start_time_ticks: u64) {
    let directory = proc_root.join(pid.to_string());
    fs::create_dir_all(&directory).expect("proc pid directory");
    let mut fields = vec!["R".to_string(), parent_pid.to_string()];
    fields.extend(std::iter::repeat_n("0".to_string(), 17));
    fields.push(start_time_ticks.to_string());
    let stat = format!("{pid} (weird ) name) {}", fields.join(" "));
    fs::write(directory.join("stat"), stat).expect("write stat");
}

/// Write a fixture terminal-cell session directory with the given child and
/// daemon pids and a `data.sock` endpoint file.
fn write_session(session_root: &Path, name: &str, child_pid: u32, daemon_pid: u32) {
    let directory = session_root.join(name);
    fs::create_dir_all(&directory).expect("session directory");
    fs::write(directory.join("child.pid"), format!("{child_pid}\n")).expect("child pid");
    fs::write(directory.join("daemon.pid"), daemon_pid.to_string()).expect("daemon pid");
    fs::write(directory.join("data.sock"), []).expect("data socket");
}

#[test]
fn process_stat_parses_parent_and_start_time_past_a_tricky_comm() {
    // Field 4 (parent pid) is 41; field 22 (start time) is 998877. The 17
    // zeros are the fixed fields between them; the two trailing zeros are the
    // fields past start time the parser ignores.
    let stat = "4242 (weird ) comm) R 41 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 998877 0 0";
    let parsed = ProcessStat::parse(stat).expect("parse stat");
    assert_eq!(parsed.parent_pid, 41);
    assert_eq!(parsed.start_time_ticks, 998877);
}

#[test]
fn process_stat_rejects_a_truncated_record() {
    assert_eq!(ProcessStat::parse("4242 (comm) R 41"), None);
}

#[test]
fn ancestry_walk_climbs_from_caller_to_the_init_process() {
    let proc_root = TempDir::new().expect("proc root");
    // caller 100 -> parent 50 -> parent 1 (init), each with a distinct start
    // time so the pin is carried per generation.
    write_stat(proc_root.path(), 100, 50, 1_000);
    write_stat(proc_root.path(), 50, 1, 2_000);
    write_stat(proc_root.path(), 1, 1, 3_000);

    let ancestors = ProcessAncestryWalk::new(proc_root.path()).ancestors_of(100);
    let chain: Vec<(u32, u64)> = ancestors
        .iter()
        .map(|ancestor| (ancestor.pid, ancestor.start_time_ticks))
        .collect();
    assert_eq!(chain, vec![(100, 1_000), (50, 2_000), (1, 3_000)]);
}

#[test]
fn ancestry_walk_stops_when_a_stat_record_is_missing() {
    let proc_root = TempDir::new().expect("proc root");
    // caller 100 -> parent 50, but 50's record is absent (the process exited).
    write_stat(proc_root.path(), 100, 50, 1_000);

    let ancestors = ProcessAncestryWalk::new(proc_root.path()).ancestors_of(100);
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0].pid, 100);
}

#[test]
fn session_record_reads_pids_and_matches_either_process() {
    let session_root = TempDir::new().expect("session root");
    write_session(
        session_root.path(),
        "session-cell-1700000000000",
        4242,
        4200,
    );

    let record =
        TerminalCellSessionRecord::read(&session_root.path().join("session-cell-1700000000000"))
            .expect("session record");
    assert_eq!(record.child_pid, Some(4242));
    assert_eq!(record.daemon_pid, Some(4200));
    assert!(record.owns_process(4242));
    assert!(record.owns_process(4200));
    assert!(!record.owns_process(9999));
    assert!(record.data_socket.ends_with("data.sock"));
}

#[test]
fn session_record_ignores_a_directory_without_the_session_prefix() {
    let session_root = TempDir::new().expect("session root");
    let directory = session_root.path().join("not-a-session");
    fs::create_dir_all(&directory).expect("directory");
    assert_eq!(TerminalCellSessionRecord::read(&directory), None);
}

#[test]
fn discovery_binds_the_caller_to_the_terminal_cell_hosting_its_harness() {
    let proc_root = TempDir::new().expect("proc root");
    let session_root = TempDir::new().expect("session root");

    // caller 100 (the agent's CLI) -> 50 (the harness / PTY child) -> 1. The
    // terminal cell records 50 as its child pid, so discovery binds the agent
    // to that cell's data socket, carrying pid 50 and its captured start time.
    write_stat(proc_root.path(), 100, 50, 1_000);
    write_stat(proc_root.path(), 50, 1, 2_000);
    write_stat(proc_root.path(), 1, 1, 3_000);
    write_session(session_root.path(), "session-cell-1700000000000", 50, 40);

    let reachability = AgentReachabilityDiscovery::new(proc_root.path(), session_root.path())
        .discover(100)
        .expect("reachability discovered");
    assert_eq!(
        reachability.endpoint_kind,
        StoredAgentEndpointKind::TerminalCell
    );
    assert_eq!(reachability.harness_pid, 50);
    assert_eq!(reachability.harness_start_time, 2_000);
    assert!(reachability.target.ends_with("data.sock"));
    assert!(reachability.target.contains("session-cell-1700000000000"));
}

#[test]
fn discovery_finds_no_reachability_when_no_ancestor_belongs_to_a_session() {
    let proc_root = TempDir::new().expect("proc root");
    let session_root = TempDir::new().expect("session root");

    write_stat(proc_root.path(), 100, 50, 1_000);
    write_stat(proc_root.path(), 50, 1, 2_000);
    write_stat(proc_root.path(), 1, 1, 3_000);
    // The session hosts unrelated pids, so no ancestor matches.
    write_session(session_root.path(), "session-cell-1700000000000", 777, 778);

    let discovery = AgentReachabilityDiscovery::new(proc_root.path(), session_root.path());
    assert_eq!(discovery.discover(100), None);
}

#[test]
fn ancestry_walk_over_real_proc_starts_at_self_and_terminates() {
    // Hermetic against the live host and a Nix build sandbox alike: the test
    // process's own `/proc/self` record always exists, so the walk starts at
    // this pid, stays bounded, and never loops (every pid is distinct). It does
    // not assert reaching pid 1, since a PID-namespace sandbox may hide the
    // ancestry above the builder — the fixture walk above proves the climb to
    // init deterministically.
    let ancestors = ProcessAncestryWalk::new("/proc").ancestors_of(std::process::id());
    assert_eq!(
        ancestors.first().expect("self ancestor").pid,
        std::process::id()
    );
    let mut seen = std::collections::BTreeSet::new();
    for ancestor in &ancestors {
        assert!(
            seen.insert(ancestor.pid),
            "the ancestry walk must not revisit a pid"
        );
    }
}
