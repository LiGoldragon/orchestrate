use std::fs;

use orchestrate::{
    ActivityFilter, ActivityQuery, LaneAssignment, LaneAuthority, LaneDetails, LaneIdentifier,
    LaneOwner, LaneStatus, Observation, OrchestrateReply, OrchestrateRequest, OrchestrateService,
    OrchestrateTables, OrchestratorTopicPath, Role, RoleName, RoleToken, ScopeReason,
    ScopeReference, SessionIdentifier, StoreLocation, StoredClaim, StoredLaneRegistration,
    TaskToken, TimestampNanos, WirePath,
};

fn store_bytes(path: &std::path::Path) -> Vec<u8> {
    fs::read(path).expect("read durable store")
}

fn expired_terminal_lane() -> StoredLaneRegistration {
    StoredLaneRegistration::new(
        LaneAssignment {
            session: SessionIdentifier::from_camel_case_name("StateOnlyFixture")
                .expect("fixture session"),
            lane: LaneIdentifier::from_wire_token("state-only-fixture").expect("fixture lane"),
            owner: LaneOwner {
                role: Role::try_new(vec![RoleToken::from_text("Operator").expect("role token")])
                    .expect("fixture role"),
                authority: LaneAuthority::Structural,
            },
            details: LaneDetails::from_text("expired terminal fixture").expect("fixture details"),
        },
        TimestampNanos::new(1),
        TimestampNanos::new(1),
        LaneStatus::Released,
    )
}

fn fixture_store(store: &StoreLocation) {
    let tables = OrchestrateTables::open(store).expect("open fixture tables");
    let terminal_lane = expired_terminal_lane();
    let terminal_claim = StoredClaim::new(
        terminal_lane.assignment.lane.clone(),
        ScopeReference::Task(TaskToken::from_wire_token("primary-ahk.1").expect("task token")),
        ScopeReason::from_text("expired terminal claim").expect("claim reason"),
        TimestampNanos::new(1),
    );
    tables
        .transition_lane_ownership(&[], &[terminal_lane], &[], &[terminal_claim])
        .expect("write expired terminal fixture");
    tables
        .append_activity(
            RoleName::from_wire_token("state-only-reader").expect("activity role"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/workspace/pure-read").expect("path"),
            ),
            ScopeReason::from_text("path activity").expect("activity reason"),
        )
        .expect("write path activity");
    tables
        .append_activity(
            RoleName::from_wire_token("state-only-reader").expect("activity role"),
            ScopeReference::Task(TaskToken::from_wire_token("primary-ahk.1").expect("task token")),
            ScopeReason::from_text("task activity").expect("activity reason"),
        )
        .expect("write task activity");
}

#[tokio::test]
async fn every_observation_and_filtered_query_leave_terminal_state_byte_identical() {
    let temporary = tempfile::tempdir().expect("temporary store directory");
    let store_path = temporary.path().join("orchestrate.sema");
    let store = StoreLocation::new(store_path.to_str().expect("utf8"));
    fixture_store(&store);

    let mut service = OrchestrateService::open(&store).expect("open service");
    let observations = [
        Observation::Roles,
        Observation::Sessions,
        Observation::SessionLanes(
            SessionIdentifier::from_camel_case_name("StateOnlyFixture").expect("session"),
        ),
        Observation::Lanes,
        Observation::Worktrees,
        Observation::Repositories,
        Observation::Topics,
        Observation::Topic(
            OrchestratorTopicPath::from_wire_token("state-only").expect("topic path"),
        ),
        Observation::Agents,
    ];

    for observation in observations {
        let before = store_bytes(&store_path);
        let _reply = service
            .handle(OrchestrateRequest::Observe(observation.clone()))
            .await;
        assert_eq!(
            store_bytes(&store_path),
            before,
            "{observation:?} must be a byte-identical store projection"
        );
    }

    let before_query = store_bytes(&store_path);
    let reply = service
        .handle(OrchestrateRequest::Query(ActivityQuery {
            limit: 1,
            filters: vec![ActivityFilter::TaskToken(
                TaskToken::from_wire_token("primary-ahk.1").expect("task token"),
            )],
        }))
        .await
        .expect("filtered query reply");
    assert_eq!(
        store_bytes(&store_path),
        before_query,
        "query must not write"
    );
    let OrchestrateReply::ActivityList(activity) = reply else {
        panic!("query must return an activity list");
    };
    assert_eq!(
        activity.records.len(),
        1,
        "query must retain its requested limit"
    );
    assert!(matches!(
        activity.records[0].scope,
        ScopeReference::Task(ref token) if token.as_str() == "primary-ahk.1"
    ));

    let before_terminal_projection = store_bytes(&store_path);
    let reply = service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .await
        .expect("terminal lane projection");
    assert_eq!(
        store_bytes(&store_path),
        before_terminal_projection,
        "terminal lane projection must not write"
    );
    let OrchestrateReply::LanesObserved(lanes) = reply else {
        panic!("lane observation must return lanes");
    };
    assert_eq!(
        lanes.lanes.len(),
        1,
        "expired terminal lane must remain observable"
    );
    assert_eq!(lanes.lanes[0].registration.status, LaneStatus::Released);
    assert_eq!(
        lanes.lanes[0].resource_claims.len(),
        1,
        "terminal claim must not be retracted by reads"
    );
}

#[test]
fn production_source_has_no_repository_or_process_probe_or_configuration_file_path() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in walk(&root) {
        let path = entry.expect("source path");
        let text = fs::read_to_string(&path).expect("source text");
        for forbidden in [
            "Command::new",
            "flock",
            "canonicalize",
            "\"/proc\"",
            "pidfd",
        ] {
            assert!(
                !text.contains(forbidden),
                "{} retains forbidden host access {forbidden}",
                path.display()
            );
        }
    }

    let configuration = fs::read_to_string(root.join("configuration.rs"))
        .expect("read daemon configuration source");
    assert!(
        !configuration.contains("std::fs"),
        "daemon configuration must not read or write a configuration file"
    );
    assert!(
        !root.join("bin/orchestrate_write_configuration.rs").exists(),
        "the removed configuration writer must not be packaged"
    );
    for client in ["bin/orchestrate.rs", "bin/meta_orchestrate.rs"] {
        let source = fs::read_to_string(root.join(client)).expect("read client source");
        assert!(
            source.contains("ComponentArgument::NotaFile"),
            "{client} must retain NotaFile request input"
        );
        assert!(
            source.contains("ComponentArgument::SignalFile"),
            "{client} must retain SignalFile request input"
        );
    }
}

fn walk(root: &std::path::Path) -> Vec<std::io::Result<std::path::PathBuf>> {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("source directory") {
            let entry = entry.map(|entry| entry.path());
            if let Ok(path) = &entry
                && path.is_dir()
            {
                pending.push(path.clone());
                continue;
            }
            paths.push(entry);
        }
    }
    paths
}
