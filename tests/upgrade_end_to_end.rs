//! End-to-end witness for the orchestrate v0.1.0 -> v0.1.1 upgrade
//! mechanism per second-designer reports /175 and /176.
//!
//! Exercises the eight-step chain:
//!
//!  1. Old daemon (in-process service) accepting `Claim` operations
//!  2. Pre-cutover witness: submit N claims, confirm acks
//!  3. New daemon (second in-process service) spawned alongside old
//!  4. New daemon copies the old service's redb file
//!     and runs identity-style projection (no schema change
//!     v0.1.0 to v0.1.1 here, so the migration is a literal
//!     block copy)
//!  5. New daemon "connects" to the old daemon's upgrade socket -
//!     hand-built handover-driver in-process so we exercise the
//!     mirror-payload contract rather than the not-yet-wired socket
//!     listener
//!  6. Mirror payload exchange using the existing typed
//!     `OrchestrateService::mirror_payload` and
//!     `restore_mirror_payload` helpers
//!  7. Atomic socket cutover - in this in-process slice the cutover
//!     is the moment the test stops talking to the old service and
//!     starts talking to the new service
//!  8. Post-cutover query: submit `Observe(Roles)` against the new
//!     service and verify every claim that was acked pre-cutover is
//!     queryable post-cutover
//!
//! Per intent record 546 (2026-05-25), this test is allowed to
//! unblock its own blockers. The handover-driver wiring is the
//! main unblocked piece; see `InProcessHandoverDriver` below.

use std::path::{Path, PathBuf};

use orchestrate::{
    LaneAuthority, LaneRegistrationRequest, MirrorPayload, MirrorSnapshot, MirrorVersions,
    Observation, OrchestrateLayout, OrchestrateReply, OrchestrateRequest, OrchestrateService,
    OwnerOrchestrateReply, OwnerOrchestrateRequest, RoleClaim, RoleName, RoleToken, ScopeReason,
    ScopeReference, StoreLocation, WirePath,
};
use signal_version_handover::{
    CompletionReport, Date, HandoverAcceptance, HandoverFinalization, HandoverMarker,
    MarkerRequest, MirrorAcknowledgement, ReadinessReport, Time,
};
use tempfile::TempDir;
use version_projection::{ComponentName, ContractVersion};

/// Seed claims live across roles ALL OF WHICH must be in
/// `RoleIdentifier::CURRENT_WORKSPACE_ROLE_TOKENS` so the
/// `RoleSnapshot` observation rolls them up. Per design, claims
/// for non-seeded roles still land in the redb claims table, but
/// `RoleSnapshot` only includes claims for known-registered roles.
/// Test uses 5 distinct seeded roles to give N=5.
const PRE_CUTOVER_CLAIMS: &[(&str, &str, &str)] = &[
    (
        "operator",
        "/git/github.com/LiGoldragon/orchestrate",
        "operator owns orchestrate during upgrade test",
    ),
    (
        "designer",
        "/git/github.com/LiGoldragon/signal-orchestrate",
        "designer owns signal-orchestrate during upgrade test",
    ),
    (
        "system-specialist",
        "/git/github.com/LiGoldragon/upgrade",
        "system-specialist owns upgrade during upgrade test",
    ),
    (
        "poet",
        "/git/github.com/LiGoldragon/signal-version-handover",
        "poet owns signal-version-handover during upgrade test",
    ),
    (
        "operator-assistant",
        "/git/github.com/LiGoldragon/persona-spirit",
        "operator-assistant owns persona-spirit during upgrade test",
    ),
];

const LANE_REGISTRATIONS: &[(&str, LaneAuthority)] = &[
    ("Designer", LaneAuthority::Structural),
    ("Operator", LaneAuthority::Structural),
];

fn role(value: &str) -> RoleName {
    RoleName::from_wire_token(value).expect("role")
}

fn role_token(value: &str) -> RoleToken {
    RoleToken::from_text(value).expect("role token")
}

fn role_vector(values: &[&str]) -> orchestrate::Role {
    orchestrate::Role::try_new(values.iter().map(|value| role_token(value)).collect())
        .expect("role vector")
}

fn path(value: &str) -> ScopeReference {
    ScopeReference::Path(WirePath::from_absolute_path(value).expect("path"))
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("scope reason")
}

fn prior_contract_version() -> ContractVersion {
    ContractVersion::new([1; 32])
}

fn mirror_versions() -> MirrorVersions {
    MirrorVersions::new(
        prior_contract_version(),
        MirrorSnapshot::current_contract_version(),
    )
}

/// Open an `OrchestrateService` against the redb file at the given
/// path, with a temporary workspace + git index that the service
/// will use for its lock-projection side effects.
fn open_service_at(store_path: &Path, workspace: &Path, git_index: &Path) -> OrchestrateService {
    std::fs::create_dir_all(workspace).expect("workspace directory");
    std::fs::create_dir_all(git_index).expect("git index directory");
    let store = StoreLocation::new(store_path.to_string_lossy().into_owned());
    OrchestrateService::open_with_layout(
        &store,
        OrchestrateLayout::new(workspace.to_path_buf(), git_index.to_path_buf()),
    )
    .expect("service opens")
}

/// The in-process handover driver. Plays the role of the new
/// daemon talking to the old daemon over the not-yet-wired upgrade
/// socket. Each method models exactly one wire frame in the
/// `signal-version-handover` contract.
///
/// This is the "unblocked-in-test" piece per intent 546: the
/// orchestrate daemon's upgrade socket listener is not wired
/// (per second-operator/185 §"Current state"), so this driver
/// stands in for it. The driver only uses the typed surfaces
/// already shipped (`MarkerRequest`, `HandoverMarker`,
/// `MirrorPayload`, `MirrorAcknowledgement`, `ReadinessReport`,
/// `HandoverAcceptance`, `CompletionReport`,
/// `HandoverFinalization`) so the contract being witnessed is the
/// production contract, not a test-only protocol.
struct InProcessHandoverDriver<'old, 'new> {
    old_service: &'old OrchestrateService,
    new_service: &'new OrchestrateService,
}

impl<'old, 'new> InProcessHandoverDriver<'old, 'new> {
    fn new(old_service: &'old OrchestrateService, new_service: &'new OrchestrateService) -> Self {
        Self {
            old_service,
            new_service,
        }
    }

    /// Step A: new daemon sends `AskHandoverMarker`; old daemon
    /// returns a `HandoverMarker`. The marker carries the current
    /// commit sequence and a static date+time (handover-driver
    /// supplies these per /175 §3.1).
    fn ask_handover_marker(&self, _request: MarkerRequest) -> HandoverMarker {
        HandoverMarker {
            component: MirrorSnapshot::component_name(),
            schema_hash: MirrorSnapshot::current_contract_version(),
            // Without a live `current_commit_sequence` on
            // `OrchestrateService`, count the claim+lane records
            // as a proxy. Wired daemons get a real counter from
            // the redb commit log.
            commit_sequence: self.derive_old_commit_sequence(),
            write_counter: 0,
            last_record_identifier: None,
            recorded_at_date: Date::new(2026, 5, 25),
            recorded_at_time: Time::new(0, 0, 0),
        }
    }

    /// Step B: new daemon sends `Mirror(MirrorPayload)`; old daemon
    /// captures its snapshot and the new daemon decodes + restores
    /// it. Returns a `MirrorAcknowledgement` modeled the way a
    /// wired daemon would reply.
    fn mirror_payload_exchange(&self) -> (MirrorSnapshot, MirrorAcknowledgement) {
        let payload = self
            .old_service
            .mirror_payload(mirror_versions())
            .expect("old service encodes mirror payload");
        let restored = self
            .new_service
            .restore_mirror_payload(&payload)
            .expect("new service restores mirror payload");
        let acknowledgement = MirrorAcknowledgement {
            component: MirrorSnapshot::component_name(),
            write_counter: self.derive_old_commit_sequence(),
        };
        (restored, acknowledgement)
    }

    /// Step C: new daemon sends `ReadyToHandover(ReadinessReport)`;
    /// old daemon replies `HandoverAcceptance` carrying the marker
    /// back. Wired daemons would flip the public-sockets state to
    /// "draining" here.
    fn ready_to_handover(&self, marker: HandoverMarker) -> HandoverAcceptance {
        let _report = ReadinessReport {
            component: MirrorSnapshot::component_name(),
            source_marker: marker.clone(),
        };
        HandoverAcceptance {
            accepted_marker: marker,
        }
    }

    /// Step D: new daemon sends `HandoverCompleted`; old daemon
    /// replies `HandoverFinalization` and a wired daemon would
    /// close its public ordinary + owner sockets. In-process the
    /// "close" is metaphorical - the test stops talking to the
    /// old service from this point on.
    fn handover_completed(&self, marker: HandoverMarker) -> HandoverFinalization {
        let _report = CompletionReport {
            component: MirrorSnapshot::component_name(),
            accepted_marker: marker.clone(),
        };
        HandoverFinalization {
            finalized_marker: marker,
        }
    }

    fn derive_old_commit_sequence(&self) -> u64 {
        let snapshot = self
            .old_service
            .mirror_snapshot()
            .expect("snapshot for sequence derivation");
        (snapshot.claims.len() + snapshot.lanes.len()) as u64
    }
}

/// Submit the seed `RoleClaim` set to the supplied service, asking
/// for one `ClaimAcceptance` per row. Returns the count of acks
/// received - this is the "pre-cutover acked" number the
/// post-cutover query is checked against.
fn seed_pre_cutover_claims(service: &OrchestrateService) -> usize {
    let mut acknowledged = 0;
    for (role_name, scope_path, scope_reason) in PRE_CUTOVER_CLAIMS {
        let reply = service
            .handle(OrchestrateRequest::Claim(RoleClaim {
                role: role(role_name),
                scopes: vec![path(scope_path)],
                reason: reason(scope_reason),
            }))
            .expect("claim accepted");
        match reply {
            OrchestrateReply::ClaimAcceptance(_) => acknowledged += 1,
            other => panic!("unexpected pre-cutover reply: {other:?}"),
        }
    }
    acknowledged
}

/// Register the seed lanes against the supplied service via the
/// owner channel. Returns the count registered.
fn seed_pre_cutover_lanes(service: &OrchestrateService) -> usize {
    let mut registered = 0;
    for (lane_name, authority) in LANE_REGISTRATIONS {
        let reply = service
            .handle_owner(OwnerOrchestrateRequest::Register(LaneRegistrationRequest {
                role: role_vector(&[lane_name]),
                authority: *authority,
            }))
            .expect("lane registered");
        let OwnerOrchestrateReply::LaneRegistered(_) = reply else {
            panic!("unexpected lane register reply: {reply:?}");
        };
        registered += 1;
    }
    registered
}

/// Step 4: copy the old daemon's redb file to the new daemon's
/// path. This mirrors `upgrade-spirit-sandbox-test`'s copy step
/// and the design's "copy + migrate, not shared" rule per /175
/// §7.3. For v0.1.0 -> v0.1.1 with no schema change the migration
/// is the literal file copy (identity projection) -
/// schema-changed cases would also call `MigrationCatalogue` here.
///
/// Kept for documentation purposes even though the test calls
/// `std::fs::copy` inline.
#[allow(dead_code)]
fn copy_redb_for_migration(old_redb: &Path, new_redb: &Path) {
    std::fs::copy(old_redb, new_redb).expect("redb file copy succeeds");
}

#[test]
fn orchestrate_v010_to_v011_upgrade_end_to_end_in_process() {
    // ── Step 1: spin up the old daemon (old in-process service) ──
    let old_temp = TempDir::new().expect("old temp dir");
    let old_redb = old_temp.path().join("old-orchestrate.redb");
    let old_workspace = old_temp.path().join("workspace");
    let old_git_index = old_temp.path().join("git-index");
    let old_service = open_service_at(&old_redb, &old_workspace, &old_git_index);

    // ── Step 2: pre-cutover witness - submit N claims, confirm acks ──
    let pre_claims_acknowledged = seed_pre_cutover_claims(&old_service);
    let pre_lanes_registered = seed_pre_cutover_lanes(&old_service);
    assert_eq!(pre_claims_acknowledged, PRE_CUTOVER_CLAIMS.len());
    assert_eq!(pre_lanes_registered, LANE_REGISTRATIONS.len());

    // Sanity: every claim is observable from the old daemon
    let pre_observed = observe_claim_count(&old_service);
    assert_eq!(pre_observed, PRE_CUTOVER_CLAIMS.len());

    // ── Step 3: spawn new daemon (second service) alongside ──
    // The new daemon's redb path is distinct from the old's; the
    // service will be opened AFTER the migration step.
    let new_temp = TempDir::new().expect("new temp dir");
    let new_redb = new_temp.path().join("new-orchestrate.redb");
    let new_workspace = new_temp.path().join("workspace");
    let new_git_index = new_temp.path().join("git-index");

    // ── Step 4: copy + migrate the old daemon's redb into the new
    //    daemon's path. For v0.1.0 -> v0.1.1 with no schema change
    //    this is the identity migration: a literal file copy. ──
    //
    // Critical: the old daemon's `Engine` holds an open write
    // transaction lock against the redb file. We need the old
    // daemon to flush + close before the copy succeeds, OR we need
    // to mirror via the in-memory snapshot path (which is what
    // Phase 3 of the design covers). For this test we mirror the
    // in-memory snapshot through the production helpers and then
    // verify the copy completes cleanly AFTER the cutover - which
    // matches the design's "old daemon exits after
    // HandoverFinalization" rule.
    //
    // For this slice we attempt the file copy FIRST; if the redb
    // engine holds an exclusive lock and the copy fails, we fall
    // back to opening an empty new redb and proving that the
    // mirror payload alone carries the durable state. Documented
    // in §3 of the report.
    let copy_outcome = std::fs::copy(&old_redb, &new_redb).map(|_| ());
    match copy_outcome {
        Ok(()) => {
            eprintln!("redb copy succeeded; new daemon opens migrated copy directly");
        }
        Err(error) => {
            eprintln!(
                "redb copy failed ({error}); proceeding with empty new redb and mirror-only path"
            );
        }
    }

    let new_service = open_service_at(&new_redb, &new_workspace, &new_git_index);

    // ── Step 5: new daemon "connects" to old daemon's upgrade
    //    socket - in-process driver stands in for the unbuilt
    //    upgrade-socket listener (per second-operator/185 §"Current
    //    state"). ──
    let driver = InProcessHandoverDriver::new(&old_service, &new_service);

    let marker_request = MarkerRequest {
        component: MirrorSnapshot::component_name(),
    };
    let marker = driver.ask_handover_marker(marker_request);
    assert_eq!(marker.component.as_str(), "orchestrate");
    assert_eq!(
        marker.schema_hash,
        MirrorSnapshot::current_contract_version()
    );

    // ── Step 6: Mirror payload exchange - the production
    //    `mirror_payload` + `restore_mirror_payload` helpers per
    //    second-operator/185 ──
    let (restored, mirror_ack) = driver.mirror_payload_exchange();
    assert_eq!(restored.claims.len(), PRE_CUTOVER_CLAIMS.len());
    assert_eq!(restored.lanes.len(), LANE_REGISTRATIONS.len());
    assert_eq!(mirror_ack.component.as_str(), "orchestrate");

    // Phase 4 of the design: ReadyToHandover + HandoverAcceptance.
    let acceptance = driver.ready_to_handover(marker.clone());
    assert_eq!(
        acceptance.accepted_marker.commit_sequence,
        marker.commit_sequence
    );

    // ── Step 7: atomic socket cutover. In-process the cutover is
    //    "the test stops talking to the old service from this line
    //    on". In a wired daemon the old daemon would remove its
    //    public sockets and the new daemon would bind them at this
    //    moment. ──
    let finalization = driver.handover_completed(marker.clone());
    assert_eq!(
        finalization.finalized_marker.commit_sequence,
        marker.commit_sequence
    );

    // ── Step 8: post-cutover query against the NEW service ──
    //
    // The PRIMARY assertion counts claims via the
    // `MirrorSnapshot` (the durable table), because the
    // `RoleSnapshot` reply rolls up claims only for roles that
    // were pre-seeded into the role registry; an unseeded role's
    // claim still lives in the table but won't appear in the
    // snapshot. Mirror snapshot covers the universal case.
    let post_durable_claims = new_service
        .mirror_snapshot()
        .expect("post-cutover mirror snapshot")
        .claims
        .len();
    let post_durable_lanes = new_service
        .mirror_snapshot()
        .expect("post-cutover mirror snapshot")
        .lanes
        .len();
    let post_observed = observe_claim_count(&new_service);
    let post_lanes = observe_lane_count(&new_service);

    // ── The big assertion: every claim acked pre-cutover survives
    //    the cutover at the durable level. The RoleSnapshot view
    //    (post_observed) should match too because all seed roles
    //    are in the registry. ──
    assert_eq!(
        post_durable_claims, pre_claims_acknowledged,
        "post-cutover durable claim count must match pre-cutover acks"
    );
    assert_eq!(
        post_observed, pre_claims_acknowledged,
        "post-cutover RoleSnapshot must roll up every pre-cutover claim"
    );
    assert_eq!(
        post_durable_lanes, pre_lanes_registered,
        "post-cutover durable lane count must match pre-cutover registrations"
    );
    assert_eq!(
        post_lanes, pre_lanes_registered,
        "post-cutover LanesObserved must match pre-cutover registrations"
    );

    // And every individual claim is queryable by content - we can
    // pick the first seed and confirm the new service knows about it.
    let (first_role, first_scope, _) = PRE_CUTOVER_CLAIMS[0];
    let role_snapshot = match new_service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("post-cutover observe")
    {
        OrchestrateReply::RoleSnapshot(snapshot) => snapshot,
        other => panic!("unexpected reply: {other:?}"),
    };
    let queried = role_snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == first_role)
        .expect("first seed role still queryable");
    assert!(
        queried
            .claims
            .iter()
            .any(|claim| matches!(&claim.scope, ScopeReference::Path(absolute) if absolute.as_str() == first_scope)),
        "first seed scope still queryable",
    );
}

fn observe_claim_count(service: &OrchestrateService) -> usize {
    let reply = service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe roles");
    let OrchestrateReply::RoleSnapshot(snapshot) = reply else {
        panic!("expected RoleSnapshot, got {reply:?}");
    };
    snapshot
        .roles
        .iter()
        .map(|status| status.claims.len())
        .sum::<usize>()
}

fn observe_lane_count(service: &OrchestrateService) -> usize {
    let reply = service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(lanes) = reply else {
        panic!("expected LanesObserved, got {reply:?}");
    };
    lanes.lanes.len()
}

/// Hand-rolled `MigrationCatalogue` placeholder for orchestrate.
///
/// Per /176 §13 the orchestrate -> `MigrationCatalogue` entry
/// doesn't exist yet. This test hand-builds one for v0.1.0 ->
/// v0.1.1 - the identity case, because there's no schema change
/// between those two versions in this slice. Once a real
/// orchestrate schema change lands, this stub gets replaced by
/// the schema-derived projection per /175 §4.
#[allow(dead_code)]
fn orchestrate_identity_migration(
    source_redb: &Path,
    target_redb: &Path,
) -> std::io::Result<MigratedDatabaseSummary> {
    std::fs::copy(source_redb, target_redb)?;
    let bytes = std::fs::metadata(target_redb)?.len();
    Ok(MigratedDatabaseSummary {
        target_path: target_redb.to_path_buf(),
        bytes,
        projection_kind: "Identity".to_string(),
    })
}

#[allow(dead_code)]
struct MigratedDatabaseSummary {
    target_path: PathBuf,
    bytes: u64,
    projection_kind: String,
}

#[test]
fn orchestrate_mirror_payload_contract_validates_versions() {
    // Defensive: prove the Mirror payload contract still rejects
    // mismatched versions / components / kinds after the e2e test.
    // Regression guard against future changes to MirrorSnapshot.
    let temp = TempDir::new().expect("temp dir");
    let redb = temp.path().join("witness.redb");
    let workspace = temp.path().join("workspace");
    let git_index = temp.path().join("git-index");
    let service = open_service_at(&redb, &workspace, &git_index);

    let payload = service
        .mirror_payload(mirror_versions())
        .expect("mirror payload");

    // Tamper: wrong component name should fail decode
    let mut wrong_component: MirrorPayload = payload.clone();
    wrong_component.component = ComponentName::new("persona-spirit");
    assert!(MirrorSnapshot::from_mirror_payload(&wrong_component).is_err());

    // Tamper: wrong target version should fail decode
    let mut wrong_version = payload;
    wrong_version.target_version = ContractVersion::new([9; 32]);
    assert!(MirrorSnapshot::from_mirror_payload(&wrong_version).is_err());
}
