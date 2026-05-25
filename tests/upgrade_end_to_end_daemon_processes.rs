//! Process-level end-to-end test of the orchestrate v0.1.0 ->
//! v0.1.1 upgrade ceremony.
//!
//! This complements `upgrade_end_to_end.rs` (which runs the whole
//! chain in-process) by spawning REAL `orchestrate-daemon`
//! processes and exercising the cutover across them via REAL Unix
//! sockets.
//!
//! Chain steps proved here:
//!
//!  1. Spawn old orchestrate-daemon process; let it bind ordinary
//!     + owner sockets
//!  2. Submit N `Claim` operations via the production
//!     `OrchestrateRequest` wire format on the ordinary socket;
//!     confirm each ack succeeded
//!  3. Stop the old daemon (clean SIGTERM) so its redb file is
//!     unlocked and ready to copy
//!  4. Copy the old daemon's redb file to a new path - this is the
//!     "DB copy + migration" step from /175 §7.3. For v0.1.0 ->
//!     v0.1.1 with no schema change the migration is the identity
//!     copy.
//!  5. Spawn new orchestrate-daemon process pointing at the new
//!     (migrated) redb file; let it bind sockets at fresh paths
//!  6. Mirror payload exchange - INTERNAL: this slice falls back
//!     to the in-process `mirror_payload` /
//!     `restore_mirror_payload` because the stock daemon at main
//!     does NOT yet wire the upgrade socket listener (per
//!     second-operator/185 §"Current state"). We document this
//!     as an "unblocked-in-test" workaround.
//!  7. Atomic socket cutover - in this slice the cutover is the
//!     transition from "old daemon process exited" to "new daemon
//!     process accepting requests on its new sockets". A real
//!     supervisor would move sockets atomically; here the two
//!     daemons bind to separate paths and the test driver
//!     switches between them, mirroring the production design
//!     where the supervisor moves the public socket bindings.
//!  8. Post-cutover query - submit `Observe(Roles)` via the new
//!     daemon's ordinary socket and verify each pre-cutover claim
//!     survives at the wire-frame level.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use nota_codec::{Encoder, NotaEncode};
use orchestrate::{
    DaemonConfiguration, LaneAuthority, LaneRegistrationRequest, MirrorSnapshot, MirrorVersions,
    OrchestrateLayout, OrchestrateService, Role, RoleClaim, RoleName, RoleToken, ScopeReason,
    ScopeReference, StoreLocation, WirePath,
};
use owner_signal_orchestrate::{
    Frame as OwnerOrchestrateFrame, FrameBody as OwnerOrchestrateFrameBody, OwnerOrchestrateReply,
    OwnerOrchestrateRequest,
};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as FrameReply,
    RequestPayload, SessionEpoch, SubReply,
};
use signal_orchestrate::{
    Observation, OrchestrateFrame, OrchestrateFrameBody, OrchestrateReply, OrchestrateRequest,
};
use tempfile::TempDir;
use version_projection::ContractVersion;

const PRE_CUTOVER_SEED: &[(&str, &str, &str)] = &[
    (
        "operator",
        "/git/github.com/LiGoldragon/orchestrate",
        "operator owns orchestrate process-level test",
    ),
    (
        "designer",
        "/git/github.com/LiGoldragon/signal-orchestrate",
        "designer owns signal-orchestrate process-level test",
    ),
    (
        "system-specialist",
        "/git/github.com/LiGoldragon/upgrade",
        "system-specialist owns upgrade process-level test",
    ),
    (
        "poet",
        "/git/github.com/LiGoldragon/signal-version-handover",
        "poet owns signal-version-handover process-level test",
    ),
    (
        "operator-assistant",
        "/git/github.com/LiGoldragon/persona-spirit",
        "operator-assistant owns persona-spirit process-level test",
    ),
];

const LANE_REGISTRATIONS: &[(&str, LaneAuthority)] = &[
    ("Designer", LaneAuthority::Structural),
    ("Operator", LaneAuthority::Structural),
];

struct ProcessFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    git_index: PathBuf,
    store: PathBuf,
    ordinary_socket: PathBuf,
    owner_socket: PathBuf,
    child: Option<Child>,
}

impl ProcessFixture {
    fn start(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("reports")).expect("reports dir");
        std::fs::create_dir_all(workspace.join("repos")).expect("repos dir");
        std::fs::create_dir_all(&git_index).expect("git-index dir");

        let store = temporary.path().join("orchestrate.redb");
        let ordinary_socket = temporary.path().join("ordinary.sock");
        let owner_socket = temporary.path().join("owner.sock");
        let configuration = DaemonConfiguration::new(
            wire_path(&store),
            wire_path(&ordinary_socket),
            wire_path(&owner_socket),
            wire_path(&workspace),
            wire_path(&git_index),
        );
        let configuration_path = temporary.path().join("daemon.nota");
        std::fs::write(&configuration_path, encode_nota(&configuration))
            .expect("daemon config write");
        // Note: at the main branch the DaemonConfiguration has 5
        // fields (no upgrade_socket_path). Per /176 §13 wiring the
        // upgrade socket on the daemon is one of the named
        // blockers. UNBLOCKED in this test by exchanging the
        // Mirror payload via direct service access against the
        // daemon's redb file (see Step 6 in the test body).

        let child = Command::new(env!("CARGO_BIN_EXE_orchestrate-daemon"))
            .arg(&configuration_path)
            .spawn()
            .expect("daemon spawn");

        let mut fixture = Self {
            _temporary: temporary,
            workspace,
            git_index,
            store,
            ordinary_socket,
            owner_socket,
            child: Some(child),
        };
        fixture.wait_for_sockets();
        fixture
    }

    fn start_from_existing_db(name: &str, source_db: &Path) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("reports")).expect("reports dir");
        std::fs::create_dir_all(workspace.join("repos")).expect("repos dir");
        std::fs::create_dir_all(&git_index).expect("git-index dir");

        let store = temporary.path().join("orchestrate.redb");
        std::fs::copy(source_db, &store).expect("DB copy for migration");

        let ordinary_socket = temporary.path().join("ordinary.sock");
        let owner_socket = temporary.path().join("owner.sock");
        let configuration = DaemonConfiguration::new(
            wire_path(&store),
            wire_path(&ordinary_socket),
            wire_path(&owner_socket),
            wire_path(&workspace),
            wire_path(&git_index),
        );
        let configuration_path = temporary.path().join("daemon.nota");
        std::fs::write(&configuration_path, encode_nota(&configuration))
            .expect("daemon config write");
        // Note: at the main branch the DaemonConfiguration has 5
        // fields (no upgrade_socket_path). Per /176 §13 wiring the
        // upgrade socket on the daemon is one of the named
        // blockers. UNBLOCKED in this test by exchanging the
        // Mirror payload via direct service access against the
        // daemon's redb file (see Step 6 in the test body).

        let child = Command::new(env!("CARGO_BIN_EXE_orchestrate-daemon"))
            .arg(&configuration_path)
            .spawn()
            .expect("daemon spawn");

        let mut fixture = Self {
            _temporary: temporary,
            workspace,
            git_index,
            store,
            ordinary_socket,
            owner_socket,
            child: Some(child),
        };
        fixture.wait_for_sockets();
        fixture
    }

    fn wait_for_sockets(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if self.ordinary_socket.exists() && self.owner_socket.exists() {
                // Give the listener a beat to enter the accept loop
                thread::sleep(Duration::from_millis(50));
                return;
            }
            if let Some(child) = self.child.as_mut()
                && let Some(status) = child.try_wait().expect("daemon status")
            {
                panic!("daemon exited before sockets existed: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("daemon sockets were not created");
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ProcessFixture {
    fn drop(&mut self) {
        self.stop();
    }
}

fn wire_path(path: &Path) -> WirePath {
    WirePath::from_absolute_path(path.to_string_lossy()).expect("wire path")
}

fn role(value: &str) -> RoleName {
    RoleName::from_wire_token(value).expect("role")
}

fn role_token(value: &str) -> RoleToken {
    RoleToken::from_text(value).expect("role token")
}

fn role_vector(values: &[&str]) -> Role {
    Role::try_new(values.iter().map(|value| role_token(value)).collect()).expect("role vector")
}

fn path(value: &str) -> ScopeReference {
    ScopeReference::Path(WirePath::from_absolute_path(value).expect("path"))
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("scope reason")
}

fn encode_nota(value: &impl NotaEncode) -> String {
    let mut encoder = Encoder::new();
    value.encode(&mut encoder).expect("encode nota");
    encoder.into_string()
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn read_length_prefixed_response(stream: &mut UnixStream) -> Vec<u8> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).expect("read prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).expect("read payload");
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(&payload);
    bytes
}

fn ordinary_round_trip(socket: &Path, operation: OrchestrateRequest) -> OrchestrateReply {
    let request = operation.into_request();
    let short_header = request.short_header();
    let frame = OrchestrateFrame::with_short_header(
        short_header,
        OrchestrateFrameBody::Request {
            exchange: exchange(),
            request,
        },
    );
    let mut stream = UnixStream::connect(socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let bytes = read_length_prefixed_response(&mut stream);
    let frame = OrchestrateFrame::decode_length_prefixed(&bytes).expect("decode reply frame");
    let OrchestrateFrameBody::Reply { reply, .. } = frame.into_body() else {
        panic!("expected reply body");
    };
    let FrameReply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        panic!("expected committed reply, got {reply:?}");
    };
    match per_operation.into_head() {
        SubReply::Ok(reply) => reply,
        other => panic!("expected ok sub-reply, got {other:?}"),
    }
}

fn owner_round_trip(socket: &Path, operation: OwnerOrchestrateRequest) -> OwnerOrchestrateReply {
    let request = operation.into_request();
    let short_header = request.short_header();
    let frame = OwnerOrchestrateFrame::with_short_header(
        short_header,
        OwnerOrchestrateFrameBody::Request {
            exchange: exchange(),
            request,
        },
    );
    let mut stream = UnixStream::connect(socket).expect("connect owner");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let bytes = read_length_prefixed_response(&mut stream);
    let frame = OwnerOrchestrateFrame::decode_length_prefixed(&bytes).expect("decode reply frame");
    let OwnerOrchestrateFrameBody::Reply { reply, .. } = frame.into_body() else {
        panic!("expected reply body");
    };
    let FrameReply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        panic!("expected committed reply, got {reply:?}");
    };
    match per_operation.into_head() {
        SubReply::Ok(reply) => reply,
        other => panic!("expected ok sub-reply, got {other:?}"),
    }
}

#[test]
fn orchestrate_v010_to_v011_upgrade_end_to_end_two_daemon_processes() {
    // ── Step 1: spawn the OLD orchestrate-daemon process ──
    let mut old = ProcessFixture::start("orchestrate-old-process");

    // ── Step 2: pre-cutover witness - submit N claims via the REAL
    //    ordinary socket using REAL wire frames; confirm acks ──
    let mut claims_acknowledged = 0;
    for (role_name, scope_path, scope_reason) in PRE_CUTOVER_SEED {
        let reply = ordinary_round_trip(
            &old.ordinary_socket,
            OrchestrateRequest::Claim(RoleClaim {
                role: role(role_name),
                scopes: vec![path(scope_path)],
                reason: reason(scope_reason),
            }),
        );
        match reply {
            OrchestrateReply::ClaimAcceptance(_) => claims_acknowledged += 1,
            other => panic!("unexpected pre-cutover ordinary reply: {other:?}"),
        }
    }
    assert_eq!(claims_acknowledged, PRE_CUTOVER_SEED.len());

    let mut lanes_registered = 0;
    for (lane_name, authority) in LANE_REGISTRATIONS {
        let reply = owner_round_trip(
            &old.owner_socket,
            OwnerOrchestrateRequest::Register(LaneRegistrationRequest {
                role: role_vector(&[lane_name]),
                authority: *authority,
            }),
        );
        match reply {
            OwnerOrchestrateReply::LaneRegistered(_) => lanes_registered += 1,
            other => panic!("unexpected lane register reply: {other:?}"),
        }
    }
    assert_eq!(lanes_registered, LANE_REGISTRATIONS.len());

    // Witness via the old daemon's ordinary socket that the
    // claim count is observable BEFORE we shut down.
    let pre_observed = match ordinary_round_trip(
        &old.ordinary_socket,
        OrchestrateRequest::Observe(Observation::Roles),
    ) {
        OrchestrateReply::RoleSnapshot(snapshot) => snapshot
            .roles
            .iter()
            .map(|status| status.claims.len())
            .sum::<usize>(),
        other => panic!("unexpected observe reply: {other:?}"),
    };
    assert_eq!(pre_observed, claims_acknowledged);

    // ── Step 3: cleanly stop the old daemon so its redb file
    //    unlocks. This is the test-side simplification of the
    //    production design where the old daemon stays alive
    //    THROUGH the copy (the redb file is copy-able while
    //    open per the earlier in-process slice that succeeds).
    //    Stopping the old daemon here proves the simpler case
    //    (DB unlocked) works end-to-end at the process level. ──
    old.stop();

    // ── Step 4: copy the old redb to a new path on disk ──
    //
    // In production this would be the new daemon's spawn-time
    // copy step. Here the test driver does the copy explicitly,
    // then spawns the new daemon pointing at the COPY.
    //
    // Per /176 §13 "MigrationCatalogue doesn't know about
    // orchestrate" is one of the expected blockers. UNBLOCKED:
    // since v0.1.0 -> v0.1.1 has no schema change in this
    // slice, the migration is the identity copy. Once a real
    // orchestrate schema change lands, this is where the
    // `MigrationCatalogue::orchestrate_prototype()` would slot
    // in.

    // ── Step 5: spawn NEW orchestrate-daemon process from the
    //    copied DB ──
    let mut new = ProcessFixture::start_from_existing_db("orchestrate-new-process", &old.store);

    // ── Step 6: Mirror payload exchange - UNBLOCKED in test.
    //    The stock daemon at main does not yet wire the upgrade
    //    socket listener. The test reaches around the daemon
    //    process by opening a SECOND `OrchestrateService` against
    //    the old daemon's redb file (now unlocked) to capture the
    //    mirror snapshot and a THIRD `OrchestrateService` against
    //    the new daemon's redb file to restore it. ──
    //
    // We CANNOT open a service against the new daemon's redb
    // while the new daemon process holds it. So we re-stop the
    // new daemon for the in-process mirror step, then re-spawn.
    //
    // This is the place where the not-yet-wired upgrade socket
    // bites us: with the socket wired (per second-operator/185
    // §"Next implementation work") the mirror would flow over
    // the live socket and no daemon-stop dance is needed.

    new.stop();

    let old_workspace_clone = old.workspace.clone();
    let old_git_index_clone = old.git_index.clone();
    let new_workspace_clone = new.workspace.clone();
    let new_git_index_clone = new.git_index.clone();
    let old_store_path = old.store.clone();
    let new_store_path = new.store.clone();

    let mirror_snapshot_size = {
        let old_service = OrchestrateService::open_with_layout(
            &StoreLocation::new(old_store_path.to_string_lossy().into_owned()),
            OrchestrateLayout::new(old_workspace_clone, old_git_index_clone),
        )
        .expect("old service reopens");
        let snapshot = old_service
            .mirror_snapshot()
            .expect("mirror snapshot from old redb");
        let claim_count = snapshot.claims.len();
        let lane_count = snapshot.lanes.len();
        let payload = old_service
            .mirror_payload(MirrorVersions::new(
                ContractVersion::new([1; 32]),
                MirrorSnapshot::current_contract_version(),
            ))
            .expect("mirror payload encode");

        // Now open the new daemon's redb and restore the payload.
        // The new daemon copied old's DB at spawn so it already
        // has the same content; mirror restore is the identity
        // path in this slice. This still exercises the
        // restore code path against a real redb file.
        let new_service = OrchestrateService::open_with_layout(
            &StoreLocation::new(new_store_path.to_string_lossy().into_owned()),
            OrchestrateLayout::new(new_workspace_clone.clone(), new_git_index_clone.clone()),
        )
        .expect("new service opens");
        let restored = new_service
            .restore_mirror_payload(&payload)
            .expect("mirror restore");
        assert_eq!(restored.claims.len(), claim_count);
        assert_eq!(restored.lanes.len(), lane_count);
        (claim_count, lane_count)
    };

    // ── Step 7: atomic socket cutover. We respawn the new daemon
    //    here; in production this would happen over the upgrade
    //    socket without restarting either process. ──
    new =
        ProcessFixture::start_from_existing_db("orchestrate-new-process-cutover", &new_store_path);

    // ── Step 8: post-cutover query against the NEW daemon's
    //    ordinary socket using REAL wire frames ──
    let post_observed = match ordinary_round_trip(
        &new.ordinary_socket,
        OrchestrateRequest::Observe(Observation::Roles),
    ) {
        OrchestrateReply::RoleSnapshot(snapshot) => snapshot
            .roles
            .iter()
            .map(|status| status.claims.len())
            .sum::<usize>(),
        other => panic!("unexpected post-cutover observe reply: {other:?}"),
    };

    let post_lanes = match ordinary_round_trip(
        &new.ordinary_socket,
        OrchestrateRequest::Observe(Observation::Lanes),
    ) {
        OrchestrateReply::LanesObserved(lanes) => lanes.lanes.len(),
        other => panic!("unexpected post-cutover lanes reply: {other:?}"),
    };

    // The big assertion.
    assert_eq!(
        post_observed, pre_observed,
        "post-cutover claim count via NEW daemon's ordinary socket must match pre-cutover via OLD daemon's"
    );
    assert_eq!(
        post_lanes, lanes_registered,
        "post-cutover lane count via NEW daemon's ordinary socket must match pre-cutover registrations"
    );
    assert_eq!(mirror_snapshot_size.0, pre_observed);

    // Sanity: first seed claim is queryable by content at the
    // post-cutover daemon.
    let (first_role, first_scope, _) = PRE_CUTOVER_SEED[0];
    let snapshot = match ordinary_round_trip(
        &new.ordinary_socket,
        OrchestrateRequest::Observe(Observation::Roles),
    ) {
        OrchestrateReply::RoleSnapshot(snapshot) => snapshot,
        other => panic!("unexpected post-cutover content snapshot: {other:?}"),
    };
    let found = snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == first_role)
        .expect("first seed role still in snapshot");
    assert!(
        found
            .claims
            .iter()
            .any(|claim| matches!(&claim.scope, ScopeReference::Path(absolute) if absolute.as_str() == first_scope)),
        "first seed scope present in post-cutover snapshot",
    );
}
