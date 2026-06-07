use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use meta_signal_orchestrate::{
    CreateRoleOrder, Frame as MetaOrchestrateFrame, FrameBody as MetaOrchestrateFrameBody,
    MetaOrchestrateReply, MetaOrchestrateRequest, RefreshRepositoryIndexOrder,
};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use orchestrate::{
    DaemonConfiguration, HarnessKind, LaneAuthority, LaneIdentifier, LaneRegistration,
    MirrorSnapshot, MirrorVersions, OrchestrateLayout, OrchestrateService, Role, RoleName,
    RoleToken, StoreLocation, StoredClaim, WirePath,
};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as FrameReply,
    RequestPayload, SessionEpoch, ShortHeader, SubReply,
};
use signal_orchestrate::{
    Observation, OrchestrateFrame, OrchestrateFrameBody, OrchestrateReply, OrchestrateRequest,
    RoleClaim, ScopeReason, ScopeReference,
};
use signal_version_handover::{
    CompletionReport, Frame as UpgradeFrame, FrameBody as UpgradeFrameBody, HandoverMarker,
    HandoverRejectionReason, MarkerRequest, MirrorPayload, Operation as UpgradeOperation,
    ReadinessReport, Reply as UpgradeReply,
};
use tempfile::TempDir;
use version_projection::ContractVersion;

struct DaemonFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    git_index: PathBuf,
    store: PathBuf,
    ordinary_socket: PathBuf,
    meta_socket: PathBuf,
    upgrade_socket: PathBuf,
    child: Child,
}

impl DaemonFixture {
    fn start(name: &str) -> Self {
        Self::start_with_legacy_locks(name, &[])
    }

    fn start_with_legacy_locks(name: &str, legacy_locks: &[(&str, &str)]) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("reports")).expect("reports directory");
        std::fs::create_dir_all(workspace.join("repos")).expect("repos directory");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\ndesigner\nsystem-operator\n",
        )
        .expect("role registry");
        for (file_name, body) in legacy_locks {
            std::fs::write(workspace.join("orchestrate").join(file_name), body)
                .expect("legacy lock");
        }
        std::fs::create_dir_all(&git_index).expect("git index directory");

        let store = temporary.path().join("orchestrate.sema");
        let ordinary_socket = temporary.path().join("ordinary.sock");
        let meta_socket = temporary.path().join("meta.sock");
        let upgrade_socket = temporary.path().join("upgrade.sock");
        let configuration = DaemonConfiguration::new(
            wire_path(&store),
            wire_path(&ordinary_socket),
            wire_path(&meta_socket),
            wire_path(&upgrade_socket),
            wire_path(&workspace),
            wire_path(&git_index),
        );
        let configuration_path = temporary.path().join("daemon.nota");
        std::fs::write(&configuration_path, encode_nota(&configuration)).expect("config write");

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
            meta_socket,
            upgrade_socket,
            child,
        };
        fixture.wait_for_sockets();
        fixture
    }

    fn wait_for_sockets(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.ordinary_socket.exists()
                && self.meta_socket.exists()
                && self.upgrade_socket.exists()
            {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("daemon status") {
                panic!("daemon exited before sockets existed: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("daemon sockets were not created");
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn cli(&self, request: impl NotaEncode) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_orchestrate"))
            .env("PERSONA_ORCHESTRATE_SOCKET", &self.ordinary_socket)
            .env("PERSONA_ORCHESTRATE_OWNER_SOCKET", &self.meta_socket)
            .arg(encode_nota(&request))
            .output()
            .expect("cli output")
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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

fn lane_identifier(value: &str) -> LaneIdentifier {
    LaneIdentifier::from_wire_token(value).expect("lane")
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("reason")
}

fn encode_nota(value: &impl NotaEncode) -> String {
    let mut encoder = Encoder::new();
    value.encode(&mut encoder).expect("encode nota");
    encoder.into_string()
}

fn decode_nota<Value: NotaDecode>(bytes: &[u8]) -> Value {
    let text = std::str::from_utf8(bytes).expect("utf8").trim();
    let mut decoder = Decoder::new(text);
    Value::decode(&mut decoder).expect("decode nota")
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn upgrade_reply(socket: &Path, operation: UpgradeOperation) -> UpgradeReply {
    let request = operation.into_request();
    let short_header = request.short_header();
    let frame = UpgradeFrame::with_short_header(
        short_header,
        UpgradeFrameBody::Request {
            exchange: exchange(),
            request,
        },
    );
    let mut stream = UnixStream::connect(socket).expect("connect upgrade");
    stream
        .write_all(
            &frame
                .encode_length_prefixed()
                .expect("encode upgrade frame"),
        )
        .expect("write upgrade frame");
    let bytes = read_length_prefixed_response(&mut stream);
    let frame = UpgradeFrame::decode_length_prefixed(&bytes).expect("decode upgrade reply");
    let UpgradeFrameBody::Reply { reply, .. } = frame.into_body() else {
        panic!("expected upgrade reply");
    };
    first_upgrade_reply(reply)
}

fn first_upgrade_reply(reply: FrameReply<UpgradeReply>) -> UpgradeReply {
    let FrameReply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        panic!("expected committed upgrade reply");
    };
    match per_operation.into_head() {
        SubReply::Ok(reply) => reply,
        SubReply::Invalidated | SubReply::Skipped | SubReply::Failed { .. } => {
            panic!("expected successful upgrade operation")
        }
    }
}

fn read_length_prefixed_response(stream: &mut UnixStream) -> Vec<u8> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .expect("read response prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut payload = vec![0_u8; length];
    stream
        .read_exact(&mut payload)
        .expect("read response payload");
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(&payload);
    bytes
}

fn complete_handover(fixture: &DaemonFixture) -> HandoverMarker {
    let component = MirrorSnapshot::component_name();
    let marker = match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component.clone(),
        }),
    ) {
        UpgradeReply::HandoverMarker(marker) => marker,
        reply => panic!("expected handover marker, got {reply:?}"),
    };
    let accepted = match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::ReadyToHandover(ReadinessReport {
            component: component.clone(),
            source_marker: marker,
        }),
    ) {
        UpgradeReply::HandoverAccepted(accepted) => accepted.accepted_marker,
        reply => panic!("expected handover acceptance, got {reply:?}"),
    };
    match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::HandoverCompleted(CompletionReport {
            component,
            accepted_marker: accepted.clone(),
        }),
    ) {
        UpgradeReply::HandoverFinalized(finalized) => finalized.finalized_marker,
        reply => panic!("expected handover finalization, got {reply:?}"),
    }
}

fn test_mirror_payload() -> MirrorPayload {
    MirrorSnapshot {
        claims: vec![StoredClaim::new(
            role("operator"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-upgrade-mirror-claim")
                    .expect("claim path"),
            ),
            reason("upgrade mirror restore"),
        )],
        lanes: vec![LaneRegistration {
            lane: lane_identifier("schema-designer-assistant"),
            role: role_vector(&["Schema", "Designer"]),
            authority: LaneAuthority::Support,
        }],
    }
    .into_mirror_payload(MirrorVersions::new(
        ContractVersion::new([1; 32]),
        MirrorSnapshot::current_contract_version(),
    ))
    .expect("mirror payload")
}

#[test]
fn cli_creates_dynamic_role_through_daemon_meta_socket() {
    let fixture = DaemonFixture::start("orchestrate-cli-role");
    let role = role("primary-orchestrate-daemon-zxq9-never-collide");

    let output = fixture.cli(MetaOrchestrateRequest::Create(CreateRoleOrder {
        role: role.clone(),
        harness: HarnessKind::Codex,
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: MetaOrchestrateReply = decode_nota(&output.stdout);
    let MetaOrchestrateReply::RoleCreated(created) = reply else {
        panic!("expected role created");
    };
    assert_eq!(created.role, role);
    assert!(Path::new(created.report_repository_path.as_str()).is_dir());
    assert!(Path::new(created.report_lane_path.as_str()).exists());

    let output = fixture.cli(OrchestrateRequest::Observe(Observation::Roles));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: OrchestrateReply = decode_nota(&output.stdout);
    let OrchestrateReply::RoleSnapshot(snapshot) = reply else {
        panic!("expected role snapshot");
    };
    assert!(snapshot.roles.iter().any(
        |status| status.role.as_wire_token() == "primary-orchestrate-daemon-zxq9-never-collide"
    ));

    let output = fixture.cli(OrchestrateRequest::Claim(RoleClaim {
        role,
        scopes: vec![ScopeReference::Path(
            WirePath::from_absolute_path("/tmp/primary-orchestrate-daemon-zxq9-never-collide")
                .expect("claim path"),
        )],
        reason: ScopeReason::from_text("daemon CLI claim projection").expect("reason"),
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lock_path = fixture
        .workspace
        .join("orchestrate")
        .join("primary-orchestrate-daemon-zxq9-never-collide.lock");
    assert_eq!(
        std::fs::read_to_string(lock_path).expect("lock file"),
        "/tmp/primary-orchestrate-daemon-zxq9-never-collide # daemon CLI claim projection\n"
    );
}

#[test]
fn daemon_imports_legacy_lock_file_claims_on_empty_store() {
    let fixture = DaemonFixture::start_with_legacy_locks(
        "orchestrate-import-legacy-lock",
        &[(
            "system-operator.lock",
            "/git/github.com/LiGoldragon/orchestrate # production cutover\n",
        )],
    );

    let output = fixture.cli(OrchestrateRequest::Observe(Observation::Roles));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: OrchestrateReply = decode_nota(&output.stdout);
    let OrchestrateReply::RoleSnapshot(snapshot) = reply else {
        panic!("expected role snapshot");
    };
    let system_operator = snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == "system-operator")
        .expect("system-operator role");
    assert!(system_operator.claims.iter().any(|claim| matches!(
        &claim.scope,
        ScopeReference::Path(path)
            if path.as_str() == "/git/github.com/LiGoldragon/orchestrate"
    )));
}

#[test]
fn upgrade_socket_serves_marker_readiness_completion_and_retires_public_paths() {
    let fixture = DaemonFixture::start("orchestrate-upgrade-complete");
    let finalized = complete_handover(&fixture);

    assert_eq!(finalized.component, MirrorSnapshot::component_name());
    assert_eq!(
        finalized.schema_hash,
        MirrorSnapshot::current_contract_version()
    );
    assert!(!fixture.ordinary_socket.exists());
    assert!(!fixture.meta_socket.exists());
    assert!(fixture.upgrade_socket.exists());
}

#[test]
fn upgrade_socket_accepts_mirror_before_readiness_and_persists_snapshot() {
    let mut fixture = DaemonFixture::start("orchestrate-upgrade-mirror");
    let component = MirrorSnapshot::component_name();
    let marker = match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component.clone(),
        }),
    ) {
        UpgradeReply::HandoverMarker(marker) => marker,
        reply => panic!("expected handover marker, got {reply:?}"),
    };

    let mirror_reply = upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::Mirror(test_mirror_payload()),
    );
    let UpgradeReply::MirrorAcknowledged(acknowledgement) = mirror_reply else {
        panic!("expected mirror acknowledgement, got {mirror_reply:?}");
    };
    assert_eq!(acknowledgement.component, component);

    let accepted_marker = match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::ReadyToHandover(ReadinessReport {
            component: component.clone(),
            source_marker: marker,
        }),
    ) {
        UpgradeReply::HandoverAccepted(accepted) => accepted.accepted_marker,
        reply => panic!("expected handover accepted, got {reply:?}"),
    };
    let finalized_marker = match upgrade_reply(
        &fixture.upgrade_socket,
        UpgradeOperation::HandoverCompleted(CompletionReport {
            component,
            accepted_marker,
        }),
    ) {
        UpgradeReply::HandoverFinalized(finalized) => finalized.finalized_marker,
        reply => panic!("expected handover finalized, got {reply:?}"),
    };
    assert_eq!(
        finalized_marker.schema_hash,
        MirrorSnapshot::current_contract_version()
    );

    fixture.stop();
    let service = OrchestrateService::open_with_layout(
        &StoreLocation::new(fixture.store.to_string_lossy().into_owned()),
        OrchestrateLayout::new(fixture.workspace.clone(), fixture.git_index.clone()),
    )
    .expect("reopen service");
    let roles = service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe roles");
    let OrchestrateReply::RoleSnapshot(roles) = roles else {
        panic!("expected role snapshot");
    };
    let operator = roles
        .roles
        .iter()
        .find(|role| role.role.as_wire_token() == "operator")
        .expect("operator role");
    assert!(operator.claims.iter().any(|claim| matches!(
        &claim.scope,
        ScopeReference::Path(path)
            if path.as_str() == "/tmp/orchestrate-upgrade-mirror-claim"
    )));

    let lanes = service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(lanes) = lanes else {
        panic!("expected lane snapshot");
    };
    assert!(
        lanes
            .lanes
            .iter()
            .any(|lane| lane.lane.as_wire_token() == "schema-designer-assistant")
    );
}

#[test]
fn upgrade_socket_rejects_wrong_mirror_target() {
    let fixture = DaemonFixture::start("orchestrate-upgrade-mirror-reject");
    let mut payload = test_mirror_payload();
    payload.target_version = ContractVersion::new([9; 32]);

    let reply = upgrade_reply(&fixture.upgrade_socket, UpgradeOperation::Mirror(payload));
    let UpgradeReply::HandoverRejected(rejection) = reply else {
        panic!("expected handover rejection, got {reply:?}");
    };
    assert_eq!(rejection.reason, HandoverRejectionReason::SchemaMismatch);
}

#[test]
fn ordinary_socket_rejects_meta_frame() {
    let fixture = DaemonFixture::start("orchestrate-meta-reject");
    let frame = MetaOrchestrateFrame::new(MetaOrchestrateFrameBody::Request {
        exchange: exchange(),
        request: MetaOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {}).into_request(),
    });
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn upgrade_socket_rejects_ordinary_frame() {
    let fixture = DaemonFixture::start("orchestrate-upgrade-reject");
    let frame = OrchestrateFrame::new(OrchestrateFrameBody::Request {
        exchange: exchange(),
        request: OrchestrateRequest::Observe(Observation::Roles).into_request(),
    });
    let mut stream = UnixStream::connect(&fixture.upgrade_socket).expect("connect upgrade");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn meta_socket_rejects_ordinary_frame() {
    let fixture = DaemonFixture::start("orchestrate-ordinary-reject");
    let frame = OrchestrateFrame::new(OrchestrateFrameBody::Request {
        exchange: exchange(),
        request: OrchestrateRequest::Observe(Observation::Roles).into_request(),
    });
    let mut stream = UnixStream::connect(&fixture.meta_socket).expect("connect meta");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn ordinary_socket_rejects_mismatched_short_header_before_dispatch() {
    let fixture = DaemonFixture::start("orchestrate-ordinary-header");
    let frame = OrchestrateFrame::with_short_header(
        ShortHeader::new(0),
        OrchestrateFrameBody::Request {
            exchange: exchange(),
            request: OrchestrateRequest::Observe(Observation::Roles).into_request(),
        },
    );
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn meta_socket_rejects_mismatched_short_header_before_dispatch() {
    let fixture = DaemonFixture::start("orchestrate-meta-header");
    let frame = MetaOrchestrateFrame::with_short_header(
        ShortHeader::new(0),
        MetaOrchestrateFrameBody::Request {
            exchange: exchange(),
            request: MetaOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {}).into_request(),
        },
    );
    let mut stream = UnixStream::connect(&fixture.meta_socket).expect("connect meta");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn daemon_rejects_non_signal_traffic_on_ordinary_socket() {
    let fixture = DaemonFixture::start("orchestrate-non-signal");
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream.write_all(b"junk").expect("write junk");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn cli_rejects_flag_style_argument_shapes() {
    let output = Command::new(env!("CARGO_BIN_EXE_orchestrate"))
        .args(["--role", "operator"])
        .output()
        .expect("cli output");
    assert!(!output.status.success());
}
