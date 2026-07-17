use std::ffi::OsString;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use meta_signal_orchestrate::{
    Frame as MetaOrchestrateFrame, FrameBody as MetaOrchestrateFrameBody, MetaOrchestrateRequest,
    RefreshRepositoryIndexOrder,
};
use nota::{NotaDecode, NotaEncode, NotaSource};
use orchestrate::{
    DaemonConfiguration, LaneAssignment, LaneAuthority, LaneDetails, LaneIdentifier, LaneOwner,
    LaneStatus, MirrorSnapshot, MirrorVersions, OrchestrateLayout, OrchestrateService, Role,
    RoleToken, SessionIdentifier, StoreLocation, StoredClaim, StoredLaneRegistration,
    TimestampNanos, WirePath,
};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as FrameReply,
    RequestPayload, SessionEpoch, ShortHeader, SubReply,
};
use signal_orchestrate::{
    Observation, OrchestrateFrame, OrchestrateFrameBody, OrchestrateReply, OrchestrateRequest,
    ScopeReason, ScopeReference,
};
// The CLI now speaks the schema-emitted `Input`/`Output` wire (matching spirit),
// so the end-to-end CLI tests build schema requests and decode schema replies.
// Schema values stay strongly newtyped at the wire edge; tests construct those
// generated nouns directly and unwrap them only when checking filesystem paths.
use meta_signal_orchestrate::schema::lib::{
    CreateRoleOrder as SchemaCreateRoleOrder, HarnessKind as SchemaHarnessKind,
    Input as MetaSchemaInput, LaneAssignment as SchemaLaneAssignment,
    LaneAuthority as SchemaLaneAuthority, LaneDetails as SchemaLaneDetails,
    LaneIdentifier as SchemaLaneIdentifier, LaneOwner as SchemaLaneOwner,
    LaneRegistrationMode as SchemaLaneRegistrationMode,
    LaneRegistrationRequest as SchemaLaneRegistrationRequest, Output as MetaSchemaOutput,
    Role as SchemaRole, SessionIdentifier as SchemaSessionIdentifier,
    SignalFrameError as MetaSignalFrameError,
};
use signal_orchestrate::schema::lib::{
    Input as SchemaInput, Observation as SchemaObservation, Output as SchemaOutput,
    RoleClaim as SchemaRoleClaim, RoleIdentifier as SchemaRoleIdentifier,
    RoleName as SchemaRoleName, RoleToken as SchemaRoleToken, RoleTokens as SchemaRoleTokens,
    ScopeReason as SchemaScopeReason, ScopeReference as SchemaScopeReference,
    WirePath as SchemaWirePath,
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
        // Daemons accept only a binary rkyv startup file (hard override:
        // daemons never parse NOTA). Encode the typed configuration to rkyv.
        let configuration_path = temporary.path().join("daemon.signal");
        std::fs::write(
            &configuration_path,
            configuration.to_signal_bytes().expect("config encode"),
        )
        .expect("config write");

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
            // The actor-shell daemon binds all three listener tiers: working
            // (ordinary), meta, and the version-handover upgrade socket.
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

    fn ordinary_cli(&self, request: impl NotaEncode) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_orchestrate"))
            .env("PERSONA_ORCHESTRATE_SOCKET", &self.ordinary_socket)
            .arg(encode_nota(&request))
            .output()
            .expect("ordinary cli output")
    }

    fn meta_cli(&self, request: impl NotaEncode) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_meta-orchestrate"))
            .env("PERSONA_ORCHESTRATE_META_SOCKET", &self.meta_socket)
            .arg(encode_nota(&request))
            .output()
            .expect("meta cli output")
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn block_on<Future: std::future::Future>(future: Future) -> Future::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn wire_path(path: &Path) -> WirePath {
    WirePath::from_absolute_path(path.to_string_lossy()).expect("wire path")
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

fn schema_role_vector(values: &[&str]) -> SchemaRole {
    SchemaRole::new(SchemaRoleTokens::new(
        values
            .iter()
            .map(|value| SchemaRoleToken::new(*value))
            .collect(),
    ))
}

fn schema_lane_registration(
    session: &str,
    lane: &str,
    role: SchemaRole,
) -> SchemaLaneRegistrationRequest {
    SchemaLaneRegistrationRequest {
        lane_assignment: SchemaLaneAssignment {
            session_identifier: SchemaSessionIdentifier::new(session),
            lane_identifier: SchemaLaneIdentifier::new(lane),
            lane_owner: SchemaLaneOwner {
                role,
                lane_authority: SchemaLaneAuthority::Structural,
            },
            lane_details: SchemaLaneDetails::new("daemon cli lane registration"),
        },
        lane_registration_mode: SchemaLaneRegistrationMode::Fresh,
    }
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("reason")
}

fn encode_nota(value: &impl NotaEncode) -> String {
    value.to_nota()
}

fn decode_nota<Value: NotaDecode>(bytes: &[u8]) -> Value {
    let text = std::str::from_utf8(bytes).expect("utf8").trim();
    NotaSource::new(text).parse::<Value>().expect("decode nota")
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
    // A handover mirror transfers live state: its lanes were last active moments
    // before the handover, so their last-activity stamp is recent. Using a recent
    // `updated_at` keeps the restored lanes clear of the idle-lane reaper, exactly
    // as a real handover of active work would.
    let recent = TimestampNanos::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos() as u64,
    );
    MirrorSnapshot {
        claims: vec![StoredClaim::new(
            lane_identifier("operator"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-upgrade-mirror-claim")
                    .expect("claim path"),
            ),
            reason("upgrade mirror restore"),
            TimestampNanos::new(1),
        )],
        lanes: vec![
            StoredLaneRegistration::new(
                LaneAssignment {
                    session: SessionIdentifier::from_camel_case_name("DaemonCliSession")
                        .expect("session"),
                    lane: lane_identifier("operator"),
                    owner: LaneOwner {
                        role: role_vector(&["Operator"]),
                        authority: LaneAuthority::Structural,
                    },
                    details: LaneDetails::from_text("daemon cli mirror operator lane")
                        .expect("lane details"),
                },
                TimestampNanos::new(1),
                recent,
                LaneStatus::Active,
            ),
            StoredLaneRegistration::new(
                LaneAssignment {
                    session: SessionIdentifier::from_camel_case_name("DaemonCliSession")
                        .expect("session"),
                    lane: lane_identifier("schema-designer-assistant"),
                    owner: LaneOwner {
                        role: role_vector(&["Schema", "Designer"]),
                        authority: LaneAuthority::Support,
                    },
                    details: LaneDetails::from_text("daemon cli mirror lane")
                        .expect("lane details"),
                },
                TimestampNanos::new(1),
                recent,
                LaneStatus::Active,
            ),
        ],
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
    let role = String::from("primary-orchestrate-daemon-zxq9-never-collide");
    let schema_role_identifier = SchemaRoleIdentifier::new(role.clone());
    let schema_role_name = SchemaRoleName::new(schema_role_identifier.clone());

    let output = fixture.meta_cli(MetaSchemaInput::Create(SchemaCreateRoleOrder {
        role_identifier: schema_role_identifier.clone(),
        harness_kind: SchemaHarnessKind::Codex,
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: MetaSchemaOutput = decode_nota(&output.stdout);
    let MetaSchemaOutput::RoleCreated(created) = reply else {
        panic!("expected role created, got {reply:?}");
    };
    assert_eq!(created.role_identifier, schema_role_identifier);
    assert!(Path::new(created.report_repository_path.payload().as_str()).is_dir());
    assert!(Path::new(created.report_lane_path.payload().as_str()).exists());

    let output = fixture.ordinary_cli(SchemaInput::Observe(SchemaObservation::Roles));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: SchemaOutput = decode_nota(&output.stdout);
    let SchemaOutput::RoleSnapshot(snapshot) = reply else {
        panic!("expected role snapshot, got {reply:?}");
    };
    assert!(
        snapshot
            .role_statuses
            .payload()
            .iter()
            .any(|status| status.role_name == schema_role_name)
    );

    let output = fixture.meta_cli(MetaSchemaInput::Register(schema_lane_registration(
        "DaemonCliSession",
        &role,
        schema_role_vector(&[
            "Primary",
            "Orchestrate",
            "Daemon",
            "Zxq9",
            "Never",
            "Collide",
        ]),
    )));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = fixture.ordinary_cli(SchemaInput::Claim(SchemaRoleClaim {
        role_name: schema_role_name,
        scope_references: vec![SchemaScopeReference::Path(SchemaWirePath::new(
            "/tmp/primary-orchestrate-daemon-zxq9-never-collide",
        ))]
        .into(),
        scope_reason: SchemaScopeReason::new("daemon CLI claim projection"),
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
fn cli_unregistered_lane_claim_returns_structured_error_output() {
    let fixture = DaemonFixture::start("orchestrate-cli-unregistered-claim");

    let output = fixture.ordinary_cli(SchemaInput::Claim(SchemaRoleClaim {
        role_name: SchemaRoleName::new(SchemaRoleIdentifier::new("unregistered-audit-lane")),
        scope_references: vec![SchemaScopeReference::Path(SchemaWirePath::new(
            "/tmp/session-lane-audit-unregistered",
        ))]
        .into(),
        scope_reason: SchemaScopeReason::new("should fail without transport failure"),
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("transport error"),
        "unregistered claim must not fail at transport layer: {stderr}"
    );

    let reply: SchemaOutput = decode_nota(&output.stdout);
    let SchemaOutput::PartialApplied(partial) = reply else {
        panic!("expected structured partial failure, got {reply:?}");
    };
    let failure = partial
        .application_failures
        .payload()
        .first()
        .expect("partial failure detail");
    assert_eq!(
        failure.scope_reason.payload(),
        "lane is not registered: unregistered-audit-lane"
    );
}

#[test]
fn cli_invalid_session_name_registration_returns_structured_error_output() {
    let fixture = DaemonFixture::start("orchestrate-cli-invalid-session-name");

    // A hyphenated session identifier is not CamelCase alphanumeric, so the
    // engine rejects it. The rejection must ride the meta reply channel as a
    // typed `PartialApplied` rather than drop the frame with an opaque
    // client-side transport error (bead primary-jf0n).
    let output = fixture.meta_cli(MetaSchemaInput::Register(schema_lane_registration(
        "os-deployment-doctrine",
        "OrchestrateTypedRejection",
        schema_role_vector(&["RustAuditor"]),
    )));
    assert!(
        output.status.success(),
        "invalid session name must not fail at transport layer: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("transport error"),
        "invalid session name must not drop the frame: {stderr}"
    );

    let reply: MetaSchemaOutput = decode_nota(&output.stdout);
    let MetaSchemaOutput::PartialApplied(partial) = reply else {
        panic!("expected structured partial failure, got {reply:?}");
    };
    let failure = partial
        .application_failures
        .payload()
        .first()
        .expect("partial failure detail");
    assert!(
        failure
            .scope_reason
            .payload()
            .contains("session identifier must be CamelCase alphanumeric"),
        "rejection must name the CamelCase rule at the call site, got: {}",
        failure.scope_reason.payload()
    );
    assert!(
        failure
            .scope_reason
            .payload()
            .contains("os-deployment-doctrine"),
        "rejection must name the offending identifier, got: {}",
        failure.scope_reason.payload()
    );
}

#[test]
fn cli_observes_sessions_session_lanes_all_lanes_and_resource_claims() {
    let fixture = DaemonFixture::start("orchestrate-cli-observe-lanes");

    let alpha_register = fixture.meta_cli(MetaSchemaInput::Register(schema_lane_registration(
        "DaemonCliObserveSession",
        "daemon-cli-alpha-observe",
        schema_role_vector(&["Daemon", "Cli", "Alpha", "Observe"]),
    )));
    assert!(
        alpha_register.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&alpha_register.stderr)
    );
    let beta_register = fixture.meta_cli(MetaSchemaInput::Register(schema_lane_registration(
        "SecondDaemonCliObserveSession",
        "daemon-cli-beta-observe",
        schema_role_vector(&["Daemon", "Cli", "Beta", "Observe"]),
    )));
    assert!(
        beta_register.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&beta_register.stderr)
    );

    let claim_output = fixture.ordinary_cli(SchemaInput::Claim(SchemaRoleClaim {
        role_name: SchemaRoleName::new(SchemaRoleIdentifier::new("daemon-cli-alpha-observe")),
        scope_references: vec![SchemaScopeReference::Path(SchemaWirePath::new(
            "/tmp/daemon-cli-alpha-observe",
        ))]
        .into(),
        scope_reason: SchemaScopeReason::new("daemon CLI observe resource claim"),
    }));
    assert!(
        claim_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&claim_output.stderr)
    );

    let sessions_output = fixture.ordinary_cli(SchemaInput::Observe(SchemaObservation::Sessions));
    assert!(
        sessions_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&sessions_output.stderr)
    );
    let sessions_reply: SchemaOutput = decode_nota(&sessions_output.stdout);
    let SchemaOutput::SessionsObserved(sessions) = sessions_reply else {
        panic!("expected sessions observed, got {sessions_reply:?}");
    };
    let daemon_session = sessions
        .payload()
        .payload()
        .iter()
        .find(|projection| {
            projection.session_identifier == SchemaSessionIdentifier::new("DaemonCliObserveSession")
        })
        .expect("daemon cli observe session");
    assert_eq!(daemon_session.integer, 1);

    let empty_session_output =
        fixture.ordinary_cli(SchemaInput::Observe(SchemaObservation::SessionLanes(
            SchemaSessionIdentifier::new("MissingDaemonCliObserveSession"),
        )));
    assert!(
        empty_session_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&empty_session_output.stderr)
    );
    let empty_session_reply: SchemaOutput = decode_nota(&empty_session_output.stdout);
    let SchemaOutput::LanesObserved(empty_lanes) = empty_session_reply else {
        panic!("expected empty lanes observed, got {empty_session_reply:?}");
    };
    assert!(empty_lanes.payload().payload().is_empty());

    let session_lanes_output = fixture.ordinary_cli(SchemaInput::Observe(
        SchemaObservation::SessionLanes(SchemaSessionIdentifier::new("DaemonCliObserveSession")),
    ));
    assert!(
        session_lanes_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&session_lanes_output.stderr)
    );
    let session_lanes_reply: SchemaOutput = decode_nota(&session_lanes_output.stdout);
    let SchemaOutput::LanesObserved(session_lanes) = session_lanes_reply else {
        panic!("expected session lanes observed, got {session_lanes_reply:?}");
    };
    assert_eq!(session_lanes.payload().payload().len(), 1);
    let alpha_lane = &session_lanes.payload().payload()[0];
    assert_eq!(
        alpha_lane.lane_registration.lane_assignment.lane_identifier,
        SchemaLaneIdentifier::new("daemon-cli-alpha-observe")
    );
    assert_eq!(
        alpha_lane.lane_registration.lane_status,
        signal_orchestrate::schema::lib::LaneStatus::Active
    );
    assert_eq!(alpha_lane.lane_resource_claims.payload().len(), 1);
    assert_eq!(
        alpha_lane.lane_resource_claims.payload()[0].scope_reason,
        SchemaScopeReason::new("daemon CLI observe resource claim")
    );

    let all_lanes_output = fixture.ordinary_cli(SchemaInput::Observe(SchemaObservation::Lanes));
    assert!(
        all_lanes_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&all_lanes_output.stderr)
    );
    let all_lanes_reply: SchemaOutput = decode_nota(&all_lanes_output.stdout);
    let SchemaOutput::LanesObserved(all_lanes) = all_lanes_reply else {
        panic!("expected all lanes observed, got {all_lanes_reply:?}");
    };
    assert!(all_lanes.payload().payload().iter().any(|projection| {
        projection.lane_registration.lane_assignment.lane_identifier
            == SchemaLaneIdentifier::new("daemon-cli-beta-observe")
    }));
}

#[test]
fn daemon_drops_legacy_lock_claims_without_registered_lanes() {
    let fixture = DaemonFixture::start_with_legacy_locks(
        "orchestrate-import-legacy-lock",
        &[(
            "system-operator.lock",
            "/git/github.com/LiGoldragon/orchestrate # production cutover\n",
        )],
    );

    let output = fixture.ordinary_cli(SchemaInput::Observe(SchemaObservation::Roles));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: SchemaOutput = decode_nota(&output.stdout);
    let SchemaOutput::RoleSnapshot(snapshot) = reply else {
        panic!("expected role snapshot, got {reply:?}");
    };
    let system_operator = snapshot
        .role_statuses
        .payload()
        .iter()
        .find(|status| {
            status.role_name == SchemaRoleName::new(SchemaRoleIdentifier::new("system-operator"))
        })
        .expect("system-operator role");
    assert!(
        system_operator.claim_entries.payload().is_empty(),
        "legacy role locks must not create ordinary claims without registered lanes"
    );
}

// The version-handover upgrade socket is the daemon's third listener tier. The
// emitter now emits an upgrade listener that routes each accepted connection
// through the engine actor's `handle_upgrade_connection`, which drives the
// handover state machine on the `&mut` engine and retires the public (working +
// meta) sockets on finalization.
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
    let mut service = service;
    let roles = block_on(service.handle(OrchestrateRequest::Observe(Observation::Roles)))
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

    let lanes = block_on(service.handle(OrchestrateRequest::Observe(Observation::Lanes)))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(lanes) = lanes else {
        panic!("expected lane snapshot");
    };
    assert!(lanes.lanes.iter().any(|lane| {
        lane.registration.assignment.lane.as_wire_token() == "schema-designer-assistant"
    }));
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
fn meta_socket_answers_ordinary_frame_with_typed_refusal() {
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
    // A foreign frame that reaches the meta engine is answered with a
    // delivered typed refusal — never a silent close, which callers cannot
    // distinguish from daemon death.
    let bytes = read_length_prefixed_response(&mut stream);
    match MetaSchemaOutput::decode_signal_frame(&bytes[4..]) {
        Err(MetaSignalFrameError::EngineRefused { .. }) => {}
        other => panic!("expected delivered meta engine refusal, got {other:?}"),
    }
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
fn meta_socket_answers_mismatched_short_header_with_typed_refusal() {
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
    // Under the refusal contract a zero short header is a live variant
    // header; the mismatched body reaches the engine and is answered with a
    // delivered typed refusal instead of a silent close.
    let bytes = read_length_prefixed_response(&mut stream);
    match MetaSchemaOutput::decode_signal_frame(&bytes[4..]) {
        Err(MetaSignalFrameError::EngineRefused { .. }) => {}
        other => panic!("expected delivered meta engine refusal, got {other:?}"),
    }
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

#[test]
fn component_clis_reject_the_other_contract_tier() {
    let meta_request = MetaSchemaInput::Create(SchemaCreateRoleOrder {
        role_identifier: SchemaRoleIdentifier::new("wrong-tier-role"),
        harness_kind: SchemaHarnessKind::Codex,
    });
    let ordinary_output = Command::new(env!("CARGO_BIN_EXE_orchestrate"))
        .arg(encode_nota(&meta_request))
        .output()
        .expect("ordinary cli output");
    assert!(!ordinary_output.status.success());
    assert!(
        String::from_utf8_lossy(&ordinary_output.stderr).contains("invalid ordinary orchestrate")
    );

    let ordinary_request = SchemaInput::Observe(SchemaObservation::Roles);
    let meta_output = Command::new(env!("CARGO_BIN_EXE_meta-orchestrate"))
        .arg(encode_nota(&ordinary_request))
        .output()
        .expect("meta cli output");
    assert!(!meta_output.status.success());
    assert!(String::from_utf8_lossy(&meta_output.stderr).contains("invalid meta orchestrate"));
}

#[test]
fn configuration_writer_rejects_non_utf8_configuration_before_creating_directories() {
    let directory = TempDir::new().expect("tempdir");
    let absolute = directory.path();
    let signal_path = absolute.join("generated").join("daemon.signal");
    let store_path = absolute.join("store").join("orchestrate.sema");
    let ordinary_socket_path = absolute.join("sockets").join("ordinary.sock");
    let meta_socket_path = absolute.join("sockets").join("meta.sock");
    let upgrade_socket_path = absolute.join("sockets").join("upgrade.sock");
    let workspace_root = absolute.join("workspace");
    let git_index_root = absolute.join(PathBuf::from(OsString::from_vec(
        b"git-index-\xff".to_vec(),
    )));

    let output = Command::new(env!("CARGO_BIN_EXE_orchestrate-write-configuration"))
        .arg(&signal_path)
        .arg(&store_path)
        .arg(&ordinary_socket_path)
        .arg(&meta_socket_path)
        .arg(&upgrade_socket_path)
        .arg(&workspace_root)
        .arg(&git_index_root)
        .output()
        .expect("writer output");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid orchestrate path"),
        "stderr was {stderr}"
    );
    assert!(
        !absolute.join("generated").exists(),
        "writer must serialize configuration before creating the signal parent"
    );
    assert!(
        !absolute.join("store").exists(),
        "writer must serialize configuration before creating the store parent"
    );
    assert!(
        !absolute.join("sockets").exists(),
        "writer must serialize configuration before creating socket parents"
    );
}
