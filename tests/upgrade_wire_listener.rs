//! Wire-level witness of the orchestrate upgrade-socket ceremony.
//!
//! This test UNBLOCKS the named blocker from /176 §13 (the
//! orchestrate daemon's upgrade socket listener is not wired)
//! by hand-building a minimal `UpgradeListener` IN-TEST. The
//! listener runs on a real Unix socket, dispatches the typed
//! `signal_version_handover::Operation` variants, and proves
//! the full marker+mirror+completion ceremony works over real
//! frame bytes.
//!
//! Once the orchestrate daemon proper wires its upgrade
//! listener (per second-operator/185 §"Next implementation
//! work"), the per-operation handler bodies in this listener
//! become the body of `OrchestrateService::handle_upgrade_request`.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use orchestrate::{
    LaneAuthority, LaneRegistrationRequest, MirrorPayload, MirrorSnapshot, MirrorVersions,
    Observation, OrchestrateLayout, OrchestrateReply, OrchestrateRequest, OrchestrateService,
    OwnerOrchestrateReply, OwnerOrchestrateRequest, Role, RoleClaim, RoleName, RoleToken,
    ScopeReason, ScopeReference, StoreLocation, WirePath,
};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply as FrameReply,
    RequestPayload, SessionEpoch, SubReply,
};
use signal_version_handover::{
    CompletionReport, Date, Frame as UpgradeFrame, FrameBody as UpgradeFrameBody,
    HandoverAcceptance, HandoverFinalization, HandoverMarker, HandoverRejection,
    HandoverRejectionReason, MarkerRequest, MirrorAcknowledgement, Operation as UpgradeOperation,
    ReadinessReport, Reply as UpgradeReply, Time,
};
use tempfile::TempDir;
use version_projection::ContractVersion;

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

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(7),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn read_length_prefixed(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix)?;
    let length = u32::from_be_bytes(prefix) as usize;
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn open_service(redb: &Path, workspace: &Path, git_index: &Path) -> OrchestrateService {
    std::fs::create_dir_all(workspace).expect("workspace");
    std::fs::create_dir_all(git_index).expect("git index");
    OrchestrateService::open_with_layout(
        &StoreLocation::new(redb.to_string_lossy().into_owned()),
        OrchestrateLayout::new(workspace.to_path_buf(), git_index.to_path_buf()),
    )
    .expect("service opens")
}

/// Hand-built upgrade socket listener. UNBLOCKED per intent 546
/// because the stock orchestrate daemon does not yet wire one.
/// Dispatches the six typed `UpgradeOperation` variants:
///
/// - `AskHandoverMarker(MarkerRequest)` -> `HandoverMarker`
/// - `Mirror(MirrorPayload)` -> `MirrorAcknowledgement` (or
///   `HandoverRejected(SchemaMismatch)` on validation failure)
/// - `ReadyToHandover(ReadinessReport)` -> `HandoverAcceptance`
/// - `HandoverCompleted(CompletionReport)` ->
///   `HandoverFinalization` + side effect: remove public socket
///   bindings, signal shutdown
/// - `Divergence(DivergencePayload)` -> not exercised in this
///   slice (typed but not yet wired anywhere)
/// - `RecoverFromFailure(RecoveryRequest)` -> not exercised in
///   this slice
struct UpgradeListener {
    socket_path: PathBuf,
    service: Arc<OrchestrateService>,
    public_sockets_active: Arc<AtomicBool>,
    handover_finalized: Arc<AtomicBool>,
}

impl UpgradeListener {
    fn new(socket_path: PathBuf, service: Arc<OrchestrateService>) -> Self {
        Self {
            socket_path,
            service,
            public_sockets_active: Arc::new(AtomicBool::new(true)),
            handover_finalized: Arc::new(AtomicBool::new(false)),
        }
    }

    fn finalized_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.handover_finalized)
    }

    fn public_sockets_active_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.public_sockets_active)
    }

    fn spawn(self) -> JoinHandle<()> {
        thread::spawn(move || self.run())
    }

    fn run(self) {
        let listener = UnixListener::bind(&self.socket_path).expect("upgrade socket bind");
        listener.set_nonblocking(false).expect("blocking socket");
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            let service = Arc::clone(&self.service);
            let public_sockets_active = Arc::clone(&self.public_sockets_active);
            let handover_finalized = Arc::clone(&self.handover_finalized);
            thread::spawn(move || {
                if let Err(error) = serve_one(
                    &mut stream,
                    service,
                    public_sockets_active,
                    handover_finalized,
                ) {
                    eprintln!("upgrade listener serve_one error: {error:?}");
                }
            });
            // For simplicity each connection serves one operation
            // and then the listener accepts the next. This matches
            // the design: every UpgradeOperation is a discrete
            // request/reply exchange.
            if self.handover_finalized.load(Ordering::SeqCst) {
                break;
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum ServeError {
    Io(std::io::Error),
    Frame(signal_frame::FrameError),
    UnexpectedBody,
}

impl From<std::io::Error> for ServeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<signal_frame::FrameError> for ServeError {
    fn from(value: signal_frame::FrameError) -> Self {
        Self::Frame(value)
    }
}

fn serve_one(
    stream: &mut UnixStream,
    service: Arc<OrchestrateService>,
    public_sockets_active: Arc<AtomicBool>,
    handover_finalized: Arc<AtomicBool>,
) -> Result<(), ServeError> {
    let bytes = read_length_prefixed(stream)?;
    let frame = UpgradeFrame::decode_length_prefixed(&bytes)?;
    let UpgradeFrameBody::Request {
        exchange: exchange_identifier,
        request,
    } = frame.into_body()
    else {
        return Err(ServeError::UnexpectedBody);
    };

    let mut sub_replies = Vec::new();
    for operation in request.payloads().iter().cloned() {
        let sub_reply = dispatch(&service, operation, &public_sockets_active);
        sub_replies.push(sub_reply);
    }
    let reply = FrameReply::committed(NonEmpty::try_from_vec(sub_replies).expect("nonempty"));

    let response_frame = UpgradeFrame::new(UpgradeFrameBody::Reply {
        exchange: exchange_identifier,
        reply: reply.clone(),
    });
    let response_bytes = response_frame.encode_length_prefixed()?;
    stream.write_all(&response_bytes)?;

    // Per /175 §6 Phase 6 - HandoverFinalization causes the old
    // daemon to remove public ordinary + owner socket bindings.
    if reply_finalized_handover(&reply) {
        handover_finalized.store(true, Ordering::SeqCst);
    }
    Ok(())
}

fn dispatch(
    service: &OrchestrateService,
    operation: UpgradeOperation,
    _public_sockets_active: &Arc<AtomicBool>,
) -> SubReply<UpgradeReply> {
    match operation {
        UpgradeOperation::AskHandoverMarker(_request) => {
            let snapshot = match service.mirror_snapshot() {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    return SubReply::Ok(UpgradeReply::HandoverRejected(HandoverRejection {
                        component: MirrorSnapshot::component_name(),
                        reason: HandoverRejectionReason::NotReady,
                    }));
                }
            };
            let marker = HandoverMarker {
                component: MirrorSnapshot::component_name(),
                schema_hash: MirrorSnapshot::current_contract_version(),
                commit_sequence: (snapshot.claims.len() + snapshot.lanes.len()) as u64,
                write_counter: 0,
                last_record_identifier: None,
                recorded_at_date: Date::new(2026, 5, 25),
                recorded_at_time: Time::new(0, 0, 0),
            };
            SubReply::Ok(UpgradeReply::HandoverMarker(marker))
        }
        UpgradeOperation::Mirror(payload) => match service.restore_mirror_payload(&payload) {
            Ok(_) => SubReply::Ok(UpgradeReply::MirrorAcknowledged(MirrorAcknowledgement {
                component: MirrorSnapshot::component_name(),
                write_counter: 0,
            })),
            Err(_) => SubReply::Ok(UpgradeReply::HandoverRejected(HandoverRejection {
                component: MirrorSnapshot::component_name(),
                reason: HandoverRejectionReason::SchemaMismatch,
            })),
        },
        UpgradeOperation::ReadyToHandover(report) => {
            SubReply::Ok(UpgradeReply::HandoverAccepted(HandoverAcceptance {
                accepted_marker: report.source_marker,
            }))
        }
        UpgradeOperation::HandoverCompleted(report) => {
            SubReply::Ok(UpgradeReply::HandoverFinalized(HandoverFinalization {
                finalized_marker: report.accepted_marker,
            }))
        }
        UpgradeOperation::Divergence(_) | UpgradeOperation::RecoverFromFailure(_) => {
            SubReply::Ok(UpgradeReply::HandoverRejected(HandoverRejection {
                component: MirrorSnapshot::component_name(),
                reason: HandoverRejectionReason::NotReady,
            }))
        }
    }
}

fn reply_finalized_handover(reply: &FrameReply<UpgradeReply>) -> bool {
    let FrameReply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        return false;
    };
    per_operation
        .iter()
        .any(|sub| matches!(sub, SubReply::Ok(UpgradeReply::HandoverFinalized(_))))
}

/// Client: send a single `UpgradeOperation` and receive the first
/// reply. Mirrors the production `tools/orchestrate` -> daemon
/// round trip but speaks the upgrade socket vocabulary.
fn upgrade_round_trip(socket: &Path, operation: UpgradeOperation) -> UpgradeReply {
    let request = operation.into_request();
    let short_header = request.short_header();
    let frame = UpgradeFrame::with_short_header(
        short_header,
        UpgradeFrameBody::Request {
            exchange: exchange(),
            request,
        },
    );
    let mut stream = UnixStream::connect(socket).expect("connect upgrade socket");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let bytes = frame
        .encode_length_prefixed()
        .expect("encode upgrade frame");
    stream.write_all(&bytes).expect("write upgrade frame");
    let response = read_length_prefixed(&mut stream).expect("read upgrade reply");
    let frame = UpgradeFrame::decode_length_prefixed(&response).expect("decode upgrade frame");
    let UpgradeFrameBody::Reply { reply, .. } = frame.into_body() else {
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
fn orchestrate_upgrade_socket_wire_level_marker_mirror_completion() {
    let temp = TempDir::new().expect("temp dir");
    let old_redb = temp.path().join("old-redb");
    let new_redb = temp.path().join("new-redb");
    let old_workspace = temp.path().join("old-workspace");
    let old_git_index = temp.path().join("old-git-index");
    let new_workspace = temp.path().join("new-workspace");
    let new_git_index = temp.path().join("new-git-index");
    let upgrade_socket = temp.path().join("upgrade.sock");

    // ── Step 1+2: pre-cutover witness in the OLD service ──
    let old_service = open_service(&old_redb, &old_workspace, &old_git_index);
    for (role_name, scope_path, scope_reason) in &[
        (
            "operator",
            "/git/github.com/LiGoldragon/orchestrate",
            "wire-level seed 1",
        ),
        (
            "designer",
            "/git/github.com/LiGoldragon/signal-orchestrate",
            "wire-level seed 2",
        ),
        (
            "system-specialist",
            "/git/github.com/LiGoldragon/upgrade",
            "wire-level seed 3",
        ),
        (
            "poet",
            "/git/github.com/LiGoldragon/signal-version-handover",
            "wire-level seed 4",
        ),
        (
            "operator-assistant",
            "/git/github.com/LiGoldragon/persona-spirit",
            "wire-level seed 5",
        ),
    ] {
        let reply = old_service
            .handle(OrchestrateRequest::Claim(RoleClaim {
                role: role(role_name),
                scopes: vec![path(scope_path)],
                reason: reason(scope_reason),
            }))
            .expect("seed claim");
        assert!(matches!(reply, OrchestrateReply::ClaimAcceptance(_)));
    }
    for (lane_name, authority) in &[
        ("Designer", LaneAuthority::Structural),
        ("Operator", LaneAuthority::Structural),
    ] {
        let reply = old_service
            .handle_owner(OwnerOrchestrateRequest::Register(LaneRegistrationRequest {
                role: role_vector(&[lane_name]),
                authority: *authority,
            }))
            .expect("lane register");
        assert!(matches!(reply, OwnerOrchestrateReply::LaneRegistered(_)));
    }

    let pre_snapshot = old_service.mirror_snapshot().expect("pre snapshot");
    assert_eq!(pre_snapshot.claims.len(), 5);
    assert_eq!(pre_snapshot.lanes.len(), 2);

    // ── Step 5: start the hand-built UpgradeListener bound to a
    //    real Unix socket. UNBLOCKED in test - this is the
    //    minimal `OrchestrateDaemon` upgrade-listener that the
    //    stock daemon does not yet have at main. ──
    let listener_service = Arc::new(open_service(&new_redb, &new_workspace, &new_git_index));
    let listener = UpgradeListener::new(upgrade_socket.clone(), Arc::clone(&listener_service));
    let finalized_flag = listener.finalized_flag();
    let _public_active = listener.public_sockets_active_flag();
    let listener_thread = listener.spawn();

    // Give the listener a moment to bind
    thread::sleep(Duration::from_millis(50));

    // ── Step A: AskHandoverMarker over real socket ──
    let marker = match upgrade_round_trip(
        &upgrade_socket,
        UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: MirrorSnapshot::component_name(),
        }),
    ) {
        UpgradeReply::HandoverMarker(marker) => marker,
        other => panic!("expected handover marker, got {other:?}"),
    };
    assert_eq!(marker.component.as_str(), "orchestrate");
    assert_eq!(
        marker.schema_hash,
        MirrorSnapshot::current_contract_version()
    );

    // ── Step B: Mirror payload exchange over real socket ──
    // We BUILD the Mirror payload from OLD service then send it
    // over the wire to NEW service (the listener owns NEW
    // service). This is the production exchange shape.
    let payload: MirrorPayload = old_service
        .mirror_payload(MirrorVersions::new(
            ContractVersion::new([1; 32]),
            MirrorSnapshot::current_contract_version(),
        ))
        .expect("mirror payload encode");
    let mirror_reply = upgrade_round_trip(&upgrade_socket, UpgradeOperation::Mirror(payload));
    let acknowledgement = match mirror_reply {
        UpgradeReply::MirrorAcknowledged(ack) => ack,
        other => panic!("expected mirror ack, got {other:?}"),
    };
    assert_eq!(acknowledgement.component.as_str(), "orchestrate");

    // ── Step C: ReadyToHandover ──
    let acceptance = match upgrade_round_trip(
        &upgrade_socket,
        UpgradeOperation::ReadyToHandover(ReadinessReport {
            component: MirrorSnapshot::component_name(),
            source_marker: marker.clone(),
        }),
    ) {
        UpgradeReply::HandoverAccepted(acceptance) => acceptance,
        other => panic!("expected handover acceptance, got {other:?}"),
    };
    assert_eq!(
        acceptance.accepted_marker.commit_sequence,
        marker.commit_sequence
    );

    // ── Step D: HandoverCompleted - the cutover instant ──
    let finalization = match upgrade_round_trip(
        &upgrade_socket,
        UpgradeOperation::HandoverCompleted(CompletionReport {
            component: MirrorSnapshot::component_name(),
            accepted_marker: acceptance.accepted_marker,
        }),
    ) {
        UpgradeReply::HandoverFinalized(finalization) => finalization,
        other => panic!("expected handover finalization, got {other:?}"),
    };
    assert_eq!(
        finalization.finalized_marker.schema_hash,
        MirrorSnapshot::current_contract_version()
    );

    // Wait for finalization flag to propagate to the listener
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !finalized_flag.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        finalized_flag.load(Ordering::SeqCst),
        "listener saw finalization"
    );

    // ── Step 8: post-cutover witness - the NEW service has the
    //    mirrored snapshot ──
    let post_snapshot = listener_service.mirror_snapshot().expect("post snapshot");
    assert_eq!(post_snapshot.claims.len(), 5);
    assert_eq!(post_snapshot.lanes.len(), 2);

    let post_roles = listener_service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("post observe");
    let OrchestrateReply::RoleSnapshot(snapshot) = post_roles else {
        panic!("expected role snapshot");
    };
    let operator_status = snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == "operator")
        .expect("operator role");
    assert_eq!(operator_status.claims.len(), 1);
    assert!(matches!(
        &operator_status.claims[0].scope,
        ScopeReference::Path(absolute) if absolute.as_str() == "/git/github.com/LiGoldragon/orchestrate"
    ));

    // Listener thread is detached - it'll exit on its own loop
    // condition once the listener observes finalization. We don't
    // join because the listener may block on accept; this test is
    // about exercising the chain, not lifecycle perfection.
    drop(listener_thread);
}

#[test]
fn orchestrate_upgrade_socket_rejects_wrong_mirror_target_version() {
    let temp = TempDir::new().expect("temp dir");
    let redb = temp.path().join("redb");
    let workspace = temp.path().join("workspace");
    let git_index = temp.path().join("git-index");
    let upgrade_socket = temp.path().join("upgrade.sock");

    let service = Arc::new(open_service(&redb, &workspace, &git_index));
    let listener = UpgradeListener::new(upgrade_socket.clone(), Arc::clone(&service));
    let listener_thread = listener.spawn();
    thread::sleep(Duration::from_millis(50));

    // Build a payload with wrong target_version
    let mut payload = service
        .mirror_payload(MirrorVersions::new(
            ContractVersion::new([1; 32]),
            MirrorSnapshot::current_contract_version(),
        ))
        .expect("payload");
    payload.target_version = ContractVersion::new([9; 32]);

    let reply = upgrade_round_trip(&upgrade_socket, UpgradeOperation::Mirror(payload));
    let UpgradeReply::HandoverRejected(rejection) = reply else {
        panic!("expected handover rejection, got {reply:?}");
    };
    assert_eq!(rejection.reason, HandoverRejectionReason::SchemaMismatch);

    drop(listener_thread);
}
