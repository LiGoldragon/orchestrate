use std::{env, os::unix::net::UnixStream, process::ExitCode};

use orchestrate::{
    LaneAssignment, LaneAuthority, LaneDetails, LaneIdentifier, LaneOwner, LaneStatus,
    MirrorSnapshot, MirrorVersions, Role, RoleToken, ScopeReason, ScopeReference,
    SessionIdentifier, StoredClaim, StoredLaneRegistration, TimestampNanos, WirePath,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as EnvelopeReply, RequestPayload,
    SessionEpoch, SubReply,
};
use signal_version_handover::{
    CompletionReport, DivergencePayload, DivergenceReason, Frame, FrameBody, HandoverMarker,
    HandoverRejectionReason, MarkerRequest, MirrorPayload, Operation, ReadinessReport,
    RecoveryRequest, Reply,
};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};
use version_projection::{ComponentName, RecordKind};

fn main() -> ExitCode {
    match UpgradeScenario::from_process_arguments().and_then(UpgradeScenario::run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-upgrade-scenario: {error}");
            ExitCode::FAILURE
        }
    }
}

struct UpgradeScenario {
    socket: String,
    phase: UpgradePhase,
}

#[derive(Clone, Copy)]
enum UpgradePhase {
    Prepare,
    CommitAdvanced,
    Finalize,
}

impl UpgradeScenario {
    fn from_process_arguments() -> Result<Self, String> {
        let mut arguments = env::args().skip(1);
        let socket = arguments
            .next()
            .ok_or_else(|| "expected upgrade socket path".to_owned())?;
        let phase = match arguments.next().as_deref() {
            Some("prepare") => UpgradePhase::Prepare,
            Some("commit-advanced") => UpgradePhase::CommitAdvanced,
            Some("finalize") => UpgradePhase::Finalize,
            Some(other) => return Err(format!("unknown upgrade phase {other}")),
            None => return Err("expected upgrade scenario phase".to_owned()),
        };
        Ok(Self { socket, phase })
    }

    fn run(self) -> Result<(), String> {
        match self.phase {
            UpgradePhase::Prepare => self.prepare(),
            UpgradePhase::CommitAdvanced => self.commit_advanced(),
            UpgradePhase::Finalize => self.finalize(),
        }
    }

    fn prepare(&self) -> Result<(), String> {
        let component = ComponentName::new("orchestrate");
        let initial_marker = self.marker(1, component.clone())?;

        expect_rejection(
            self.request(
                2,
                Operation::HandoverCompleted(CompletionReport {
                    component: component.clone(),
                    accepted_marker: initial_marker.clone(),
                }),
            )?,
            HandoverRejectionReason::NotReady,
        )?;

        expect_divergence_acknowledgement(self.request(
            3,
            Operation::Divergence(DivergencePayload {
                component: component.clone(),
                source_version: initial_marker.schema_hash,
                target_version: initial_marker.schema_hash,
                reason: DivergenceReason::TargetUnavailable,
                kind: RecordKind::new("State"),
                payload: vec![1],
            }),
        )?)?;

        expect_rejection(
            self.request(
                4,
                Operation::Mirror(MirrorPayload {
                    component: component.clone(),
                    source_version: initial_marker.schema_hash,
                    target_version: initial_marker.schema_hash,
                    kind: RecordKind::new("State"),
                    payload: vec![1],
                }),
            )?,
            HandoverRejectionReason::SchemaMismatch,
        )?;

        let payload = mirror_payload(initial_marker.schema_hash)?;
        expect_mirror_acknowledgement(self.request(5, Operation::Mirror(payload))?, &component)?;
        let restored_marker = self.marker(6, component.clone())?;
        expect_recovered(
            self.request(
                7,
                Operation::RecoverFromFailure(RecoveryRequest {
                    component,
                    failure_identifier: restored_marker.commit_sequence,
                }),
            )?,
            true,
        )
    }

    fn commit_advanced(&self) -> Result<(), String> {
        let component = ComponentName::new("orchestrate");
        let mut stale_marker = self.marker(10, component.clone())?;
        let Some(stale_sequence) = stale_marker.commit_sequence.checked_sub(1) else {
            return Err("cannot witness CommitSequenceAdvanced at genesis".to_owned());
        };
        stale_marker.commit_sequence = stale_sequence;
        expect_rejection(
            self.request(
                11,
                Operation::ReadyToHandover(ReadinessReport {
                    component,
                    source_marker: stale_marker,
                }),
            )?,
            HandoverRejectionReason::CommitSequenceAdvanced,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        let component = ComponentName::new("orchestrate");
        let marker = self.marker(20, component.clone())?;
        let accepted_marker = expect_accepted(self.request(
            21,
            Operation::ReadyToHandover(ReadinessReport {
                component: component.clone(),
                source_marker: marker.clone(),
            }),
        )?)?;

        expect_rejection(
            self.request(
                22,
                Operation::ReadyToHandover(ReadinessReport {
                    component: component.clone(),
                    source_marker: marker,
                }),
            )?,
            HandoverRejectionReason::AlreadyInHandover,
        )?;
        expect_rejection(
            self.request(
                23,
                Operation::Mirror(mirror_payload(accepted_marker.schema_hash)?),
            )?,
            HandoverRejectionReason::NotReady,
        )?;

        let mut stale_marker = accepted_marker.clone();
        stale_marker.write_counter += 1;
        expect_rejection(
            self.request(
                24,
                Operation::HandoverCompleted(CompletionReport {
                    component: component.clone(),
                    accepted_marker: stale_marker,
                }),
            )?,
            HandoverRejectionReason::CommitSequenceAdvanced,
        )?;
        expect_finalized(self.request(
            25,
            Operation::HandoverCompleted(CompletionReport {
                component: component.clone(),
                accepted_marker,
            }),
        )?)?;
        expect_recovered(
            self.request(
                26,
                Operation::RecoverFromFailure(RecoveryRequest {
                    component,
                    failure_identifier: 0,
                }),
            )?,
            false,
        )
    }

    fn marker(&self, sequence: u64, component: ComponentName) -> Result<HandoverMarker, String> {
        expect_marker(self.request(
            sequence,
            Operation::AskHandoverMarker(MarkerRequest { component }),
        )?)
    }

    fn request(&self, sequence: u64, operation: Operation) -> Result<Reply, String> {
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::new(sequence),
        );
        let request = operation.into_request();
        let frame = Frame::with_short_header(
            request.short_header(),
            FrameBody::Request { exchange, request },
        );
        let bytes = frame.encode().map_err(|error| error.to_string())?;
        let mut stream = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let codec = LengthPrefixedCodec::default();
        codec
            .write_body(&mut stream, &RuntimeFrameBody::new(bytes))
            .map_err(|error| error.to_string())?;
        let response = codec
            .read_body(&mut stream)
            .map_err(|error| error.to_string())?
            .into_bytes();
        let frame = Frame::decode(&response).map_err(|error| error.to_string())?;
        let FrameBody::Reply { reply, .. } = frame.into_body() else {
            return Err("upgrade server returned a request frame".to_owned());
        };
        match reply {
            EnvelopeReply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(reply) => Ok(reply),
                other => Err(format!("upgrade server did not commit: {other:?}")),
            },
            EnvelopeReply::Rejected { reason } => {
                Err(format!("upgrade server rejected: {reason:?}"))
            }
        }
    }
}

fn mirror_payload(version: version_projection::ContractVersion) -> Result<MirrorPayload, String> {
    let source = mirrored_lane("MirrorSource", "mirror-source")?;
    let target = mirrored_lane("MirrorTarget", "mirror-target")?;
    let conflict = mirrored_lane("MirrorConflict", "mirror-conflict")?;
    MirrorSnapshot {
        lanes: vec![source.clone(), target, conflict],
        claims: vec![
            StoredClaim::new(
                source.assignment.lane.clone(),
                ScopeReference::Path(
                    WirePath::from_absolute_path("/scenario/mirror-retained")
                        .map_err(|error| error.to_string())?,
                ),
                ScopeReason::from_text("known mirrored claim")
                    .map_err(|error| error.to_string())?,
                TimestampNanos::new(1),
            ),
            StoredClaim::new(
                source.assignment.lane.clone(),
                ScopeReference::Path(
                    WirePath::from_absolute_path("/scenario/mirror-handoff")
                        .map_err(|error| error.to_string())?,
                ),
                ScopeReason::from_text("handoff source").map_err(|error| error.to_string())?,
                TimestampNanos::new(1),
            ),
            StoredClaim::new(
                LaneIdentifier::from_wire_token("mirror-conflict")
                    .map_err(|error| error.to_string())?,
                ScopeReference::Path(
                    WirePath::from_absolute_path("/scenario/mirror-handoff/conflict")
                        .map_err(|error| error.to_string())?,
                ),
                ScopeReason::from_text("mirrored conflict").map_err(|error| error.to_string())?,
                TimestampNanos::new(1),
            ),
        ],
    }
    .into_mirror_payload(MirrorVersions::new(version, version))
    .map_err(|error| error.to_string())
}

fn mirrored_lane(session: &str, lane: &str) -> Result<StoredLaneRegistration, String> {
    let role = Role::try_new(vec![
        RoleToken::from_text(session).map_err(|error| error.to_string())?,
    ])
    .map_err(|error| error.to_string())?;
    let assignment = LaneAssignment {
        session: SessionIdentifier::from_camel_case_name(session)
            .map_err(|error| error.to_string())?,
        lane: LaneIdentifier::from_wire_token(lane).map_err(|error| error.to_string())?,
        owner: LaneOwner {
            role,
            authority: LaneAuthority::Structural,
        },
        details: LaneDetails::from_text("known mirrored lane")
            .map_err(|error| error.to_string())?,
    };
    Ok(StoredLaneRegistration::new(
        assignment,
        TimestampNanos::new(1),
        TimestampNanos::new(1),
        LaneStatus::Active,
    ))
}

fn expect_marker(reply: Reply) -> Result<HandoverMarker, String> {
    match reply {
        Reply::HandoverMarker(marker) => Ok(marker),
        other => Err(format!("expected HandoverMarker, got {other:?}")),
    }
}

fn expect_divergence_acknowledgement(reply: Reply) -> Result<(), String> {
    match reply {
        Reply::DivergenceAcknowledged(_) => Ok(()),
        other => Err(format!("expected DivergenceAcknowledged, got {other:?}")),
    }
}

fn expect_mirror_acknowledgement(reply: Reply, component: &ComponentName) -> Result<(), String> {
    match reply {
        Reply::MirrorAcknowledged(acknowledgement)
            if acknowledgement.component == *component && acknowledgement.write_counter > 0 =>
        {
            Ok(())
        }
        other => Err(format!("expected MirrorAcknowledged, got {other:?}")),
    }
}

fn expect_rejection(reply: Reply, expected: HandoverRejectionReason) -> Result<(), String> {
    match reply {
        Reply::HandoverRejected(rejection) if rejection.reason == expected => Ok(()),
        other => Err(format!("expected {expected:?} rejection, got {other:?}")),
    }
}

fn expect_recovered(reply: Reply, expected: bool) -> Result<(), String> {
    match reply {
        Reply::RecoveryCompleted(result) if result.recovered == expected => Ok(()),
        other => Err(format!(
            "expected RecoveryCompleted({expected}), got {other:?}"
        )),
    }
}

fn expect_accepted(reply: Reply) -> Result<HandoverMarker, String> {
    match reply {
        Reply::HandoverAccepted(acceptance) => Ok(acceptance.accepted_marker),
        other => Err(format!("expected HandoverAccepted, got {other:?}")),
    }
}

fn expect_finalized(reply: Reply) -> Result<(), String> {
    match reply {
        Reply::HandoverFinalized(_) => Ok(()),
        other => Err(format!("expected HandoverFinalized, got {other:?}")),
    }
}
