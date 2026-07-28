use std::{env, os::unix::net::UnixStream, process::ExitCode};

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as EnvelopeReply, RequestPayload,
    SessionEpoch, SubReply,
};
use signal_version_handover::{
    CompletionReport, DivergencePayload, DivergenceReason, Frame, FrameBody, HandoverMarker,
    MarkerRequest, MirrorPayload, Operation, ReadinessReport, RecoveryRequest, Reply,
};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};
use version_projection::{ComponentName, RecordKind};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-upgrade-scenario: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let socket = env::args()
        .nth(1)
        .ok_or_else(|| "expected upgrade socket path".to_owned())?;
    let component = ComponentName::new("orchestrate");

    let marker = expect_marker(request(
        &socket,
        1,
        Operation::AskHandoverMarker(MarkerRequest {
            component: component.clone(),
        }),
    )?)?;

    expect_divergence_acknowledgement(request(
        &socket,
        2,
        Operation::Divergence(DivergencePayload {
            component: component.clone(),
            source_version: marker.schema_hash,
            target_version: marker.schema_hash,
            reason: DivergenceReason::TargetUnavailable,
            kind: RecordKind::new("State"),
            payload: vec![1],
        }),
    )?)?;

    // A deliberately non-archived mirror payload must be rejected by the
    // daemon's typed schema-mismatch path. It covers the real socket route
    // without claiming that arbitrary bytes can be restored.
    expect_schema_mismatch(request(
        &socket,
        3,
        Operation::Mirror(MirrorPayload {
            component: component.clone(),
            source_version: marker.schema_hash,
            target_version: marker.schema_hash,
            kind: RecordKind::new("State"),
            payload: vec![1],
        }),
    )?)?;

    expect_recovered(request(
        &socket,
        4,
        Operation::RecoverFromFailure(RecoveryRequest {
            component: component.clone(),
            failure_identifier: 1,
        }),
    )?)?;

    let accepted_marker = expect_accepted(request(
        &socket,
        5,
        Operation::ReadyToHandover(ReadinessReport {
            component: component.clone(),
            source_marker: marker,
        }),
    )?)?;

    // Finalization intentionally comes last: it retires the public sockets.
    expect_finalized(request(
        &socket,
        6,
        Operation::HandoverCompleted(CompletionReport {
            component,
            accepted_marker,
        }),
    )?)?;
    Ok(())
}

fn request(socket: &str, sequence: u64, operation: Operation) -> Result<Reply, String> {
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
    let mut stream = UnixStream::connect(socket).map_err(|error| error.to_string())?;
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
        EnvelopeReply::Rejected { reason } => Err(format!("upgrade server rejected: {reason:?}")),
    }
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

fn expect_schema_mismatch(reply: Reply) -> Result<(), String> {
    match reply {
        Reply::HandoverRejected(rejection)
            if matches!(
                rejection.reason,
                signal_version_handover::HandoverRejectionReason::SchemaMismatch
            ) =>
        {
            Ok(())
        }
        other => Err(format!("expected schema-mismatch rejection, got {other:?}")),
    }
}

fn expect_recovered(reply: Reply) -> Result<(), String> {
    match reply {
        Reply::RecoveryCompleted(result) if result.recovered => Ok(()),
        other => Err(format!(
            "expected successful RecoveryCompleted, got {other:?}"
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
