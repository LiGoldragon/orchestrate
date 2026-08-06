use std::{
    env,
    os::unix::net::{UnixListener, UnixStream},
    process::ExitCode,
};

use meta_signal_harness::{
    CodexContinuationIdentifier, ContinuationHandle, HarnessKind, MetaHarnessFrame,
    MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest, ModelResolved, ModelUnavailable,
    ModelUnavailableReason, NamedModel,
};
use signal_frame::{NonEmpty, OperationDispatchError, Reply, SubReply};
use signal_orchestrate::HarnessName;
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

fn main() -> ExitCode {
    match WorkflowHarness::from_process_arguments().and_then(WorkflowHarness::serve) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-workflow-harness: {error}");
            ExitCode::FAILURE
        }
    }
}

struct WorkflowHarness {
    listener: UnixListener,
    codec: LengthPrefixedCodec,
}

impl WorkflowHarness {
    fn from_process_arguments() -> Result<Self, String> {
        let socket = env::args()
            .nth(1)
            .ok_or_else(|| "expected fake meta-harness socket path".to_owned())?;
        Ok(Self {
            listener: UnixListener::bind(socket).map_err(|error| error.to_string())?,
            codec: LengthPrefixedCodec::default(),
        })
    }

    fn serve(self) -> Result<(), String> {
        self.serve_one(WorkflowHarnessReply::Resolved)?;
        self.serve_one(WorkflowHarnessReply::Unavailable)
    }

    fn serve_one(&self, disposition: WorkflowHarnessReply) -> Result<(), String> {
        let (mut stream, _) = self.listener.accept().map_err(|error| error.to_string())?;
        let body = self
            .codec
            .read_body(&mut stream)
            .map_err(|error| error.to_string())?;
        let frame = MetaHarnessFrame::decode(body.bytes()).map_err(|error| error.to_string())?;
        let actual_route = frame.short_header().route();
        let MetaHarnessFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err("expected a meta-harness request frame".to_owned());
        };
        let expected_route = request.route().map_err(|error| error.to_string())?;
        if actual_route != expected_route {
            return Err(OperationDispatchError::HeaderRouteMismatch {
                expected: expected_route,
                actual: actual_route,
            }
            .to_string());
        }
        let MetaHarnessRequest::ResolveModel(request) = request.payloads().head().clone() else {
            return Err("expected a ResolveModel meta-harness request".to_owned());
        };
        let reply = MetaHarnessFrame::new(
            expected_route,
            MetaHarnessFrameBody::Reply {
                exchange,
                reply: Reply::committed(NonEmpty::single(SubReply::Ok(disposition.reply(request)))),
            },
        );
        self.write_reply(&mut stream, reply)
    }

    fn write_reply(&self, stream: &mut UnixStream, reply: MetaHarnessFrame) -> Result<(), String> {
        self.codec
            .write_body(
                stream,
                &RuntimeFrameBody::new(reply.encode().map_err(|error| error.to_string())?),
            )
            .map_err(|error| error.to_string())
    }
}

enum WorkflowHarnessReply {
    Resolved,
    Unavailable,
}

impl WorkflowHarnessReply {
    fn reply(self, request: meta_signal_harness::ModelResolutionRequest) -> MetaHarnessReply {
        match self {
            Self::Resolved => MetaHarnessReply::ModelResolved(ModelResolved {
                harness: HarnessName::new("stateful-scenario"),
                harness_kind: HarnessKind::Codex,
                model: NamedModel::new("stateful-scenario-model"),
                effort: request.model.effort,
                continuation: ContinuationHandle::Codex(CodexContinuationIdentifier::new(
                    "stateful-scenario",
                )),
            }),
            Self::Unavailable => MetaHarnessReply::ModelUnavailable(ModelUnavailable {
                request,
                reason: ModelUnavailableReason::ModelNotKnown,
            }),
        }
    }
}
