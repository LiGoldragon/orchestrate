use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
    ModelResolutionRequest,
};
use signal_criome::{
    EvaluationDecision, OperationDigest, WorkflowProvenanceDigest, WorkflowReceipt,
};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};
use signal_orchestrate::{
    HostName, ModelAttestation, ModelName, OrchestrateReply, ProviderName,
    ResolvedWorkflowRunRequest, ScopeReason, StepLog, StepOutcome, WorkflowReceiptProduced,
    WorkflowResolutionUnavailable, WorkflowResolvedReceiptProduced, WorkflowRunDigest,
    WorkflowRunHandle, WorkflowRunLog, WorkflowRunLogReported, WorkflowRunObservation,
    WorkflowRunObservationClosed, WorkflowRunObservationOpened, WorkflowRunObservationToken,
    WorkflowRunRequest, WorkflowRunResolution, WorkflowRunSnapshot, WorkflowStepName,
};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::{Error, OrchestrateTables, Result, StoredWorkflowRunResolution};

const DEFAULT_META_HARNESS_SOCKET: &str = "/tmp/meta-harness.sock";
const META_HARNESS_SOCKET_VARIABLE: &str = "HARNESS_META_SOCKET";

pub trait HarnessModelResolver {
    fn resolve_model(&self, request: ModelResolutionRequest) -> Result<MetaHarnessReply>;
}

#[derive(Debug, Clone)]
pub struct WorkflowRunner<Resolver> {
    provider: ProviderName,
    model: ModelName,
    host: HostName,
    step: WorkflowStepName,
    resolver: Resolver,
}

#[derive(Debug, Clone)]
pub struct FixtureModelResolver {
    reply: Option<MetaHarnessReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessResolver {
    socket_path: PathBuf,
    codec: LengthPrefixedCodec,
}

impl WorkflowRunner<FixtureModelResolver> {
    pub fn fixture() -> Result<Self> {
        Self::fixture_with_resolution(None)
    }

    pub fn fixture_with_resolution(reply: Option<MetaHarnessReply>) -> Result<Self> {
        Self::new(FixtureModelResolver::new(reply))
    }
}

impl WorkflowRunner<MetaHarnessResolver> {
    pub fn from_process_harness() -> Result<Self> {
        Self::new(MetaHarnessResolver::from_process())
    }
}

impl<Resolver> WorkflowRunner<Resolver>
where
    Resolver: HarnessModelResolver,
{
    pub fn new(resolver: Resolver) -> Result<Self> {
        Ok(Self {
            provider: ProviderName::from_wire_token("fixture-provider")?,
            model: ModelName::from_wire_token("fixture-model")?,
            host: HostName::from_wire_token("local-orchestrate")?,
            step: WorkflowStepName::from_wire_token("fixture-agent")?,
            resolver,
        })
    }

    pub fn run(&self, request: WorkflowRunRequest) -> Result<OrchestrateReply> {
        let handle = self.handle_for(&request)?;
        let receipt = self.receipt_for(&request, &handle);
        Ok(OrchestrateReply::WorkflowReceiptProduced(
            WorkflowReceiptProduced { handle, receipt },
        ))
    }

    pub fn run_resolved_workflow(
        &self,
        request: ResolvedWorkflowRunRequest,
        tables: &OrchestrateTables,
    ) -> Result<OrchestrateReply> {
        let handle = self.handle_for(&request.workflow_run)?;
        let reply = self
            .resolver
            .resolve_model(request.model_resolution.clone())?;
        let stamped_at = tables.current_timestamp()?;
        match reply {
            MetaHarnessReply::ModelResolved(resolution) => {
                let stored = StoredWorkflowRunResolution::resolved(
                    handle.clone(),
                    request.clone(),
                    resolution.clone(),
                    stamped_at,
                );
                tables.insert_workflow_model_resolution(&stored)?;
                let run = WorkflowRunResolution { handle, resolution };
                let receipt = self.receipt_for(&request.workflow_run, &run.handle);
                Ok(OrchestrateReply::WorkflowResolvedReceiptProduced(
                    WorkflowResolvedReceiptProduced { run, receipt },
                ))
            }
            MetaHarnessReply::ModelUnavailable(unavailable) => {
                let stored = StoredWorkflowRunResolution::unavailable(
                    handle.clone(),
                    request.clone(),
                    unavailable.clone(),
                    stamped_at,
                );
                tables.insert_workflow_model_resolution(&stored)?;
                Ok(OrchestrateReply::WorkflowResolutionUnavailable(
                    WorkflowResolutionUnavailable {
                        handle,
                        request,
                        unavailable,
                    },
                ))
            }
            MetaHarnessReply::RequestUnimplemented(unimplemented) => {
                Err(Error::HarnessResolutionUnimplemented {
                    operation: format!("{:?}", unimplemented.operation),
                })
            }
            other => Err(Error::UnexpectedHarnessReply {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn report_log(&self, request: WorkflowRunRequest) -> Result<OrchestrateReply> {
        let handle = self.handle_for(&request)?;
        let log = self.log_for(&request, &handle);
        Ok(OrchestrateReply::WorkflowRunLogReported(
            WorkflowRunLogReported { log },
        ))
    }

    pub fn open_observation(
        &self,
        observation: WorkflowRunObservation,
    ) -> Result<OrchestrateReply> {
        let token = WorkflowRunObservationToken {
            run: observation.run.clone(),
        };
        let snapshot = WorkflowRunSnapshot {
            handle: WorkflowRunHandle {
                run: observation.run,
            },
            latest_log: None,
            receipt: None,
        };
        Ok(OrchestrateReply::WorkflowRunObservationOpened(
            WorkflowRunObservationOpened { token, snapshot },
        ))
    }

    pub fn close_observation(&self, token: WorkflowRunObservationToken) -> OrchestrateReply {
        OrchestrateReply::WorkflowRunObservationClosed(WorkflowRunObservationClosed { token })
    }

    fn handle_for(&self, request: &WorkflowRunRequest) -> Result<WorkflowRunHandle> {
        let run = format!(
            "workflow-run-{}-{}-{}",
            request.workflow.object_digest().as_str(),
            request.operation.digest.as_str(),
            request.contract.object_digest().as_str()
        );
        Ok(WorkflowRunHandle {
            run: WorkflowRunDigest::from_wire_token(run)?,
        })
    }

    fn receipt_for(
        &self,
        request: &WorkflowRunRequest,
        handle: &WorkflowRunHandle,
    ) -> WorkflowReceipt {
        WorkflowReceipt {
            workflow: request.workflow.clone(),
            operation: OperationDigest::new(request.operation.digest.clone()),
            outcome: EvaluationDecision::Authorized,
            provenance: WorkflowProvenanceDigest::from_bytes(handle.run.as_str().as_bytes()),
        }
    }

    fn log_for(&self, request: &WorkflowRunRequest, handle: &WorkflowRunHandle) -> WorkflowRunLog {
        WorkflowRunLog {
            run: handle.run.clone(),
            step_logs: vec![StepLog {
                step: self.step.clone(),
                attestation: ModelAttestation {
                    provider: self.provider.clone(),
                    model: self.model.clone(),
                    host: self.host.clone(),
                    call: OperationDigest::new(request.operation.digest.clone()),
                },
                outcome: StepOutcome::Produced(EvaluationDecision::Authorized),
            }],
        }
    }

    pub fn unavailable_reason(&self) -> ScopeReason {
        ScopeReason::from_text("workflow runner unavailable").expect("static reason")
    }
}

impl FixtureModelResolver {
    pub fn new(reply: Option<MetaHarnessReply>) -> Self {
        Self { reply }
    }
}

impl HarnessModelResolver for FixtureModelResolver {
    fn resolve_model(&self, _request: ModelResolutionRequest) -> Result<MetaHarnessReply> {
        self.reply
            .clone()
            .ok_or(Error::HarnessResolverNotConfigured)
    }
}

impl MetaHarnessResolver {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(
            std::env::var(META_HARNESS_SOCKET_VARIABLE)
                .unwrap_or_else(|_| DEFAULT_META_HARNESS_SOCKET.to_string()),
        )
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    fn exchange(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::new(0),
        )
    }

    fn reply_from_frame(&self, frame: MetaHarnessFrame) -> Result<MetaHarnessReply> {
        match frame.into_body() {
            MetaHarnessFrameBody::Reply { reply, .. } => self.reply_output(reply),
            other => Err(Error::UnexpectedHarnessFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_output(&self, reply: Reply<MetaHarnessReply>) -> Result<MetaHarnessReply> {
        match reply {
            Reply::Accepted {
                outcome: AcceptedOutcome::Committed,
                per_operation,
            } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::HarnessReplyNotCommitted {
                    outcome: format!("{other:?}"),
                }),
            },
            Reply::Accepted { outcome, .. } => Err(Error::HarnessReplyNotCommitted {
                outcome: format!("{outcome:?}"),
            }),
            Reply::Rejected { reason } => Err(Error::HarnessReplyRejected { reason }),
        }
    }
}

impl HarnessModelResolver for MetaHarnessResolver {
    fn resolve_model(&self, request: ModelResolutionRequest) -> Result<MetaHarnessReply> {
        let frame = MetaHarnessFrame::new(MetaHarnessFrameBody::Request {
            exchange: self.exchange(),
            request: signal_frame::Request::from_payload(MetaHarnessRequest::ResolveModel(request)),
        });
        let mut stream = UnixStream::connect(&self.socket_path)?;
        self.codec
            .write_body(&mut stream, &RuntimeFrameBody::new(frame.encode()?))
            .map_err(Error::HarnessTransportFrame)?;
        let body = self
            .codec
            .read_body(&mut stream)
            .map_err(Error::HarnessTransportFrame)?;
        self.reply_from_frame(MetaHarnessFrame::decode(body.bytes())?)
    }
}
