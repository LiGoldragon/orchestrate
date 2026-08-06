use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
    ModelResolutionRequest, SessionLaunchRequest,
};
use signal_criome::schema::lib::{z2VSX9, z2VSrv, z2VY7s, z2VZZu, z2VbxF};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
    WireRoute,
};
use signal_orchestrate::{
    HostName, ModelAttestation, ModelName, OrchestrateReply, ProviderName,
    ResolvedWorkflowRunRequest, ScopeReason, StepLog, StepOutcome, WorkflowReceiptProduced,
    WorkflowResolutionUnavailable, WorkflowRunDigest, WorkflowRunHandle, WorkflowRunLog,
    WorkflowRunLogReported, WorkflowRunObservation, WorkflowRunObservationClosed,
    WorkflowRunObservationOpened, WorkflowRunObservationToken, WorkflowRunRequest,
    WorkflowRunResolution, WorkflowRunSnapshot, WorkflowStepName,
};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::{Error, OrchestrateTables, Result, StoredWorkflowRunResolution};

const DEFAULT_META_HARNESS_SOCKET: &str = "/tmp/meta-harness.sock";
const META_HARNESS_SOCKET_VARIABLE: &str = "HARNESS_META_SOCKET";

pub trait HarnessModelResolver {
    fn resolve_model(&self, request: ModelResolutionRequest) -> Result<MetaHarnessReply>;

    /// Command the harness component to launch a session carrying a
    /// pre-minted agent identity (packet 2.2). Rides the same owner-only
    /// meta-harness channel as model resolution.
    fn launch_session(&self, request: SessionLaunchRequest) -> Result<MetaHarnessReply>;
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
    launch_reply: Option<MetaHarnessReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessResolver {
    socket_path: PathBuf,
    codec: LengthPrefixedCodec,
}

struct WorkflowRunIdentity {
    workflow_digest: String,
    operation_digest: String,
    contract_digest: String,
    model_resolution_digest: Option<String>,
}

impl WorkflowRunIdentity {
    fn workflow(request: &WorkflowRunRequest) -> Self {
        Self {
            workflow_digest: request.workflow.payload().payload().to_string(),
            operation_digest: request.operation.field_1.payload().to_string(),
            contract_digest: request.contract.payload().payload().to_string(),
            model_resolution_digest: None,
        }
    }

    fn resolved(request: &ResolvedWorkflowRunRequest) -> Result<Self> {
        let model_resolution_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&request.model_resolution)
                .map_err(|error| Error::WorkflowResolutionArchiveEncode {
                    message: error.to_string(),
                })?
                .to_vec();
        let model_resolution_digest = blake3::hash(&model_resolution_bytes).to_hex().to_string();
        Ok(Self {
            model_resolution_digest: Some(model_resolution_digest),
            ..Self::workflow(&request.workflow_run)
        })
    }

    fn handle(&self) -> Result<WorkflowRunHandle> {
        let run = match &self.model_resolution_digest {
            Some(model_resolution_digest) => format!(
                "workflow-run-{}-{}-{}-{}",
                self.workflow_digest,
                self.operation_digest,
                self.contract_digest,
                model_resolution_digest
            ),
            None => format!(
                "workflow-run-{}-{}-{}",
                self.workflow_digest, self.operation_digest, self.contract_digest
            ),
        };
        Ok(WorkflowRunHandle {
            run: WorkflowRunDigest::from_wire_token(run)?,
        })
    }
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
        let handle = self.resolved_handle_for(&request)?;
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
                Ok(OrchestrateReply::WorkflowResolutionAccepted(run))
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
        WorkflowRunIdentity::workflow(request).handle()
    }

    fn resolved_handle_for(
        &self,
        request: &ResolvedWorkflowRunRequest,
    ) -> Result<WorkflowRunHandle> {
        WorkflowRunIdentity::resolved(request)?.handle()
    }

    fn receipt_for(&self, request: &WorkflowRunRequest, handle: &WorkflowRunHandle) -> z2VSX9 {
        z2VSX9 {
            field_0: request.workflow.clone(),
            field_1: z2VSrv::new(request.operation.field_1.clone()),
            field_2: z2VZZu::z2VPcz,
            field_3: z2VY7s::new(z2VbxF::new(
                blake3::hash(handle.run.as_str().as_bytes())
                    .to_hex()
                    .to_string(),
            )),
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
                    call: z2VSrv::new(request.operation.field_1.clone()),
                },
                outcome: StepOutcome::Produced(z2VZZu::z2VPcz),
            }],
        }
    }

    pub fn unavailable_reason(&self) -> ScopeReason {
        ScopeReason::from_text("workflow runner unavailable").expect("static reason")
    }
}

impl FixtureModelResolver {
    pub fn new(reply: Option<MetaHarnessReply>) -> Self {
        Self {
            reply,
            launch_reply: None,
        }
    }

    pub fn with_launch_reply(launch_reply: MetaHarnessReply) -> Self {
        Self {
            reply: None,
            launch_reply: Some(launch_reply),
        }
    }
}

impl HarnessModelResolver for FixtureModelResolver {
    fn resolve_model(&self, _request: ModelResolutionRequest) -> Result<MetaHarnessReply> {
        self.reply
            .clone()
            .ok_or(Error::HarnessResolverNotConfigured)
    }

    fn launch_session(&self, _request: SessionLaunchRequest) -> Result<MetaHarnessReply> {
        self.launch_reply
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

    fn reply_from_frame(
        &self,
        frame: MetaHarnessFrame,
        expected_exchange: ExchangeIdentifier,
        expected_route: WireRoute,
    ) -> Result<MetaHarnessReply> {
        let actual_route = frame.short_header().route();
        match frame.into_body() {
            MetaHarnessFrameBody::Reply { exchange, reply } => {
                if exchange != expected_exchange {
                    return Err(Error::HarnessReplyExchangeMismatch {
                        expected: expected_exchange,
                        actual: exchange,
                    });
                }
                if actual_route != expected_route {
                    return Err(Error::HarnessReplyRouteMismatch {
                        expected: expected_route,
                        actual: actual_route,
                    });
                }
                self.reply_output(reply)
            }
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

impl MetaHarnessResolver {
    fn submit(&self, request: MetaHarnessRequest) -> Result<MetaHarnessReply> {
        let exchange = self.exchange();
        let frame = request.into_frame(exchange)?;
        let route = frame.short_header().route();
        let mut stream = UnixStream::connect(&self.socket_path)?;
        self.codec
            .write_body(&mut stream, &RuntimeFrameBody::new(frame.encode()?))
            .map_err(Error::HarnessTransportFrame)?;
        let body = self
            .codec
            .read_body(&mut stream)
            .map_err(Error::HarnessTransportFrame)?;
        self.reply_from_frame(MetaHarnessFrame::decode(body.bytes())?, exchange, route)
    }
}

impl HarnessModelResolver for MetaHarnessResolver {
    fn resolve_model(&self, request: ModelResolutionRequest) -> Result<MetaHarnessReply> {
        self.submit(MetaHarnessRequest::ResolveModel(request))
    }

    fn launch_session(&self, request: SessionLaunchRequest) -> Result<MetaHarnessReply> {
        self.submit(MetaHarnessRequest::LaunchSession(request))
    }
}
