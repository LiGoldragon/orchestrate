use signal_criome::{
    EvaluationDecision, OperationDigest, WorkflowProvenanceDigest, WorkflowReceipt,
};
use signal_orchestrate::{
    HostName, ModelAttestation, ModelName, OrchestrateReply, ProviderName, ScopeReason, StepLog,
    StepOutcome, WorkflowReceiptProduced, WorkflowRunDigest, WorkflowRunHandle, WorkflowRunLog,
    WorkflowRunLogReported, WorkflowRunObservation, WorkflowRunObservationClosed,
    WorkflowRunObservationOpened, WorkflowRunObservationToken, WorkflowRunRequest,
    WorkflowRunSnapshot, WorkflowStepName,
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct WorkflowRunner {
    provider: ProviderName,
    model: ModelName,
    host: HostName,
    step: WorkflowStepName,
}

impl WorkflowRunner {
    pub fn fixture() -> Result<Self> {
        Ok(Self {
            provider: ProviderName::from_wire_token("fixture-provider")?,
            model: ModelName::from_wire_token("fixture-model")?,
            host: HostName::from_wire_token("local-orchestrate")?,
            step: WorkflowStepName::from_wire_token("fixture-agent")?,
        })
    }

    pub fn run(&self, request: WorkflowRunRequest) -> Result<OrchestrateReply> {
        let handle = self.handle_for(&request)?;
        let receipt = self.receipt_for(&request, &handle);
        Ok(OrchestrateReply::WorkflowReceiptProduced(
            WorkflowReceiptProduced { handle, receipt },
        ))
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
