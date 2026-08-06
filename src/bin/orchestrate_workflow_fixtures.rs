use dotos::DotosEncode;
use signal_criome::schema::lib::{z2VLn9, z2VaDY, z2VbxF, z2VdZJ, z2VduC, z2Vevu};
use signal_orchestrate::{
    CapabilityProfile, CodexContinuationIdentifier, ContinuationHandle, ContinuationRequest,
    EffortRequest, ModelRequest, ModelResolutionRequest, ModelSelector, OrchestrateRequest,
    ResolvedWorkflowRunRequest, WorkflowRunDigest, WorkflowRunObservation,
    WorkflowRunObservationToken, WorkflowRunRequest,
};

fn main() {
    let workflow = workflow_request("stateful-workflow");
    for input in [
        OrchestrateRequest::RunWorkflow(workflow.clone()),
        OrchestrateRequest::ObserveWorkflowRun(WorkflowRunObservation {
            run: workflow_run_digest("stateful-workflow-run"),
        }),
        OrchestrateRequest::WorkflowRunObservationRetraction(WorkflowRunObservationToken {
            run: workflow_run_digest("stateful-workflow-run"),
        }),
        resolved_workflow_request("stateful-workflow-absent"),
        resolved_workflow_request("stateful-workflow-accepted"),
        resolved_workflow_request("stateful-workflow-unavailable"),
    ] {
        println!("{}", input.to_dotos());
    }
}

fn resolved_workflow_request(workflow: &str) -> OrchestrateRequest {
    OrchestrateRequest::RunResolvedWorkflow(ResolvedWorkflowRunRequest {
        workflow_run: workflow_request(workflow),
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::CapabilityProfile(CapabilityProfile::new("orchestrator")),
                effort: EffortRequest::High,
            },
            continuation: ContinuationRequest::Prefer(ContinuationHandle::Codex(
                CodexContinuationIdentifier::new(workflow),
            )),
        },
    })
}

fn workflow_request(workflow: &str) -> WorkflowRunRequest {
    WorkflowRunRequest {
        workflow: z2VdZJ::new(z2VbxF::new(workflow.to_owned())),
        operation: z2VaDY {
            field_0: z2VduC::z2VemG,
            field_1: z2VbxF::new("stateful-operation".to_owned()),
            field_2: z2VLn9::z2Vccv,
        },
        contract: z2Vevu::new(z2VbxF::new("stateful-contract".to_owned())),
    }
}

fn workflow_run_digest(value: &str) -> WorkflowRunDigest {
    WorkflowRunDigest::from_wire_token(value).expect("workflow run digest")
}
