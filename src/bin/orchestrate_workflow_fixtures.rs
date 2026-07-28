use nota::NotaEncode;
use signal_orchestrate::schema::lib::{
    AuthorizedObjectKind, AuthorizedObjectReference, CapabilityProfile,
    CodexContinuationIdentifier, ComponentKind, ContinuationHandle, ContinuationRequest,
    ContractDigest, EffortRequest, Input, ModelRequest, ModelResolutionRequest, ModelSelector,
    ObjectDigest, ResolvedWorkflowRunRequest, WorkflowDigest, WorkflowRunDigest,
    WorkflowRunObservation, WorkflowRunObservationToken, WorkflowRunRequest,
};

fn main() {
    let workflow = workflow_request();
    for input in [
        Input::RunWorkflow(workflow.clone()),
        Input::ObserveWorkflowRun(WorkflowRunObservation::new(WorkflowRunDigest::new(
            "stateful-workflow-run",
        ))),
        Input::WorkflowRunObservationRetraction(WorkflowRunObservationToken::new(
            WorkflowRunDigest::new("stateful-workflow-run"),
        )),
        Input::RunResolvedWorkflow(ResolvedWorkflowRunRequest {
            workflow_run_request: workflow,
            model_resolution_request: ModelResolutionRequest {
                model_request: ModelRequest {
                    model_selector: ModelSelector::CapabilityProfile(CapabilityProfile::new(
                        "orchestrator",
                    )),
                    effort_request: EffortRequest::High,
                },
                continuation_request: ContinuationRequest::Prefer(ContinuationHandle::Codex(
                    CodexContinuationIdentifier::new("stateful-workflow"),
                )),
            },
        }),
    ] {
        println!("{}", input.to_nota());
    }
}

fn workflow_request() -> WorkflowRunRequest {
    WorkflowRunRequest {
        workflow_digest: WorkflowDigest::new(ObjectDigest::new("stateful-workflow")),
        authorized_object_reference: AuthorizedObjectReference {
            component_kind: ComponentKind::Spirit,
            object_digest: ObjectDigest::new("stateful-operation"),
            authorized_object_kind: AuthorizedObjectKind::Head,
        },
        contract_digest: ContractDigest::new(ObjectDigest::new("stateful-contract")),
    }
}
