use owner_signal_persona_orchestrate::{OwnerOperationKind, OwnerOrchestrateRequest};
use signal_persona_orchestrate::{OperationKind, OrchestrateRequest};
use signal_sema::SemaOperation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredOperation<Kind> {
    kind: Kind,
    effects: Vec<SemaOperation>,
}

impl<Kind> LoweredOperation<Kind> {
    pub fn new(kind: Kind, effects: Vec<SemaOperation>) -> Self {
        Self { kind, effects }
    }

    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    pub fn effects(&self) -> &[SemaOperation] {
        &self.effects
    }
}

pub struct OperationLowering;

impl OperationLowering {
    pub fn ordinary(operation: &OrchestrateRequest) -> LoweredOperation<OperationKind> {
        let effect = match operation {
            OrchestrateRequest::Claim(_) => SemaOperation::Assert,
            OrchestrateRequest::Release(_) => SemaOperation::Retract,
            OrchestrateRequest::Handoff(_) => SemaOperation::Mutate,
            OrchestrateRequest::Observe(_) => SemaOperation::Match,
            OrchestrateRequest::Submit(_) => SemaOperation::Assert,
            OrchestrateRequest::Query(_) => SemaOperation::Match,
            OrchestrateRequest::Watch(_) => SemaOperation::Subscribe,
            OrchestrateRequest::Unwatch(_) => SemaOperation::Retract,
        };
        LoweredOperation::new(operation.operation_kind(), vec![effect])
    }

    pub fn owner(operation: &OwnerOrchestrateRequest) -> LoweredOperation<OwnerOperationKind> {
        let effect = match operation {
            OwnerOrchestrateRequest::Create(_) => SemaOperation::Mutate,
            OwnerOrchestrateRequest::Retire(_) => SemaOperation::Retract,
            OwnerOrchestrateRequest::Refresh(_) => SemaOperation::Mutate,
        };
        LoweredOperation::new(operation.operation_kind(), vec![effect])
    }
}
