//! The thin end-to-end proof of the guard substrate's local plane.
//!
//! Offline, non-blocking, one step. It demonstrates the whole local seam:
//!
//! (a) orchestrate's `WorkflowEngine` runs a one-step workflow whose single step
//!     is dispatched to the `agent` component via its built-in `FixtureProvider`
//!     (no network, no key) with `OutputMode::Nota`, parses the fixture
//!     completion into `EvaluationDecision::Authorized`, combines it under
//!     `CombinationRule::Unanimous`, and produces an unsigned local-plane
//!     `WorkflowReceipt { outcome: Authorized, .. }` (the receipt criome adopts
//!     by trust per Spirit `ic4o`);
//! (b) that receipt, placed in criome's `Evidence.workflow_receipts`, makes
//!     criome's LANDED `Rule::Workflow` evaluator return `Authorized`;
//! (c) the negative path: the same criome evaluation with no matching receipt
//!     returns `Escalate(Workflow <digest>)`.
//!
//! This proves produce → adopt and escalate-when-absent across the whole local
//! seam with no live provider and no blocking.

use criome::language::{ContractStore, KeyRegistry};
use criome::master_key::MasterKey;
use orchestrate::WorkflowEngine;
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizedObjectKind, AuthorizedObjectReference,
    ComponentKind, Contract, ContractDigest, EscalationTarget, EvaluationDecision, Evidence,
    Identity, ObjectDigest, OperationDigest, RequiredSignatureThreshold, Rule, SignatureEnvelope,
    SignatureScheme, TimeSignature, TimeWindow, TimestampNanos, WorkflowDigest, WorkflowGuard,
    WorkflowReceipt,
};
use signal_orchestrate::{
    CombinationRule, WorkflowDefinition, WorkflowRunRequest, WorkflowStep, WorkflowStepName,
};

/// The guarded operation the run judges (content-addressed reference + digest).
const OPERATION_BYTES: &[u8] = b"spirit intent admit: thin slice operation";
/// The content-addressed workflow definition digest the criome contract names.
const WORKFLOW_BYTES: &[u8] = b"guardian workflow: thin slice";
/// The criome guard contract digest the run satisfies.
const CONTRACT_BYTES: &[u8] = b"guard contract: thin slice";

/// A signed attested moment, mirroring criome's own test clock so the evidence
/// stamp verifies (criome runs `stamp.rejection_reason` before the rule).
struct AttestedClock {
    authority: Identity,
    key: MasterKey,
}

impl AttestedClock {
    fn new() -> Self {
        Self {
            authority: Identity::cluster("timekeeper".to_owned()),
            key: MasterKey::generate().expect("test key"),
        }
    }

    fn registry(&self) -> KeyRegistry {
        let mut registry = KeyRegistry::new();
        registry.admit(self.authority.clone(), self.key.public_key());
        registry
    }

    fn moment(&self, opens_at: u64, closes_at: u64) -> AttestedMoment {
        let proposition = AttestedMomentProposition::new(
            TimeWindow {
                opens_at: TimestampNanos::new(opens_at),
                closes_at: TimestampNanos::new(closes_at),
            },
            RequiredSignatureThreshold::new(1),
            vec![self.authority.clone()],
        );
        let signature = self.sign_moment(&proposition);
        AttestedMoment::new(proposition, vec![signature])
    }

    fn sign_moment(&self, proposition: &AttestedMomentProposition) -> TimeSignature {
        let bytes = criome::language::AttestedMomentStatement::new(proposition)
            .to_signing_bytes()
            .expect("moment statement");
        TimeSignature {
            signer: self.authority.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.key.public_key(),
                signature: self.key.sign(bytes.as_slice()),
            },
        }
    }
}

fn workflow_digest() -> WorkflowDigest {
    WorkflowDigest::new(ObjectDigest::from_bytes(WORKFLOW_BYTES))
}

fn operation_reference() -> AuthorizedObjectReference {
    AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(OPERATION_BYTES),
        kind: AuthorizedObjectKind::Operation,
    }
}

fn operation_digest() -> OperationDigest {
    OperationDigest::new(ObjectDigest::from_bytes(OPERATION_BYTES))
}

fn run_request() -> WorkflowRunRequest {
    WorkflowRunRequest {
        workflow: workflow_digest(),
        operation: operation_reference(),
        contract: ContractDigest::from_bytes(CONTRACT_BYTES),
    }
}

/// A one-step workflow: step `guardian`, `Unanimous`, no escalation.
fn guardian_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        steps: vec![WorkflowStep {
            name: WorkflowStepName::from_wire_token("guardian").expect("step name"),
            prompt: ObjectDigest::from_bytes(b"guardian-prompt template"),
            provider: None,
            dependencies: Vec::new(),
        }],
        combination: CombinationRule::Unanimous,
        escalation: None,
    }
}

/// (a) orchestrate runs the one-step workflow through the offline fixture agent
/// and produces an `Authorized` `WorkflowReceipt`.
async fn produce_receipt() -> WorkflowReceipt {
    let mut engine = WorkflowEngine::fixture().expect("fixture engine");
    let produced = engine
        .run_workflow(&guardian_workflow(), &run_request())
        .await
        .expect("workflow run produces a receipt");
    let receipt = produced.receipt;
    assert_eq!(
        receipt.outcome,
        EvaluationDecision::Authorized,
        "the fixture guardian step must combine to Authorized"
    );
    assert_eq!(receipt.workflow, workflow_digest());
    assert_eq!(receipt.operation, operation_digest());
    receipt
}

#[tokio::test]
async fn fixture_workflow_produces_authorized_receipt() {
    let receipt = produce_receipt().await;
    assert_eq!(receipt.outcome, EvaluationDecision::Authorized);
}

/// (a)+(b) the receipt orchestrate produced, placed in criome's Evidence, makes
/// the landed `Rule::Workflow` evaluator adopt `Authorized`.
#[tokio::test]
async fn produced_receipt_makes_criome_authorize() {
    let receipt = produce_receipt().await;

    let clock = AttestedClock::new();
    let registry = clock.registry();
    let mut store = ContractStore::new();
    let digest = store
        .admit(Contract::new(Rule::Workflow(WorkflowGuard {
            workflow: workflow_digest(),
            executor: Identity::host("orchestrate".to_owned()),
        })))
        .expect("admit workflow guard contract");

    let evidence = Evidence::new(
        ComponentKind::Spirit,
        operation_digest(),
        clock.moment(1, 10),
        Vec::new(),
        Vec::new(),
    )
    .with_workflow_receipts(vec![receipt]);

    assert_eq!(
        store.evaluate(&digest, &evidence, &registry),
        Ok(EvaluationDecision::Authorized),
        "criome must adopt the produced receipt's Authorized outcome"
    );
}

/// (c) the negative path: with no matching receipt, criome escalates to the
/// workflow that must run.
#[test]
fn missing_receipt_makes_criome_escalate() {
    let clock = AttestedClock::new();
    let registry = clock.registry();
    let mut store = ContractStore::new();
    let digest = store
        .admit(Contract::new(Rule::Workflow(WorkflowGuard {
            workflow: workflow_digest(),
            executor: Identity::host("orchestrate".to_owned()),
        })))
        .expect("admit workflow guard contract");

    let evidence = Evidence::new(
        ComponentKind::Spirit,
        operation_digest(),
        clock.moment(1, 10),
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(
        store.evaluate(&digest, &evidence, &registry),
        Ok(EvaluationDecision::Escalate(EscalationTarget::Workflow(
            workflow_digest()
        ))),
        "with no receipt criome must escalate to the guardian workflow"
    );
}