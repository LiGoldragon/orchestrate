use meta_signal_harness::MetaHarnessReply;
use orchestrate::{
    ActivityFilter, ActivityQuery, ActivitySubmission, AgentActivityRead, ApplicationFailure,
    ApplicationFailureReason, ApplicationSuccess, BoundedTableReaper, BoundedTableReclamation,
    CURRENT_ACTIVITY_LIMIT, CreateRoleOrder, DownstreamComponent, HarnessKind,
    LaneAlreadyRegisteredResolution, LaneAssignment, LaneAuthority, LaneDetails, LaneIdentifier,
    LaneOwner, LaneReconciliation, LaneRegistrationMode, LaneRegistrationRequest, LaneRegistry,
    LaneUnregistrationRequest, MetaOrchestrateReply, MetaOrchestrateRequest, MissionDescription,
    Observation, ObservationSubscription, OrchestrateLayout, OrchestrateReply, OrchestrateRequest,
    OrchestrateService, OrchestrateTables, OrchestratorAgentRegistration, OrchestratorTopicPath,
    PartialApplied, RefreshRepositoryIndexOrder, ResolvedWorkflowRunRequest, RetireRoleOrder,
    Retirement, Role, RoleClaim, RoleHandoff, RoleName, RoleRelease, RoleToken, ScopeReason,
    ScopeReference, SessionClearRequest, SessionIdentifier, StoreLocation, StoredClaim,
    StoredLaneRegistration, StoredWorkflowModelResolutionOutcome, TaskToken, TimestampNanos,
    TopicName, TopicSelection, WirePath, WorkflowResolutionUnavailable, WorkflowRunRequest,
    WorkflowRunner,
};
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizedObjectKind, AuthorizedObjectReference,
    ComponentKind, Contract, ContractDigest, EscalationTarget, EvaluationDecision, Evidence,
    Identity, OperationDigest, RequiredSignatureThreshold, Rule, SignatureEnvelope,
    SignatureScheme, TimeSignature, TimeWindow, TimestampNanos as CriomeTimestampNanos,
    WorkflowDigest, WorkflowGuard,
};
use signal_harness::{
    CapabilityProfile, CodexContinuationIdentifier, ContinuationHandle, ContinuationRequest,
    EffortRequest, HarnessKind as ResolvedHarnessKind, HarnessName, ModelRequest,
    ModelResolutionRequest, ModelResolved, ModelSelector, ModelUnavailable, ModelUnavailableReason,
    NamedModel,
};
use std::{cell::RefCell, path::PathBuf, rc::Rc};
use tempfile::TempDir;

struct Fixture {
    _temporary: TempDir,
    service: OrchestrateService,
}

struct LayoutFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    git_index: PathBuf,
    service: OrchestrateService,
}

#[derive(Clone)]
struct RecordingModelResolver {
    captured: Rc<RefCell<Vec<ModelResolutionRequest>>>,
    reply: MetaHarnessReply,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\ndesigner\nsystem-operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let mut service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace, git_index),
        )
        .expect("service opens");
        register_standard_lanes(&mut service);
        Self {
            _temporary: temporary,
            service,
        }
    }

    fn handle(&mut self, request: OrchestrateRequest) -> orchestrate::Result<OrchestrateReply> {
        block_on(self.service.handle(request))
    }

    fn handle_meta(
        &mut self,
        request: MetaOrchestrateRequest,
    ) -> orchestrate::Result<MetaOrchestrateReply> {
        block_on(self.service.handle_meta(request))
    }
}

fn block_on<Future: std::future::Future>(future: Future) -> Future::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

impl RecordingModelResolver {
    fn new(reply: MetaHarnessReply) -> Self {
        Self {
            captured: Rc::new(RefCell::new(Vec::new())),
            reply,
        }
    }

    fn captured_requests(&self) -> Vec<ModelResolutionRequest> {
        self.captured.borrow().clone()
    }
}

impl orchestrate::HarnessModelResolver for RecordingModelResolver {
    fn resolve_model(
        &self,
        request: ModelResolutionRequest,
    ) -> orchestrate::Result<MetaHarnessReply> {
        self.captured.borrow_mut().push(request);
        Ok(self.reply.clone())
    }
}

impl LayoutFixture {
    fn new(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("reports")).expect("reports directory");
        std::fs::create_dir_all(workspace.join("repos")).expect("repos directory");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\ndesigner\nsystem-operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let mut service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace.clone(), git_index.clone())
                .with_worktree_index_root(temporary.path().join("wt")),
        )
        .expect("service opens");
        register_standard_lanes(&mut service);
        Self {
            _temporary: temporary,
            workspace,
            git_index,
            service,
        }
    }

    fn handle(&mut self, request: OrchestrateRequest) -> orchestrate::Result<OrchestrateReply> {
        block_on(self.service.handle(request))
    }

    fn handle_meta(
        &mut self,
        request: MetaOrchestrateRequest,
    ) -> orchestrate::Result<MetaOrchestrateReply> {
        block_on(self.service.handle_meta(request))
    }
}

fn path(value: &str) -> ScopeReference {
    ScopeReference::Path(WirePath::from_absolute_path(value).expect("path"))
}

fn task(value: &str) -> ScopeReference {
    ScopeReference::Task(TaskToken::from_wire_token(value).expect("task token"))
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("scope reason")
}

fn role(value: &str) -> RoleName {
    RoleName::from_wire_token(value).expect("role")
}

fn role_token(value: &str) -> RoleToken {
    RoleToken::from_text(value).expect("role token")
}

fn role_vector(values: &[&str]) -> Role {
    Role::try_new(values.iter().map(|value| role_token(value)).collect()).expect("role vector")
}

fn lane(value: &str) -> LaneIdentifier {
    LaneIdentifier::from_wire_token(value).expect("lane")
}

fn session(value: &str) -> SessionIdentifier {
    SessionIdentifier::from_camel_case_name(value).expect("session")
}

fn register_standard_lanes(service: &mut OrchestrateService) {
    for (lane_name, role) in [
        ("operator", role_vector(&["Operator"])),
        ("designer", role_vector(&["Designer"])),
        ("system-operator", role_vector(&["System", "Operator"])),
        ("schema-operator", role_vector(&["Schema", "Operator"])),
        ("schema-designer", role_vector(&["Schema", "Designer"])),
    ] {
        let request = MetaOrchestrateRequest::Register(lane_registration(
            "FixtureClaimSession",
            lane_name,
            role,
        ));
        block_on(service.handle_meta(request)).expect("standard lane registration");
    }
}

fn lane_registration(session_name: &str, lane_name: &str, role: Role) -> LaneRegistrationRequest {
    LaneRegistrationRequest {
        assignment: LaneAssignment {
            session: session(session_name),
            lane: lane(lane_name),
            owner: LaneOwner {
                role,
                authority: LaneAuthority::Structural,
            },
            details: LaneDetails::from_text("ledger lane registration").expect("lane details"),
        },
        mode: LaneRegistrationMode::Fresh,
    }
}

fn operator() -> RoleName {
    role("operator")
}

fn operator_assistant() -> RoleName {
    role("operator-assistant")
}

fn designer() -> RoleName {
    role("designer")
}

fn current_workspace_roles() -> Vec<RoleName> {
    let mut roles = ["operator", "designer", "system-operator"]
        .into_iter()
        .map(role)
        .collect::<Vec<_>>();
    roles.sort();
    roles
}

fn workflow_resolution_tables(name: &str) -> (TempDir, OrchestrateTables) {
    let temporary = tempfile::Builder::new()
        .prefix(name)
        .tempdir()
        .expect("temporary directory");
    let store = StoreLocation::new(
        temporary
            .path()
            .join("orchestrate.sema")
            .to_string_lossy()
            .into_owned(),
    );
    let tables = OrchestrateTables::open(&store).expect("workflow resolution tables");
    (temporary, tables)
}

fn workflow_resolution_run_request() -> WorkflowRunRequest {
    let operation = OperationDigest::from_bytes(b"resolved workflow operation");
    WorkflowRunRequest {
        workflow: WorkflowDigest::from_bytes(b"resolved workflow"),
        operation: AuthorizedObjectReference {
            component_kind: ComponentKind::Spirit,
            object_digest: operation.object_digest().clone(),
            authorized_object_kind: AuthorizedObjectKind::Head,
        },
        contract: ContractDigest::from_bytes(b"resolved workflow contract"),
    }
}

fn signed_time_evidence(operation: OperationDigest) -> (Evidence, criome::language::KeyRegistry) {
    let timekeeper = criome::master_key::MasterKey::generate().expect("timekeeper key");
    let timekeeper_identity = Identity::host("timekeeper".to_string());
    let proposition = AttestedMomentProposition::new(
        TimeWindow {
            opens_at: CriomeTimestampNanos::new(1),
            closes_at: CriomeTimestampNanos::new(2),
        },
        RequiredSignatureThreshold::new(1),
        vec![timekeeper_identity.clone()],
    );
    let statement = criome::language::AttestedMomentStatement::new(&proposition)
        .to_signing_bytes()
        .expect("time statement");
    let stamp = AttestedMoment::new(
        proposition,
        vec![TimeSignature {
            identity: timekeeper_identity.clone(),
            signature_envelope: SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: timekeeper.public_key(),
                bls_signature: timekeeper.sign(&statement),
            },
        }],
    );
    let mut registry = criome::language::KeyRegistry::new();
    registry.admit(timekeeper_identity, timekeeper.public_key());
    (
        Evidence::new(
            ComponentKind::Spirit,
            operation,
            stamp,
            Vec::new(),
            Vec::new(),
        ),
        registry,
    )
}

#[test]
fn lane_identifier_derivation_follows_role_vector_authority_and_ordinal() {
    let cases = [
        (
            role_vector(&["Designer"]),
            LaneAuthority::Structural,
            0,
            "designer",
        ),
        (
            role_vector(&["Designer"]),
            LaneAuthority::Structural,
            1,
            "second-designer",
        ),
        (
            role_vector(&["Note", "Designer"]),
            LaneAuthority::Support,
            0,
            "note-designer-assistant",
        ),
        (
            role_vector(&["PersonaSignal", "Designer"]),
            LaneAuthority::Structural,
            0,
            "persona-signal-designer",
        ),
    ];

    for (role, authority, prior_count, expected) in cases {
        let lane = LaneRegistry::derive_identifier(&role, authority, prior_count)
            .expect("derive identifier");
        assert_eq!(lane.as_wire_token(), expected);
    }
}

#[test]
fn workflow_fixture_receipt_authorizes_criome_workflow_guard() {
    let workflow = WorkflowDigest::from_bytes(b"fixture workflow");
    let operation = OperationDigest::from_bytes(b"spirit guarded head");
    let contract = Contract::root(Rule::workflow(WorkflowGuard {
        workflow_digest: workflow.clone(),
        identity: Identity::host("orchestrate".to_string()),
    }));
    let mut contracts = criome::language::ContractStore::new();
    let contract_digest = contracts.admit(contract).expect("admit workflow contract");
    let object = AuthorizedObjectReference {
        component_kind: ComponentKind::Spirit,
        object_digest: operation.object_digest().clone(),
        authorized_object_kind: AuthorizedObjectKind::Head,
    };
    let (evidence, registry) = signed_time_evidence(operation.clone());

    assert_eq!(
        contracts
            .evaluate(&contract_digest, &evidence, &registry)
            .expect("evaluate without receipt"),
        EvaluationDecision::Escalate(EscalationTarget::Workflow(workflow.clone()))
    );

    let mut fixture = Fixture::new("orchestrate-workflow-fixture");
    let reply = fixture
        .handle(OrchestrateRequest::RunWorkflow(WorkflowRunRequest {
            workflow: workflow.clone(),
            operation: object,
            contract: contract_digest.clone(),
        }))
        .expect("run workflow");
    let OrchestrateReply::WorkflowReceiptProduced(produced) = reply else {
        panic!("expected workflow receipt, got {reply:?}");
    };
    assert_eq!(produced.receipt.workflow_digest, workflow);
    assert_eq!(produced.receipt.operation_digest, operation);
    assert_eq!(
        produced.receipt.evaluation_decision,
        EvaluationDecision::Authorized
    );

    let authorized_evidence = evidence.with_workflow_receipts(vec![produced.receipt]);
    assert_eq!(
        contracts
            .evaluate(&contract_digest, &authorized_evidence, &registry)
            .expect("evaluate with receipt"),
        EvaluationDecision::Authorized
    );
}

#[test]
fn resolved_workflow_exact_model_returns_resolution_without_receipt_and_stores_opaque_continuation()
{
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-exact-model");
    let requested = ResolvedWorkflowRunRequest {
        workflow_run: workflow_resolution_run_request(),
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::Exact(NamedModel::new("gpt-5-codex")),
                effort: EffortRequest::High,
            },
            continuation: ContinuationRequest::Fresh,
        },
    };
    let resolved = ModelResolved {
        harness: HarnessName::new("codex-main"),
        harness_kind: ResolvedHarnessKind::Codex,
        model: NamedModel::new("gpt-5-codex"),
        effort: EffortRequest::High,
        continuation: ContinuationHandle::Codex(CodexContinuationIdentifier::new("codex-turn-9")),
    };
    let resolver = RecordingModelResolver::new(MetaHarnessReply::ModelResolved(resolved.clone()));

    let reply = WorkflowRunner::new(resolver.clone())
        .expect("runner")
        .run_resolved_workflow(requested.clone(), &tables)
        .expect("resolved workflow run");

    assert_eq!(
        resolver.captured_requests(),
        vec![requested.model_resolution.clone()]
    );
    let OrchestrateReply::WorkflowResolutionAccepted(run) = reply else {
        panic!("expected workflow resolution acceptance without receipt, got {reply:?}");
    };
    assert_eq!(run.resolution, resolved);
    let stored = tables
        .workflow_model_resolution_record(&run.handle)
        .expect("stored resolution")
        .expect("resolution row");
    assert_eq!(stored.request, requested);
    assert_eq!(
        stored.outcome,
        StoredWorkflowModelResolutionOutcome::Resolved(resolved)
    );
}

#[test]
fn resolved_workflow_model_resolution_identity_prevents_same_workflow_storage_collision() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-resolution-collision");
    let workflow_run = workflow_resolution_run_request();
    let exact_request = ResolvedWorkflowRunRequest {
        workflow_run: workflow_run.clone(),
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::Exact(NamedModel::new("gpt-5-codex")),
                effort: EffortRequest::High,
            },
            continuation: ContinuationRequest::Fresh,
        },
    };
    let profile_request = ResolvedWorkflowRunRequest {
        workflow_run,
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::CapabilityProfile(CapabilityProfile::new("orchestrator")),
                effort: EffortRequest::Maximum,
            },
            continuation: ContinuationRequest::Prefer(ContinuationHandle::Codex(
                CodexContinuationIdentifier::new("codex-turn-preferred"),
            )),
        },
    };
    let resolved = ModelResolved {
        harness: HarnessName::new("codex-main"),
        harness_kind: ResolvedHarnessKind::Codex,
        model: NamedModel::new("gpt-5-codex"),
        effort: EffortRequest::High,
        continuation: ContinuationHandle::Codex(CodexContinuationIdentifier::new("codex-turn-9")),
    };
    let runner = WorkflowRunner::new(RecordingModelResolver::new(
        MetaHarnessReply::ModelResolved(resolved),
    ))
    .expect("runner");

    let first_reply = runner
        .run_resolved_workflow(exact_request.clone(), &tables)
        .expect("first resolved workflow run");
    let second_reply = runner
        .run_resolved_workflow(profile_request.clone(), &tables)
        .expect("second resolved workflow run");

    let OrchestrateReply::WorkflowResolutionAccepted(first_run) = first_reply else {
        panic!("expected first workflow resolution acceptance, got {first_reply:?}");
    };
    let OrchestrateReply::WorkflowResolutionAccepted(second_run) = second_reply else {
        panic!("expected second workflow resolution acceptance, got {second_reply:?}");
    };
    assert_ne!(
        first_run.handle, second_run.handle,
        "different model-resolution requests for the same workflow must have distinct run handles"
    );

    let records = tables
        .workflow_model_resolution_records()
        .expect("stored workflow model resolutions");
    assert_eq!(records.len(), 2, "resolution attempts must not overwrite");
    assert!(
        records
            .iter()
            .any(|record| record.handle == first_run.handle && record.request == exact_request)
    );
    assert!(
        records
            .iter()
            .any(|record| record.handle == second_run.handle && record.request == profile_request)
    );
}

#[test]
fn resolved_workflow_capability_unavailable_is_stored_without_fallback() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-capability-unavailable");
    let requested = ResolvedWorkflowRunRequest {
        workflow_run: workflow_resolution_run_request(),
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::CapabilityProfile(CapabilityProfile::new("orchestrator")),
                effort: EffortRequest::Maximum,
            },
            continuation: ContinuationRequest::Require(ContinuationHandle::Codex(
                CodexContinuationIdentifier::new("codex-turn-required"),
            )),
        },
    };
    let unavailable = ModelUnavailable {
        request: requested.model_resolution.clone(),
        reason: ModelUnavailableReason::CapabilityUnsupported,
    };
    let resolver =
        RecordingModelResolver::new(MetaHarnessReply::ModelUnavailable(unavailable.clone()));

    let reply = WorkflowRunner::new(resolver.clone())
        .expect("runner")
        .run_resolved_workflow(requested.clone(), &tables)
        .expect("unavailable workflow run");

    assert_eq!(
        resolver.captured_requests(),
        vec![requested.model_resolution.clone()]
    );
    let OrchestrateReply::WorkflowResolutionUnavailable(WorkflowResolutionUnavailable {
        handle,
        unavailable: surfaced,
        ..
    }) = reply
    else {
        panic!("expected typed workflow model unavailable, got {reply:?}");
    };
    assert_eq!(surfaced, unavailable);
    let stored = tables
        .workflow_model_resolution_record(&handle)
        .expect("stored resolution")
        .expect("resolution row");
    assert_eq!(stored.request, requested);
    assert_eq!(
        stored.outcome,
        StoredWorkflowModelResolutionOutcome::Unavailable(unavailable)
    );
}

#[test]
fn observation_subscription_allocates_tokens_and_closes_them() {
    let mut fixture = Fixture::new("orchestrate-observation");

    let first = fixture
        .handle(OrchestrateRequest::Watch(ObservationSubscription {
            include_operations: true,
            include_effects: false,
        }))
        .expect("watch");
    let OrchestrateReply::ObservationOpened(first) = first else {
        panic!("expected observation opened");
    };

    let second = fixture
        .handle(OrchestrateRequest::Watch(ObservationSubscription {
            include_operations: false,
            include_effects: true,
        }))
        .expect("watch");
    let OrchestrateReply::ObservationOpened(second) = second else {
        panic!("expected observation opened");
    };

    assert_eq!(first.token.value(), 1);
    assert_eq!(second.token.value(), 2);

    let closed = fixture
        .handle(OrchestrateRequest::Unwatch(first.token))
        .expect("unwatch");
    let OrchestrateReply::ObservationClosed(closed) = closed else {
        panic!("expected observation closed");
    };
    assert_eq!(closed.token, first.token);
}

#[test]
fn claim_conflict_release_and_handoff_use_orchestrate_tables() {
    let mut fixture = Fixture::new("orchestrate-claims");
    let scope = path("/git/github.com/LiGoldragon/orchestrate");

    let accepted = fixture
        .handle(orchestrate::OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![scope.clone()],
            reason: reason("operator owns the migration"),
        }))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    let rejected = fixture
        .handle(orchestrate::OrchestrateRequest::Claim(RoleClaim {
            role: designer(),
            scopes: vec![scope.clone()],
            reason: reason("conflict probe"),
        }))
        .expect("conflict");
    let OrchestrateReply::ClaimRejection(rejection) = rejected else {
        panic!("expected claim rejection");
    };
    assert_eq!(rejection.role, designer());
    assert_eq!(rejection.conflicts[0].held_by, operator());
    let lanes = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("FixtureClaimSession"),
        )))
        .expect("observe claim lanes");
    let OrchestrateReply::LanesObserved(lanes) = lanes else {
        panic!("expected lanes observed");
    };
    let operator_lane = lanes
        .lanes
        .iter()
        .find(|lane| lane.registration.assignment.lane.as_wire_token() == "operator")
        .expect("operator lane projection");
    assert_eq!(
        operator_lane
            .registration
            .assignment
            .session
            .as_wire_token(),
        "FixtureClaimSession"
    );
    assert_eq!(operator_lane.resource_claims.len(), 1);
    assert_eq!(operator_lane.resource_claims[0].scope, scope);
    assert_eq!(
        operator_lane.resource_claims[0].claimed_at.value()
            + operator_lane.resource_claims[0].age.value(),
        operator_lane.observed_at.value()
    );

    let handoff = fixture
        .handle(orchestrate::OrchestrateRequest::Handoff(RoleHandoff {
            from: operator(),
            to: designer(),
            scopes: vec![scope.clone()],
            reason: reason("handoff to designer"),
        }))
        .expect("handoff");
    assert!(matches!(handoff, OrchestrateReply::HandoffAcceptance(_)));

    let snapshot = fixture
        .handle(orchestrate::OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe");
    let OrchestrateReply::RoleSnapshot(snapshot) = snapshot else {
        panic!("expected role snapshot");
    };
    let designer_status = snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == "designer")
        .expect("designer status");
    assert_eq!(designer_status.claims[0].scope, scope);
    assert!(designer_status.claims[0].claimed_at.value() > 0);
    assert!(designer_status.claims[0].age.value() > 0);

    let operator_scope = path("/git/github.com/LiGoldragon/orchestrate-operator-followup");
    let operator_claim = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![operator_scope.clone()],
            reason: reason("operator keeps separate work"),
        }))
        .expect("operator separate claim");
    assert!(matches!(
        operator_claim,
        OrchestrateReply::ClaimAcceptance(_)
    ));

    let released = fixture
        .handle(orchestrate::OrchestrateRequest::Release(RoleRelease {
            role: designer(),
        }))
        .expect("release");
    let OrchestrateReply::ReleaseAcknowledgment(acknowledgment) = released else {
        panic!("expected release acknowledgment");
    };
    assert_eq!(acknowledgment.released_scopes, vec![scope]);
    let after_release = fixture
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe after release");
    let OrchestrateReply::RoleSnapshot(after_release) = after_release else {
        panic!("expected role snapshot");
    };
    let operator_status = after_release
        .roles
        .iter()
        .find(|status| status.role == operator())
        .expect("operator status");
    assert!(
        operator_status
            .claims
            .iter()
            .any(|claim| claim.scope == operator_scope)
    );
}

#[test]
fn claim_under_unregistered_lane_fails_clearly() {
    let mut fixture = Fixture::new("orchestrate-unregistered-lane-claim");
    let failure = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("unregistered-lane"),
            scopes: vec![path("/tmp/unregistered-lane-claim")],
            reason: reason("must fail without lane registration"),
        }))
        .expect_err("unregistered lane must fail");
    assert!(matches!(
        failure,
        orchestrate::Error::LaneNotRegistered { lane } if lane == "unregistered-lane"
    ));
}

#[test]
fn activity_submission_query_and_observation_are_store_stamped() {
    let mut fixture = Fixture::new("orchestrate-activity");
    let scope = task("primary-hrhz");

    let acknowledgment = fixture
        .handle(orchestrate::OrchestrateRequest::Submit(
            ActivitySubmission {
                role: operator_assistant(),
                scope: scope.clone(),
                reason: reason("carve out orchestration machinery"),
            },
        ))
        .expect("activity");
    assert!(matches!(
        acknowledgment,
        OrchestrateReply::ActivityAcknowledgment(_)
    ));

    let list = fixture
        .handle(orchestrate::OrchestrateRequest::Query(ActivityQuery {
            limit: 10,
            filters: vec![ActivityFilter::TaskToken(
                TaskToken::from_wire_token("primary-hrhz").expect("task token"),
            )],
        }))
        .expect("query");
    let OrchestrateReply::ActivityList(list) = list else {
        panic!("expected activity list");
    };
    assert_eq!(list.records.len(), 1);
    assert_eq!(list.records[0].scope, scope);
    assert!(list.records[0].stamped_at.value() > 0);

    let snapshot = fixture
        .handle(orchestrate::OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe");
    let OrchestrateReply::RoleSnapshot(snapshot) = snapshot else {
        panic!("expected role snapshot");
    };
    assert_eq!(
        snapshot.recent_activity[0].reason.as_str(),
        "carve out orchestration machinery"
    );
}

#[test]
fn repeated_activity_is_bounded_to_current_reality() {
    let mut fixture = Fixture::new("orchestrate-bounded-activity");
    for slot in 0..=CURRENT_ACTIVITY_LIMIT {
        fixture
            .handle(OrchestrateRequest::Submit(ActivitySubmission {
                role: operator(),
                scope: task("bounded-activity"),
                reason: reason(&format!("activity {slot}")),
            }))
            .expect("submit activity");
    }
    let OrchestrateReply::ActivityList(records) = fixture
        .handle(OrchestrateRequest::Query(ActivityQuery {
            limit: (CURRENT_ACTIVITY_LIMIT + 1) as u32,
            filters: Vec::new(),
        }))
        .expect("query activities")
    else {
        panic!("expected bounded activity list");
    };
    assert_eq!(records.records.len(), CURRENT_ACTIVITY_LIMIT);
    assert_eq!(
        records
            .records
            .first()
            .expect("newest retained")
            .reason
            .as_str(),
        "activity 256"
    );
    assert_eq!(
        records
            .records
            .last()
            .expect("oldest retained")
            .reason
            .as_str(),
        "activity 1"
    );
}

#[test]
fn partial_downstream_failure_records_divergence_and_returns_typed_reply() {
    let fixture = Fixture::new("orchestrate-divergence");
    let partial = PartialApplied {
        succeeded: vec![ApplicationSuccess {
            component: DownstreamComponent::Router,
            detail: reason("channel 42 installed"),
        }],
        failed: vec![ApplicationFailure {
            component: DownstreamComponent::Harness,
            reason: ApplicationFailureReason::Unreachable,
            detail: reason("codex-7 transcript is gone"),
        }],
    };

    let reply = fixture
        .service
        .record_partial_application(partial.clone())
        .expect("record partial application");

    let OrchestrateReply::PartialApplied(observed) = reply else {
        panic!("expected partial applied reply");
    };
    assert_eq!(observed, partial);

    let divergences = fixture.service.divergences().expect("stored divergences");
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].slot, 0);
    assert_eq!(divergences[0].clone().into_partial_applied(), partial);
    assert!(divergences[0].stamped_at.value() > 0);
}

#[test]
fn role_observation_includes_current_workspace_lanes() {
    let mut fixture = Fixture::new("orchestrate-roles");

    let snapshot = fixture
        .handle(orchestrate::OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe");
    let OrchestrateReply::RoleSnapshot(snapshot) = snapshot else {
        panic!("expected role snapshot");
    };
    let roles = snapshot
        .roles
        .iter()
        .map(|status| status.role.clone())
        .collect::<Vec<_>>();

    assert_eq!(roles, current_workspace_roles());
}

#[test]
fn dynamic_role_creation_creates_report_lane_and_lock_identity() {
    let mut fixture = LayoutFixture::new("orchestrate-dynamic-role");
    let role = role("primary-orchestrate-mvp-zxq9-never-collide");

    let reply = fixture
        .handle_meta(MetaOrchestrateRequest::Create(CreateRoleOrder {
            role: role.clone(),
            harness: HarnessKind::Codex,
        }))
        .expect("create role");
    let MetaOrchestrateReply::RoleCreated(created) = reply else {
        panic!("expected role created");
    };
    assert_eq!(created.role, role);
    assert_eq!(created.harness, HarnessKind::Codex);
    assert!(std::path::Path::new(created.report_repository_path.as_str()).is_dir());
    assert!(std::path::Path::new(created.report_lane_path.as_str()).exists());
    let lock_path = fixture
        .workspace
        .join("orchestrate")
        .join("primary-orchestrate-mvp-zxq9-never-collide.lock");
    assert_eq!(std::fs::read_to_string(&lock_path).expect("lock file"), "");

    let snapshot = fixture
        .handle(orchestrate::OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe");
    let OrchestrateReply::RoleSnapshot(snapshot) = snapshot else {
        panic!("expected role snapshot");
    };
    let created_status = snapshot
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == "primary-orchestrate-mvp-zxq9-never-collide")
        .expect("created role status");
    assert_eq!(created_status.harness, HarnessKind::Codex);

    let scope = path("/tmp/primary-orchestrate-mvp-zxq9-never-collide");
    let lane_registered = fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "DynamicRoleSession",
            "primary-orchestrate-mvp-zxq9-never-collide",
            role_vector(&["Primary", "Orchestrate", "Mvp", "Zxq9", "Never", "Collide"]),
        )))
        .expect("register dynamic role lane");
    assert!(matches!(
        lane_registered,
        MetaOrchestrateReply::LaneRegistered(_)
    ));

    let accepted = fixture
        .handle(orchestrate::OrchestrateRequest::Claim(RoleClaim {
            role: created_status.role.clone(),
            scopes: vec![scope.clone()],
            reason: reason("dynamic role owns its work"),
        }))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));
    assert_eq!(
        std::fs::read_to_string(lock_path).expect("lock file"),
        "/tmp/primary-orchestrate-mvp-zxq9-never-collide # dynamic role owns its work\n"
    );
}

#[test]
fn role_retirement_removes_claims_and_lock_projection() {
    let mut fixture = LayoutFixture::new("orchestrate-retired-role-claims");
    let retired_role = role("primary-orchestrate-retirement-zxq9-never-collide");
    let survivor_role = role("primary-orchestrate-survivor-zxq9-never-collide");
    let scope = path("/tmp/primary-orchestrate-retired-role-claim");

    let created = fixture
        .handle_meta(MetaOrchestrateRequest::Create(CreateRoleOrder {
            role: retired_role.clone(),
            harness: HarnessKind::Codex,
        }))
        .expect("create retired role");
    assert!(matches!(created, MetaOrchestrateReply::RoleCreated(_)));

    let survivor = fixture
        .handle_meta(MetaOrchestrateRequest::Create(CreateRoleOrder {
            role: survivor_role.clone(),
            harness: HarnessKind::Codex,
        }))
        .expect("create survivor role");
    assert!(matches!(survivor, MetaOrchestrateReply::RoleCreated(_)));

    let retired_lane = fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "RetirementSession",
            "primary-orchestrate-retirement-zxq9-never-collide",
            role_vector(&[
                "Primary",
                "Orchestrate",
                "Retirement",
                "Zxq9",
                "Never",
                "Collide",
            ]),
        )))
        .expect("register retired role lane");
    assert!(matches!(
        retired_lane,
        MetaOrchestrateReply::LaneRegistered(_)
    ));
    let survivor_lane = fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "RetirementSession",
            "primary-orchestrate-survivor-zxq9-never-collide",
            role_vector(&[
                "Primary",
                "Orchestrate",
                "Survivor",
                "Zxq9",
                "Never",
                "Collide",
            ]),
        )))
        .expect("register survivor role lane");
    assert!(matches!(
        survivor_lane,
        MetaOrchestrateReply::LaneRegistered(_)
    ));

    let accepted = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: retired_role.clone(),
            scopes: vec![scope.clone()],
            reason: reason("retired role claim must not become hidden state"),
        }))
        .expect("claim before retirement");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    let lock_path = fixture
        .workspace
        .join("orchestrate")
        .join("primary-orchestrate-retirement-zxq9-never-collide.lock");
    assert!(lock_path.exists());

    let retired = fixture
        .handle_meta(MetaOrchestrateRequest::Retire(Retirement::Role(
            RetireRoleOrder {
                role: retired_role.clone(),
            },
        )))
        .expect("retire role");
    assert!(matches!(retired, MetaOrchestrateReply::RoleRetired(_)));
    assert!(!lock_path.exists());

    let survivor_claim = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: survivor_role.clone(),
            scopes: vec![scope.clone()],
            reason: reason("survivor can claim the released scope"),
        }))
        .expect("claim after retirement");
    assert!(matches!(
        survivor_claim,
        OrchestrateReply::ClaimAcceptance(_)
    ));

    let snapshot = fixture
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe roles");
    let OrchestrateReply::RoleSnapshot(snapshot) = snapshot else {
        panic!("expected role snapshot");
    };
    assert!(
        snapshot
            .roles
            .iter()
            .all(|status| status.role != retired_role)
    );
    let survivor_status = snapshot
        .roles
        .iter()
        .find(|status| status.role == survivor_role)
        .expect("survivor role status");
    assert_eq!(survivor_status.claims[0].scope, scope);
}

#[test]
fn path_overlap_uses_component_boundaries_not_substrings() {
    let mut fixture = Fixture::new("orchestrate-path-boundaries");
    let schema_help = path("/home/li/wt/github.com/LiGoldragon/schema/schema-help");
    let help_design = path("/home/li/wt/github.com/LiGoldragon/schema/schema-help-design");

    let first = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("schema-operator"),
            scopes: vec![schema_help],
            reason: reason("schema help implementation"),
        }))
        .expect("schema-help claim");
    assert!(matches!(first, OrchestrateReply::ClaimAcceptance(_)));

    let second = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("schema-designer"),
            scopes: vec![help_design],
            reason: reason("schema help design"),
        }))
        .expect("schema-help-design claim");
    assert!(matches!(second, OrchestrateReply::ClaimAcceptance(_)));
}

#[test]
fn claim_cleanup_removes_rows_for_missing_lanes() {
    let temporary = tempfile::Builder::new()
        .prefix("orchestrate-orphan-claims")
        .tempdir()
        .expect("temporary directory");
    let store = StoreLocation::new(
        temporary
            .path()
            .join("orchestrate.sema")
            .to_string_lossy()
            .into_owned(),
    );
    let tables = OrchestrateTables::open(&store).expect("tables open");
    tables
        .insert_lane(&StoredLaneRegistration::active(
            lane_registration("CleanupSession", "operator", role_vector(&["Operator"])).assignment,
            TimestampNanos::new(1),
        ))
        .expect("insert lane");
    tables
        .replace_all_claims(&[
            StoredClaim::new(
                lane("operator"),
                path("/tmp/visible-claim"),
                reason("visible claim"),
                TimestampNanos::new(1),
            ),
            StoredClaim::new(
                lane("retired-role-never-visible"),
                path("/tmp/orphan-claim"),
                reason("orphan claim"),
                TimestampNanos::new(1),
            ),
        ])
        .expect("insert claims");

    let removed = tables
        .remove_claims_without_lanes()
        .expect("remove orphan claims");
    assert_eq!(removed.len(), 1);
    assert_eq!(
        removed[0].lane.as_wire_token(),
        "retired-role-never-visible"
    );
    let remaining = tables.claim_records().expect("remaining claims");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].lane, lane("operator"));
}

#[test]
fn reconcile_reaps_idle_active_and_expired_terminal_lanes_only() {
    let temporary = tempfile::Builder::new()
        .prefix("orchestrate-lane-reconcile")
        .tempdir()
        .expect("temporary directory");
    let store = StoreLocation::new(
        temporary
            .path()
            .join("orchestrate.sema")
            .to_string_lossy()
            .into_owned(),
    );
    let tables = OrchestrateTables::open(&store).expect("tables open");

    const HOUR_NANOS: u64 = 60 * 60 * 1_000_000_000;
    let now = tables.current_timestamp().expect("clock").value();
    let at = |offset_hours: u64| TimestampNanos::new(now - offset_hours * HOUR_NANOS);

    // A lane whose last activity is recent survives regardless of its age at
    // registration: liveness is idle time, not lifetime.
    let fresh_active = StoredLaneRegistration::new(
        lane_registration(
            "ReconcileSession",
            "fresh-active",
            role_vector(&["Operator"]),
        )
        .assignment,
        at(500),
        at(0),
        orchestrate::LaneStatus::Active,
    );
    // A leaked lane: Active but idle far past the generous liveness window.
    let idle_active = StoredLaneRegistration::new(
        lane_registration(
            "ReconcileSession",
            "idle-active",
            role_vector(&["Designer"]),
        )
        .assignment,
        at(500),
        at(25),
        orchestrate::LaneStatus::Active,
    );
    // A terminal record just past its short retention window.
    let expired_terminal = StoredLaneRegistration::new(
        lane_registration(
            "ReconcileSession",
            "expired-terminal",
            role_vector(&["Schema", "Designer"]),
        )
        .assignment,
        at(500),
        at(2),
        orchestrate::LaneStatus::Released,
    );
    // A terminal record still inside its retention window: kept for post-mortem.
    let recent_terminal = StoredLaneRegistration::new(
        lane_registration(
            "ReconcileSession",
            "recent-terminal",
            role_vector(&["System", "Operator"]),
        )
        .assignment,
        at(500),
        at(0),
        orchestrate::LaneStatus::Released,
    );
    for registration in [
        &fresh_active,
        &idle_active,
        &expired_terminal,
        &recent_terminal,
    ] {
        tables.insert_lane(registration).expect("insert lane");
    }
    tables
        .replace_all_claims(&[
            StoredClaim::new(
                lane("idle-active"),
                path("/tmp/reap-idle-active"),
                reason("leaked active claim"),
                at(25),
            ),
            StoredClaim::new(
                lane("expired-terminal"),
                path("/tmp/reap-terminal"),
                reason("terminal claim must not outlive lane"),
                at(2),
            ),
            StoredClaim::new(
                lane("recent-terminal"),
                path("/tmp/keep-terminal"),
                reason("postmortem claim remains inside retention"),
                at(0),
            ),
        ])
        .expect("insert claims");
    assert_eq!(tables.lane_records().expect("lanes").len(), 4);

    let reconciliation: LaneReconciliation =
        LaneRegistry::new(&tables).reconcile().expect("reconcile");
    assert_eq!(reconciliation.reaped_idle_active, 1);
    assert_eq!(reconciliation.reaped_terminal, 1);
    assert_eq!(reconciliation.total_reaped(), 2);

    let survivors: Vec<String> = tables
        .lane_records()
        .expect("lanes")
        .into_iter()
        .map(|registration| registration.assignment.lane.as_wire_token().to_string())
        .collect();
    assert_eq!(survivors.len(), 2);
    assert!(survivors.contains(&"fresh-active".to_string()));
    assert!(survivors.contains(&"recent-terminal".to_string()));
    assert!(!survivors.contains(&"idle-active".to_string()));
    assert!(!survivors.contains(&"expired-terminal".to_string()));
    let remaining_claim_lanes: Vec<String> = tables
        .claim_records()
        .expect("claims")
        .into_iter()
        .map(|claim| claim.lane.as_wire_token().to_string())
        .collect();
    assert_eq!(remaining_claim_lanes, vec!["recent-terminal"]);
}

fn lane_tables(prefix: &str) -> (TempDir, OrchestrateTables) {
    let temporary = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("temporary directory");
    let store = StoreLocation::new(
        temporary
            .path()
            .join("orchestrate.sema")
            .to_string_lossy()
            .into_owned(),
    );
    let tables = OrchestrateTables::open(&store).expect("tables open");
    (temporary, tables)
}

fn worker_statuses(tables: &OrchestrateTables) -> Vec<orchestrate::LaneStatus> {
    tables
        .lane_records()
        .expect("lanes")
        .into_iter()
        .filter(|record| record.assignment.lane == lane("worker"))
        .map(|record| record.status)
        .collect()
}

// A released record still inside its retention window would linger for post-
// mortem, but it is finished work: a Fresh registration for the same name must
// supersede it — dropping the dead record and its stale claims — rather than
// conflict, so a released name never squats until the reaper catches up.
#[test]
fn fresh_register_supersedes_released_record_and_drops_its_stale_claims() {
    let (_temporary, tables) = lane_tables("orchestrate-fresh-supersede");
    let registry = LaneRegistry::new(&tables);
    let now = tables.current_timestamp().expect("clock").value();

    let dead = StoredLaneRegistration::new(
        lane_registration("SupersedeSession", "worker", role_vector(&["Operator"])).assignment,
        TimestampNanos::new(now),
        TimestampNanos::new(now),
        orchestrate::LaneStatus::Released,
    );
    let bystander = StoredLaneRegistration::new(
        lane_registration("SupersedeSession", "bystander", role_vector(&["Designer"])).assignment,
        TimestampNanos::new(now),
        TimestampNanos::new(now),
        orchestrate::LaneStatus::Released,
    );
    tables.insert_lane(&dead).expect("insert dead lane");
    tables
        .insert_lane(&bystander)
        .expect("insert bystander lane");
    tables
        .replace_all_claims(&[
            StoredClaim::new(
                lane("worker"),
                path("/tmp/worker-stale"),
                reason("stale claim on released lane"),
                TimestampNanos::new(now),
            ),
            StoredClaim::new(
                lane("bystander"),
                path("/tmp/bystander-keep"),
                reason("unrelated retained claim"),
                TimestampNanos::new(now),
            ),
        ])
        .expect("seed claims");

    let request = lane_registration("SupersedeSession", "worker", role_vector(&["Operator"]));
    let reply = registry
        .register(request)
        .expect("fresh register over released record");
    let MetaOrchestrateReply::LaneRegistered(registered) = reply else {
        panic!("fresh register over a released record must succeed with LaneRegistered");
    };
    assert_eq!(
        registered.registration.status,
        orchestrate::LaneStatus::Active
    );

    // Exactly one "worker" record survives and it is the new Active one: the
    // dead record is gone, not duplicated.
    assert_eq!(
        worker_statuses(&tables),
        vec![orchestrate::LaneStatus::Active]
    );

    // The stale "worker" claim is dropped; the unrelated "bystander" record and
    // its claim keep their full retention, untouched by the targeted supersede.
    let claim_lanes: Vec<String> = tables
        .claim_records()
        .expect("claims")
        .into_iter()
        .map(|claim| claim.lane.as_wire_token().to_string())
        .collect();
    assert_eq!(claim_lanes, vec!["bystander"]);
    assert!(
        tables
            .lane_records()
            .expect("lanes")
            .iter()
            .any(|record| record.assignment.lane == lane("bystander")
                && record.status == orchestrate::LaneStatus::Released)
    );
}

// Recovery inherits a live lane; over a closed record it genuinely re-registers
// rather than reporting a hollow success — the reply type distinguishes the two.
#[test]
fn recovery_inherits_a_live_lane_and_reactivates_a_released_one() {
    let (_temporary, tables) = lane_tables("orchestrate-recovery-honesty");
    let registry = LaneRegistry::new(&tables);

    let fresh = lane_registration("RecoverySession", "worker", role_vector(&["Operator"]));
    let registered = registry
        .register(fresh.clone())
        .expect("initial fresh register");
    assert!(matches!(
        registered,
        MetaOrchestrateReply::LaneRegistered(_)
    ));

    // Recovery over the live lane inherits it.
    let mut recovery = fresh.clone();
    recovery.mode = LaneRegistrationMode::Recovery;
    let inherited = registry
        .register(recovery.clone())
        .expect("recovery over active lane");
    let MetaOrchestrateReply::LaneAlreadyRegistered(inherited) = inherited else {
        panic!("recovery over a live lane must inherit it");
    };
    assert_eq!(
        inherited.resolution,
        LaneAlreadyRegisteredResolution::RecoveryInherited
    );

    // Release the lane, then Recovery over the released record reactivates it
    // with a truthful LaneRegistered — never a silent no-op that leaves the lane
    // Released while claiming success.
    registry
        .unregister(LaneUnregistrationRequest {
            session: session("RecoverySession"),
            lane: lane("worker"),
            details: LaneDetails::from_text("released before recovery").expect("details"),
        })
        .expect("unregister to released");
    assert_eq!(
        worker_statuses(&tables),
        vec![orchestrate::LaneStatus::Released]
    );

    let reactivated = registry
        .register(recovery)
        .expect("recovery over released record");
    let MetaOrchestrateReply::LaneRegistered(reactivated) = reactivated else {
        panic!("recovery over a released record must re-register, not report a hollow success");
    };
    assert_eq!(
        reactivated.registration.status,
        orchestrate::LaneStatus::Active
    );
    assert_eq!(
        worker_statuses(&tables),
        vec![orchestrate::LaneStatus::Active]
    );
}

const REAP_HOUR_NANOS: u64 = 60 * 60 * 1_000_000_000;

fn backdate_agent_activity(
    tables: &OrchestrateTables,
    agent: &orchestrate::OrchestratorAgentIdentifier,
    hours_ago: u64,
) {
    let mut record = tables
        .orchestrator_agent_record(agent)
        .expect("read agent")
        .expect("agent present");
    let now = tables.current_timestamp().expect("clock").value();
    record.last_activity = TimestampNanos::new(now - hours_ago * REAP_HOUR_NANOS);
    tables
        .insert_orchestrator_agent(&record)
        .expect("backdate agent activity");
}

#[test]
fn bounded_reaper_retires_idle_agents_and_deletes_retired_agents_with_their_topic_seats() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-agent-reap");
    let now = tables.current_timestamp().expect("clock");

    // A fresh agent: recently registered, kept whatever its future age.
    let fresh = tables
        .register_orchestrator_agent(
            session("KeepSession"),
            MissionDescription::from_text("stay active").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register fresh agent");
    // A leaked Active agent, idle far past the liveness window: reaper retires it.
    let idle = tables
        .register_orchestrator_agent(
            session("LeakSession"),
            MissionDescription::from_text("leaked agent").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register idle agent");
    // An already-Retired agent past its terminal retention: reaper deletes it.
    let retired = tables
        .register_orchestrator_agent(
            session("DoneSession"),
            MissionDescription::from_text("finished agent").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register retired agent");
    tables
        .retire_orchestrator_agent(&retired.agent_identifier)
        .expect("retire agent");

    // Seat both the idle and the retired agent on a shared topic, so seat reaping
    // tracks agent reaping: the deleted agent's seat goes, the surviving (now
    // retired) agent's seat stays.
    let topic = OrchestratorTopicPath::from_wire_token("engineering").expect("topic path");
    tables
        .ensure_orchestrator_topic(
            topic.clone(),
            TopicName::from_text("engineering").expect("topic name"),
            None,
        )
        .expect("create topic");
    tables
        .seat_agent_on_topic(idle.agent_identifier.clone(), topic.clone())
        .expect("seat idle agent");
    tables
        .seat_agent_on_topic(retired.agent_identifier.clone(), topic.clone())
        .expect("seat retired agent");

    backdate_agent_activity(&tables, &idle.agent_identifier, 25);
    backdate_agent_activity(&tables, &retired.agent_identifier, 2);

    let reclamation: BoundedTableReclamation = BoundedTableReaper::new(now)
        .reconcile(&tables)
        .expect("reconcile");
    assert_eq!(reclamation.retired_idle_agents, 1);
    assert_eq!(reclamation.reaped_retired_agents, 1);

    let agents = tables.orchestrator_agent_records().expect("agents");
    let status_of = |identifier: &orchestrate::OrchestratorAgentIdentifier| {
        agents
            .iter()
            .find(|agent| &agent.agent_identifier == identifier)
            .map(|agent| agent.status)
    };
    assert_eq!(
        status_of(&fresh.agent_identifier),
        Some(orchestrate::OrchestratorAgentStatus::Active)
    );
    assert_eq!(
        status_of(&idle.agent_identifier),
        Some(orchestrate::OrchestratorAgentStatus::Retired)
    );
    assert_eq!(status_of(&retired.agent_identifier), None);

    // The deleted agent's topic seat is gone; the retired-but-present agent's
    // seat remains, so the topic is still populated.
    let members = tables
        .topic_member_identifiers(&topic)
        .expect("topic members");
    assert!(members.contains(&idle.agent_identifier));
    assert!(!members.contains(&retired.agent_identifier));
}

#[test]
fn bounded_reaper_reaps_aged_empty_topics_and_keeps_populated_topics() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-topic-reap");
    let registered_now = tables.current_timestamp().expect("clock").value();
    // Reconcile from a future instant so the just-created topics read as aged.
    let future = TimestampNanos::new(registered_now + 25 * REAP_HOUR_NANOS);

    let empty = OrchestratorTopicPath::from_wire_token("abandoned").expect("empty topic path");
    tables
        .ensure_orchestrator_topic(
            empty.clone(),
            TopicName::from_text("abandoned").expect("name"),
            None,
        )
        .expect("create empty topic");

    let populated = OrchestratorTopicPath::from_wire_token("staffed").expect("populated path");
    tables
        .ensure_orchestrator_topic(
            populated.clone(),
            TopicName::from_text("staffed").expect("name"),
            None,
        )
        .expect("create populated topic");
    let agent = tables
        .register_orchestrator_agent(
            session("TopicSession"),
            MissionDescription::from_text("hold the topic").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register agent");
    // Keep the agent live at the future instant so the topic stays populated.
    let mut agent_record = tables
        .orchestrator_agent_record(&agent.agent_identifier)
        .expect("read")
        .expect("present");
    agent_record.last_activity = future;
    tables
        .insert_orchestrator_agent(&agent_record)
        .expect("refresh agent");
    tables
        .seat_agent_on_topic(agent.agent_identifier.clone(), populated.clone())
        .expect("seat agent");

    let reclamation = BoundedTableReaper::new(future)
        .reconcile(&tables)
        .expect("reconcile");
    assert_eq!(reclamation.reaped_empty_topics, 1);

    let topic_paths: Vec<String> = tables
        .orchestrator_topic_records()
        .expect("topics")
        .into_iter()
        .map(|topic| topic.path.as_str().to_string())
        .collect();
    assert!(topic_paths.contains(&"staffed".to_string()));
    assert!(!topic_paths.contains(&"abandoned".to_string()));
}

#[test]
fn bounded_reaper_reaps_workflow_model_resolutions_past_retention() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-workflow-reap");
    let requested = ResolvedWorkflowRunRequest {
        workflow_run: workflow_resolution_run_request(),
        model_resolution: ModelResolutionRequest {
            model: ModelRequest {
                selector: ModelSelector::Exact(NamedModel::new("gpt-5-codex")),
                effort: EffortRequest::High,
            },
            continuation: ContinuationRequest::Fresh,
        },
    };
    let resolved = ModelResolved {
        harness: HarnessName::new("codex-main"),
        harness_kind: ResolvedHarnessKind::Codex,
        model: NamedModel::new("gpt-5-codex"),
        effort: EffortRequest::High,
        continuation: ContinuationHandle::Codex(CodexContinuationIdentifier::new("codex-turn-1")),
    };
    let resolver = RecordingModelResolver::new(MetaHarnessReply::ModelResolved(resolved));
    WorkflowRunner::new(resolver)
        .expect("runner")
        .run_resolved_workflow(requested, &tables)
        .expect("resolved workflow run");
    assert_eq!(
        tables
            .workflow_model_resolution_records()
            .expect("resolutions")
            .len(),
        1
    );

    let stamped_now = tables.current_timestamp().expect("clock").value();
    // Inside retention: the resolution survives.
    let within_retention = TimestampNanos::new(stamped_now + REAP_HOUR_NANOS);
    let kept = BoundedTableReaper::new(within_retention)
        .reconcile(&tables)
        .expect("reconcile within retention");
    assert_eq!(kept.reaped_workflow_resolutions, 0);
    assert_eq!(
        tables
            .workflow_model_resolution_records()
            .expect("resolutions")
            .len(),
        1
    );

    // Past retention: the resolution is reaped.
    let past_retention = TimestampNanos::new(stamped_now + 25 * REAP_HOUR_NANOS);
    let reaped = BoundedTableReaper::new(past_retention)
        .reconcile(&tables)
        .expect("reconcile past retention");
    assert_eq!(reaped.reaped_workflow_resolutions, 1);
    assert!(
        tables
            .workflow_model_resolution_records()
            .expect("resolutions")
            .is_empty()
    );
}

#[test]
fn bounded_reaper_reaps_terminal_worktree_tombstones_and_keeps_active_and_abandoned() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-worktree-reap");
    let registered_now = tables.current_timestamp().expect("clock").value();
    let old = TimestampNanos::new(registered_now);
    // Reconcile from a future instant so every row reads as aged past retention.
    let future = TimestampNanos::new(registered_now + 25 * REAP_HOUR_NANOS);

    let worktree = |branch: &str, status| orchestrate::StoredWorktree {
        repository: orchestrate::RepositoryName::from_text("orchestrate").expect("repository"),
        branch: orchestrate::BranchName::from_text(branch).expect("branch"),
        path: orchestrate::WirePath::from_absolute_path(format!("/tmp/reap-{branch}"))
            .expect("path"),
        owning_lane: orchestrate::LaneName::from_text("designer").expect("lane"),
        status,
        purpose: orchestrate::PurposeText::from_text("interim reaping fixture").expect("purpose"),
        last_activity: old,
        pushed_state: orchestrate::PushedState::AncestorOfMain,
    };

    // Active work is never reaped by age; an Abandoned row awaits ConcludeWorktree
    // reclaim and is left in place; only concluded tombstones age out.
    tables
        .insert_worktree(&worktree(
            "live-feature",
            orchestrate::WorktreeStatus::Active,
        ))
        .expect("insert active");
    tables
        .insert_worktree(&worktree(
            "leaked-feature",
            orchestrate::WorktreeStatus::Abandoned,
        ))
        .expect("insert abandoned");
    tables
        .insert_worktree(&worktree(
            "done-feature",
            orchestrate::WorktreeStatus::Recycled,
        ))
        .expect("insert recycled");

    let reclamation = BoundedTableReaper::new(future)
        .reconcile(&tables)
        .expect("reconcile");
    assert_eq!(reclamation.reaped_terminal_worktrees, 1);

    let mut remaining: Vec<String> = tables
        .worktree_records()
        .expect("worktrees")
        .into_iter()
        .map(|worktree| worktree.branch.as_str().to_string())
        .collect();
    remaining.sort();
    assert_eq!(remaining, vec!["leaked-feature", "live-feature"]);
}

#[test]
fn lane_registry_register_observe_set_authority_and_retire_are_store_backed() {
    let mut fixture = Fixture::new("orchestrate-lane-registry");
    let designer_role = role_vector(&["Designer"]);

    let first = fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "LedgerSession",
            "designer-ledger",
            designer_role.clone(),
        )))
        .expect("register first lane");
    let MetaOrchestrateReply::LaneRegistered(first) = first else {
        panic!("expected lane registered");
    };
    assert_eq!(
        first.registration.assignment.lane.as_wire_token(),
        "designer-ledger"
    );

    let second = fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "LedgerSession",
            "second-designer-ledger",
            designer_role,
        )))
        .expect("register second lane");
    let MetaOrchestrateReply::LaneRegistered(second) = second else {
        panic!("expected lane registered");
    };
    assert_eq!(
        second.registration.assignment.lane.as_wire_token(),
        "second-designer-ledger"
    );

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("LedgerSession"),
        )))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected lanes observed");
    };
    assert_eq!(observed.lanes.len(), 2);
    assert!(observed.lanes.iter().any(|registration| {
        registration.registration.assignment.lane.as_wire_token() == "designer-ledger"
    }));
    assert!(observed.lanes.iter().any(|registration| {
        registration.registration.assignment.lane.as_wire_token() == "second-designer-ledger"
    }));

    let set = fixture
        .handle_meta(MetaOrchestrateRequest::SetAuthority(
            meta_signal_orchestrate::LaneAuthorityChange {
                lane: lane("designer-ledger"),
                authority: LaneAuthority::Support,
            },
        ))
        .expect("set authority");
    let MetaOrchestrateReply::LaneAuthoritySet(set) = set else {
        panic!("expected authority set");
    };
    assert_eq!(set.lane.as_wire_token(), "designer-ledger");
    assert_eq!(set.authority, LaneAuthority::Support);

    let retired = fixture
        .handle_meta(MetaOrchestrateRequest::Retire(Retirement::Lane(lane(
            "designer-ledger",
        ))))
        .expect("retire lane");
    let MetaOrchestrateReply::LaneRetired(retired) = retired else {
        panic!("expected lane retired");
    };
    assert_eq!(retired.lane.as_wire_token(), "designer-ledger");

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("LedgerSession"),
        )))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected lanes observed");
    };
    assert_eq!(observed.lanes.len(), 1);
    assert_eq!(
        observed.lanes[0]
            .registration
            .assignment
            .lane
            .as_wire_token(),
        "second-designer-ledger"
    );

    let missing = fixture.handle_meta(MetaOrchestrateRequest::Retire(Retirement::Lane(lane(
        "missing-designer",
    ))));
    assert!(matches!(
        missing,
        Err(orchestrate::Error::LaneNotRegistered { lane })
            if lane == "missing-designer"
    ));
}

#[test]
fn observe_projects_sessions_all_lanes_session_lanes_and_resource_claims() {
    let mut fixture = Fixture::new("orchestrate-lane-observe-projections");
    fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "AlphaObserveSession",
            "alpha-observe-worker",
            role_vector(&["Alpha", "Observe", "Worker"]),
        )))
        .expect("register alpha lane");
    fixture
        .handle_meta(MetaOrchestrateRequest::Register(lane_registration(
            "BetaObserveSession",
            "beta-observe-worker",
            role_vector(&["Beta", "Observe", "Worker"]),
        )))
        .expect("register beta lane");

    let claimed_scope = path("/tmp/orchestrate-observe-alpha");
    let claimed_reason = reason("alpha lane projects its resource claim");
    fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("alpha-observe-worker"),
            scopes: vec![claimed_scope.clone()],
            reason: claimed_reason.clone(),
        }))
        .expect("claim alpha lane resource");

    fixture
        .handle_meta(MetaOrchestrateRequest::Unregister(
            LaneUnregistrationRequest {
                session: session("BetaObserveSession"),
                lane: lane("beta-observe-worker"),
                details: LaneDetails::from_text("handover ended beta lane").expect("details"),
            },
        ))
        .expect("unregister beta lane");

    let sessions = fixture
        .handle(OrchestrateRequest::Observe(Observation::Sessions))
        .expect("observe sessions");
    let OrchestrateReply::SessionsObserved(sessions) = sessions else {
        panic!("expected sessions observed");
    };
    let alpha_session = sessions
        .sessions
        .iter()
        .find(|projection| projection.session == session("AlphaObserveSession"))
        .expect("alpha session projection");
    assert_eq!(alpha_session.active_lanes, 1);
    let beta_session = sessions
        .sessions
        .iter()
        .find(|projection| projection.session == session("BetaObserveSession"))
        .expect("beta session projection remains until session clear");
    assert_eq!(beta_session.active_lanes, 0);

    let session_lanes = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("AlphaObserveSession"),
        )))
        .expect("observe alpha session lanes");
    let OrchestrateReply::LanesObserved(session_lanes) = session_lanes else {
        panic!("expected alpha session lanes observed");
    };
    assert_eq!(session_lanes.lanes.len(), 1);
    let alpha_lane = &session_lanes.lanes[0];
    assert_eq!(
        alpha_lane.registration.assignment.lane.as_wire_token(),
        "alpha-observe-worker"
    );
    assert_eq!(
        alpha_lane.registration.status,
        orchestrate::LaneStatus::Active
    );
    assert_eq!(
        alpha_lane.registration.assignment.details.as_str(),
        "ledger lane registration"
    );
    assert!(alpha_lane.observed_at.value() >= alpha_lane.registration.registered_at.value());
    // Lane age is measured from last activity (`updated_at`), which a claim on
    // this lane refreshed, so age is bounded by — and here strictly less than —
    // time since registration.
    assert!(
        alpha_lane.age.value()
            <= alpha_lane.observed_at.value() - alpha_lane.registration.registered_at.value()
    );
    assert_eq!(alpha_lane.resource_claims.len(), 1);
    assert_eq!(alpha_lane.resource_claims[0].scope, claimed_scope);
    assert_eq!(alpha_lane.resource_claims[0].reason, claimed_reason);
    assert!(
        alpha_lane.resource_claims[0].claimed_at.value()
            >= alpha_lane.registration.registered_at.value()
    );

    let all_lanes = fixture
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe all lanes");
    let OrchestrateReply::LanesObserved(all_lanes) = all_lanes else {
        panic!("expected all lanes observed");
    };
    let beta_lane = all_lanes
        .lanes
        .iter()
        .find(|projection| projection.registration.assignment.lane == lane("beta-observe-worker"))
        .expect("beta lane visible in all-lane projection");
    assert_eq!(
        beta_lane.registration.status,
        orchestrate::LaneStatus::Released
    );
    assert!(
        all_lanes
            .lanes
            .iter()
            .any(|projection| projection.registration.assignment.lane
                == lane("alpha-observe-worker")
                && projection.resource_claims.len() == 1)
    );
}

#[test]
fn lane_lifecycle_reports_duplicates_unregisters_and_clears_session_rows() {
    let mut fixture = Fixture::new("orchestrate-lane-lifecycle");
    let registration = lane_registration(
        "LifecycleSession",
        "meta-lifecycle",
        role_vector(&["Meta", "Lifecycle"]),
    );

    let registered = fixture
        .handle_meta(MetaOrchestrateRequest::Register(registration.clone()))
        .expect("register lifecycle lane");
    let MetaOrchestrateReply::LaneRegistered(registered) = registered else {
        panic!("expected lane registered");
    };
    assert_eq!(
        registered.registration.status,
        orchestrate::LaneStatus::Active
    );

    fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("meta-lifecycle"),
            scopes: vec![path("/tmp/meta-lifecycle")],
            reason: reason("duplicate projection includes resource claims"),
        }))
        .expect("claim lane resource");

    let fresh_duplicate = fixture
        .handle_meta(MetaOrchestrateRequest::Register(registration.clone()))
        .expect("duplicate fresh register");
    let MetaOrchestrateReply::LaneAlreadyRegistered(fresh_duplicate) = fresh_duplicate else {
        panic!("expected already registered fresh reply");
    };
    assert_eq!(
        fresh_duplicate.resolution,
        LaneAlreadyRegisteredResolution::FreshConflict
    );
    assert_eq!(fresh_duplicate.active.resource_claims.len(), 1);
    assert_eq!(
        fresh_duplicate
            .active
            .registration
            .assignment
            .details
            .as_str(),
        "ledger lane registration"
    );
    assert_eq!(
        fresh_duplicate.active.registration.status,
        orchestrate::LaneStatus::Active
    );
    assert!(
        fresh_duplicate.active.observed_at.value()
            >= fresh_duplicate.active.registration.registered_at.value()
    );

    let mut recovery_request = registration.clone();
    recovery_request.mode = LaneRegistrationMode::Recovery;
    let recovery_duplicate = fixture
        .handle_meta(MetaOrchestrateRequest::Register(recovery_request))
        .expect("duplicate recovery register");
    let MetaOrchestrateReply::LaneAlreadyRegistered(recovery_duplicate) = recovery_duplicate else {
        panic!("expected already registered recovery reply");
    };
    assert_eq!(
        recovery_duplicate.resolution,
        LaneAlreadyRegisteredResolution::RecoveryInherited
    );

    let unregistered = fixture
        .handle_meta(MetaOrchestrateRequest::Unregister(
            LaneUnregistrationRequest {
                session: session("LifecycleSession"),
                lane: lane("meta-lifecycle"),
                details: LaneDetails::from_text("handover ended active lane").expect("details"),
            },
        ))
        .expect("unregister lane");
    let MetaOrchestrateReply::LaneUnregistered(unregistered) = unregistered else {
        panic!("expected lane unregistered");
    };
    assert_eq!(unregistered.lane.as_wire_token(), "meta-lifecycle");

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("LifecycleSession"),
        )))
        .expect("observe session lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected session lanes observed");
    };
    assert_eq!(observed.lanes.len(), 1);
    assert_eq!(
        observed.lanes[0].registration.status,
        orchestrate::LaneStatus::Released
    );

    let second_registration = lane_registration(
        "LifecycleSession",
        "session-clear-worker",
        role_vector(&["Session", "Clear", "Worker"]),
    );
    let second_registered = fixture
        .handle_meta(MetaOrchestrateRequest::Register(second_registration))
        .expect("register second lane before clear");
    assert!(matches!(
        second_registered,
        MetaOrchestrateReply::LaneRegistered(_)
    ));

    let cleared = fixture
        .handle_meta(MetaOrchestrateRequest::ClearSession(SessionClearRequest {
            session: session("LifecycleSession"),
            details: LaneDetails::from_text("session ended").expect("details"),
        }))
        .expect("clear session");
    let MetaOrchestrateReply::SessionCleared(cleared) = cleared else {
        panic!("expected session cleared");
    };
    assert_eq!(cleared.session.as_wire_token(), "LifecycleSession");
    assert_eq!(cleared.cleared_lanes, 2);

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::SessionLanes(
            session("LifecycleSession"),
        )))
        .expect("observe cleared session lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected cleared session lanes observed");
    };
    assert!(observed.lanes.is_empty());
}

#[test]
fn repository_refresh_indexes_local_checkouts_and_workspace_links() {
    let mut fixture = LayoutFixture::new("orchestrate-repositories");
    let repository_name = "primary-orchestrate-refresh-zxq9-never-collide";
    std::fs::create_dir_all(fixture.git_index.join(repository_name)).expect("repository");

    let reply = fixture
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");
    let MetaOrchestrateReply::RepositoryIndexRefreshed(refreshed) = reply else {
        panic!("expected repository index refresh");
    };
    assert_eq!(refreshed.repositories(), 1);

    let repositories = fixture.service.repositories().expect("repositories");
    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0].name.as_str(), repository_name);
    assert!(
        fixture
            .workspace
            .join("repos")
            .join(repository_name)
            .exists()
    );
}

fn add_origin_remote(checkout: &std::path::Path, url: &str) {
    let remote = std::process::Command::new("jj")
        .arg("--ignore-working-copy")
        .arg("-R")
        .arg(checkout)
        .args(["git", "remote", "add", "origin", url])
        .output()
        .expect("run jj git remote add");
    assert!(
        remote.status.success(),
        "jj git remote add failed: {}",
        String::from_utf8_lossy(&remote.stderr)
    );
}

#[test]
fn repository_refresh_derives_real_identity_and_observe_round_trips() {
    let mut fixture = LayoutFixture::new("orchestrate-repository-identity");
    let checkout = make_contention_repository(&fixture, "identified");
    add_origin_remote(&checkout, "https://github.com/LiGoldragon/identified.git");
    std::fs::create_dir_all(fixture.git_index.join("bare-no-remote")).expect("bare directory");

    fixture
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::Repositories))
        .expect("observe repositories");
    let OrchestrateReply::RepositoriesObserved(reply) = observed else {
        panic!("expected RepositoriesObserved, got {observed:?}");
    };
    assert_eq!(reply.repositories.len(), 2);
    let bare = &reply.repositories[0];
    assert_eq!(bare.name.as_str(), "bare-no-remote");
    assert!(
        matches!(
            &bare.identity,
            orchestrate::RepositoryIdentityState::IdentityUnknown(_)
        ),
        "a directory without a readable remote keeps a typed identity gap: {:?}",
        bare.identity
    );
    let identified = &reply.repositories[1];
    assert_eq!(identified.name.as_str(), "identified");
    match &identified.identity {
        orchestrate::RepositoryIdentityState::Identified(identity) => {
            assert_eq!(
                identity.canonical_text(),
                "github.com/LiGoldragon/identified"
            );
        }
        other => panic!("expected identified repository, got {other:?}"),
    }
}

#[test]
fn workspace_link_path_names_the_same_repository_in_contention() {
    let mut fixture = LayoutFixture::new("orchestrate-link-contention");
    make_contention_repository(&fixture, "linked");
    fixture
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");
    let index_path = fixture.service.repositories().expect("repositories")[0]
        .path
        .as_str()
        .to_owned();
    let link_path = fixture
        .workspace
        .join("repos")
        .join("linked")
        .to_string_lossy()
        .into_owned();

    // The operator holds the repository main through the git-index path.
    let accepted = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![path(&index_path)],
            reason: reason("working on main"),
        }))
        .expect("operator claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    // The designer reaches the SAME repository through its other local path —
    // the workspace `repos/<name>` link. Two local paths, one repository: the
    // claim is answered as repository-main contention, not a plain conflict.
    let contended = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: designer(),
            scopes: vec![path(&link_path)],
            reason: reason("editing through the workspace link"),
        }))
        .expect("designer claim");
    let OrchestrateReply::RepositoryMainContended(contention) = contended else {
        panic!("expected RepositoryMainContended, got {contended:?}");
    };
    assert_eq!(contention.repository.as_str(), "linked");
    assert_eq!(contention.holder, operator());
}

#[test]
fn worktree_request_names_the_absent_repository_by_identity() {
    let mut fixture = LayoutFixture::new("orchestrate-absent-repository");
    let checkout = make_contention_repository(&fixture, "vanishing");
    add_origin_remote(&checkout, "https://github.com/LiGoldragon/vanishing.git");
    fixture
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");

    // The local hosting disappears after indexing; the identity survives in
    // the registry.
    std::fs::remove_dir_all(&checkout).expect("remove checkout");

    let reply = fixture
        .handle(OrchestrateRequest::RequestWorktree(
            orchestrate::WorktreeRequest {
                repository: orchestrate::RepositoryName::from_text("vanishing").expect("name"),
                branch: orchestrate::BranchName::from_text("ghost-branch").expect("branch"),
                owning_lane: orchestrate::LaneName::from_text("designer").expect("lane"),
                purpose: orchestrate::PurposeText::from_text("absent repository witness")
                    .expect("purpose"),
            },
        ))
        .expect("request worktree");
    let OrchestrateReply::WorktreeRequestRejected(rejected) = reply else {
        panic!("expected WorktreeRequestRejected, got {reply:?}");
    };
    let orchestrate::WorktreeRequestRejection::RepositoryAbsentLocally(identity) = rejected.reason
    else {
        panic!("expected RepositoryAbsentLocally, got {:?}", rejected.reason);
    };
    assert_eq!(identity.canonical_text(), "github.com/LiGoldragon/vanishing");
}

#[test]
fn activity_path_prefix_matches_path_boundaries() {
    let mut fixture = Fixture::new("orchestrate-prefix");
    let persona_scope = path("/git/github.com/LiGoldragon/persona");
    let orchestrate_scope = path("/git/github.com/LiGoldragon/orchestrate");

    for scope in [persona_scope.clone(), orchestrate_scope] {
        fixture
            .handle(orchestrate::OrchestrateRequest::Submit(
                ActivitySubmission {
                    role: operator_assistant(),
                    scope,
                    reason: reason("prefix boundary witness"),
                },
            ))
            .expect("activity");
    }

    let list = fixture
        .handle(orchestrate::OrchestrateRequest::Query(ActivityQuery {
            limit: 10,
            filters: vec![ActivityFilter::PathPrefix(
                WirePath::from_absolute_path("/git/github.com/LiGoldragon/persona")
                    .expect("prefix"),
            )],
        }))
        .expect("query");
    let OrchestrateReply::ActivityList(list) = list else {
        panic!("expected activity list");
    };

    assert_eq!(list.records.len(), 1);
    assert_eq!(list.records[0].scope, persona_scope);
}

fn register_explicit(
    fixture: &mut Fixture,
    session_name: &str,
    mission_text: &str,
    paths: &[&str],
) -> OrchestrateReply {
    let topic_selection = TopicSelection::Explicit(
        paths
            .iter()
            .map(|path| OrchestratorTopicPath::from_wire_token(*path).expect("topic path"))
            .collect(),
    );
    fixture
        .handle(OrchestrateRequest::RegisterAgent(
            OrchestratorAgentRegistration {
                session: session(session_name),
                mission: MissionDescription::from_text(mission_text).expect("mission"),
                harness: HarnessKind::Codex,
                topic_selection,
                minted_identity: signal_orchestrate::MintedIdentitySelection::None,
            },
        ))
        .expect("explicit registration reply")
}

fn observed_topic_paths(fixture: &mut Fixture) -> Vec<String> {
    let topics = fixture
        .handle(OrchestrateRequest::Observe(Observation::Topics))
        .expect("observe topics");
    let OrchestrateReply::TopicTree(topics) = topics else {
        panic!("expected topic tree");
    };
    topics
        .topics
        .into_iter()
        .map(|topic| topic.path.as_str().to_string())
        .collect()
}

#[test]
fn explicit_registration_creates_a_named_topic_and_seats_the_agent() {
    let mut fixture = Fixture::new("orchestrator-agent-registration");

    // Explicit registration lets an agent author its own topic: an absent topic
    // is created at registration (not rejected as unknown), and the agent is
    // seated on it. The reply's assigned topics reflect that reality.
    let explicit = register_explicit(
        &mut fixture,
        "OrchestratorAgentRegistration",
        "maintain the explicit engineering topic",
        &["engineering"],
    );
    let OrchestrateReply::AgentRegistered(registered) = explicit else {
        panic!("explicit registration must create the named topic and seat the agent");
    };
    assert_eq!(
        registered.assignment_source,
        orchestrate::TopicAssignmentSource::Explicit
    );
    let assigned: Vec<&str> = registered
        .assigned_topics
        .iter()
        .map(|topic| topic.path.as_str())
        .collect();
    assert_eq!(assigned, vec!["engineering"]);

    // The created topic now stands in the tree and the agent appears in the
    // directory seated on it.
    assert_eq!(observed_topic_paths(&mut fixture), vec!["engineering"]);
    let directory = fixture
        .handle(OrchestrateRequest::Observe(Observation::Agents))
        .expect("observe agents");
    let OrchestrateReply::AgentDirectory(directory) = directory else {
        panic!("expected agent directory");
    };
    assert_eq!(directory.agents.len(), 1);
    assert_eq!(
        directory.agents[0].topics,
        vec![OrchestratorTopicPath::from_wire_token("engineering").expect("topic path")]
    );
}

#[test]
fn explicit_registration_creates_every_implied_parent_topic() {
    let mut fixture = Fixture::new("orchestrator-nested-topic");

    // Registering a nested path creates the intermediate parents (root first)
    // and seats the agent on the leaf it named.
    let explicit = register_explicit(
        &mut fixture,
        "OrchestratorNested",
        "coordinate the messaging build",
        &["coordination/messaging"],
    );
    let OrchestrateReply::AgentRegistered(registered) = explicit else {
        panic!("nested registration must create the parent and leaf topics");
    };
    let assigned: Vec<&str> = registered
        .assigned_topics
        .iter()
        .map(|topic| topic.path.as_str())
        .collect();
    assert_eq!(assigned, vec!["coordination/messaging"]);

    // Both the parent and the leaf now stand in the tree, with the parent link
    // recorded.
    let topics = fixture
        .handle(OrchestrateRequest::Observe(Observation::Topics))
        .expect("observe topics");
    let OrchestrateReply::TopicTree(topics) = topics else {
        panic!("expected topic tree");
    };
    let mut paths: Vec<&str> = topics
        .topics
        .iter()
        .map(|topic| topic.path.as_str())
        .collect();
    paths.sort_unstable();
    assert_eq!(paths, vec!["coordination", "coordination/messaging"]);
    let leaf = topics
        .topics
        .iter()
        .find(|topic| topic.path.as_str() == "coordination/messaging")
        .expect("leaf topic");
    assert_eq!(
        leaf.parent,
        Some(OrchestratorTopicPath::from_wire_token("coordination").expect("parent path"))
    );
}

#[test]
fn explicit_registration_joins_an_existing_topic_without_duplicating_it() {
    let mut fixture = Fixture::new("orchestrator-join-topic");

    register_explicit(
        &mut fixture,
        "OrchestratorFirst",
        "found the engineering topic",
        &["engineering/backend"],
    );
    // A second agent naming an already-created path joins it: no duplicate rows,
    // and both the parent and the leaf remain single.
    let second = register_explicit(
        &mut fixture,
        "OrchestratorSecond",
        "join the existing backend topic",
        &["engineering/backend"],
    );
    let OrchestrateReply::AgentRegistered(_) = second else {
        panic!("second registration must join the existing topic");
    };

    let mut paths = observed_topic_paths(&mut fixture);
    paths.sort();
    assert_eq!(paths, vec!["engineering", "engineering/backend"]);

    // The topic now carries both agents as members.
    let detail = fixture
        .handle(OrchestrateRequest::Observe(Observation::Topic(
            OrchestratorTopicPath::from_wire_token("engineering/backend").expect("topic path"),
        )))
        .expect("observe topic");
    let OrchestrateReply::TopicDetail(detail) = detail else {
        panic!("expected topic detail");
    };
    assert_eq!(detail.member_agent_identifiers.len(), 2);
}

#[test]
fn automatic_registration_fails_closed_without_the_topic_judge() {
    let mut fixture = Fixture::new("orchestrator-automatic-registration");

    // The topic judge is shelved this phase: Automatic seating fails closed
    // with `JudgeUnavailable`, carrying the current (empty) topic list so the
    // caller can retry with an explicit selection. No catch-all fallback seat.
    let automatic = fixture
        .handle(OrchestrateRequest::RegisterAgent(
            OrchestratorAgentRegistration {
                session: session("OrchestratorAutomaticRegistration"),
                mission: MissionDescription::from_text("requires the topic judge")
                    .expect("mission"),
                harness: HarnessKind::Codex,
                topic_selection: TopicSelection::Automatic,
                minted_identity: signal_orchestrate::MintedIdentitySelection::None,
            },
        ))
        .expect("automatic registration reply");
    let OrchestrateReply::AgentRegistrationRejected(rejected) = automatic else {
        panic!("automatic registration must fail closed without the topic judge");
    };
    assert_eq!(
        rejected.reason,
        orchestrate::AgentRegistrationRejectionReason::JudgeUnavailable
    );
    assert!(rejected.available_topics.is_empty());

    // The failed automatic registration seated no agent and created no topic.
    assert!(observed_topic_paths(&mut fixture).is_empty());
    let directory = fixture
        .handle(OrchestrateRequest::Observe(Observation::Agents))
        .expect("observe agents");
    let OrchestrateReply::AgentDirectory(directory) = directory else {
        panic!("expected agent directory");
    };
    assert!(directory.agents.is_empty());
}

// ─── Repository-main contention (contention-flow MVP) ─────

/// A real jj repository inside the fixture's git index, so contention
/// scaffolding and the repository refresh see a genuine checkout.
fn make_contention_repository(fixture: &LayoutFixture, name: &str) -> PathBuf {
    let path = fixture.git_index.join(name);
    std::fs::create_dir_all(&path).expect("repository directory");
    let init = std::process::Command::new("jj")
        .args(["--no-pager", "git", "init", "--colocate"])
        .arg(&path)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj git init");
    assert!(
        init.status.success(),
        "jj git init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    for arguments in [
        vec!["config", "set", "--repo", "user.name", "smoke"],
        vec!["config", "set", "--repo", "user.email", "smoke@example.invalid"],
    ] {
        let output = std::process::Command::new("jj")
            .arg("--no-pager")
            .arg("-R")
            .arg(&path)
            .args(&arguments)
            .output()
            .expect("run jj");
        assert!(output.status.success());
    }
    std::fs::write(path.join("base.txt"), "base\n").expect("base file");
    for arguments in [
        vec!["describe", "-m", "base commit"],
        vec!["bookmark", "create", "main", "-r", "@"],
        vec!["new"],
    ] {
        let output = std::process::Command::new("jj")
            .arg("--no-pager")
            .arg("-R")
            .arg(&path)
            .args(&arguments)
            .output()
            .expect("run jj");
        assert!(
            output.status.success(),
            "jj {:?} failed: {}",
            arguments,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    path
}

#[test]
fn contended_repository_main_answers_with_scaffolded_feature_worktree() {
    let mut fixture = LayoutFixture::new("orchestrate-main-contention");
    make_contention_repository(&fixture, "contended");
    fixture
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");
    let repository_path = fixture.service.repositories().expect("repositories")[0]
        .path
        .as_str()
        .to_owned();

    // The operator takes the repository main — the working-on-main default.
    let accepted = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![path(&repository_path)],
            reason: reason("working on main"),
        }))
        .expect("operator claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    // The designer hits the taken main and is answered automatically: holder
    // named, and a feature worktree scaffolded with the designer's lane name
    // as the branch.
    let contended = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: designer(),
            scopes: vec![path(&repository_path)],
            reason: reason("designer needs the same repo"),
        }))
        .expect("designer claim");
    let OrchestrateReply::RepositoryMainContended(contention) = contended else {
        panic!("expected RepositoryMainContended, got {contended:?}");
    };
    assert_eq!(contention.repository.as_str(), "contended");
    assert_eq!(contention.holder, operator());
    assert_eq!(contention.held_reason.as_str(), "working on main");
    let orchestrate::FeatureWorktree::Scaffolded(worktree) = &contention.redirect else {
        panic!("expected scaffolded feature worktree, got {:?}", contention.redirect);
    };
    assert_eq!(worktree.branch.as_str(), "designer");
    assert_eq!(worktree.owning_lane.as_str(), "designer");
    assert!(
        std::path::Path::new(worktree.path.as_str()).join(".jj").exists(),
        "scaffolded feature worktree is a real jj workspace"
    );

    // Claiming again while contended returns the STANDING worktree, not a
    // second scaffold.
    let again = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: designer(),
            scopes: vec![path(&repository_path)],
            reason: reason("designer retries"),
        }))
        .expect("designer reclaim");
    let OrchestrateReply::RepositoryMainContended(retry) = again else {
        panic!("expected RepositoryMainContended, got {again:?}");
    };
    assert!(matches!(
        retry.redirect,
        orchestrate::FeatureWorktree::Existing(_)
    ));

    // The operator releases main and is told, in the release acknowledgment
    // itself, that a branch was started off the repo while it was held.
    let released = fixture
        .handle(OrchestrateRequest::Release(RoleRelease { role: operator() }))
        .expect("operator release");
    let OrchestrateReply::ReleaseAcknowledgment(acknowledgment) = released else {
        panic!("expected ReleaseAcknowledgment, got {released:?}");
    };
    assert_eq!(acknowledgment.started_branches.len(), 1);
    assert_eq!(acknowledgment.started_branches[0].branch.as_str(), "designer");
    assert_eq!(
        acknowledgment.started_branches[0].repository.as_str(),
        "contended"
    );
}

#[test]
fn narrow_path_conflict_stays_a_plain_claim_rejection() {
    let mut fixture = Fixture::new("orchestrate-narrow-conflict");
    // No repository records exist (no refresh): a conflict on an ordinary
    // path must keep the plain typed rejection, never a contention answer.
    let scope = path("/git/github.com/LiGoldragon/persona/src/lib.rs");
    let accepted = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![scope.clone()],
            reason: reason("narrow file claim"),
        }))
        .expect("operator claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));
    let rejected = fixture
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: designer(),
            scopes: vec![scope],
            reason: reason("competing narrow claim"),
        }))
        .expect("designer claim");
    assert!(matches!(rejected, OrchestrateReply::ClaimRejection(_)));
}

// ─── Orchestrator identity mint (mint-relocation) ─────

#[test]
fn minted_identity_is_allocated_then_bound_active_by_preminted_registration() {
    let mut fixture = Fixture::new("orchestrator-mint-preminted-bind");

    // The orchestrator mints ahead of launch: the reservation carries the
    // launch intent and sits `Allocated` in the directory.
    let minted = fixture
        .handle(OrchestrateRequest::MintAgentIdentity(
            orchestrate::AgentIdentityMintRequest {
                session: session("PreLaunchMint"),
                mission: MissionDescription::from_text("launch a worker").expect("mission"),
                harness: HarnessKind::Claude,
            },
        ))
        .expect("mint reply");
    let OrchestrateReply::AgentIdentityMinted(minted) = minted else {
        panic!("expected AgentIdentityMinted");
    };
    assert_eq!(minted.agent_identifier.as_str().len(), 4);
    let directory = fixture
        .handle(OrchestrateRequest::Observe(Observation::Agents))
        .expect("observe agents");
    let OrchestrateReply::AgentDirectory(directory) = directory else {
        panic!("expected agent directory");
    };
    assert_eq!(directory.agents.len(), 1);
    assert_eq!(
        directory.agents[0].status,
        orchestrate::OrchestratorAgentStatus::Allocated
    );

    // Registration with the pre-minted identity binds, it does not mint: the
    // same identifier turns `Active` and no second row appears.
    let registered = fixture
        .handle(OrchestrateRequest::RegisterAgent(
            OrchestratorAgentRegistration {
                session: session("PreLaunchMint"),
                mission: MissionDescription::from_text("launch a worker").expect("mission"),
                harness: HarnessKind::Claude,
                topic_selection: TopicSelection::Explicit(vec![
                    OrchestratorTopicPath::from_wire_token("workers").expect("topic path"),
                ]),
                minted_identity: signal_orchestrate::MintedIdentitySelection::PreMinted(
                    minted.agent_identifier.clone(),
                ),
            },
        ))
        .expect("preminted registration reply");
    let OrchestrateReply::AgentRegistered(registered) = registered else {
        panic!("expected AgentRegistered");
    };
    assert_eq!(registered.agent_identifier, minted.agent_identifier);
    let directory = fixture
        .handle(OrchestrateRequest::Observe(Observation::Agents))
        .expect("observe agents");
    let OrchestrateReply::AgentDirectory(directory) = directory else {
        panic!("expected agent directory");
    };
    assert_eq!(directory.agents.len(), 1);
    assert_eq!(
        directory.agents[0].status,
        orchestrate::OrchestratorAgentStatus::Active
    );
}

#[test]
fn registering_with_an_unknown_preminted_identity_is_a_typed_caller_rejection() {
    let mut fixture = Fixture::new("orchestrator-unknown-preminted");

    let error = fixture
        .handle(OrchestrateRequest::RegisterAgent(
            OrchestratorAgentRegistration {
                session: session("UnknownPreMint"),
                mission: MissionDescription::from_text("bind a ghost identity").expect("mission"),
                harness: HarnessKind::Codex,
                topic_selection: TopicSelection::Explicit(vec![
                    OrchestratorTopicPath::from_wire_token("workers").expect("topic path"),
                ]),
                minted_identity: signal_orchestrate::MintedIdentitySelection::PreMinted(
                    orchestrate::OrchestratorAgentIdentifier::from_wire_token("zzzz")
                        .expect("identifier"),
                ),
            },
        ))
        .expect_err("unknown pre-minted identity must be refused");
    assert!(
        error.is_caller_rejection(),
        "unknown pre-minted identity rides the typed refusal channel, got {error:?}"
    );

    // The refused registration seated nothing.
    let directory = fixture
        .handle(OrchestrateRequest::Observe(Observation::Agents))
        .expect("observe agents");
    let OrchestrateReply::AgentDirectory(directory) = directory else {
        panic!("expected agent directory");
    };
    assert!(directory.agents.is_empty());
}

/// Write a fixture `/proc/<pid>/stat` whose parent pid (field 4) and start time
/// (field 22) land where the activity read's parser reads them.
fn write_process_stat(process_root: &std::path::Path, pid: u32, parent_pid: u32, start: u64) {
    let directory = process_root.join(pid.to_string());
    std::fs::create_dir_all(&directory).expect("proc pid directory");
    let mut fields = vec!["R".to_string(), parent_pid.to_string()];
    fields.extend(std::iter::repeat_n("0".to_string(), 17));
    fields.push(start.to_string());
    std::fs::write(
        directory.join("stat"),
        format!("{pid} (harness) {}", fields.join(" ")),
    )
    .expect("write stat");
}

fn terminal_cell_reachability(target: &std::path::Path, pid: u32, start: u64) -> orchestrate::StoredAgentReachability {
    orchestrate::StoredAgentReachability {
        endpoint_kind: orchestrate::StoredAgentEndpointKind::TerminalCell,
        target: target.to_string_lossy().into_owned(),
        harness_pid: pid,
        harness_start_time: start,
    }
}

#[test]
fn bounded_reaper_refreshes_idle_aged_agent_with_live_command_child() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-busy-refresh");
    let process_root = tempfile::TempDir::new().expect("proc root");
    // The harness (pid 100) holds a live command child (pid 101): an agent
    // inside one long-running command, silent for a day.
    write_process_stat(process_root.path(), 100, 1, 5);
    write_process_stat(process_root.path(), 101, 100, 6);

    let busy = tables
        .register_orchestrator_agent(
            session("BusyBuild"),
            MissionDescription::from_text("hours-long build").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register busy agent");
    // The session directory does not exist: output silence — the child is the
    // only evidence.
    tables
        .attach_agent_reachability(
            &busy.agent_identifier,
            terminal_cell_reachability(
                std::path::Path::new("/nonexistent-session/data.sock"),
                100,
                5,
            ),
        )
        .expect("attach reachability");
    backdate_agent_activity(&tables, &busy.agent_identifier, 25);
    let backdated = tables
        .orchestrator_agent_record(&busy.agent_identifier)
        .expect("read")
        .expect("present")
        .last_activity;

    let now = tables.current_timestamp().expect("clock");
    let reclamation = BoundedTableReaper::new(now)
        .with_activity_read(AgentActivityRead::new(process_root.path()))
        .reconcile(&tables)
        .expect("reconcile");

    assert_eq!(reclamation.refreshed_busy_agents, 1);
    assert_eq!(reclamation.retired_idle_agents, 0);
    let refreshed = tables
        .orchestrator_agent_record(&busy.agent_identifier)
        .expect("read")
        .expect("present");
    assert_eq!(
        refreshed.status,
        orchestrate::OrchestratorAgentStatus::Active,
        "a live command child keeps the agent Active however long it has run"
    );
    assert!(
        refreshed.last_activity.value() > backdated.value(),
        "positive activity re-arms the idle deadline"
    );
}

#[test]
fn bounded_reaper_retires_idle_aged_agent_with_no_observed_activity() {
    let (_temporary, tables) = workflow_resolution_tables("orchestrate-stale-retire");
    let process_root = tempfile::TempDir::new().expect("proc root");
    // The harness (pid 100) is alive but childless: no command in flight.
    write_process_stat(process_root.path(), 100, 1, 5);
    // The session directory exists but holds nothing: no artifact evidence.
    let session_directory = tempfile::TempDir::new().expect("session dir");

    let stale = tables
        .register_orchestrator_agent(
            session("StaleHarness"),
            MissionDescription::from_text("abandoned at a prompt").expect("mission"),
            HarnessKind::Codex,
            signal_orchestrate::MintedIdentitySelection::None,
        )
        .expect("register stale agent");
    tables
        .attach_agent_reachability(
            &stale.agent_identifier,
            terminal_cell_reachability(&session_directory.path().join("data.sock"), 100, 5),
        )
        .expect("attach reachability");
    backdate_agent_activity(&tables, &stale.agent_identifier, 25);

    let now = tables.current_timestamp().expect("clock");
    let reclamation = BoundedTableReaper::new(now)
        .with_activity_read(AgentActivityRead::new(process_root.path()))
        .reconcile(&tables)
        .expect("reconcile");

    assert_eq!(reclamation.refreshed_busy_agents, 0);
    assert_eq!(reclamation.retired_idle_agents, 1);
    assert_eq!(
        tables
            .orchestrator_agent_record(&stale.agent_identifier)
            .expect("read")
            .expect("present")
            .status,
        orchestrate::OrchestratorAgentStatus::Retired,
        "silent and childless past the idle window retires as before"
    );
}
