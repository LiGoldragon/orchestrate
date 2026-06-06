use orchestrate::{
    ActivityFilter, ActivityQuery, ActivitySubmission, ApplicationFailure,
    ApplicationFailureReason, ApplicationSuccess, CreateRoleOrder, DownstreamComponent,
    HarnessKind, LaneAuthority, LaneIdentifier, LaneRegistrationRequest, LaneRegistry,
    MetaOrchestrateReply, MetaOrchestrateRequest, Observation, ObservationSubscription,
    ObservationToken, OperationKind, OperationLowering, OrchestrateLayout, OrchestrateReply,
    OrchestrateRequest, OrchestrateService, PartialApplied, RefreshRepositoryIndexOrder,
    RetireRoleOrder, Retirement, Role, RoleClaim, RoleHandoff, RoleName, RoleRelease, RoleToken,
    ScopeReason, ScopeReference, StoreLocation, TaskToken, WirePath,
};
use signal_sema::SemaOperation;
use std::path::PathBuf;
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
                .join("orchestrate.redb")
                .to_string_lossy()
                .into_owned(),
        );
        let service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace, git_index),
        )
        .expect("service opens");
        Self {
            _temporary: temporary,
            service,
        }
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
                .join("orchestrate.redb")
                .to_string_lossy()
                .into_owned(),
        );
        let service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace.clone(), git_index.clone()),
        )
        .expect("service opens");
        Self {
            _temporary: temporary,
            workspace,
            git_index,
            service,
        }
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
fn ordinary_contract_operations_lower_to_sema_effects() {
    let cases = [
        (
            OrchestrateRequest::Claim(RoleClaim {
                role: operator(),
                scopes: vec![path("/tmp/orchestrate-lowering-claim")],
                reason: reason("lowering claim"),
            }),
            OperationKind::Claim,
            SemaOperation::Assert,
        ),
        (
            OrchestrateRequest::Release(RoleRelease { role: operator() }),
            OperationKind::Release,
            SemaOperation::Retract,
        ),
        (
            OrchestrateRequest::Handoff(RoleHandoff {
                from: operator(),
                to: designer(),
                scopes: vec![path("/tmp/orchestrate-lowering-handoff")],
                reason: reason("lowering handoff"),
            }),
            OperationKind::Handoff,
            SemaOperation::Mutate,
        ),
        (
            OrchestrateRequest::Observe(Observation::Roles),
            OperationKind::Observe,
            SemaOperation::Match,
        ),
        (
            OrchestrateRequest::Submit(ActivitySubmission {
                role: operator(),
                scope: task("primary-lowering"),
                reason: reason("lowering activity"),
            }),
            OperationKind::Submit,
            SemaOperation::Assert,
        ),
        (
            OrchestrateRequest::Query(ActivityQuery {
                limit: 5,
                filters: vec![],
            }),
            OperationKind::Query,
            SemaOperation::Match,
        ),
        (
            OrchestrateRequest::Watch(ObservationSubscription {
                include_operations: true,
                include_sema_effects: true,
            }),
            OperationKind::Watch,
            SemaOperation::Subscribe,
        ),
        (
            OrchestrateRequest::Unwatch(ObservationToken::new(1)),
            OperationKind::Unwatch,
            SemaOperation::Retract,
        ),
    ];

    for (operation, kind, effect) in cases {
        let lowered = OperationLowering::ordinary(&operation);
        assert_eq!(*lowered.kind(), kind);
        assert_eq!(lowered.effects(), &[effect]);
    }
}

#[test]
fn meta_contract_operations_lower_to_sema_effects() {
    let cases = [
        (
            MetaOrchestrateRequest::Create(CreateRoleOrder {
                role: role("primary-lowering-owner-create"),
                harness: HarnessKind::Codex,
            }),
            meta_signal_orchestrate::MetaOperationKind::Create,
            SemaOperation::Mutate,
        ),
        (
            MetaOrchestrateRequest::Retire(Retirement::Role(RetireRoleOrder {
                role: role("primary-lowering-owner-retire"),
            })),
            meta_signal_orchestrate::MetaOperationKind::Retire,
            SemaOperation::Retract,
        ),
        (
            MetaOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {}),
            meta_signal_orchestrate::MetaOperationKind::Refresh,
            SemaOperation::Mutate,
        ),
        (
            MetaOrchestrateRequest::Register(LaneRegistrationRequest {
                role: role_vector(&["Designer"]),
                authority: LaneAuthority::Structural,
            }),
            meta_signal_orchestrate::MetaOperationKind::Register,
            SemaOperation::Mutate,
        ),
        (
            MetaOrchestrateRequest::SetAuthority(meta_signal_orchestrate::LaneAuthorityChange {
                lane: lane("designer"),
                authority: LaneAuthority::Support,
            }),
            meta_signal_orchestrate::MetaOperationKind::SetAuthority,
            SemaOperation::Mutate,
        ),
    ];

    for (operation, kind, effect) in cases {
        let lowered = OperationLowering::meta(&operation);
        assert_eq!(*lowered.kind(), kind);
        assert_eq!(lowered.effects(), &[effect]);
    }
}

#[test]
fn observation_subscription_allocates_tokens_and_closes_them() {
    let fixture = Fixture::new("orchestrate-observation");

    let first = fixture
        .service
        .handle(OrchestrateRequest::Watch(ObservationSubscription {
            include_operations: true,
            include_sema_effects: false,
        }))
        .expect("watch");
    let OrchestrateReply::ObservationOpened(first) = first else {
        panic!("expected observation opened");
    };

    let second = fixture
        .service
        .handle(OrchestrateRequest::Watch(ObservationSubscription {
            include_operations: false,
            include_sema_effects: true,
        }))
        .expect("watch");
    let OrchestrateReply::ObservationOpened(second) = second else {
        panic!("expected observation opened");
    };

    assert_eq!(first.token.value(), 1);
    assert_eq!(second.token.value(), 2);

    let closed = fixture
        .service
        .handle(OrchestrateRequest::Unwatch(first.token))
        .expect("unwatch");
    let OrchestrateReply::ObservationClosed(closed) = closed else {
        panic!("expected observation closed");
    };
    assert_eq!(closed.token, first.token);
}

#[test]
fn claim_conflict_release_and_handoff_use_orchestrate_tables() {
    let fixture = Fixture::new("orchestrate-claims");
    let scope = path("/git/github.com/LiGoldragon/orchestrate");

    let accepted = fixture
        .service
        .handle(orchestrate::OrchestrateRequest::Claim(RoleClaim {
            role: operator(),
            scopes: vec![scope.clone()],
            reason: reason("operator owns the migration"),
        }))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    let rejected = fixture
        .service
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

    let handoff = fixture
        .service
        .handle(orchestrate::OrchestrateRequest::Handoff(RoleHandoff {
            from: operator(),
            to: designer(),
            scopes: vec![scope.clone()],
            reason: reason("handoff to designer"),
        }))
        .expect("handoff");
    assert!(matches!(handoff, OrchestrateReply::HandoffAcceptance(_)));

    let snapshot = fixture
        .service
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

    let released = fixture
        .service
        .handle(orchestrate::OrchestrateRequest::Release(RoleRelease {
            role: designer(),
        }))
        .expect("release");
    let OrchestrateReply::ReleaseAcknowledgment(acknowledgment) = released else {
        panic!("expected release acknowledgment");
    };
    assert_eq!(acknowledgment.released_scopes, vec![scope]);
}

#[test]
fn activity_submission_query_and_observation_are_store_stamped() {
    let fixture = Fixture::new("orchestrate-activity");
    let scope = task("primary-hrhz");

    let acknowledgment = fixture
        .service
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
        .service
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
        .service
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
    let fixture = Fixture::new("orchestrate-roles");

    let snapshot = fixture
        .service
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
    let fixture = LayoutFixture::new("orchestrate-dynamic-role");
    let role = role("primary-orchestrate-mvp-zxq9-never-collide");

    let reply = fixture
        .service
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
        .service
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
    let accepted = fixture
        .service
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
fn lane_registry_register_observe_set_authority_and_retire_are_store_backed() {
    let fixture = Fixture::new("orchestrate-lane-registry");
    let designer_role = role_vector(&["Designer"]);

    let first = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::Register(LaneRegistrationRequest {
            role: designer_role.clone(),
            authority: LaneAuthority::Structural,
        }))
        .expect("register first lane");
    let MetaOrchestrateReply::LaneRegistered(first) = first else {
        panic!("expected lane registered");
    };
    assert_eq!(first.registration.lane.as_wire_token(), "designer");

    let second = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::Register(LaneRegistrationRequest {
            role: designer_role,
            authority: LaneAuthority::Structural,
        }))
        .expect("register second lane");
    let MetaOrchestrateReply::LaneRegistered(second) = second else {
        panic!("expected lane registered");
    };
    assert_eq!(second.registration.lane.as_wire_token(), "second-designer");

    let observed = fixture
        .service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected lanes observed");
    };
    assert_eq!(observed.lanes.len(), 2);
    assert!(
        observed
            .lanes
            .iter()
            .any(|registration| registration.lane.as_wire_token() == "designer")
    );
    assert!(
        observed
            .lanes
            .iter()
            .any(|registration| registration.lane.as_wire_token() == "second-designer")
    );

    let set = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::SetAuthority(
            meta_signal_orchestrate::LaneAuthorityChange {
                lane: lane("designer"),
                authority: LaneAuthority::Support,
            },
        ))
        .expect("set authority");
    let MetaOrchestrateReply::LaneAuthoritySet(set) = set else {
        panic!("expected authority set");
    };
    assert_eq!(set.lane.as_wire_token(), "designer");
    assert_eq!(set.authority, LaneAuthority::Support);

    let retired = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::Retire(Retirement::Lane(lane(
            "designer",
        ))))
        .expect("retire lane");
    let MetaOrchestrateReply::LaneRetired(retired) = retired else {
        panic!("expected lane retired");
    };
    assert_eq!(retired.lane.as_wire_token(), "designer");

    let observed = fixture
        .service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(observed) = observed else {
        panic!("expected lanes observed");
    };
    assert_eq!(observed.lanes.len(), 1);
    assert_eq!(observed.lanes[0].lane.as_wire_token(), "second-designer");

    let missing = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::Retire(Retirement::Lane(lane(
            "missing-designer",
        ))));
    assert!(matches!(
        missing,
        Err(orchestrate::Error::LaneNotRegistered { lane })
            if lane == "missing-designer"
    ));
}

#[test]
fn repository_refresh_indexes_local_checkouts_and_workspace_links() {
    let fixture = LayoutFixture::new("orchestrate-repositories");
    let repository_name = "primary-orchestrate-refresh-zxq9-never-collide";
    std::fs::create_dir_all(fixture.git_index.join(repository_name)).expect("repository");

    let reply = fixture
        .service
        .handle_meta(MetaOrchestrateRequest::Refresh(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");
    let MetaOrchestrateReply::RepositoryIndexRefreshed(refreshed) = reply else {
        panic!("expected repository index refresh");
    };
    assert_eq!(refreshed.repositories, 1);

    let repositories = fixture.service.repositories().expect("repositories");
    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0].name, repository_name);
    assert!(
        fixture
            .workspace
            .join("repos")
            .join(repository_name)
            .exists()
    );
}

#[test]
fn activity_path_prefix_matches_path_boundaries() {
    let fixture = Fixture::new("orchestrate-prefix");
    let persona_scope = path("/git/github.com/LiGoldragon/persona");
    let orchestrate_scope = path("/git/github.com/LiGoldragon/orchestrate");

    for scope in [persona_scope.clone(), orchestrate_scope] {
        fixture
            .service
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
        .service
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
