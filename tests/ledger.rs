use persona_orchestrate::{
    ActivityFilter, ActivityQuery, ActivitySubmission, CreateRoleOrder, HarnessKind,
    OrchestrateLayout, OrchestrateReply, OrchestrateService, OwnerOrchestrateReply,
    OwnerOrchestrateRequest, RefreshRepositoryIndexOrder, RoleClaim, RoleHandoff, RoleName,
    RoleObservation, RoleRelease, ScopeReason, ScopeReference, StoreLocation, TaskToken, WirePath,
};
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
        std::fs::create_dir_all(&workspace).expect("workspace directory");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("persona-orchestrate.redb")
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
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("persona-orchestrate.redb")
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

fn operator() -> RoleName {
    role("operator")
}

fn operator_assistant() -> RoleName {
    role("operator-assistant")
}

fn second_operator_assistant() -> RoleName {
    role("second-operator-assistant")
}

fn designer() -> RoleName {
    role("designer")
}

fn second_designer_assistant() -> RoleName {
    role("second-designer-assistant")
}

fn second_system_assistant() -> RoleName {
    role("second-system-assistant")
}

fn current_workspace_roles() -> Vec<RoleName> {
    let mut roles = RoleName::CURRENT_WORKSPACE_ROLE_TOKENS
        .into_iter()
        .map(role)
        .collect::<Vec<_>>();
    roles.sort();
    roles
}

#[test]
fn claim_conflict_release_and_handoff_use_orchestrate_tables() {
    let fixture = Fixture::new("orchestrate-claims");
    let scope = path("/git/github.com/LiGoldragon/persona-orchestrate");

    let accepted = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleClaim(
            RoleClaim {
                role: operator(),
                scopes: vec![scope.clone()],
                reason: reason("operator owns the migration"),
            },
        ))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    let rejected = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleClaim(
            RoleClaim {
                role: designer(),
                scopes: vec![scope.clone()],
                reason: reason("conflict probe"),
            },
        ))
        .expect("conflict");
    let OrchestrateReply::ClaimRejection(rejection) = rejected else {
        panic!("expected claim rejection");
    };
    assert_eq!(rejection.role, designer());
    assert_eq!(rejection.conflicts[0].held_by, operator());

    let handoff = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleHandoff(
            RoleHandoff {
                from: operator(),
                to: designer(),
                scopes: vec![scope.clone()],
                reason: reason("handoff to designer"),
            },
        ))
        .expect("handoff");
    assert!(matches!(handoff, OrchestrateReply::HandoffAcceptance(_)));

    let snapshot = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleObservation(
            RoleObservation,
        ))
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
        .handle(persona_orchestrate::OrchestrateRequest::RoleRelease(
            RoleRelease { role: designer() },
        ))
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
        .handle(persona_orchestrate::OrchestrateRequest::ActivitySubmission(
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
        .handle(persona_orchestrate::OrchestrateRequest::ActivityQuery(
            ActivityQuery {
                limit: 10,
                filters: vec![ActivityFilter::TaskToken(
                    TaskToken::from_wire_token("primary-hrhz").expect("task token"),
                )],
            },
        ))
        .expect("query");
    let OrchestrateReply::ActivityList(list) = list else {
        panic!("expected activity list");
    };
    assert_eq!(list.records.len(), 1);
    assert_eq!(list.records[0].scope, scope);
    assert!(list.records[0].stamped_at.value() > 0);

    let snapshot = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleObservation(
            RoleObservation,
        ))
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
fn role_observation_includes_current_workspace_lanes() {
    let fixture = Fixture::new("orchestrate-roles");

    let snapshot = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleObservation(
            RoleObservation,
        ))
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
    assert!(roles.contains(&second_operator_assistant()));
    assert!(roles.contains(&second_designer_assistant()));
    assert!(roles.contains(&second_system_assistant()));
}

#[test]
fn dynamic_role_creation_creates_report_lane_and_lock_identity() {
    let fixture = LayoutFixture::new("orchestrate-dynamic-role");
    let role = role("primary-orchestrate-mvp-zxq9-never-collide");

    let reply = fixture
        .service
        .handle_owner(OwnerOrchestrateRequest::CreateRoleOrder(CreateRoleOrder {
            role: role.clone(),
            harness: HarnessKind::Codex,
        }))
        .expect("create role");
    let OwnerOrchestrateReply::RoleCreated(created) = reply else {
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
        .handle(persona_orchestrate::OrchestrateRequest::RoleObservation(
            RoleObservation,
        ))
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
        .handle(persona_orchestrate::OrchestrateRequest::RoleClaim(
            RoleClaim {
                role: created_status.role.clone(),
                scopes: vec![scope.clone()],
                reason: reason("dynamic role owns its work"),
            },
        ))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));
    assert_eq!(
        std::fs::read_to_string(lock_path).expect("lock file"),
        "/tmp/primary-orchestrate-mvp-zxq9-never-collide # dynamic role owns its work\n"
    );
}

#[test]
fn repository_refresh_indexes_local_checkouts_and_workspace_links() {
    let fixture = LayoutFixture::new("orchestrate-repositories");
    let repository_name = "primary-orchestrate-refresh-zxq9-never-collide";
    std::fs::create_dir_all(fixture.git_index.join(repository_name)).expect("repository");

    let reply = fixture
        .service
        .handle_owner(OwnerOrchestrateRequest::RefreshRepositoryIndexOrder(
            RefreshRepositoryIndexOrder {},
        ))
        .expect("refresh repositories");
    let OwnerOrchestrateReply::RepositoryIndexRefreshed(refreshed) = reply else {
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
    let persona_orchestrate_scope = path("/git/github.com/LiGoldragon/persona-orchestrate");

    for scope in [persona_scope.clone(), persona_orchestrate_scope] {
        fixture
            .service
            .handle(persona_orchestrate::OrchestrateRequest::ActivitySubmission(
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
        .handle(persona_orchestrate::OrchestrateRequest::ActivityQuery(
            ActivityQuery {
                limit: 10,
                filters: vec![ActivityFilter::PathPrefix(
                    WirePath::from_absolute_path("/git/github.com/LiGoldragon/persona")
                        .expect("prefix"),
                )],
            },
        ))
        .expect("query");
    let OrchestrateReply::ActivityList(list) = list else {
        panic!("expected activity list");
    };

    assert_eq!(list.records.len(), 1);
    assert_eq!(list.records[0].scope, persona_scope);
}
