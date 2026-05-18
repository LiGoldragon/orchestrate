use persona_orchestrate::{
    ActivityFilter, ActivityQuery, ActivitySubmission, OrchestrateReply, OrchestrateService,
    RoleClaim, RoleHandoff, RoleName, RoleObservation, RoleRelease, ScopeReason, ScopeReference,
    StoreLocation, TaskToken, WirePath,
};
use tempfile::TempDir;

struct Fixture {
    _temporary: TempDir,
    service: OrchestrateService,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("persona-orchestrate.redb")
                .to_string_lossy()
                .into_owned(),
        );
        let service = OrchestrateService::open(&store).expect("service opens");
        Self {
            _temporary: temporary,
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

#[test]
fn claim_conflict_release_and_handoff_use_orchestrate_tables() {
    let fixture = Fixture::new("orchestrate-claims");
    let scope = path("/git/github.com/LiGoldragon/persona-orchestrate");

    let accepted = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleClaim(
            RoleClaim {
                role: RoleName::Operator,
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
                role: RoleName::Designer,
                scopes: vec![scope.clone()],
                reason: reason("conflict probe"),
            },
        ))
        .expect("conflict");
    let OrchestrateReply::ClaimRejection(rejection) = rejected else {
        panic!("expected claim rejection");
    };
    assert_eq!(rejection.role, RoleName::Designer);
    assert_eq!(rejection.conflicts[0].held_by, RoleName::Operator);

    let handoff = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleHandoff(
            RoleHandoff {
                from: RoleName::Operator,
                to: RoleName::Designer,
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
    let designer = snapshot
        .roles
        .iter()
        .find(|status| status.role == RoleName::Designer)
        .expect("designer status");
    assert_eq!(designer.claims[0].scope, scope);

    let released = fixture
        .service
        .handle(persona_orchestrate::OrchestrateRequest::RoleRelease(
            RoleRelease {
                role: RoleName::Designer,
            },
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
                role: RoleName::OperatorAssistant,
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
        .map(|status| status.role)
        .collect::<Vec<_>>();

    assert_eq!(roles, RoleName::ALL);
    assert!(roles.contains(&RoleName::SecondOperatorAssistant));
    assert!(roles.contains(&RoleName::SecondDesignerAssistant));
    assert!(roles.contains(&RoleName::SecondSystemAssistant));
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
                    role: RoleName::OperatorAssistant,
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
