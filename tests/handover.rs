use orchestrate::{
    Error, LaneAuthority, LaneRegistrationRequest, MetaOrchestrateReply, MetaOrchestrateRequest,
    MirrorSnapshot, MirrorVersions, Observation, OrchestrateLayout, OrchestrateReply,
    OrchestrateRequest, OrchestrateService, Role, RoleClaim, RoleName, RoleToken, ScopeReason,
    ScopeReference, StoreLocation, WirePath,
};
use tempfile::TempDir;
use version_projection::{ComponentName, ContractVersion, RecordKind};

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

fn role(value: &str) -> RoleName {
    RoleName::from_wire_token(value).expect("role")
}

fn role_token(value: &str) -> RoleToken {
    RoleToken::from_text(value).expect("role token")
}

fn role_vector(values: &[&str]) -> Role {
    Role::try_new(values.iter().map(|value| role_token(value)).collect()).expect("role vector")
}

fn path(value: &str) -> ScopeReference {
    ScopeReference::Path(WirePath::from_absolute_path(value).expect("path"))
}

fn reason(value: &str) -> ScopeReason {
    ScopeReason::from_text(value).expect("scope reason")
}

fn prior_contract_version() -> ContractVersion {
    ContractVersion::new([1; 32])
}

fn mirror_versions() -> MirrorVersions {
    MirrorVersions::new(
        prior_contract_version(),
        MirrorSnapshot::current_contract_version(),
    )
}

#[test]
fn mirror_payload_carries_claim_and_lane_state_between_services() {
    let old = Fixture::new("orchestrate-old-handover");
    let claim_scope = path("/git/github.com/LiGoldragon/orchestrate");

    let accepted = old
        .service
        .handle(OrchestrateRequest::Claim(RoleClaim {
            role: role("operator"),
            scopes: vec![claim_scope.clone()],
            reason: reason("handover mirror owns orchestrate"),
        }))
        .expect("claim");
    assert!(matches!(accepted, OrchestrateReply::ClaimAcceptance(_)));

    let registered = old
        .service
        .handle_meta(MetaOrchestrateRequest::Register(LaneRegistrationRequest {
            role: role_vector(&["Designer"]),
            authority: LaneAuthority::Structural,
        }))
        .expect("register lane");
    let MetaOrchestrateReply::LaneRegistered(registered) = registered else {
        panic!("expected lane registered");
    };
    assert_eq!(registered.registration.lane.as_wire_token(), "designer");

    let payload = old
        .service
        .mirror_payload(mirror_versions())
        .expect("mirror payload");
    assert_eq!(payload.component.as_str(), "orchestrate");
    assert_eq!(payload.kind.as_str(), "MirrorSnapshot");

    let new = Fixture::new("orchestrate-new-handover");
    let restored = new
        .service
        .restore_mirror_payload(&payload)
        .expect("restore mirror");
    assert_eq!(restored.claims.len(), 1);
    assert_eq!(restored.lanes.len(), 1);

    let roles = new
        .service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("observe roles");
    let OrchestrateReply::RoleSnapshot(roles) = roles else {
        panic!("expected role snapshot");
    };
    let operator = roles
        .roles
        .iter()
        .find(|status| status.role.as_wire_token() == "operator")
        .expect("operator role");
    assert_eq!(operator.claims[0].scope, claim_scope);

    let lanes = new
        .service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .expect("observe lanes");
    let OrchestrateReply::LanesObserved(lanes) = lanes else {
        panic!("expected lanes observed");
    };
    assert_eq!(lanes.lanes[0].lane.as_wire_token(), "designer");
}

#[test]
fn mirror_payload_rejects_wrong_component_kind_target_and_archive() {
    let fixture = Fixture::new("orchestrate-handover-rejection");
    let payload = fixture
        .service
        .mirror_payload(mirror_versions())
        .expect("mirror payload");

    let mut wrong_component = payload.clone();
    wrong_component.component = ComponentName::new("persona-spirit");
    assert!(matches!(
        MirrorSnapshot::from_mirror_payload(&wrong_component),
        Err(Error::MirrorComponentMismatch { .. })
    ));

    let mut wrong_kind = payload.clone();
    wrong_kind.kind = RecordKind::new("StampedEntry");
    assert!(matches!(
        MirrorSnapshot::from_mirror_payload(&wrong_kind),
        Err(Error::MirrorKindMismatch { .. })
    ));

    let mut wrong_target = payload.clone();
    wrong_target.target_version = ContractVersion::new([9; 32]);
    assert!(matches!(
        MirrorSnapshot::from_mirror_payload(&wrong_target),
        Err(Error::MirrorTargetVersionMismatch { .. })
    ));

    let mut wrong_archive = payload;
    wrong_archive.payload = vec![1, 2, 3, 4];
    assert!(matches!(
        MirrorSnapshot::from_mirror_payload(&wrong_archive),
        Err(Error::MirrorArchiveDecode { .. })
    ));
}
