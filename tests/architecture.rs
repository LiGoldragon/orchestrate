use meta_signal_orchestrate::{
    LaneRegistrationRequest, MetaOrchestrateReply, MetaOrchestrateRequest,
};
use orchestrate::{
    Error, LaneAuthority, MetaRequestExecution, OrchestrateLayout, OrchestrateNexusEngine,
    OrchestrateReply, OrchestrateRequest, OrchestrateRequestExecution, OrchestrateSemaEngine,
    OrchestrateService, Role, RoleClaim, RoleName, RoleToken, ScopeReason, ScopeReference,
    StoreLocation, TaskToken,
};
use signal_frame::{AcceptedOutcome, NonEmpty, Reply, Request, RequestPayload, SubReply};
use signal_orchestrate::Observation;
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

struct DirectDependencyWitness;

impl DirectDependencyWitness {
    fn nexus_engine_implements_generated_trait(engine: &mut OrchestrateNexusEngine<'_>) {
        fn accepts<EngineType>(_: &mut EngineType)
        where
            EngineType: orchestrate::schema::nexus::NexusEngine,
        {
        }

        accepts(engine);
    }

    fn sema_engine_implements_generated_trait(engine: &mut OrchestrateSemaEngine<'_>) {
        fn accepts<EngineType>(_: &mut EngineType)
        where
            EngineType: orchestrate::schema::sema::SemaEngine,
        {
        }

        accepts(engine);
    }

    fn triad_runtime_workers_are_linked() {
        let _workers = triad_runtime::BoundedWorkers::new(1);
    }

    fn signal_frame_request_payloads_are_linked() {
        let _ordinary = role_claim().into_request();
        let _meta = MetaOrchestrateRequest::Register(LaneRegistrationRequest {
            role: role_vector(&["Designer"]),
            authority: LaneAuthority::Structural,
        })
        .into_request();
    }

    fn sema_engine_error_flows_through_component_error() {
        let error = sema_engine::Error::TableAlreadyRegistered {
            table: "architecture".to_string(),
        };
        let _component_error = Error::from(error);
    }

    fn version_projection_types_are_linked() {
        let _component = version_projection::ComponentName::new("orchestrate");
    }
}

#[test]
fn orchestrate_cli_cannot_open_component_database() {
    let source = include_str!("../src/bin/orchestrate.rs");
    let forbidden = [
        "OrchestrateService",
        "OrchestrateTables",
        "StoreLocation",
        "sema_engine",
        "orchestrate.sema",
        "PERSONA_ORCHESTRATE_STORE",
    ];

    for token in forbidden {
        assert!(
            !source.contains(token),
            "CLI source must not contain direct store token {token}"
        );
    }
}

#[test]
fn orchestrate_direct_dependencies_have_type_level_witnesses() {
    let fixture = Fixture::new("orchestrate-direct-dependencies");
    let mut nexus = OrchestrateNexusEngine::new(&fixture.service);
    let mut sema = OrchestrateSemaEngine::new(&fixture.service);

    DirectDependencyWitness::nexus_engine_implements_generated_trait(&mut nexus);
    DirectDependencyWitness::sema_engine_implements_generated_trait(&mut sema);
    DirectDependencyWitness::triad_runtime_workers_are_linked();
    DirectDependencyWitness::signal_frame_request_payloads_are_linked();
    DirectDependencyWitness::sema_engine_error_flows_through_component_error();
    DirectDependencyWitness::version_projection_types_are_linked();
}

#[test]
fn ordinary_requests_execute_through_generated_nexus_engine() {
    let fixture = Fixture::new("orchestrate-ordinary-nexus");

    let (reply, engine_error) =
        OrchestrateRequestExecution::new(&fixture.service, role_claim().into_request()).execute();

    let Reply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        panic!("expected committed executor reply");
    };
    let SubReply::Ok(OrchestrateReply::ClaimAcceptance(acceptance)) = per_operation.into_head()
    else {
        panic!("expected claim acceptance");
    };
    assert_eq!(acceptance.role, role("operator"));
    assert!(engine_error.is_none());
}

#[test]
fn meta_requests_execute_through_generated_nexus_engine() {
    let fixture = Fixture::new("orchestrate-meta-nexus");
    let request = MetaOrchestrateRequest::Register(LaneRegistrationRequest {
        role: role_vector(&["Schema", "Designer"]),
        authority: LaneAuthority::Structural,
    });

    let (reply, engine_error) =
        MetaRequestExecution::new(&fixture.service, request.into_request()).execute();

    let Reply::Accepted {
        outcome: AcceptedOutcome::Committed,
        per_operation,
    } = reply
    else {
        panic!("expected committed executor reply");
    };
    let SubReply::Ok(MetaOrchestrateReply::LaneRegistered(registration)) =
        per_operation.into_head()
    else {
        panic!("expected lane registration");
    };
    assert_eq!(
        registration.registration.lane.as_wire_token(),
        "schema-designer"
    );
    assert!(engine_error.is_none());
}

#[test]
fn generated_nexus_path_rejects_multi_payload_atomic_batches_before_commit() {
    let fixture = Fixture::new("orchestrate-nexus-multi-payload");
    let request = Request::from_payloads(NonEmpty::from_head_and_tail(
        role_claim(),
        vec![role_claim()],
    ));

    let (reply, engine_error) =
        OrchestrateRequestExecution::new(&fixture.service, request).execute();

    let Reply::Accepted {
        outcome: AcceptedOutcome::BatchAborted { .. },
        per_operation,
    } = reply
    else {
        panic!("expected batch-aborted generated nexus reply");
    };
    assert!(matches!(per_operation.into_head(), SubReply::Invalidated));
    assert!(matches!(
        engine_error,
        Some(Error::UnsupportedAtomicBatch { operation_count: 2 })
    ));

    let OrchestrateReply::RoleSnapshot(snapshot) = fixture
        .service
        .handle(OrchestrateRequest::Observe(Observation::Roles))
        .expect("roles observe")
    else {
        panic!("expected role snapshot");
    };
    let operator = snapshot
        .roles
        .into_iter()
        .find(|status| status.role == role("operator"))
        .expect("operator role exists");
    assert!(
        operator.claims.is_empty(),
        "multi-payload batch must not commit the first claim before rejecting"
    );
}

#[test]
fn orchestrate_source_does_not_depend_on_old_executor() {
    let sources = [
        include_str!("../Cargo.toml"),
        include_str!("../src/lib.rs"),
        include_str!("../src/service.rs"),
        include_str!("../src/execution.rs"),
    ];
    let hyphenated_name = ["signal", "executor"].join("-");
    let module_name = ["signal", "executor"].join("_");

    for source in sources {
        assert!(
            !source.contains(&hyphenated_name) && !source.contains(&module_name),
            "orchestrate migrated execution must not name the old execution crate"
        );
    }
}

#[test]
fn daemon_has_no_manual_listener_shortcuts() {
    let daemon = include_str!("../src/daemon.rs");

    for forbidden in ["UnixListener", "std::thread", "thread::spawn", "fn accept_"] {
        assert!(
            !daemon.contains(forbidden),
            "daemon source must not contain manual listener shortcut {forbidden}"
        );
    }
}

fn role_claim() -> OrchestrateRequest {
    OrchestrateRequest::Claim(RoleClaim {
        role: role("operator"),
        scopes: vec![ScopeReference::Task(
            TaskToken::from_wire_token("primary-architecture").expect("task token"),
        )],
        reason: ScopeReason::from_text("architecture dependency witness").expect("scope reason"),
    })
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
