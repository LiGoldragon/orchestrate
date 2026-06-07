use meta_signal_orchestrate::{
    LaneRegistrationRequest, MetaOrchestrateReply, MetaOrchestrateRequest,
};
use orchestrate::{
    Error, LaneAuthority, MetaCommand, MetaCommandExecutor, MetaEffect, MetaLowering,
    OrchestrateLayout, OrchestrateReply, OrchestrateRequest, OrchestrateService, OrdinaryCommand,
    OrdinaryCommandExecutor, OrdinaryEffect, OrdinaryLowering, Role, RoleClaim, RoleName,
    RoleToken, ScopeReason, ScopeReference, StoreLocation, TaskToken,
};
use signal_executor::{CommandExecutor, Executor, Lowering, ObserverSet};
use signal_frame::{AcceptedOutcome, Reply, RequestPayload, SubReply};
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

struct DirectDependencyWitness;

impl DirectDependencyWitness {
    fn ordinary_lowering_is_signal_executor_lowering(lowering: &OrdinaryLowering) {
        fn accepts<LoweringType>(_: &LoweringType)
        where
            LoweringType: Lowering<
                    Operation = OrchestrateRequest,
                    Reply = OrchestrateReply,
                    Command = OrdinaryCommand,
                    ComponentEffect = OrdinaryEffect,
                >,
        {
        }

        accepts(lowering);
    }

    fn meta_lowering_is_signal_executor_lowering(lowering: &MetaLowering) {
        fn accepts<LoweringType>(_: &LoweringType)
        where
            LoweringType: Lowering<
                    Operation = MetaOrchestrateRequest,
                    Reply = MetaOrchestrateReply,
                    Command = MetaCommand,
                    ComponentEffect = MetaEffect,
                >,
        {
        }

        accepts(lowering);
    }

    fn ordinary_executor_is_signal_executor_command_executor(
        executor: &OrdinaryCommandExecutor<'_>,
    ) {
        fn accepts<ExecutorType>(_: &ExecutorType)
        where
            ExecutorType: CommandExecutor<
                    Command = OrdinaryCommand,
                    ComponentEffect = OrdinaryEffect,
                    Error = Error,
                >,
        {
        }

        accepts(executor);
    }

    fn meta_executor_is_signal_executor_command_executor(executor: &MetaCommandExecutor<'_>) {
        fn accepts<ExecutorType>(_: &ExecutorType)
        where
            ExecutorType:
                CommandExecutor<Command = MetaCommand, ComponentEffect = MetaEffect, Error = Error>,
        {
        }

        accepts(executor);
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
        "orchestrate.redb",
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
    let ordinary_lowering = OrdinaryLowering;
    let meta_lowering = MetaLowering;
    let ordinary_executor = OrdinaryCommandExecutor::new(&fixture.service);
    let meta_executor = MetaCommandExecutor::new(&fixture.service);

    DirectDependencyWitness::ordinary_lowering_is_signal_executor_lowering(&ordinary_lowering);
    DirectDependencyWitness::meta_lowering_is_signal_executor_lowering(&meta_lowering);
    DirectDependencyWitness::ordinary_executor_is_signal_executor_command_executor(
        &ordinary_executor,
    );
    DirectDependencyWitness::meta_executor_is_signal_executor_command_executor(&meta_executor);
    DirectDependencyWitness::triad_runtime_workers_are_linked();
    DirectDependencyWitness::signal_frame_request_payloads_are_linked();
    DirectDependencyWitness::sema_engine_error_flows_through_component_error();
    DirectDependencyWitness::version_projection_types_are_linked();
}

#[test]
fn ordinary_requests_execute_through_signal_executor() {
    let fixture = Fixture::new("orchestrate-ordinary-executor");
    let mut executor = Executor::new(
        OrdinaryLowering,
        OrdinaryCommandExecutor::new(&fixture.service),
        ObserverSet::no_op(),
    );

    let reply = futures::executor::block_on(executor.execute(role_claim().into_request()));

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
    assert!(executor.take_last_engine_error().is_none());
}

#[test]
fn meta_requests_execute_through_signal_executor() {
    let fixture = Fixture::new("orchestrate-meta-executor");
    let mut executor = Executor::new(
        MetaLowering,
        MetaCommandExecutor::new(&fixture.service),
        ObserverSet::no_op(),
    );
    let request = MetaOrchestrateRequest::Register(LaneRegistrationRequest {
        role: role_vector(&["Schema", "Designer"]),
        authority: LaneAuthority::Structural,
    });

    let reply = futures::executor::block_on(executor.execute(request.into_request()));

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
    assert!(executor.take_last_engine_error().is_none());
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
