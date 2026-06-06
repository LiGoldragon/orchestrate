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
fn orchestrate_cli_speaks_only_to_daemon_sockets() {
    let source = include_str!("../src/bin/orchestrate.rs");
    assert!(source.contains("signal_frame::signal_cli!"));
    assert!(source.contains("working: signal_orchestrate::Frame"));
    assert!(source.contains("owner: meta_signal_orchestrate::Frame"));
    assert!(!source.contains("OrchestrateService"));
    assert!(!source.contains("OrchestrateFrame::new"));
    assert!(!source.contains("MetaOrchestrateFrame::new"));
}

#[test]
fn orchestrate_uses_signal_executor_for_both_signal_contracts() {
    let manifest = include_str!("../Cargo.toml");
    let execution = include_str!("../src/execution.rs");

    assert!(manifest.contains("signal-executor"));
    assert!(execution.contains("impl CommandExecutor for OrdinaryCommandExecutor"));
    assert!(execution.contains("impl CommandExecutor for MetaCommandExecutor"));
    assert!(execution.contains("impl LoweringTrait for OrdinaryLowering"));
    assert!(execution.contains("impl LoweringTrait for MetaLowering"));
}

#[test]
fn daemon_routes_signal_requests_through_executor_backed_service() {
    let daemon = include_str!("../src/daemon.rs");

    assert!(daemon.contains("handle_request(request)"));
    assert!(daemon.contains("handle_meta_request(request)"));
    assert!(!daemon.contains("service.handle(operation)"));
    assert!(!daemon.contains("service.handle_meta(operation)"));
    assert!(!daemon.contains("single_payload"));
}

#[test]
fn daemon_uses_triad_multi_listener_runtime_instead_of_manual_accept_loops() {
    let manifest = include_str!("../Cargo.toml");
    let daemon = include_str!("../src/daemon.rs");

    assert!(manifest.contains("triad-runtime"));
    assert!(daemon.contains("MultiListenerDaemon::new"));
    assert!(daemon.contains("impl MultiListenerRuntime for OrchestrateRuntime"));
    assert!(daemon.contains("BoundedWorkers::new"));
    assert!(daemon.contains("validate_bind_preconditions"));
    assert!(!daemon.contains("UnixListener"));
    assert!(!daemon.contains("std::thread"));
    assert!(!daemon.contains("thread::spawn"));
    assert!(!daemon.contains("fn accept_"));
}
