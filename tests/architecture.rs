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
    assert_eq!(
        source.trim(),
        "signal_frame::signal_cli!(orchestrate, signal_orchestrate);"
    );
    assert!(!source.contains("OrchestrateService"));
    assert!(!source.contains("OrchestrateFrame::new"));
    assert!(!source.contains("OwnerOrchestrateFrame::new"));
}

#[test]
fn orchestrate_uses_signal_executor_for_both_signal_contracts() {
    let manifest = include_str!("../Cargo.toml");
    let execution = include_str!("../src/execution.rs");

    assert!(manifest.contains("signal-executor"));
    assert!(execution.contains("impl CommandExecutor for OrdinaryCommandExecutor"));
    assert!(execution.contains("impl CommandExecutor for OwnerCommandExecutor"));
    assert!(execution.contains("impl LoweringTrait for OrdinaryLowering"));
    assert!(execution.contains("impl LoweringTrait for OwnerLowering"));
}

#[test]
fn daemon_routes_signal_requests_through_executor_backed_service() {
    let daemon = include_str!("../src/daemon.rs");

    assert!(daemon.contains("handle_request(request)"));
    assert!(daemon.contains("handle_owner_request(request)"));
    assert!(!daemon.contains("service.handle(operation)"));
    assert!(!daemon.contains("service.handle_owner(operation)"));
    assert!(!daemon.contains("single_payload"));
}
