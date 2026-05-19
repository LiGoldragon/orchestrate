#[test]
fn persona_orchestrate_cli_cannot_open_component_database() {
    let source = include_str!("../src/bin/persona-orchestrate.rs");
    let forbidden = [
        "OrchestrateService",
        "OrchestrateTables",
        "StoreLocation",
        "sema_engine",
        "persona-orchestrate.redb",
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
fn persona_orchestrate_cli_speaks_only_to_daemon_sockets() {
    let source = include_str!("../src/bin/persona-orchestrate.rs");
    assert!(source.contains("UnixStream::connect"));
    assert!(source.contains("OrchestrateFrame::new"));
    assert!(source.contains("OwnerOrchestrateFrame::new"));
}
