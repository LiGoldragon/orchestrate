use persona_orchestrate::{ClaimState, RoleName, ScopeReference, WirePath};

fn operator() -> RoleName {
    RoleName::from_wire_token("operator").expect("role")
}

#[test]
fn claim_state_records_scope_once() {
    let mut state = ClaimState::new(operator());
    let scope =
        ScopeReference::Path(WirePath::from_absolute_path("/tmp/persona").expect("test path"));

    state.claim(scope.clone());
    state.claim(scope.clone());

    assert!(state.owns(&scope));
    assert_eq!(state.role(), operator());
}
