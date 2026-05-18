use persona_orchestrate::{ClaimState, RoleName, ScopeReference, WirePath};

#[test]
fn claim_state_records_scope_once() {
    let mut state = ClaimState::new(RoleName::Operator);
    let scope = ScopeReference::Path(WirePath::new("/tmp/persona"));

    state.claim(scope.clone());
    state.claim(scope.clone());

    assert!(state.owns(&scope));
    assert_eq!(state.role(), RoleName::Operator);
}
