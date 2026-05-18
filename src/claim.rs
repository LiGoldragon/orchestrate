use signal_persona_orchestrate::{RoleName, ScopeReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimState {
    role: RoleName,
    scopes: Vec<ScopeReference>,
}

impl ClaimState {
    pub fn new(role: RoleName) -> Self {
        Self {
            role,
            scopes: Vec::new(),
        }
    }

    pub fn claim(&mut self, scope: ScopeReference) {
        if !self.scopes.iter().any(|current| current == &scope) {
            self.scopes.push(scope);
        }
    }

    pub fn owns(&self, scope: &ScopeReference) -> bool {
        self.scopes.iter().any(|current| current == scope)
    }

    pub fn role(&self) -> RoleName {
        self.role
    }
}
