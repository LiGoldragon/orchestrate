use signal_persona_orchestrate::ScopeReference;

use crate::{OrchestrateLayout, OrchestrateTables, Result, StoredClaim};

pub struct LockProjection<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> LockProjection<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn project(&self) -> Result<()> {
        let claims = self.tables.claim_records()?;
        for role in self.tables.role_records()? {
            let lock_path = self.layout.role_lock_path(&role.role);
            if let Some(parent) = lock_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let body = claims
                .iter()
                .filter(|claim| claim.role == role.role)
                .map(lock_line)
                .collect::<Vec<_>>()
                .join("\n");
            let body = if body.is_empty() {
                String::new()
            } else {
                format!("{body}\n")
            };
            std::fs::write(lock_path, body)?;
        }
        Ok(())
    }
}

fn lock_line(claim: &StoredClaim) -> String {
    format!("{} # {}", scope_text(&claim.scope), claim.reason.as_str())
}

fn scope_text(scope: &ScopeReference) -> String {
    match scope {
        ScopeReference::Path(path) => path.as_str().to_string(),
        ScopeReference::Task(task) => format!("[{}]", task.as_str()),
    }
}
