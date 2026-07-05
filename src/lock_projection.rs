use signal_orchestrate::ScopeReference;

use crate::{LaneRegistry, OrchestrateLayout, OrchestrateTables, Result, StoredClaim};

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
        let lanes = self.tables.lane_records()?;
        for role in self.tables.role_records()? {
            let lock_path = self.layout.role_lock_path(&role.role);
            if let Some(parent) = lock_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let role_lanes = lanes
                .iter()
                .filter(|lane| lane.status == signal_orchestrate::LaneStatus::Active)
                .filter_map(|lane| {
                    LaneRegistry::role_name_for(&lane.assignment.owner.role)
                        .ok()
                        .filter(|owner| owner == &role.role)
                        .map(|_| lane.assignment.lane.clone())
                })
                .collect::<Vec<_>>();
            let body = claims
                .iter()
                .filter(|claim| role_lanes.iter().any(|lane| lane == &claim.lane))
                .map(Self::lock_line)
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

    fn lock_line(claim: &StoredClaim) -> String {
        format!(
            "{} # {}",
            Self::scope_text(&claim.scope),
            claim.reason.as_str()
        )
    }

    fn scope_text(scope: &ScopeReference) -> String {
        match scope {
            ScopeReference::Path(path) => path.as_str().to_string(),
            ScopeReference::Task(task) => format!("[{}]", task.as_str()),
        }
    }
}
