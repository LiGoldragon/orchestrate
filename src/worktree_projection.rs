//! Projects the worktrees table to `orchestrate/worktrees.nota` — the GC
//! manifest (Spirit eh5a), the worktree sibling of [`crate::LockProjection`]'s
//! `.lock` files. One positional NOTA `Worktree` record per line.

use nota_next::NotaEncode;
use signal_orchestrate::Worktree;

use crate::{OrchestrateLayout, OrchestrateTables, Result};

pub struct WorktreeProjection<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> WorktreeProjection<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn project(&self) -> Result<()> {
        let projection_path = self.layout.worktrees_projection_path();
        if let Some(parent) = projection_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut records = self.tables.worktree_records()?;
        records.sort_by(|left, right| {
            (left.repository.as_str(), left.branch.as_str())
                .cmp(&(right.repository.as_str(), right.branch.as_str()))
        });
        let body = records
            .into_iter()
            .map(|record| Worktree::from(record).to_nota())
            .collect::<Vec<_>>()
            .join("\n");
        let body = if body.is_empty() {
            String::new()
        } else {
            format!("{body}\n")
        };
        std::fs::write(projection_path, body)?;
        Ok(())
    }
}
