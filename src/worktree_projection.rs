//! Projects the worktrees table to `orchestrate/worktrees.nota` — the GC
//! manifest (Spirit eh5a), the worktree sibling of [`crate::LockProjection`]'s
//! `.lock` files. One positional NOTA `Worktree` record per line.
//!
//! [`WorktreeProjection::project`] writes the file.
//! [`WorktreeProjection::gc_candidates`] reads it back and returns entries
//! in [`WorktreeStatus::Archived`] or [`WorktreeStatus::Recycled`] state.

use nota::{NotaEncode, NotaSource};
use signal_orchestrate::{Worktree, WorktreeStatus};

use crate::{Error, OrchestrateLayout, OrchestrateTables, Result};

pub struct WorktreeProjection<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> WorktreeProjection<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    /// Read `worktrees.nota` from disk and return entries in
    /// [`WorktreeStatus::Archived`] or [`WorktreeStatus::Recycled`] state —
    /// the GC candidates the daemon or an external agent can act on.
    ///
    /// Returns an empty `Vec` when the projection file is absent (no
    /// worktrees have been registered yet). Each non-empty line is decoded
    /// through the NOTA codec into a [`Worktree`]; a decode error surfaces
    /// as [`Error::Nota`].
    pub fn gc_candidates(&self) -> Result<Vec<Worktree>> {
        let path = self.layout.worktrees_projection_path();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(Error::Io(error)),
        };
        let mut candidates = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let worktree = NotaSource::new(line).parse::<Worktree>()?;
            if matches!(
                worktree.status,
                WorktreeStatus::Archived | WorktreeStatus::Recycled
            ) {
                candidates.push(worktree);
            }
        }
        Ok(candidates)
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
