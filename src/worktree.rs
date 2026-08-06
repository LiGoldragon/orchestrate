//! Durable worktree lifecycle state.
//!
//! Orchestrate records worktree facts supplied by the owner. It has no checkout
//! probing, lifecycle side effect, locking, or VCS invocation.

use meta_signal_orchestrate::{
    ArchiveWorktreeOrder, MetaOrchestrateReply, RegisterWorktree, WorktreeArchived,
    WorktreeIndexRefreshed, WorktreeRegistered,
};
use signal_orchestrate::{
    MainIntegration, OrchestrateReply, Worktree, WorktreeConcluded, WorktreeConclusion,
    WorktreeConclusionRequest, WorktreeRequest, WorktreeRequestRejected, WorktreeRequestRejection,
    WorktreeStatus, WorktreesObserved,
};

use crate::{Error, OrchestrateTables, Result, StoredWorktree};

pub struct WorktreeRegistry<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> WorktreeRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    /// The full worktree record is the caller's explicit state assertion.
    pub fn register(&self, order: RegisterWorktree) -> Result<MetaOrchestrateReply> {
        let worktree = order.worktree;
        self.tables
            .insert_worktree(&StoredWorktree::from(worktree.clone()))?;
        Ok(MetaOrchestrateReply::WorktreeRegistered(
            WorktreeRegistered { worktree },
        ))
    }

    /// Observe the durable worktree rows without changing state or host facts.
    pub fn refresh(&self) -> Result<MetaOrchestrateReply> {
        let count = self.tables.worktree_records()?.len().min(u32::MAX as usize) as u32;
        Ok(MetaOrchestrateReply::WorktreeIndexRefreshed(
            WorktreeIndexRefreshed::new(count),
        ))
    }

    pub fn archive(&self, order: ArchiveWorktreeOrder) -> Result<MetaOrchestrateReply> {
        let Some(mut worktree) = self
            .tables
            .worktree_records()?
            .into_iter()
            .find(|record| record.path == order.path)
        else {
            return Err(Error::WorktreeNotFound {
                path: order.path.as_str().to_owned(),
            });
        };
        worktree.status = WorktreeStatus::Archived;
        self.tables.insert_worktree(&worktree)?;
        Ok(MetaOrchestrateReply::WorktreeArchived(WorktreeArchived {
            worktree: worktree.into(),
        }))
    }

    /// `RequestWorktree` has no path or freshness facts to record.  It is
    /// intentionally refused instead of scaffolding a host checkout.
    pub fn request(&self, _order: WorktreeRequest) -> Result<OrchestrateReply> {
        Ok(OrchestrateReply::WorktreeRequestRejected(
            WorktreeRequestRejected {
                reason: WorktreeRequestRejection::RepositoryNotFound,
            },
        ))
    }

    /// Conclusion is a durable status transition only.  A `Merged` assertion
    /// is retained as merged; a rejected branch is retained as archived.  No
    /// checkout is removed and no VCS ancestry is inspected.
    pub fn conclude(&self, order: WorktreeConclusionRequest) -> Result<OrchestrateReply> {
        let mut matches = self
            .tables
            .worktree_records()?
            .into_iter()
            .filter(|record| {
                record.owning_lane == order.owning_lane && record.status == WorktreeStatus::Active
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(Error::WorktreeLaneNotFound {
                lane: order.owning_lane.as_str().to_owned(),
            });
        }
        if matches.len() != 1 {
            return Err(Error::WorktreeLaneAmbiguous {
                lane: order.owning_lane.as_str().to_owned(),
                worktrees: matches
                    .iter()
                    .map(|record| record.path.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        let mut worktree = matches.pop().expect("checked exactly one");
        worktree.status = match order.disposition {
            WorktreeConclusion::Merged => WorktreeStatus::Merged,
            WorktreeConclusion::Rejected => WorktreeStatus::Archived,
        };
        self.tables.insert_worktree(&worktree)?;
        Ok(OrchestrateReply::WorktreeConcluded(WorktreeConcluded {
            worktree: worktree.into(),
            integration: match order.disposition {
                WorktreeConclusion::Merged => MainIntegration::AlreadyAncestor,
                WorktreeConclusion::Rejected => MainIntegration::Discarded,
            },
        }))
    }

    pub fn observe(&self) -> Result<OrchestrateReply> {
        Ok(OrchestrateReply::WorktreesObserved(WorktreesObserved {
            worktrees: self
                .tables
                .worktree_records()?
                .into_iter()
                .map(Worktree::from)
                .collect(),
        }))
    }
}
