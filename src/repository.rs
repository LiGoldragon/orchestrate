//! Durable repository records.
//!
//! The registry deliberately does not discover repositories from the host.
//! Repository rows are supplied through an owning state transition and are
//! observed exactly as stored.  The retained refresh order is therefore a
//! read-free acknowledgement of the currently registered state.

use meta_signal_orchestrate::{MetaOrchestrateReply, RepositoryIndexRefreshed};
use signal_orchestrate::{OrchestrateReply, RepositoriesObserved, Repository};

use crate::{OrchestrateTables, Result};

pub struct RepositoryRegistry<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> RepositoryRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    /// Preserve the public order without treating the host filesystem as a
    /// source of truth.  There is no mutation and no absent row is forgotten.
    pub fn refresh(&self) -> Result<MetaOrchestrateReply> {
        let count = self
            .tables
            .repository_records()?
            .len()
            .min(u32::MAX as usize) as u32;
        Ok(MetaOrchestrateReply::RepositoryIndexRefreshed(
            RepositoryIndexRefreshed::new(count),
        ))
    }

    pub fn observe(&self) -> Result<OrchestrateReply> {
        let repositories = self
            .tables
            .repository_records()?
            .into_iter()
            .map(Repository::from)
            .collect();
        Ok(OrchestrateReply::RepositoriesObserved(
            RepositoriesObserved { repositories },
        ))
    }
}
