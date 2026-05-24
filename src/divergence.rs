use signal_orchestrate::{OrchestrateReply, PartialApplied};

use crate::{OrchestrateTables, Result};

pub struct DivergenceLedger<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> DivergenceLedger<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    pub fn record_partial_application(&self, partial: PartialApplied) -> Result<OrchestrateReply> {
        self.tables.append_divergence(partial.clone())?;
        Ok(OrchestrateReply::PartialApplied(partial))
    }
}
