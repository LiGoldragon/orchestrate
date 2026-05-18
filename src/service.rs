use signal_persona_orchestrate::{OrchestrateReply, OrchestrateRequest};

use crate::{ActivityLedger, ClaimLedger, OrchestrateTables, Result, StoreLocation};

pub struct OrchestrateService {
    tables: OrchestrateTables,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Ok(Self {
            tables: OrchestrateTables::open(store)?,
        })
    }

    pub fn handle(&self, request: OrchestrateRequest) -> Result<OrchestrateReply> {
        match request {
            OrchestrateRequest::RoleClaim(claim) => {
                ClaimLedger::new(&self.tables).apply_claim(claim)
            }
            OrchestrateRequest::RoleRelease(release) => {
                ClaimLedger::new(&self.tables).apply_release(release)
            }
            OrchestrateRequest::RoleHandoff(handoff) => {
                ClaimLedger::new(&self.tables).apply_handoff(handoff)
            }
            OrchestrateRequest::RoleObservation(observation) => {
                ClaimLedger::new(&self.tables).observe(observation)
            }
            OrchestrateRequest::ActivitySubmission(submission) => {
                ActivityLedger::new(&self.tables).submit(submission)
            }
            OrchestrateRequest::ActivityQuery(query) => {
                ActivityLedger::new(&self.tables).query(query)
            }
        }
    }
}
