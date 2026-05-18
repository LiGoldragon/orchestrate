use signal_persona_orchestrate::{OrchestrateReply, OrchestrateRequest};
use std::sync::Mutex;

use crate::{ActivityLedger, ClaimLedger, Error, OrchestrateTables, Result, StoreLocation};

pub struct OrchestrateService {
    tables: OrchestrateTables,
    sequence: Mutex<()>,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Ok(Self {
            tables: OrchestrateTables::open(store)?,
            sequence: Mutex::new(()),
        })
    }

    pub fn handle(&self, request: OrchestrateRequest) -> Result<OrchestrateReply> {
        let _sequence = self
            .sequence
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
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
