use owner_signal_persona_orchestrate::{
    OwnerOrchestrateReply, OwnerOrchestrateRequest, Retirement,
};
use signal_persona_orchestrate::{
    Observation, ObservationClosed, ObservationOpened, ObservationToken, OrchestrateReply,
    OrchestrateRequest,
};
use std::sync::Mutex;

use crate::{
    ActivityLedger, ClaimLedger, Error, LaneRegistry, LockProjection, OrchestrateLayout,
    OrchestrateTables, RepositoryRegistry, Result, RoleRegistry, StoreLocation,
};

pub struct OrchestrateService {
    tables: OrchestrateTables,
    layout: OrchestrateLayout,
    sequence: Mutex<()>,
    next_observation_token: Mutex<u64>,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Self::open_with_layout(store, OrchestrateLayout::primary_workspace())
    }

    pub fn open_with_layout(store: &StoreLocation, layout: OrchestrateLayout) -> Result<Self> {
        let tables = OrchestrateTables::open(store)?;
        RoleRegistry::new(&tables, &layout).seed_current_workspace_roles()?;
        Ok(Self {
            tables,
            layout,
            sequence: Mutex::new(()),
            next_observation_token: Mutex::new(1),
        })
    }

    pub fn handle(&self, request: OrchestrateRequest) -> Result<OrchestrateReply> {
        let _sequence = self
            .sequence
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        match request {
            OrchestrateRequest::Claim(claim) => {
                let reply = ClaimLedger::new(&self.tables).apply_claim(claim)?;
                self.project_locks()?;
                Ok(reply)
            }
            OrchestrateRequest::Release(release) => {
                let reply = ClaimLedger::new(&self.tables).apply_release(release)?;
                self.project_locks()?;
                Ok(reply)
            }
            OrchestrateRequest::Handoff(handoff) => {
                let reply = ClaimLedger::new(&self.tables).apply_handoff(handoff)?;
                self.project_locks()?;
                Ok(reply)
            }
            OrchestrateRequest::Observe(Observation::Roles) => {
                ClaimLedger::new(&self.tables).observe()
            }
            OrchestrateRequest::Observe(Observation::Lanes) => {
                LaneRegistry::new(&self.tables).observe()
            }
            OrchestrateRequest::Submit(submission) => {
                ActivityLedger::new(&self.tables).submit(submission)
            }
            OrchestrateRequest::Query(query) => ActivityLedger::new(&self.tables).query(query),
            OrchestrateRequest::Watch(_subscription) => {
                let token = self.next_observation_token()?;
                Ok(OrchestrateReply::ObservationOpened(ObservationOpened {
                    token,
                }))
            }
            OrchestrateRequest::Unwatch(token) => {
                Ok(OrchestrateReply::ObservationClosed(ObservationClosed {
                    token,
                }))
            }
        }
    }

    pub fn handle_owner(&self, request: OwnerOrchestrateRequest) -> Result<OwnerOrchestrateReply> {
        let _sequence = self
            .sequence
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        match request {
            OwnerOrchestrateRequest::Create(order) => {
                let reply = RoleRegistry::new(&self.tables, &self.layout).create_role(order)?;
                self.project_locks()?;
                Ok(reply)
            }
            OwnerOrchestrateRequest::Retire(Retirement::Role(order)) => {
                let reply = RoleRegistry::new(&self.tables, &self.layout).retire_role(order)?;
                self.project_locks()?;
                Ok(reply)
            }
            OwnerOrchestrateRequest::Retire(Retirement::Lane(lane)) => {
                LaneRegistry::new(&self.tables).retire(lane)
            }
            OwnerOrchestrateRequest::Refresh(_order) => {
                RepositoryRegistry::new(&self.tables, &self.layout).refresh()
            }
            OwnerOrchestrateRequest::Register(request) => {
                LaneRegistry::new(&self.tables).register(request)
            }
            OwnerOrchestrateRequest::SetAuthority(change) => {
                LaneRegistry::new(&self.tables).set_authority(change)
            }
        }
    }

    pub fn roles(&self) -> Result<Vec<crate::StoredRole>> {
        self.tables.role_records()
    }

    pub fn repositories(&self) -> Result<Vec<crate::StoredRepository>> {
        self.tables.repository_records()
    }

    fn project_locks(&self) -> Result<()> {
        LockProjection::new(&self.tables, &self.layout).project()
    }

    fn next_observation_token(&self) -> Result<ObservationToken> {
        let mut next = self
            .next_observation_token
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        let token = ObservationToken::new(*next);
        *next += 1;
        Ok(token)
    }
}
