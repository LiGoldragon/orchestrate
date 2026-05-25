use owner_signal_orchestrate::{OwnerOrchestrateReply, OwnerOrchestrateRequest};
use signal_executor::{Executor, ObserverSet};
use signal_frame::{AcceptedOutcome, Reply, Request, RequestPayload, SubReply};
use signal_orchestrate::{ObservationToken, OrchestrateReply, OrchestrateRequest, PartialApplied};
use std::sync::{Mutex, MutexGuard};

use crate::{
    Error, LockProjection, MirrorSnapshot, MirrorVersions, OrchestrateLayout, OrchestrateTables,
    OrdinaryCommandExecutor, OrdinaryLowering, OwnerCommandExecutor, OwnerLowering, Result,
    RoleRegistry, StoreLocation, StoredDivergence,
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
        let (reply, engine_error) = self.execute_request(request.into_request());
        first_committed_payload(reply, engine_error)
    }

    pub fn handle_owner(&self, request: OwnerOrchestrateRequest) -> Result<OwnerOrchestrateReply> {
        let (reply, engine_error) = self.execute_owner_request(request.into_request());
        first_committed_payload(reply, engine_error)
    }

    pub fn handle_request(&self, request: Request<OrchestrateRequest>) -> Reply<OrchestrateReply> {
        let (reply, _engine_error) = self.execute_request(request);
        reply
    }

    pub fn handle_owner_request(
        &self,
        request: Request<OwnerOrchestrateRequest>,
    ) -> Reply<OwnerOrchestrateReply> {
        let (reply, _engine_error) = self.execute_owner_request(request);
        reply
    }

    fn execute_request(
        &self,
        request: Request<OrchestrateRequest>,
    ) -> (Reply<OrchestrateReply>, Option<Error>) {
        let command_executor = OrdinaryCommandExecutor::new(self);
        let mut executor = Executor::new(OrdinaryLowering, command_executor, ObserverSet::no_op());
        let reply = futures::executor::block_on(executor.execute(request));
        let engine_error = executor.take_last_engine_error();
        (reply, engine_error)
    }

    fn execute_owner_request(
        &self,
        request: Request<OwnerOrchestrateRequest>,
    ) -> (Reply<OwnerOrchestrateReply>, Option<Error>) {
        let command_executor = OwnerCommandExecutor::new(self);
        let mut executor = Executor::new(OwnerLowering, command_executor, ObserverSet::no_op());
        let reply = futures::executor::block_on(executor.execute(request));
        let engine_error = executor.take_last_engine_error();
        (reply, engine_error)
    }

    pub fn roles(&self) -> Result<Vec<crate::StoredRole>> {
        self.tables.role_records()
    }

    pub fn repositories(&self) -> Result<Vec<crate::StoredRepository>> {
        self.tables.repository_records()
    }

    pub fn record_partial_application(&self, partial: PartialApplied) -> Result<OrchestrateReply> {
        crate::DivergenceLedger::new(&self.tables).record_partial_application(partial)
    }

    pub fn divergences(&self) -> Result<Vec<StoredDivergence>> {
        self.tables.divergence_records()
    }

    pub fn mirror_snapshot(&self) -> Result<MirrorSnapshot> {
        MirrorSnapshot::capture(&self.tables)
    }

    pub fn mirror_payload(
        &self,
        versions: MirrorVersions,
    ) -> Result<signal_version_handover::MirrorPayload> {
        self.mirror_snapshot()?.into_mirror_payload(versions)
    }

    pub fn restore_mirror_payload(
        &self,
        payload: &signal_version_handover::MirrorPayload,
    ) -> Result<MirrorSnapshot> {
        let snapshot = MirrorSnapshot::from_mirror_payload(payload)?;
        snapshot.restore_into(&self.tables)?;
        Ok(snapshot)
    }

    pub(crate) fn tables(&self) -> &OrchestrateTables {
        &self.tables
    }

    pub(crate) fn layout(&self) -> &OrchestrateLayout {
        &self.layout
    }

    pub(crate) fn lock_sequence(&self) -> Result<MutexGuard<'_, ()>> {
        self.sequence
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)
    }

    pub(crate) fn project_locks(&self) -> Result<()> {
        LockProjection::new(&self.tables, &self.layout).project()
    }

    pub(crate) fn next_observation_token(&self) -> Result<ObservationToken> {
        let mut next = self
            .next_observation_token
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        let token = ObservationToken::new(*next);
        *next += 1;
        Ok(token)
    }
}

fn first_committed_payload<Payload>(
    reply: Reply<Payload>,
    engine_error: Option<Error>,
) -> Result<Payload> {
    match reply {
        Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        } => match per_operation.into_head() {
            SubReply::Ok(payload) => Ok(payload),
            SubReply::Invalidated | SubReply::Failed { .. } | SubReply::Skipped => {
                Err(Error::ExecutorReplyNotCommitted)
            }
        },
        Reply::Accepted { .. } => match engine_error {
            Some(error) => Err(error),
            None => Err(Error::ExecutorReplyNotCommitted),
        },
        Reply::Rejected { reason } => Err(Error::ExecutorReplyRejected { reason }),
    }
}
