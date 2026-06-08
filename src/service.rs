use meta_signal_orchestrate::{MetaOrchestrateReply, MetaOrchestrateRequest};
use signal_frame::{
    AcceptedOutcome, NonEmpty, Reply, Request, RequestPayload, RequestRejectionReason, SubReply,
};
use signal_orchestrate::{ObservationToken, OrchestrateReply, OrchestrateRequest, PartialApplied};
use signal_version_handover::{
    CompletionReport, DivergenceAcknowledgement, HandoverAcceptance, HandoverFinalization,
    HandoverMarker, HandoverRejection, HandoverRejectionReason, MarkerRequest,
    MirrorAcknowledgement, MirrorPayload, Operation as UpgradeOperation, ReadinessReport,
    RecoveryResult, Reply as UpgradeReply,
};
use std::sync::{Mutex, MutexGuard};
use version_projection::ComponentName;

use crate::{
    Error, LegacyLockImport, LockProjection, MetaRequestExecution, MirrorSnapshot, MirrorVersions,
    OrchestrateLayout, OrchestrateRequestExecution, OrchestrateTables, Result, RoleRegistry,
    StoreLocation, StoredDivergence,
    handover::{HandoverClockReading, HandoverState},
};

pub struct OrchestrateService {
    tables: OrchestrateTables,
    layout: OrchestrateLayout,
    sequence: Mutex<()>,
    next_observation_token: Mutex<u64>,
    handover: Mutex<HandoverState>,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Self::open_with_layout(store, OrchestrateLayout::primary_workspace())
    }

    pub fn open_with_layout(store: &StoreLocation, layout: OrchestrateLayout) -> Result<Self> {
        let tables = OrchestrateTables::open(store)?;
        RoleRegistry::new(&tables, &layout).seed_current_workspace_roles()?;
        LegacyLockImport::new(&tables, &layout).import_if_store_has_no_claims()?;
        Ok(Self {
            tables,
            layout,
            sequence: Mutex::new(()),
            next_observation_token: Mutex::new(1),
            handover: Mutex::new(HandoverState::Active),
        })
    }

    pub fn handle(&self, request: OrchestrateRequest) -> Result<OrchestrateReply> {
        let (reply, engine_error) = self.execute_request(request.into_request());
        first_committed_payload(reply, engine_error)
    }

    pub fn handle_meta(&self, request: MetaOrchestrateRequest) -> Result<MetaOrchestrateReply> {
        let (reply, engine_error) = self.execute_meta_request(request.into_request());
        first_committed_payload(reply, engine_error)
    }

    pub fn handle_request(&self, request: Request<OrchestrateRequest>) -> Reply<OrchestrateReply> {
        let (reply, _engine_error) = self.execute_request(request);
        reply
    }

    pub fn handle_meta_request(
        &self,
        request: Request<MetaOrchestrateRequest>,
    ) -> Reply<MetaOrchestrateReply> {
        let (reply, _engine_error) = self.execute_meta_request(request);
        reply
    }

    pub fn handle_upgrade_request(
        &self,
        request: Request<UpgradeOperation>,
    ) -> Reply<UpgradeReply> {
        let replies = request
            .payloads()
            .iter()
            .cloned()
            .map(|operation| self.handle_upgrade_operation(operation))
            .collect::<Result<Vec<_>>>();
        match replies {
            Ok(replies) => Reply::committed(
                NonEmpty::try_from_vec(replies).expect("request payloads are non-empty"),
            ),
            Err(_) => Reply::rejected(RequestRejectionReason::Internal),
        }
    }

    fn execute_request(
        &self,
        request: Request<OrchestrateRequest>,
    ) -> (Reply<OrchestrateReply>, Option<Error>) {
        OrchestrateRequestExecution::new(self, request).execute()
    }

    fn execute_meta_request(
        &self,
        request: Request<MetaOrchestrateRequest>,
    ) -> (Reply<MetaOrchestrateReply>, Option<Error>) {
        MetaRequestExecution::new(self, request).execute()
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

    pub fn restore_mirror_payload(&self, payload: &MirrorPayload) -> Result<MirrorSnapshot> {
        let snapshot = MirrorSnapshot::from_mirror_payload(payload)?;
        snapshot.restore_into(&self.tables)?;
        Ok(snapshot)
    }

    pub fn handover_marker(&self, component: ComponentName) -> Result<HandoverMarker> {
        let sequence = self.tables.current_commit_sequence()?;
        let clock = HandoverClockReading::now()?;
        Ok(HandoverMarker {
            component,
            schema_hash: MirrorSnapshot::current_contract_version(),
            commit_sequence: sequence,
            write_counter: sequence,
            last_record_identifier: None,
            recorded_at_date: clock.date,
            recorded_at_time: clock.time,
        })
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

    fn handle_upgrade_operation(
        &self,
        operation: UpgradeOperation,
    ) -> Result<SubReply<UpgradeReply>> {
        let reply = match operation {
            UpgradeOperation::AskHandoverMarker(request) => self.ask_handover_marker(request)?,
            UpgradeOperation::ReadyToHandover(report) => self.ready_to_handover(report)?,
            UpgradeOperation::HandoverCompleted(report) => self.handover_completed(report)?,
            UpgradeOperation::Mirror(payload) => self.restore_mirror(payload)?,
            UpgradeOperation::Divergence(payload) => {
                UpgradeReply::DivergenceAcknowledged(DivergenceAcknowledgement {
                    component: payload.component,
                    divergence_identifier: 0,
                })
            }
            UpgradeOperation::RecoverFromFailure(request) => {
                let recovered = self.recover_handover()?;
                UpgradeReply::RecoveryCompleted(RecoveryResult {
                    component: request.component,
                    recovered,
                })
            }
        };
        Ok(SubReply::Ok(reply))
    }

    fn ask_handover_marker(&self, request: MarkerRequest) -> Result<UpgradeReply> {
        Ok(UpgradeReply::HandoverMarker(
            self.handover_marker(request.component)?,
        ))
    }

    fn ready_to_handover(&self, report: ReadinessReport) -> Result<UpgradeReply> {
        let current_marker = self.handover_marker(report.component.clone())?;
        let mut state = self
            .handover
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        match &*state {
            HandoverState::Active
                if current_marker.commit_sequence == report.source_marker.commit_sequence =>
            {
                *state = HandoverState::Ready {
                    accepted_marker: current_marker.clone(),
                };
                Ok(UpgradeReply::HandoverAccepted(HandoverAcceptance {
                    accepted_marker: current_marker,
                }))
            }
            HandoverState::Active => Ok(reject_handover(
                report.component,
                HandoverRejectionReason::CommitSequenceAdvanced,
            )),
            HandoverState::Ready { .. } | HandoverState::Complete => Ok(reject_handover(
                report.component,
                HandoverRejectionReason::AlreadyInHandover,
            )),
        }
    }

    fn handover_completed(&self, report: CompletionReport) -> Result<UpgradeReply> {
        let current_marker = self.handover_marker(report.component.clone())?;
        let mut state = self
            .handover
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        match &*state {
            HandoverState::Ready { accepted_marker }
                if accepted_marker == &report.accepted_marker
                    && current_marker.commit_sequence == accepted_marker.commit_sequence =>
            {
                *state = HandoverState::Complete;
                Ok(UpgradeReply::HandoverFinalized(HandoverFinalization {
                    finalized_marker: report.accepted_marker,
                }))
            }
            HandoverState::Ready { .. } => Ok(reject_handover(
                report.component,
                HandoverRejectionReason::CommitSequenceAdvanced,
            )),
            HandoverState::Active | HandoverState::Complete => Ok(reject_handover(
                report.component,
                HandoverRejectionReason::NotReady,
            )),
        }
    }

    fn restore_mirror(&self, payload: MirrorPayload) -> Result<UpgradeReply> {
        let component = payload.component.clone();
        let state = self
            .handover
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        if matches!(*state, HandoverState::Complete) {
            return Ok(reject_handover(
                component,
                HandoverRejectionReason::NotReady,
            ));
        }
        drop(state);

        match self.restore_mirror_payload(&payload) {
            Ok(_) => Ok(UpgradeReply::MirrorAcknowledged(MirrorAcknowledgement {
                component,
                write_counter: self.tables.current_commit_sequence()?,
            })),
            Err(
                Error::MirrorComponentMismatch { .. }
                | Error::MirrorKindMismatch { .. }
                | Error::MirrorTargetVersionMismatch { .. }
                | Error::MirrorArchiveDecode { .. },
            ) => Ok(reject_handover(
                component,
                HandoverRejectionReason::SchemaMismatch,
            )),
            Err(error) => Err(error),
        }
    }

    fn recover_handover(&self) -> Result<bool> {
        let mut state = self
            .handover
            .lock()
            .map_err(|_| Error::ServiceSequencePoisoned)?;
        match &*state {
            HandoverState::Active => Ok(true),
            HandoverState::Ready { .. } => {
                *state = HandoverState::Active;
                Ok(true)
            }
            HandoverState::Complete => Ok(false),
        }
    }
}

fn reject_handover(component: ComponentName, reason: HandoverRejectionReason) -> UpgradeReply {
    UpgradeReply::HandoverRejected(HandoverRejection { component, reason })
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
