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
use version_projection::ComponentName;

use crate::{
    Error, LegacyLockImport, LockProjection, MetaRequestExecution, MirrorSnapshot, MirrorVersions,
    OrchestrateLayout, OrchestrateRequestExecution, OrchestrateTables, PublicSocketRetirement,
    Result, RoleRegistry, StoreLocation, StoredDivergence,
    handover::{HandoverClockReading, HandoverState},
};

/// The orchestrate engine. It is owned exclusively by the schema-emitted
/// `EngineActor`, so the actor mailbox serialises every request and each
/// handler holds `&mut self` — no component-internal lock is required. The
/// write-sequence gate, observation-token counter, and handover state machine
/// that previously needed a `Mutex` are now plain fields mutated under `&mut`.
pub struct OrchestrateService {
    tables: OrchestrateTables,
    layout: OrchestrateLayout,
    next_observation_token: u64,
    handover: HandoverState,
    public_sockets: PublicSocketRetirement,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Self::open_with_layout(store, OrchestrateLayout::primary_workspace())
    }

    pub fn open_with_layout(store: &StoreLocation, layout: OrchestrateLayout) -> Result<Self> {
        let tables = OrchestrateTables::open(store)?;
        RoleRegistry::new(&tables, &layout).seed_current_workspace_roles()?;
        LegacyLockImport::new(&tables, &layout).import_if_store_has_no_claims()?;
        tables.remove_claims_without_roles()?;
        Ok(Self {
            tables,
            layout,
            next_observation_token: 1,
            handover: HandoverState::Active,
            public_sockets: PublicSocketRetirement::none(),
        })
    }

    /// Register the ordinary and meta socket paths the engine retires once a
    /// handover finalizes — the version-handover protocol's last step is to
    /// stop accepting public (working + meta) traffic on the retiring instance.
    /// The daemon's `build_runtime` calls this with the configured paths; tests
    /// that open the engine directly leave it empty (`none`).
    pub fn with_public_socket_retirement(mut self, retirement: PublicSocketRetirement) -> Self {
        self.public_sockets = retirement;
        self
    }

    pub async fn handle(&mut self, request: OrchestrateRequest) -> Result<OrchestrateReply> {
        let (reply, engine_error) = self.execute_request(request.into_request()).await;
        Self::first_committed_payload(reply, engine_error)
    }

    pub async fn handle_meta(
        &mut self,
        request: MetaOrchestrateRequest,
    ) -> Result<MetaOrchestrateReply> {
        let (reply, engine_error) = self.execute_meta_request(request.into_request()).await;
        Self::first_committed_payload(reply, engine_error)
    }

    pub async fn handle_request(
        &mut self,
        request: Request<OrchestrateRequest>,
    ) -> Reply<OrchestrateReply> {
        let (reply, _engine_error) = self.execute_request(request).await;
        reply
    }

    pub async fn handle_meta_request(
        &mut self,
        request: Request<MetaOrchestrateRequest>,
    ) -> Reply<MetaOrchestrateReply> {
        let (reply, _engine_error) = self.execute_meta_request(request).await;
        reply
    }

    pub fn handle_upgrade_request(
        &mut self,
        request: Request<UpgradeOperation>,
    ) -> Result<Reply<UpgradeReply>> {
        let operations: Vec<UpgradeOperation> = request.payloads().iter().cloned().collect();
        let mut replies = Vec::with_capacity(operations.len());
        for operation in operations {
            match self.handle_upgrade_operation(operation) {
                Ok(reply) => replies.push(reply),
                Err(_) => return Ok(Reply::rejected(RequestRejectionReason::Internal)),
            }
        }
        let reply = Reply::committed(
            NonEmpty::try_from_vec(replies).expect("request payloads are non-empty"),
        );
        if Self::reply_finalized_handover(&reply) {
            self.public_sockets.retire()?;
        }
        Ok(reply)
    }

    /// Whether an upgrade reply committed a `HandoverFinalized` — the signal to
    /// retire the public (working + meta) sockets on the retiring instance.
    fn reply_finalized_handover(reply: &Reply<UpgradeReply>) -> bool {
        let Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        } = reply
        else {
            return false;
        };
        per_operation
            .iter()
            .any(|sub_reply| matches!(sub_reply, SubReply::Ok(UpgradeReply::HandoverFinalized(_))))
    }

    async fn execute_request(
        &mut self,
        request: Request<OrchestrateRequest>,
    ) -> (Reply<OrchestrateReply>, Option<Error>) {
        OrchestrateRequestExecution::new(self, request)
            .execute()
            .await
    }

    async fn execute_meta_request(
        &mut self,
        request: Request<MetaOrchestrateRequest>,
    ) -> (Reply<MetaOrchestrateReply>, Option<Error>) {
        MetaRequestExecution::new(self, request).execute().await
    }

    pub fn roles(&self) -> Result<Vec<crate::StoredRole>> {
        self.tables.role_records()
    }

    pub fn repositories(&self) -> Result<Vec<crate::StoredRepository>> {
        self.tables.repository_records()
    }

    pub fn worktrees(&self) -> Result<Vec<crate::StoredWorktree>> {
        self.tables.worktree_records()
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

    pub(crate) fn project_locks(&self) -> Result<()> {
        LockProjection::new(&self.tables, &self.layout).project()
    }

    pub(crate) fn project_worktrees(&self) -> Result<()> {
        crate::WorktreeProjection::new(&self.tables, &self.layout).project()
    }

    pub(crate) fn next_observation_token(&mut self) -> Result<ObservationToken> {
        let token = ObservationToken::new(self.next_observation_token);
        self.next_observation_token += 1;
        Ok(token)
    }

    fn handle_upgrade_operation(
        &mut self,
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

    fn ready_to_handover(&mut self, report: ReadinessReport) -> Result<UpgradeReply> {
        let current_marker = self.handover_marker(report.component.clone())?;
        match &self.handover {
            HandoverState::Active
                if current_marker.commit_sequence == report.source_marker.commit_sequence =>
            {
                self.handover = HandoverState::Ready {
                    accepted_marker: current_marker.clone(),
                };
                Ok(UpgradeReply::HandoverAccepted(HandoverAcceptance {
                    accepted_marker: current_marker,
                }))
            }
            HandoverState::Mirrored { restored_marker }
                if current_marker.commit_sequence == restored_marker.commit_sequence =>
            {
                self.handover = HandoverState::Ready {
                    accepted_marker: current_marker.clone(),
                };
                Ok(UpgradeReply::HandoverAccepted(HandoverAcceptance {
                    accepted_marker: current_marker,
                }))
            }
            HandoverState::Active => Ok(Self::reject_handover(
                report.component,
                HandoverRejectionReason::CommitSequenceAdvanced,
            )),
            HandoverState::Mirrored { .. } => Ok(Self::reject_handover(
                report.component,
                HandoverRejectionReason::CommitSequenceAdvanced,
            )),
            HandoverState::Ready { .. } | HandoverState::Complete => Ok(Self::reject_handover(
                report.component,
                HandoverRejectionReason::AlreadyInHandover,
            )),
        }
    }

    fn handover_completed(&mut self, report: CompletionReport) -> Result<UpgradeReply> {
        let current_marker = self.handover_marker(report.component.clone())?;
        match &self.handover {
            HandoverState::Ready { accepted_marker }
                if accepted_marker == &report.accepted_marker
                    && current_marker.commit_sequence == accepted_marker.commit_sequence =>
            {
                self.handover = HandoverState::Complete;
                Ok(UpgradeReply::HandoverFinalized(HandoverFinalization {
                    finalized_marker: report.accepted_marker,
                }))
            }
            HandoverState::Ready { .. } => Ok(Self::reject_handover(
                report.component,
                HandoverRejectionReason::CommitSequenceAdvanced,
            )),
            HandoverState::Active | HandoverState::Mirrored { .. } | HandoverState::Complete => Ok(
                Self::reject_handover(report.component, HandoverRejectionReason::NotReady),
            ),
        }
    }

    fn restore_mirror(&mut self, payload: MirrorPayload) -> Result<UpgradeReply> {
        let component = payload.component.clone();
        if matches!(
            self.handover,
            HandoverState::Ready { .. } | HandoverState::Complete
        ) {
            return Ok(Self::reject_handover(
                component,
                HandoverRejectionReason::NotReady,
            ));
        }

        match self.restore_mirror_payload(&payload) {
            Ok(_) => {
                let restored_marker = self.handover_marker(component.clone())?;
                self.handover = HandoverState::Mirrored {
                    restored_marker: restored_marker.clone(),
                };
                Ok(UpgradeReply::MirrorAcknowledged(MirrorAcknowledgement {
                    component,
                    write_counter: restored_marker.write_counter,
                }))
            }
            Err(
                Error::MirrorComponentMismatch { .. }
                | Error::MirrorKindMismatch { .. }
                | Error::MirrorTargetVersionMismatch { .. }
                | Error::MirrorArchiveDecode { .. },
            ) => Ok(Self::reject_handover(
                component,
                HandoverRejectionReason::SchemaMismatch,
            )),
            Err(error) => Err(error),
        }
    }

    fn recover_handover(&mut self) -> Result<bool> {
        match &self.handover {
            HandoverState::Active => Ok(true),
            HandoverState::Mirrored { .. } | HandoverState::Ready { .. } => {
                self.handover = HandoverState::Active;
                Ok(true)
            }
            HandoverState::Complete => Ok(false),
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
}
