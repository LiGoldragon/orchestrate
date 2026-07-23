use meta_signal_orchestrate::{MetaOrchestrateReply, MetaOrchestrateRequest};
use signal_frame::{
    AcceptedOutcome, NonEmpty, Reply, Request, RequestPayload, RequestRejectionReason, SubReply,
};
use signal_orchestrate::{
    ObservationToken, OrchestrateReply, OrchestrateRequest, PartialApplied, TimestampNanos,
};
use signal_version_handover::{
    CompletionReport, DivergenceAcknowledgement, HandoverAcceptance, HandoverFinalization,
    HandoverMarker, HandoverRejection, HandoverRejectionReason, MarkerRequest,
    MirrorAcknowledgement, MirrorPayload, Operation as UpgradeOperation, ReadinessReport,
    RecoveryResult, Reply as UpgradeReply,
};
use version_projection::ComponentName;

use crate::{
    Error, HarnessLivenessReconciliation, HarnessLivenessWatch, LaneReclaimer, LegacyLockImport,
    LockProjection, MetaRequestExecution, MirrorSnapshot, MirrorVersions, OrchestrateLayout,
    OrchestrateRequestExecution, OrchestrateTables, PublicSocketRetirement, Result, RoleRegistry,
    StoreLocation, StoredDivergence, WatchedHarnessProcess,
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
    /// The kernel-vouched process identifier of the peer whose working request
    /// is currently in flight, when the daemon boundary supplied it. The actor
    /// mailbox serialises requests, so exactly one working request is ever in
    /// flight; the registration handler reads this to discover the caller's
    /// reachability. Direct contract-level callers (tests) leave it `None`, so
    /// registration simply lands without reachability.
    pending_caller_process_id: Option<u32>,
    /// The router working socket a discovered agent's registration is propagated
    /// to, so the minted identity becomes a live router delivery target. `None`
    /// when the daemon was not configured with a router socket (or in tests):
    /// registration then lands without router propagation and no degradation is
    /// recorded. The router is a co-resident peer, so propagation is best-effort.
    router_registration_endpoint: Option<std::path::PathBuf>,
    /// The messenger working socket minted identities and discovered endpoints
    /// are pushed to — the orchestrator is the mint, the messenger's registry
    /// is the durable consumer view. `None` when the daemon was not configured
    /// with a messenger socket (or in tests): identities then land in
    /// orchestrate's own registry only, with no degradation recorded. The
    /// messenger is a co-resident peer, so the push is best-effort.
    messenger_registration_endpoint: Option<std::path::PathBuf>,
    /// The daemon-lifecycle deadline worker. It receives state-derived expiry
    /// deadlines after lane mutations and re-enters through Signal at expiry;
    /// it never opens or mutates the store itself.
    lane_reclaimer: Option<LaneReclaimer>,
    /// The engine-side harness liveness truth read, run at the head of every
    /// ordinary turn: an `Active` agent whose pinned harness process generation
    /// is gone from `/proc` is marked with the typed `Dead` status.
    harness_liveness: HarnessLivenessReconciliation,
    /// The daemon-lifecycle kernel exit watcher. It holds a pidfd per watched
    /// harness process and re-enters through Signal when the kernel pushes an
    /// exit; it never opens or mutates the store itself. `None` in tests and
    /// store-only openings.
    harness_liveness_watch: Option<HarnessLivenessWatch>,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Self::open_with_layout(store, OrchestrateLayout::primary_workspace())
    }

    pub fn open_with_layout(store: &StoreLocation, layout: OrchestrateLayout) -> Result<Self> {
        let tables = OrchestrateTables::open(store)?;
        RoleRegistry::new(&tables, &layout).seed_current_workspace_roles()?;
        LegacyLockImport::new(&tables, &layout).import_if_store_has_no_claims()?;
        let service = Self {
            tables,
            layout,
            next_observation_token: 1,
            handover: HandoverState::Active,
            public_sockets: PublicSocketRetirement::none(),
            pending_caller_process_id: None,
            router_registration_endpoint: None,
            messenger_registration_endpoint: None,
            lane_reclaimer: None,
            harness_liveness: HarnessLivenessReconciliation::from_process_environment(),
            harness_liveness_watch: None,
        };
        // Reap dead durable state at startup: every daemon restart (each deploy)
        // hard-deletes terminal lane records past retention, Active lanes idle
        // past the liveness window, retired/idle orchestrator agents, orphaned
        // topic seats, empty topics, aged workflow model resolutions, and worktree
        // rows whose checkout has vanished — so a store that accumulated leaked
        // and terminal records while an older daemon ran comes up reflecting only
        // real state.
        service.reconcile_bounded_state()?;
        Ok(service)
    }

    /// Attach lifecycle-driven lane expiry work to this daemon's ordinary
    /// socket. It uses exact store-derived deadlines, not an interval scan.
    pub fn with_lane_reclamation_socket(
        mut self,
        ordinary_socket_path: std::path::PathBuf,
    ) -> Result<Self> {
        let deadline = self.next_reclamation_deadline()?;
        self.lane_reclaimer = Some(LaneReclaimer::spawn(ordinary_socket_path, deadline));
        Ok(self)
    }

    /// Attach the kernel exit watcher to this daemon's ordinary socket and arm
    /// it from durable state: every `Active` agent's pinned harness process is
    /// watched immediately, so a harness that dies while the daemon runs is
    /// pushed dead by the kernel rather than aged out by the idle backstop.
    pub fn with_harness_liveness_watch(
        mut self,
        ordinary_socket_path: std::path::PathBuf,
    ) -> Result<Self> {
        let watch =
            HarnessLivenessWatch::spawn(ordinary_socket_path, std::path::PathBuf::from("/proc"))?;
        watch.reconcile(WatchedHarnessProcess::desired_set(&self.tables)?);
        self.harness_liveness_watch = Some(watch);
        Ok(self)
    }

    /// Publish the current earliest expiry after an engine turn. The worker
    /// waits for this exact deadline and sends one internal event when it is
    /// due; no human observation or background polling is involved.
    pub(crate) fn reschedule_lane_reclamation(&self) -> Result<()> {
        let Some(reclaimer) = &self.lane_reclaimer else {
            return Ok(());
        };
        reclaimer.reschedule(self.next_reclamation_deadline()?);
        Ok(())
    }

    /// Push the desired harness watch set after an engine turn: the exit
    /// watcher opens pidfds for newly registered reachability and drops fds
    /// whose agents went terminal. State flows engine → watcher only; the
    /// watcher's own signal back is the ordinary Signal re-entry.
    pub(crate) fn reschedule_harness_liveness_watch(&self) -> Result<()> {
        let Some(watch) = &self.harness_liveness_watch else {
            return Ok(());
        };
        watch.reconcile(WatchedHarnessProcess::desired_set(&self.tables)?);
        Ok(())
    }

    /// The earliest durable expiry across every deadline-driven store: the lane
    /// registry and the interim-bounded orchestrator-seat and workflow-resolution
    /// tables. The single reclamation worker sleeps to this instant and re-enters
    /// through the ordinary Signal path — no interval scan.
    fn next_reclamation_deadline(&self) -> Result<Option<TimestampNanos>> {
        let lane_deadline = crate::LaneRegistry::new(&self.tables).next_reclamation_deadline()?;
        let now = self.tables.current_timestamp()?;
        let table_deadline = crate::BoundedTableReaper::new(now).next_deadline(&self.tables)?;
        Ok(match (lane_deadline, table_deadline) {
            (Some(lane), Some(table)) => Some(if lane.value() <= table.value() {
                lane
            } else {
                table
            }),
            (Some(lane), None) => Some(lane),
            (None, table) => table,
        })
    }

    /// Reap every dead durable record in one pass — dead lanes, orphaned claims,
    /// retired and idle orchestrator agents, orphaned topic seats, empty topics,
    /// aged workflow model resolutions, and worktree rows whose checkout vanished.
    /// This runs at startup and on every ordinary engine turn, reflecting the
    /// same reconcile-on-read discipline the lane registry already applies, so
    /// the interim-bounded stores never accumulate dead records between the
    /// deadline worker's timed wakes.
    pub(crate) fn reconcile_bounded_state(&self) -> Result<()> {
        let now = self.tables.current_timestamp()?;
        // Liveness truth first: an `Active` agent whose pinned harness process
        // generation is gone from `/proc` becomes typed `Dead` this turn — the
        // kernel exit watcher only wakes the turn, the transition is always
        // derived here from process truth.
        self.harness_liveness.reconcile(&self.tables)?;
        let lane_reconciliation = crate::LaneRegistry::new(&self.tables).reconcile()?;
        self.tables.remove_claims_without_lanes()?;
        let table_reclamation = crate::BoundedTableReaper::new(now).reconcile(&self.tables)?;
        // A lane reap that flagged orphaned worktrees `Abandoned`, or a terminal
        // worktree tombstone reaped past its retention, changed the worktree
        // table, so refresh the GC manifest to match.
        if lane_reconciliation.flagged_abandoned_worktrees > 0
            || table_reclamation.reaped_missing_worktrees > 0
            || table_reclamation.reaped_terminal_worktrees > 0
        {
            crate::WorktreeProjection::new(&self.tables, &self.layout).project()?;
        }
        Ok(())
    }

    /// Register the router working socket a discovered agent's registration is
    /// propagated to. The daemon's `build_runtime` calls this with the configured
    /// path; tests and router-less deployments leave it unset, so registration
    /// lands without router propagation.
    pub fn with_router_registration_endpoint(
        mut self,
        endpoint: Option<std::path::PathBuf>,
    ) -> Self {
        self.router_registration_endpoint = endpoint;
        self
    }

    /// The router working socket to propagate a discovered registration to, when
    /// configured.
    pub(crate) fn router_registration_endpoint(&self) -> Option<&std::path::Path> {
        self.router_registration_endpoint.as_deref()
    }

    /// Register the messenger working socket minted identities and discovered
    /// endpoints are pushed to. The daemon's `build_runtime` calls this with
    /// the configured path; tests and messenger-less deployments leave it
    /// unset, so identities land in orchestrate's registry only.
    pub fn with_messenger_registration_endpoint(
        mut self,
        endpoint: Option<std::path::PathBuf>,
    ) -> Self {
        self.messenger_registration_endpoint = endpoint;
        self
    }

    /// The messenger working socket to push identities and endpoints to, when
    /// configured.
    /// The bounded triage-audit window, oldest first — the read surface for
    /// operators and tests witnessing how orchestrator messages were triaged.
    pub fn orchestrator_triage_records(
        &self,
    ) -> crate::Result<Vec<crate::StoredOrchestratorTriageRecord>> {
        self.tables.orchestrator_triage_records()
    }

    pub(crate) fn messenger_registration_endpoint(&self) -> Option<&std::path::Path> {
        self.messenger_registration_endpoint.as_deref()
    }

    /// Record the peer process identifier for the working request about to be
    /// driven, so the registration handler can discover its reachability. The
    /// daemon boundary sets this from the accepted connection's kernel-vouched
    /// credentials; it is cleared once the request completes.
    pub fn set_pending_caller_process_id(&mut self, process_id: Option<u32>) {
        self.pending_caller_process_id = process_id;
    }

    /// Take the pending caller process identifier, clearing it. Called once by
    /// the registration handler.
    pub(crate) fn take_pending_caller_process_id(&mut self) -> Option<u32> {
        self.pending_caller_process_id.take()
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
