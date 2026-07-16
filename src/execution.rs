use meta_signal_orchestrate as meta_contract;
use meta_signal_orchestrate::schema::lib as meta_schema;
use signal_frame::{
    BatchFailureReason, CommitStatus, NonEmpty, Reply, RetryClassification, SubReply,
};
use signal_harness as harness_contract;
use signal_orchestrate as ordinary_contract;
use signal_orchestrate::schema::lib as ordinary_schema;

use crate::schema::{nexus as nexus_schema, sema as sema_schema};
use crate::{
    ActivityLedger, AgentReachabilityDiscovery, ClaimLedger, Error, LaneRegistry,
    OrchestrateService, RepositoryRegistry, Result, RoleRegistry, RouterActorRegistration,
    RouterRegistrationDegradation, StoredAgentReachability, WorkflowRunner, WorktreeRegistry,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalTier {
    Ordinary,
    Meta,
}

pub struct OrchestrateNexusEngine<'service> {
    sema: OrchestrateSemaEngine<'service>,
    signal_tier: Option<SignalTier>,
    last_error: Option<Error>,
}

pub struct OrchestrateSemaEngine<'service> {
    service: &'service mut OrchestrateService,
    last_error: Option<Error>,
}

trait ProjectInto<Target> {
    fn project_into(self) -> Result<Target>;
}

impl OrchestrateService {
    /// Drive one ordinary working `Input` (the schema-emitted root the daemon
    /// decodes off the wire) through the nexus runner and return the schema
    /// `Output` root the daemon encodes back. This is the actor handler's
    /// working entry — it runs the runner natively on the Tokio runtime, the
    /// `&mut self` access coming from the engine actor's mailbox.
    pub async fn handle_signal_input(
        &mut self,
        input: ordinary_schema::Input,
    ) -> Result<ordinary_schema::Output> {
        self.handle_signal_input_from_caller(input, None).await
    }

    /// Drive one ordinary working `Input`, carrying the peer's kernel-vouched
    /// process identifier so the registration handler can discover the caller's
    /// reachability. The daemon boundary supplies the pid from the accepted
    /// connection's credentials; the pid is recorded for the duration of this
    /// one request and cleared before returning, correct because the actor
    /// mailbox serialises requests.
    pub async fn handle_signal_input_from_caller(
        &mut self,
        input: ordinary_schema::Input,
        caller_process_id: Option<u32>,
    ) -> Result<ordinary_schema::Output> {
        self.set_pending_caller_process_id(caller_process_id);
        let output = self.drive_ordinary_signal_input(input).await;
        self.set_pending_caller_process_id(None);
        output
    }

    async fn drive_ordinary_signal_input(
        &mut self,
        input: ordinary_schema::Input,
    ) -> Result<ordinary_schema::Output> {
        let signal_input = nexus_schema::SignalInput::ordinary_input(input);
        let output = match OrchestrateRequestExecution::drive_nexus(self, signal_input).await {
            Ok(nexus_schema::SignalOutput::OrdinaryOutput(output)) => Ok(output),
            Ok(nexus_schema::SignalOutput::MetaOutput(_)) => Err(Error::NexusReplyTierMismatch {
                expected: "ordinary",
                actual: "meta",
            }),
            Err(error) => Err(error),
        };
        // A caller rejection (an invalid domain value in a well-formed request)
        // rides the typed reply channel carrying its reason: the daemon spine
        // turns an `Err` return into a silent connection drop, so a rejection
        // such as a non-CamelCase session name or an unregistered-lane claim
        // must reply to stay diagnosable at the call site. Infrastructure and
        // malformed-frame failures still propagate and fail closed.
        match output {
            Ok(output) => Ok(output),
            Err(error) if error.is_caller_rejection() => {
                Ok(ordinary_schema::Output::partial_applied(
                    SchemaFailure::from_error(&error).partial_applied(),
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Drive one meta `Input` (the owner-only policy root) through the nexus
    /// runner and return the meta schema `Output` root.
    pub async fn handle_signal_meta_input(
        &mut self,
        input: meta_schema::Input,
    ) -> Result<meta_schema::Output> {
        let signal_input = nexus_schema::SignalInput::meta_input(input);
        let output = match MetaRequestExecution::drive_nexus(self, signal_input).await {
            Ok(nexus_schema::SignalOutput::MetaOutput(output)) => Ok(output),
            Ok(nexus_schema::SignalOutput::OrdinaryOutput(_)) => {
                Err(Error::NexusReplyTierMismatch {
                    expected: "meta",
                    actual: "ordinary",
                })
            }
            Err(error) => Err(error),
        };
        // Same wire-boundary rule as the ordinary tier: a caller rejection (e.g.
        // an invalid session/lane identifier) returns a typed `PartialApplied`
        // reply carrying its reason rather than propagating an `Err` the daemon
        // would drop as an opaque transport failure. Malformed frames that
        // decode leniently into a garbage request surface as infrastructure
        // failures and still fail closed.
        match output {
            Ok(output) => Ok(output),
            Err(error) if error.is_caller_rejection() => Ok(meta_schema::Output::partial_applied(
                SchemaFailure::from_error(&error).partial_applied(),
            )),
            Err(error) => Err(error),
        }
    }
}

impl<'service> OrchestrateNexusEngine<'service> {
    const MAXIMUM_NEXUS_STEPS: usize = 8;

    pub fn new(service: &'service mut OrchestrateService) -> Self {
        Self {
            sema: OrchestrateSemaEngine::new(service),
            signal_tier: None,
            last_error: None,
        }
    }

    pub fn take_last_error(&mut self) -> Option<Error> {
        self.last_error
            .take()
            .or_else(|| self.sema.take_last_error())
    }

    async fn drive_until_reply(
        &mut self,
        input: nexus_schema::SignalInput,
    ) -> Result<nexus_schema::SignalOutput> {
        let mut work =
            nexus_schema::NexusWork::signal_arrived(input).with_origin_route(Self::origin_route());
        for _step in 0..Self::MAXIMUM_NEXUS_STEPS {
            let origin_route = work.origin_route();
            let action = nexus_schema::NexusEngine::execute(self, work)
                .await
                .into_root();
            match action {
                nexus_schema::NexusAction::ReplyToSignal(reply) => {
                    return Ok(reply.into_payload());
                }
                nexus_schema::NexusAction::CommandSemaWrite(command) => {
                    let output = self.apply_sema_write(origin_route, command.into_payload());
                    work = nexus_schema::NexusWork::sema_write_completed(output)
                        .with_origin_route(origin_route);
                }
                nexus_schema::NexusAction::CommandSemaRead(command) => {
                    let output = self.observe_sema_read(origin_route, command.into_payload());
                    work = nexus_schema::NexusWork::sema_read_completed(output)
                        .with_origin_route(origin_route);
                }
                nexus_schema::NexusAction::Continue(next) => {
                    work = next.into_payload().with_origin_route(origin_route);
                }
            }
        }
        Ok(self.rejection_output())
    }

    fn apply_sema_write(
        &mut self,
        origin_route: nexus_schema::OriginRoute,
        input: sema_schema::SemaWriteInput,
    ) -> sema_schema::SemaWriteOutput {
        let output = sema_schema::SemaEngine::apply(
            &mut self.sema,
            input.with_origin_route(Self::sema_origin_route(origin_route)),
        )
        .into_root();
        if let Some(error) = self.sema.take_last_error() {
            self.last_error = Some(error);
        }
        output
    }

    fn observe_sema_read(
        &self,
        origin_route: nexus_schema::OriginRoute,
        input: sema_schema::SemaReadInput,
    ) -> sema_schema::SemaReadOutput {
        sema_schema::SemaEngine::observe(
            &self.sema,
            input.with_origin_route(Self::sema_origin_route(origin_route)),
        )
        .into_root()
    }

    fn origin_route() -> nexus_schema::OriginRoute {
        nexus_schema::OriginRoute::new(0)
    }

    fn sema_origin_route(origin_route: nexus_schema::OriginRoute) -> sema_schema::OriginRoute {
        sema_schema::OriginRoute::new(origin_route.payload())
    }

    fn project_signal_arrived(
        &mut self,
        input: nexus_schema::SignalInput,
    ) -> nexus_schema::NexusAction {
        match input {
            nexus_schema::SignalInput::OrdinaryInput(input) => {
                self.signal_tier = Some(SignalTier::Ordinary);
                nexus_schema::NexusAction::command_sema_write(
                    sema_schema::SemaWriteInput::apply_ordinary(input),
                )
            }
            nexus_schema::SignalInput::MetaInput(input) => {
                self.signal_tier = Some(SignalTier::Meta);
                nexus_schema::NexusAction::command_sema_write(
                    sema_schema::SemaWriteInput::apply_meta(input),
                )
            }
        }
    }

    fn project_sema_write_completed(
        &self,
        output: sema_schema::SemaWriteOutput,
    ) -> nexus_schema::NexusAction {
        let signal_output = match output {
            sema_schema::SemaWriteOutput::OrdinaryApplied(output) => {
                nexus_schema::SignalOutput::ordinary_output(output)
            }
            sema_schema::SemaWriteOutput::MetaApplied(output) => {
                nexus_schema::SignalOutput::meta_output(output)
            }
            sema_schema::SemaWriteOutput::WriteRejected(_) => self.rejection_output(),
        };
        nexus_schema::NexusAction::reply_to_signal(signal_output)
    }

    fn project_sema_read_completed(
        &self,
        output: sema_schema::SemaReadOutput,
    ) -> nexus_schema::NexusAction {
        let signal_output = match output {
            sema_schema::SemaReadOutput::RolesRead(snapshot) => {
                nexus_schema::SignalOutput::ordinary_output(ordinary_schema::Output::role_snapshot(
                    snapshot,
                ))
            }
            sema_schema::SemaReadOutput::LanesRead(lanes) => {
                nexus_schema::SignalOutput::ordinary_output(ordinary_schema::Output::LanesObserved(
                    lanes,
                ))
            }
            sema_schema::SemaReadOutput::ActivityRead(activity) => {
                nexus_schema::SignalOutput::ordinary_output(ordinary_schema::Output::ActivityList(
                    activity,
                ))
            }
            sema_schema::SemaReadOutput::ReadMiss(_) => self.rejection_output(),
        };
        nexus_schema::NexusAction::reply_to_signal(signal_output)
    }

    fn rejection_output(&self) -> nexus_schema::SignalOutput {
        match self.signal_tier.unwrap_or(SignalTier::Ordinary) {
            SignalTier::Ordinary => nexus_schema::SignalOutput::ordinary_output(
                ordinary_schema::Output::partial_applied(SchemaFailure::new().partial_applied()),
            ),
            SignalTier::Meta => nexus_schema::SignalOutput::meta_output(
                meta_schema::Output::partial_applied(SchemaFailure::new().partial_applied()),
            ),
        }
    }
}

impl sema_schema::SemaEngine for OrchestrateSemaEngine<'_> {
    fn apply_inner(
        &mut self,
        input: sema_schema::sema::Sema<sema_schema::sema::WriteInput>,
    ) -> sema_schema::sema::Sema<sema_schema::sema::WriteOutput> {
        let origin_route = input.origin_route();
        let output = match self.apply_write(input.into_root()) {
            Ok(output) => output,
            Err(error) => {
                let rejection = self.write_rejection_reason(&error);
                self.last_error = Some(error);
                sema_schema::SemaWriteOutput::write_rejected(rejection)
            }
        };
        output.with_origin_route(origin_route)
    }

    fn observe_inner(
        &self,
        input: sema_schema::sema::Sema<sema_schema::sema::ReadInput>,
    ) -> sema_schema::sema::Sema<sema_schema::sema::ReadOutput> {
        let origin_route = input.origin_route();
        let output = self.observe_read(input.into_root()).unwrap_or_else(|_| {
            sema_schema::SemaReadOutput::read_miss(sema_schema::ReadMissReason::NotBuiltYet)
        });
        output.with_origin_route(origin_route)
    }
}

impl nexus_schema::NexusEngine for OrchestrateNexusEngine<'_> {
    fn decide(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            nexus_schema::NexusWork::SignalArrived(input) => {
                self.project_signal_arrived(input.into_payload())
            }
            nexus_schema::NexusWork::SemaReadCompleted(output) => {
                self.project_sema_read_completed(output.into_payload())
            }
            nexus_schema::NexusWork::SemaWriteCompleted(output) => {
                self.project_sema_write_completed(output.into_payload())
            }
        };
        action.with_origin_route(origin_route)
    }
}

impl<'service> OrchestrateSemaEngine<'service> {
    pub fn new(service: &'service mut OrchestrateService) -> Self {
        Self {
            service,
            last_error: None,
        }
    }

    pub fn take_last_error(&mut self) -> Option<Error> {
        self.last_error.take()
    }

    fn apply_write(
        &mut self,
        input: sema_schema::SemaWriteInput,
    ) -> Result<sema_schema::SemaWriteOutput> {
        // The actor mailbox serialises every write; no sequence lock is needed.
        match input {
            sema_schema::SemaWriteInput::ApplyOrdinary(input) => {
                let request = input.into_payload().project_into()?;
                let reply = self.apply_ordinary_request(request)?;
                Ok(sema_schema::SemaWriteOutput::ordinary_applied(
                    reply.project_into()?,
                ))
            }
            sema_schema::SemaWriteInput::ApplyMeta(input) => {
                let request = input.into_payload().project_into()?;
                let reply = self.apply_meta_request(request)?;
                Ok(sema_schema::SemaWriteOutput::meta_applied(
                    reply.project_into()?,
                ))
            }
        }
    }

    fn observe_read(
        &self,
        input: sema_schema::SemaReadInput,
    ) -> Result<sema_schema::SemaReadOutput> {
        match input {
            sema_schema::SemaReadInput::ReadRoles(_) => {
                let reply = ClaimLedger::new(self.service.tables()).observe()?;
                let ordinary_schema::Output::RoleSnapshot(snapshot) = reply.project_into()? else {
                    return Err(Error::SchemaBridge {
                        message: "role observation did not produce RoleSnapshot".to_string(),
                    });
                };
                Ok(sema_schema::SemaReadOutput::roles_read(snapshot))
            }
            sema_schema::SemaReadInput::ReadLanes(_) => {
                let reply = LaneRegistry::new(self.service.tables()).observe()?;
                let ordinary_schema::Output::LanesObserved(lanes) = reply.project_into()? else {
                    return Err(Error::SchemaBridge {
                        message: "lane observation did not produce LanesObserved".to_string(),
                    });
                };
                Ok(sema_schema::SemaReadOutput::lanes_read(lanes))
            }
            sema_schema::SemaReadInput::ReadActivity(_) => {
                let reply = ActivityLedger::new(self.service.tables()).query(
                    ordinary_contract::ActivityQuery {
                        limit: 64,
                        filters: Vec::new(),
                    },
                )?;
                let ordinary_schema::Output::ActivityList(activity) = reply.project_into()? else {
                    return Err(Error::SchemaBridge {
                        message: "activity observation did not produce ActivityList".to_string(),
                    });
                };
                Ok(sema_schema::SemaReadOutput::activity_read(activity))
            }
        }
    }

    fn apply_ordinary_request(
        &mut self,
        request: ordinary_contract::OrchestrateRequest,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let reply = match request {
            ordinary_contract::OrchestrateRequest::Claim(claim) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_claim(claim)?;
                self.service.project_locks()?;
                reply
            }
            ordinary_contract::OrchestrateRequest::Release(release) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_release(release)?;
                self.service.project_locks()?;
                reply
            }
            ordinary_contract::OrchestrateRequest::Handoff(handoff) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_handoff(handoff)?;
                self.service.project_locks()?;
                reply
            }
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Roles,
            ) => ClaimLedger::new(self.service.tables()).observe()?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Sessions,
            ) => LaneRegistry::new(self.service.tables()).observe_sessions()?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::SessionLanes(session),
            ) => LaneRegistry::new(self.service.tables()).observe_session(session)?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Lanes,
            ) => LaneRegistry::new(self.service.tables()).observe()?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Worktrees,
            ) => WorktreeRegistry::new(self.service.tables(), self.service.layout()).observe()?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Topics,
            ) => ordinary_contract::OrchestrateReply::TopicTree(ordinary_contract::TopicTree {
                topics: self.orchestrator_topics()?,
            }),
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Topic(path),
            ) => self.observe_orchestrator_topic(path)?,
            ordinary_contract::OrchestrateRequest::Observe(
                ordinary_contract::Observation::Agents,
            ) => self.observe_orchestrator_agents()?,
            ordinary_contract::OrchestrateRequest::Submit(submission) => {
                ActivityLedger::new(self.service.tables()).submit(submission)?
            }
            ordinary_contract::OrchestrateRequest::Query(query) => {
                ActivityLedger::new(self.service.tables()).query(query)?
            }
            ordinary_contract::OrchestrateRequest::RunWorkflow(request) => {
                WorkflowRunner::fixture()?.run(request)?
            }
            ordinary_contract::OrchestrateRequest::RunResolvedWorkflow(request) => {
                WorkflowRunner::from_process_harness()?
                    .run_resolved_workflow(request, self.service.tables())?
            }
            ordinary_contract::OrchestrateRequest::ObserveWorkflowRun(observation) => {
                WorkflowRunner::fixture()?.open_observation(observation)?
            }
            ordinary_contract::OrchestrateRequest::WorkflowRunObservationRetraction(token) => {
                WorkflowRunner::fixture()?.close_observation(token)
            }
            ordinary_contract::OrchestrateRequest::Watch(_subscription) => {
                ordinary_contract::OrchestrateReply::ObservationOpened(
                    ordinary_contract::ObservationOpened {
                        token: self.service.next_observation_token()?,
                    },
                )
            }
            ordinary_contract::OrchestrateRequest::Unwatch(token) => {
                ordinary_contract::OrchestrateReply::ObservationClosed(
                    ordinary_contract::ObservationClosed { token },
                )
            }
            ordinary_contract::OrchestrateRequest::RegisterAgent(registration) => {
                self.register_orchestrator_agent(registration)?
            }
            ordinary_contract::OrchestrateRequest::RequestWorktree(order) => {
                let reply = WorktreeRegistry::new(self.service.tables(), self.service.layout())
                    .request(order)?;
                self.service.project_worktrees()?;
                reply
            }
            ordinary_contract::OrchestrateRequest::ConcludeWorktree(order) => {
                let reply = WorktreeRegistry::new(self.service.tables(), self.service.layout())
                    .conclude(order)?;
                self.service.project_worktrees()?;
                reply
            }
        };
        self.service.reschedule_lane_reclamation()?;
        Ok(reply)
    }

    fn apply_meta_request(
        &mut self,
        request: meta_contract::MetaOrchestrateRequest,
    ) -> Result<meta_contract::MetaOrchestrateReply> {
        let reply = match request {
            meta_contract::MetaOrchestrateRequest::Create(order) => {
                let reply = RoleRegistry::new(self.service.tables(), self.service.layout())
                    .create_role(order)?;
                self.service.project_locks()?;
                reply
            }
            meta_contract::MetaOrchestrateRequest::Retire(meta_contract::Retirement::Role(
                order,
            )) => {
                let reply = RoleRegistry::new(self.service.tables(), self.service.layout())
                    .retire_role(order)?;
                self.service.project_locks()?;
                reply
            }
            meta_contract::MetaOrchestrateRequest::Retire(meta_contract::Retirement::Lane(
                lane,
            )) => LaneRegistry::new(self.service.tables()).retire(lane)?,
            meta_contract::MetaOrchestrateRequest::Refresh(_order) => {
                RepositoryRegistry::new(self.service.tables(), self.service.layout()).refresh()?
            }
            meta_contract::MetaOrchestrateRequest::Register(request) => {
                LaneRegistry::new(self.service.tables()).register(request)?
            }
            meta_contract::MetaOrchestrateRequest::Unregister(request) => {
                LaneRegistry::new(self.service.tables()).unregister(request)?
            }
            meta_contract::MetaOrchestrateRequest::ClearSession(request) => {
                LaneRegistry::new(self.service.tables()).clear_session(request)?
            }
            meta_contract::MetaOrchestrateRequest::SetAuthority(change) => {
                LaneRegistry::new(self.service.tables()).set_authority(change)?
            }
            meta_contract::MetaOrchestrateRequest::RegisterWorktree(order) => {
                let reply = WorktreeRegistry::new(self.service.tables(), self.service.layout())
                    .register(order)?;
                self.service.project_worktrees()?;
                reply
            }
            meta_contract::MetaOrchestrateRequest::RefreshWorktreeIndex(_order) => {
                let reply = WorktreeRegistry::new(self.service.tables(), self.service.layout())
                    .refresh()?;
                self.service.project_worktrees()?;
                reply
            }
            meta_contract::MetaOrchestrateRequest::ArchiveWorktree(order) => {
                let reply = WorktreeRegistry::new(self.service.tables(), self.service.layout())
                    .archive(order)?;
                self.service.project_worktrees()?;
                reply
            }
        };
        self.service.reschedule_lane_reclamation()?;
        Ok(reply)
    }

    fn write_rejection_reason(&self, error: &Error) -> sema_schema::WriteRejectionReason {
        match error {
            Error::SignalOrchestrate(_) | Error::SchemaBridge { .. } => {
                sema_schema::WriteRejectionReason::InvalidOperation
            }
            _ => sema_schema::WriteRejectionReason::DependencyUnavailable,
        }
    }
}

pub struct OrchestrateRequestExecution<'service> {
    service: &'service mut OrchestrateService,
    request: signal_frame::Request<ordinary_contract::OrchestrateRequest>,
}

pub struct MetaRequestExecution<'service> {
    service: &'service mut OrchestrateService,
    request: signal_frame::Request<meta_contract::MetaOrchestrateRequest>,
}

impl<'service> OrchestrateRequestExecution<'service> {
    pub fn new(
        service: &'service mut OrchestrateService,
        request: signal_frame::Request<ordinary_contract::OrchestrateRequest>,
    ) -> Self {
        Self { service, request }
    }

    pub async fn execute(self) -> (Reply<ordinary_contract::OrchestrateReply>, Option<Error>) {
        if let Some(error) = self.single_payload_error() {
            return (Self::batch_aborted_reply(), Some(error));
        }
        let input = match self.request.payloads.into_head().project_into() {
            Ok(input) => nexus_schema::SignalInput::ordinary_input(input),
            Err(error) => return (Self::batch_aborted_reply(), Some(error)),
        };
        let output = Self::drive_nexus(self.service, input).await;
        let reply = match output {
            Ok(nexus_schema::SignalOutput::OrdinaryOutput(output)) => output.project_into(),
            Ok(nexus_schema::SignalOutput::MetaOutput(_)) => Err(Error::NexusReplyTierMismatch {
                expected: "ordinary",
                actual: "meta",
            }),
            Err(error) => Err(error),
        };
        match reply {
            Ok(reply) => (
                Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
                None,
            ),
            Err(error) => (Self::batch_aborted_reply(), Some(error)),
        }
    }

    fn single_payload_error(&self) -> Option<Error> {
        let operation_count = self.request.payloads().len();
        (operation_count != 1).then_some(Error::UnsupportedAtomicBatch { operation_count })
    }

    async fn drive_nexus(
        service: &mut OrchestrateService,
        input: nexus_schema::SignalInput,
    ) -> Result<nexus_schema::SignalOutput> {
        let mut engine = OrchestrateNexusEngine::new(service);
        let output = engine.drive_until_reply(input).await;
        if let Some(error) = engine.take_last_error() {
            return Err(error);
        }
        output
    }

    fn batch_aborted_reply() -> Reply<ordinary_contract::OrchestrateReply> {
        Reply::batch_aborted(
            BatchFailureReason::EngineRejected,
            RetryClassification::NotRetryable,
            CommitStatus::NotCommitted,
            NonEmpty::single(SubReply::Invalidated),
        )
    }
}

impl<'service> MetaRequestExecution<'service> {
    pub fn new(
        service: &'service mut OrchestrateService,
        request: signal_frame::Request<meta_contract::MetaOrchestrateRequest>,
    ) -> Self {
        Self { service, request }
    }

    pub async fn execute(self) -> (Reply<meta_contract::MetaOrchestrateReply>, Option<Error>) {
        if let Some(error) = self.single_payload_error() {
            return (Self::batch_aborted_reply(), Some(error));
        }
        let input = match self.request.payloads.into_head().project_into() {
            Ok(input) => nexus_schema::SignalInput::meta_input(input),
            Err(error) => return (Self::batch_aborted_reply(), Some(error)),
        };
        let output = Self::drive_nexus(self.service, input).await;
        let reply = match output {
            Ok(nexus_schema::SignalOutput::MetaOutput(output)) => output.project_into(),
            Ok(nexus_schema::SignalOutput::OrdinaryOutput(_)) => {
                Err(Error::NexusReplyTierMismatch {
                    expected: "meta",
                    actual: "ordinary",
                })
            }
            Err(error) => Err(error),
        };
        match reply {
            Ok(reply) => (
                Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
                None,
            ),
            Err(error) => (Self::batch_aborted_reply(), Some(error)),
        }
    }

    fn single_payload_error(&self) -> Option<Error> {
        let operation_count = self.request.payloads().len();
        (operation_count != 1).then_some(Error::UnsupportedAtomicBatch { operation_count })
    }

    async fn drive_nexus(
        service: &mut OrchestrateService,
        input: nexus_schema::SignalInput,
    ) -> Result<nexus_schema::SignalOutput> {
        let mut engine = OrchestrateNexusEngine::new(service);
        let output = engine.drive_until_reply(input).await;
        if let Some(error) = engine.take_last_error() {
            return Err(error);
        }
        output
    }

    fn batch_aborted_reply() -> Reply<meta_contract::MetaOrchestrateReply> {
        Reply::batch_aborted(
            BatchFailureReason::EngineRejected,
            RetryClassification::NotRetryable,
            CommitStatus::NotCommitted,
            NonEmpty::single(SubReply::Invalidated),
        )
    }
}

struct SchemaFailure {
    detail: ordinary_schema::ScopeReason,
}

impl SchemaFailure {
    fn new() -> Self {
        Self {
            detail: ordinary_schema::ScopeReason::new(
                "orchestrate nexus runner could not produce a committed reply",
            ),
        }
    }

    fn from_error(error: &Error) -> Self {
        Self {
            detail: ordinary_schema::ScopeReason::new(error.to_string()),
        }
    }

    fn partial_applied(self) -> ordinary_schema::PartialApplied {
        ordinary_schema::PartialApplied {
            application_successes: Vec::new().into(),
            application_failures: vec![ordinary_schema::ApplicationFailure {
                downstream_component: ordinary_schema::DownstreamComponent::System,
                application_failure_reason: ordinary_schema::ApplicationFailureReason::Unknown,
                scope_reason: self.detail,
            }]
            .into(),
        }
    }
}

impl signal_frame::BatchErrorClassification for Error {
    fn batch_failure_reason(&self) -> BatchFailureReason {
        BatchFailureReason::EngineRejected
    }

    fn retry_classification(&self) -> RetryClassification {
        RetryClassification::NotRetryable
    }

    fn commit_status(&self) -> CommitStatus {
        CommitStatus::NotCommitted
    }
}

impl<Source, Target> ProjectInto<Vec<Target>> for Vec<Source>
where
    Source: ProjectInto<Target>,
{
    fn project_into(self) -> Result<Vec<Target>> {
        self.into_iter().map(Source::project_into).collect()
    }
}

/// Bridge a schema-emitted `Vector` wrapper newtype (e.g. `RoleTokens`,
/// `ScopeReferences`) against the contract's bare `Vec<Inner>`. The newest
/// `schema-rust` emits each `(Vector T)` as a distinct newtype with
/// `new` / `into_payload` / `From<Vec<T>>`; the contract keeps a plain
/// `Vec`. Each invocation wires both directions through the element
/// `ProjectInto` and the existing `Vec` blanket above.
macro_rules! vector_wrapper_projection {
    ($wrapper:ty, $contract_inner:ty, $schema_inner:ty) => {
        impl ProjectInto<$wrapper> for Vec<$contract_inner> {
            fn project_into(self) -> Result<$wrapper> {
                let elements: Vec<$schema_inner> = self.project_into()?;
                Ok(<$wrapper>::new(elements))
            }
        }

        impl ProjectInto<Vec<$contract_inner>> for $wrapper {
            fn project_into(self) -> Result<Vec<$contract_inner>> {
                self.into_payload().project_into()
            }
        }
    };
}

vector_wrapper_projection!(
    ordinary_schema::RoleTokens,
    ordinary_contract::RoleToken,
    ordinary_schema::RoleToken
);
vector_wrapper_projection!(
    ordinary_schema::ScopeReferences,
    ordinary_contract::ScopeReference,
    ordinary_schema::ScopeReference
);
vector_wrapper_projection!(
    ordinary_schema::ActivityFilters,
    ordinary_contract::ActivityFilter,
    ordinary_schema::ActivityFilter
);
vector_wrapper_projection!(
    ordinary_schema::ScopeConflicts,
    ordinary_contract::ScopeConflict,
    ordinary_schema::ScopeConflict
);
vector_wrapper_projection!(
    ordinary_schema::LaneRegistrations,
    ordinary_contract::LaneRegistration,
    ordinary_schema::LaneRegistration
);
vector_wrapper_projection!(
    ordinary_schema::SessionProjections,
    ordinary_contract::SessionProjection,
    ordinary_schema::SessionProjection
);
vector_wrapper_projection!(
    ordinary_schema::LaneProjections,
    ordinary_contract::LaneProjection,
    ordinary_schema::LaneProjection
);
vector_wrapper_projection!(
    ordinary_schema::LaneResourceClaims,
    ordinary_contract::LaneResourceClaim,
    ordinary_schema::LaneResourceClaim
);
vector_wrapper_projection!(
    ordinary_schema::RoleStatuses,
    ordinary_contract::RoleStatus,
    ordinary_schema::RoleStatus
);
vector_wrapper_projection!(
    ordinary_schema::Activities,
    ordinary_contract::Activity,
    ordinary_schema::Activity
);
vector_wrapper_projection!(
    ordinary_schema::ClaimEntries,
    ordinary_contract::ClaimEntry,
    ordinary_schema::ClaimEntry
);
vector_wrapper_projection!(
    ordinary_schema::ApplicationSuccesses,
    ordinary_contract::ApplicationSuccess,
    ordinary_schema::ApplicationSuccess
);
vector_wrapper_projection!(
    ordinary_schema::ApplicationFailures,
    ordinary_contract::ApplicationFailure,
    ordinary_schema::ApplicationFailure
);
vector_wrapper_projection!(
    ordinary_schema::Worktrees,
    ordinary_contract::Worktree,
    ordinary_schema::Worktree
);
impl ProjectInto<ordinary_schema::WorkflowRunDigest> for ordinary_contract::WorkflowRunDigest {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunDigest> {
        Ok(ordinary_schema::WorkflowRunDigest::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunDigest> for ordinary_schema::WorkflowRunDigest {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunDigest> {
        ordinary_contract::WorkflowRunDigest::from_wire_token(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::WorkflowStepName> for ordinary_contract::WorkflowStepName {
    fn project_into(self) -> Result<ordinary_schema::WorkflowStepName> {
        Ok(ordinary_schema::WorkflowStepName::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowStepName> for ordinary_schema::WorkflowStepName {
    fn project_into(self) -> Result<ordinary_contract::WorkflowStepName> {
        ordinary_contract::WorkflowStepName::from_wire_token(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::ProviderName> for ordinary_contract::ProviderName {
    fn project_into(self) -> Result<ordinary_schema::ProviderName> {
        Ok(ordinary_schema::ProviderName::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::ProviderName> for ordinary_schema::ProviderName {
    fn project_into(self) -> Result<ordinary_contract::ProviderName> {
        ordinary_contract::ProviderName::from_wire_token(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::ModelName> for ordinary_contract::ModelName {
    fn project_into(self) -> Result<ordinary_schema::ModelName> {
        Ok(ordinary_schema::ModelName::new(self.as_str().to_string()))
    }
}

impl ProjectInto<ordinary_contract::ModelName> for ordinary_schema::ModelName {
    fn project_into(self) -> Result<ordinary_contract::ModelName> {
        ordinary_contract::ModelName::from_wire_token(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::HostName> for ordinary_contract::HostName {
    fn project_into(self) -> Result<ordinary_schema::HostName> {
        Ok(ordinary_schema::HostName::new(self.as_str().to_string()))
    }
}

impl ProjectInto<ordinary_contract::HostName> for ordinary_schema::HostName {
    fn project_into(self) -> Result<ordinary_contract::HostName> {
        ordinary_contract::HostName::from_wire_token(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::ObjectDigest> for signal_criome::ObjectDigest {
    fn project_into(self) -> Result<ordinary_schema::ObjectDigest> {
        Ok(ordinary_schema::ObjectDigest::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<signal_criome::ObjectDigest> for ordinary_schema::ObjectDigest {
    fn project_into(self) -> Result<signal_criome::ObjectDigest> {
        Ok(signal_criome::ObjectDigest::new(self.into_payload()))
    }
}

impl ProjectInto<ordinary_schema::ContractDigest> for signal_criome::ContractDigest {
    fn project_into(self) -> Result<ordinary_schema::ContractDigest> {
        Ok(ordinary_schema::ContractDigest::new(
            self.object_digest().clone().project_into()?,
        ))
    }
}

impl ProjectInto<signal_criome::ContractDigest> for ordinary_schema::ContractDigest {
    fn project_into(self) -> Result<signal_criome::ContractDigest> {
        Ok(signal_criome::ContractDigest::new(
            self.into_payload().project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_schema::OperationDigest> for signal_criome::OperationDigest {
    fn project_into(self) -> Result<ordinary_schema::OperationDigest> {
        Ok(ordinary_schema::OperationDigest::new(
            self.object_digest().clone().project_into()?,
        ))
    }
}

impl ProjectInto<signal_criome::OperationDigest> for ordinary_schema::OperationDigest {
    fn project_into(self) -> Result<signal_criome::OperationDigest> {
        Ok(signal_criome::OperationDigest::new(
            self.into_payload().project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_schema::WorkflowDigest> for signal_criome::WorkflowDigest {
    fn project_into(self) -> Result<ordinary_schema::WorkflowDigest> {
        Ok(ordinary_schema::WorkflowDigest::new(
            self.object_digest().clone().project_into()?,
        ))
    }
}

impl ProjectInto<signal_criome::WorkflowDigest> for ordinary_schema::WorkflowDigest {
    fn project_into(self) -> Result<signal_criome::WorkflowDigest> {
        Ok(signal_criome::WorkflowDigest::new(
            self.into_payload().project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_schema::WorkflowProvenanceDigest>
    for signal_criome::WorkflowProvenanceDigest
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowProvenanceDigest> {
        Ok(ordinary_schema::WorkflowProvenanceDigest::new(
            self.object_digest().clone().project_into()?,
        ))
    }
}

impl ProjectInto<signal_criome::WorkflowProvenanceDigest>
    for ordinary_schema::WorkflowProvenanceDigest
{
    fn project_into(self) -> Result<signal_criome::WorkflowProvenanceDigest> {
        Ok(signal_criome::WorkflowProvenanceDigest::new(
            self.into_payload().project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_schema::ComponentKind> for signal_criome::ComponentKind {
    fn project_into(self) -> Result<ordinary_schema::ComponentKind> {
        Ok(match self {
            signal_criome::ComponentKind::Spirit => ordinary_schema::ComponentKind::Spirit,
            signal_criome::ComponentKind::Criome => ordinary_schema::ComponentKind::Criome,
            signal_criome::ComponentKind::Router => ordinary_schema::ComponentKind::Router,
            signal_criome::ComponentKind::Mirror => ordinary_schema::ComponentKind::Mirror,
            signal_criome::ComponentKind::Lojix => ordinary_schema::ComponentKind::Lojix,
            signal_criome::ComponentKind::Persona => ordinary_schema::ComponentKind::Persona,
            signal_criome::ComponentKind::Agent => ordinary_schema::ComponentKind::Agent,
        })
    }
}

impl ProjectInto<signal_criome::ComponentKind> for ordinary_schema::ComponentKind {
    fn project_into(self) -> Result<signal_criome::ComponentKind> {
        Ok(match self {
            ordinary_schema::ComponentKind::Spirit => signal_criome::ComponentKind::Spirit,
            ordinary_schema::ComponentKind::Criome => signal_criome::ComponentKind::Criome,
            ordinary_schema::ComponentKind::Router => signal_criome::ComponentKind::Router,
            ordinary_schema::ComponentKind::Mirror => signal_criome::ComponentKind::Mirror,
            ordinary_schema::ComponentKind::Lojix => signal_criome::ComponentKind::Lojix,
            ordinary_schema::ComponentKind::Persona => signal_criome::ComponentKind::Persona,
            ordinary_schema::ComponentKind::Agent => signal_criome::ComponentKind::Agent,
        })
    }
}

impl ProjectInto<ordinary_schema::AuthorizedObjectKind> for signal_criome::AuthorizedObjectKind {
    fn project_into(self) -> Result<ordinary_schema::AuthorizedObjectKind> {
        Ok(match self {
            signal_criome::AuthorizedObjectKind::Operation => {
                ordinary_schema::AuthorizedObjectKind::Operation
            }
            signal_criome::AuthorizedObjectKind::Contract => {
                ordinary_schema::AuthorizedObjectKind::Contract
            }
            signal_criome::AuthorizedObjectKind::Agreement => {
                ordinary_schema::AuthorizedObjectKind::Agreement
            }
            signal_criome::AuthorizedObjectKind::Time => {
                ordinary_schema::AuthorizedObjectKind::Time
            }
            signal_criome::AuthorizedObjectKind::Head => {
                ordinary_schema::AuthorizedObjectKind::Head
            }
        })
    }
}

impl ProjectInto<signal_criome::AuthorizedObjectKind> for ordinary_schema::AuthorizedObjectKind {
    fn project_into(self) -> Result<signal_criome::AuthorizedObjectKind> {
        Ok(match self {
            ordinary_schema::AuthorizedObjectKind::Operation => {
                signal_criome::AuthorizedObjectKind::Operation
            }
            ordinary_schema::AuthorizedObjectKind::Contract => {
                signal_criome::AuthorizedObjectKind::Contract
            }
            ordinary_schema::AuthorizedObjectKind::Agreement => {
                signal_criome::AuthorizedObjectKind::Agreement
            }
            ordinary_schema::AuthorizedObjectKind::Time => {
                signal_criome::AuthorizedObjectKind::Time
            }
            ordinary_schema::AuthorizedObjectKind::Head => {
                signal_criome::AuthorizedObjectKind::Head
            }
        })
    }
}

impl ProjectInto<ordinary_schema::AuthorizedObjectReference>
    for signal_criome::AuthorizedObjectReference
{
    fn project_into(self) -> Result<ordinary_schema::AuthorizedObjectReference> {
        Ok(ordinary_schema::AuthorizedObjectReference {
            component_kind: self.component_kind.project_into()?,
            object_digest: self.object_digest.project_into()?,
            authorized_object_kind: self.authorized_object_kind.project_into()?,
        })
    }
}

impl ProjectInto<signal_criome::AuthorizedObjectReference>
    for ordinary_schema::AuthorizedObjectReference
{
    fn project_into(self) -> Result<signal_criome::AuthorizedObjectReference> {
        Ok(signal_criome::AuthorizedObjectReference {
            component_kind: self.component_kind.project_into()?,
            object_digest: self.object_digest.project_into()?,
            authorized_object_kind: self.authorized_object_kind.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::EvaluationRejectionReason>
    for signal_criome::EvaluationRejectionReason
{
    fn project_into(self) -> Result<ordinary_schema::EvaluationRejectionReason> {
        Ok(match self {
            signal_criome::EvaluationRejectionReason::OutsideTimeWindow => {
                ordinary_schema::EvaluationRejectionReason::OutsideTimeWindow
            }
            signal_criome::EvaluationRejectionReason::TimeNotProven => {
                ordinary_schema::EvaluationRejectionReason::TimeNotProven
            }
            signal_criome::EvaluationRejectionReason::AgreementMissing => {
                ordinary_schema::EvaluationRejectionReason::AgreementMissing
            }
            signal_criome::EvaluationRejectionReason::SignatureMissing(_)
            | signal_criome::EvaluationRejectionReason::QuorumShort(_) => {
                return Err(Error::SchemaBridge {
                    message:
                        "signal-orchestrate schema mirror cannot carry detailed criome rejection"
                            .to_string(),
                });
            }
        })
    }
}

impl ProjectInto<signal_criome::EvaluationRejectionReason>
    for ordinary_schema::EvaluationRejectionReason
{
    fn project_into(self) -> Result<signal_criome::EvaluationRejectionReason> {
        Ok(match self {
            ordinary_schema::EvaluationRejectionReason::OutsideTimeWindow => {
                signal_criome::EvaluationRejectionReason::OutsideTimeWindow
            }
            ordinary_schema::EvaluationRejectionReason::TimeNotProven => {
                signal_criome::EvaluationRejectionReason::TimeNotProven
            }
            ordinary_schema::EvaluationRejectionReason::AgreementMissing => {
                signal_criome::EvaluationRejectionReason::AgreementMissing
            }
        })
    }
}

impl ProjectInto<ordinary_schema::EscalationTarget> for signal_criome::EscalationTarget {
    fn project_into(self) -> Result<ordinary_schema::EscalationTarget> {
        Ok(match self {
            signal_criome::EscalationTarget::Psyche => ordinary_schema::EscalationTarget::Psyche,
            signal_criome::EscalationTarget::Workflow(workflow) => {
                ordinary_schema::EscalationTarget::Workflow(workflow.project_into()?)
            }
            signal_criome::EscalationTarget::SmarterAgent(_) => {
                return Err(Error::SchemaBridge {
                    message:
                        "signal-orchestrate schema mirror cannot carry smarter-agent escalation"
                            .to_string(),
                });
            }
        })
    }
}

impl ProjectInto<signal_criome::EscalationTarget> for ordinary_schema::EscalationTarget {
    fn project_into(self) -> Result<signal_criome::EscalationTarget> {
        Ok(match self {
            ordinary_schema::EscalationTarget::Psyche => signal_criome::EscalationTarget::Psyche,
            ordinary_schema::EscalationTarget::Workflow(workflow) => {
                signal_criome::EscalationTarget::Workflow(workflow.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::EvaluationDecision> for signal_criome::EvaluationDecision {
    fn project_into(self) -> Result<ordinary_schema::EvaluationDecision> {
        Ok(match self {
            signal_criome::EvaluationDecision::Authorized => {
                ordinary_schema::EvaluationDecision::Authorized
            }
            signal_criome::EvaluationDecision::Deferred => {
                ordinary_schema::EvaluationDecision::Deferred
            }
            signal_criome::EvaluationDecision::NonJudgement => {
                ordinary_schema::EvaluationDecision::NonJudgement
            }
            signal_criome::EvaluationDecision::Escalate(target) => {
                ordinary_schema::EvaluationDecision::Escalate(target.project_into()?)
            }
            signal_criome::EvaluationDecision::Rejected(reason) => {
                ordinary_schema::EvaluationDecision::Rejected(reason.project_into()?)
            }
        })
    }
}

impl ProjectInto<signal_criome::EvaluationDecision> for ordinary_schema::EvaluationDecision {
    fn project_into(self) -> Result<signal_criome::EvaluationDecision> {
        Ok(match self {
            ordinary_schema::EvaluationDecision::Authorized => {
                signal_criome::EvaluationDecision::Authorized
            }
            ordinary_schema::EvaluationDecision::Deferred => {
                signal_criome::EvaluationDecision::Deferred
            }
            ordinary_schema::EvaluationDecision::NonJudgement => {
                signal_criome::EvaluationDecision::NonJudgement
            }
            ordinary_schema::EvaluationDecision::Escalate(target) => {
                signal_criome::EvaluationDecision::Escalate(target.project_into()?)
            }
            ordinary_schema::EvaluationDecision::Rejected(reason) => {
                signal_criome::EvaluationDecision::Rejected(reason.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowReceipt> for signal_criome::WorkflowReceipt {
    fn project_into(self) -> Result<ordinary_schema::WorkflowReceipt> {
        Ok(ordinary_schema::WorkflowReceipt {
            workflow_digest: self.workflow_digest.project_into()?,
            operation_digest: self.operation_digest.project_into()?,
            evaluation_decision: self.evaluation_decision.project_into()?,
            workflow_provenance_digest: self.workflow_provenance_digest.project_into()?,
        })
    }
}

impl ProjectInto<signal_criome::WorkflowReceipt> for ordinary_schema::WorkflowReceipt {
    fn project_into(self) -> Result<signal_criome::WorkflowReceipt> {
        Ok(signal_criome::WorkflowReceipt {
            workflow_digest: self.workflow_digest.project_into()?,
            operation_digest: self.operation_digest.project_into()?,
            evaluation_decision: self.evaluation_decision.project_into()?,
            workflow_provenance_digest: self.workflow_provenance_digest.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunRequest> for ordinary_contract::WorkflowRunRequest {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunRequest> {
        Ok(ordinary_schema::WorkflowRunRequest {
            workflow_digest: self.workflow.project_into()?,
            authorized_object_reference: self.operation.project_into()?,
            contract_digest: self.contract.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunRequest> for ordinary_schema::WorkflowRunRequest {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunRequest> {
        Ok(ordinary_contract::WorkflowRunRequest {
            workflow: self.workflow_digest.project_into()?,
            operation: self.authorized_object_reference.project_into()?,
            contract: self.contract_digest.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::NamedModel> for harness_contract::NamedModel {
    fn project_into(self) -> Result<ordinary_schema::NamedModel> {
        Ok(ordinary_schema::NamedModel::new(self.as_str().to_string()))
    }
}

impl ProjectInto<harness_contract::NamedModel> for ordinary_schema::NamedModel {
    fn project_into(self) -> Result<harness_contract::NamedModel> {
        Ok(harness_contract::NamedModel::new(self.into_payload()))
    }
}

impl ProjectInto<ordinary_schema::CapabilityProfile> for harness_contract::CapabilityProfile {
    fn project_into(self) -> Result<ordinary_schema::CapabilityProfile> {
        Ok(ordinary_schema::CapabilityProfile::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<harness_contract::CapabilityProfile> for ordinary_schema::CapabilityProfile {
    fn project_into(self) -> Result<harness_contract::CapabilityProfile> {
        Ok(harness_contract::CapabilityProfile::new(
            self.into_payload(),
        ))
    }
}

impl ProjectInto<ordinary_schema::ModelSelector> for harness_contract::ModelSelector {
    fn project_into(self) -> Result<ordinary_schema::ModelSelector> {
        Ok(match self {
            harness_contract::ModelSelector::Exact(model) => {
                ordinary_schema::ModelSelector::Exact(model.project_into()?)
            }
            harness_contract::ModelSelector::CapabilityProfile(profile) => {
                ordinary_schema::ModelSelector::CapabilityProfile(profile.project_into()?)
            }
        })
    }
}

impl ProjectInto<harness_contract::ModelSelector> for ordinary_schema::ModelSelector {
    fn project_into(self) -> Result<harness_contract::ModelSelector> {
        Ok(match self {
            ordinary_schema::ModelSelector::Exact(model) => {
                harness_contract::ModelSelector::Exact(model.project_into()?)
            }
            ordinary_schema::ModelSelector::CapabilityProfile(profile) => {
                harness_contract::ModelSelector::CapabilityProfile(profile.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::EffortRequest> for harness_contract::EffortRequest {
    fn project_into(self) -> Result<ordinary_schema::EffortRequest> {
        Ok(match self {
            harness_contract::EffortRequest::Minimal => ordinary_schema::EffortRequest::Minimal,
            harness_contract::EffortRequest::Low => ordinary_schema::EffortRequest::Low,
            harness_contract::EffortRequest::Medium => ordinary_schema::EffortRequest::Medium,
            harness_contract::EffortRequest::High => ordinary_schema::EffortRequest::High,
            harness_contract::EffortRequest::ExtraHigh => ordinary_schema::EffortRequest::ExtraHigh,
            harness_contract::EffortRequest::Maximum => ordinary_schema::EffortRequest::Maximum,
        })
    }
}

impl ProjectInto<harness_contract::EffortRequest> for ordinary_schema::EffortRequest {
    fn project_into(self) -> Result<harness_contract::EffortRequest> {
        Ok(match self {
            ordinary_schema::EffortRequest::Minimal => harness_contract::EffortRequest::Minimal,
            ordinary_schema::EffortRequest::Low => harness_contract::EffortRequest::Low,
            ordinary_schema::EffortRequest::Medium => harness_contract::EffortRequest::Medium,
            ordinary_schema::EffortRequest::High => harness_contract::EffortRequest::High,
            ordinary_schema::EffortRequest::ExtraHigh => harness_contract::EffortRequest::ExtraHigh,
            ordinary_schema::EffortRequest::Maximum => harness_contract::EffortRequest::Maximum,
        })
    }
}

impl ProjectInto<ordinary_schema::ModelRequest> for harness_contract::ModelRequest {
    fn project_into(self) -> Result<ordinary_schema::ModelRequest> {
        Ok(ordinary_schema::ModelRequest {
            model_selector: self.selector.project_into()?,
            effort_request: self.effort.project_into()?,
        })
    }
}

impl ProjectInto<harness_contract::ModelRequest> for ordinary_schema::ModelRequest {
    fn project_into(self) -> Result<harness_contract::ModelRequest> {
        Ok(harness_contract::ModelRequest {
            selector: self.model_selector.project_into()?,
            effort: self.effort_request.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ClaudeSessionIdentifier>
    for harness_contract::ClaudeSessionIdentifier
{
    fn project_into(self) -> Result<ordinary_schema::ClaudeSessionIdentifier> {
        Ok(ordinary_schema::ClaudeSessionIdentifier::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<harness_contract::ClaudeSessionIdentifier>
    for ordinary_schema::ClaudeSessionIdentifier
{
    fn project_into(self) -> Result<harness_contract::ClaudeSessionIdentifier> {
        Ok(harness_contract::ClaudeSessionIdentifier::new(
            self.into_payload(),
        ))
    }
}

impl ProjectInto<ordinary_schema::CodexContinuationIdentifier>
    for harness_contract::CodexContinuationIdentifier
{
    fn project_into(self) -> Result<ordinary_schema::CodexContinuationIdentifier> {
        Ok(ordinary_schema::CodexContinuationIdentifier::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<harness_contract::CodexContinuationIdentifier>
    for ordinary_schema::CodexContinuationIdentifier
{
    fn project_into(self) -> Result<harness_contract::CodexContinuationIdentifier> {
        Ok(harness_contract::CodexContinuationIdentifier::new(
            self.into_payload(),
        ))
    }
}

impl ProjectInto<ordinary_schema::PiContinuationIdentifier>
    for harness_contract::PiContinuationIdentifier
{
    fn project_into(self) -> Result<ordinary_schema::PiContinuationIdentifier> {
        Ok(ordinary_schema::PiContinuationIdentifier::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<harness_contract::PiContinuationIdentifier>
    for ordinary_schema::PiContinuationIdentifier
{
    fn project_into(self) -> Result<harness_contract::PiContinuationIdentifier> {
        Ok(harness_contract::PiContinuationIdentifier::new(
            self.into_payload(),
        ))
    }
}

impl ProjectInto<ordinary_schema::ContinuationHandle> for harness_contract::ContinuationHandle {
    fn project_into(self) -> Result<ordinary_schema::ContinuationHandle> {
        Ok(match self {
            harness_contract::ContinuationHandle::Claude(handle) => {
                ordinary_schema::ContinuationHandle::Claude(handle.project_into()?)
            }
            harness_contract::ContinuationHandle::Codex(handle) => {
                ordinary_schema::ContinuationHandle::Codex(handle.project_into()?)
            }
            harness_contract::ContinuationHandle::Pi(handle) => {
                ordinary_schema::ContinuationHandle::Pi(handle.project_into()?)
            }
        })
    }
}

impl ProjectInto<harness_contract::ContinuationHandle> for ordinary_schema::ContinuationHandle {
    fn project_into(self) -> Result<harness_contract::ContinuationHandle> {
        Ok(match self {
            ordinary_schema::ContinuationHandle::Claude(handle) => {
                harness_contract::ContinuationHandle::Claude(handle.project_into()?)
            }
            ordinary_schema::ContinuationHandle::Codex(handle) => {
                harness_contract::ContinuationHandle::Codex(handle.project_into()?)
            }
            ordinary_schema::ContinuationHandle::Pi(handle) => {
                harness_contract::ContinuationHandle::Pi(handle.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ContinuationRequest> for harness_contract::ContinuationRequest {
    fn project_into(self) -> Result<ordinary_schema::ContinuationRequest> {
        Ok(match self {
            harness_contract::ContinuationRequest::Fresh => {
                ordinary_schema::ContinuationRequest::Fresh
            }
            harness_contract::ContinuationRequest::Prefer(handle) => {
                ordinary_schema::ContinuationRequest::Prefer(handle.project_into()?)
            }
            harness_contract::ContinuationRequest::Require(handle) => {
                ordinary_schema::ContinuationRequest::Require(handle.project_into()?)
            }
        })
    }
}

impl ProjectInto<harness_contract::ContinuationRequest> for ordinary_schema::ContinuationRequest {
    fn project_into(self) -> Result<harness_contract::ContinuationRequest> {
        Ok(match self {
            ordinary_schema::ContinuationRequest::Fresh => {
                harness_contract::ContinuationRequest::Fresh
            }
            ordinary_schema::ContinuationRequest::Prefer(handle) => {
                harness_contract::ContinuationRequest::Prefer(handle.project_into()?)
            }
            ordinary_schema::ContinuationRequest::Require(handle) => {
                harness_contract::ContinuationRequest::Require(handle.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ModelResolutionRequest>
    for harness_contract::ModelResolutionRequest
{
    fn project_into(self) -> Result<ordinary_schema::ModelResolutionRequest> {
        Ok(ordinary_schema::ModelResolutionRequest {
            model_request: self.model.project_into()?,
            continuation_request: self.continuation.project_into()?,
        })
    }
}

impl ProjectInto<harness_contract::ModelResolutionRequest>
    for ordinary_schema::ModelResolutionRequest
{
    fn project_into(self) -> Result<harness_contract::ModelResolutionRequest> {
        Ok(harness_contract::ModelResolutionRequest {
            model: self.model_request.project_into()?,
            continuation: self.continuation_request.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::HarnessName> for harness_contract::HarnessName {
    fn project_into(self) -> Result<ordinary_schema::HarnessName> {
        Ok(ordinary_schema::HarnessName::new(self.as_str().to_string()))
    }
}

impl ProjectInto<harness_contract::HarnessName> for ordinary_schema::HarnessName {
    fn project_into(self) -> Result<harness_contract::HarnessName> {
        Ok(harness_contract::HarnessName::new(self.into_payload()))
    }
}

impl ProjectInto<ordinary_schema::ResolvedHarnessKind> for harness_contract::HarnessKind {
    fn project_into(self) -> Result<ordinary_schema::ResolvedHarnessKind> {
        Ok(match self {
            harness_contract::HarnessKind::Codex => ordinary_schema::ResolvedHarnessKind::Codex,
            harness_contract::HarnessKind::Claude => ordinary_schema::ResolvedHarnessKind::Claude,
            harness_contract::HarnessKind::Pi => ordinary_schema::ResolvedHarnessKind::Pi,
            harness_contract::HarnessKind::Fixture => ordinary_schema::ResolvedHarnessKind::Fixture,
        })
    }
}

impl ProjectInto<harness_contract::HarnessKind> for ordinary_schema::ResolvedHarnessKind {
    fn project_into(self) -> Result<harness_contract::HarnessKind> {
        Ok(match self {
            ordinary_schema::ResolvedHarnessKind::Codex => harness_contract::HarnessKind::Codex,
            ordinary_schema::ResolvedHarnessKind::Claude => harness_contract::HarnessKind::Claude,
            ordinary_schema::ResolvedHarnessKind::Pi => harness_contract::HarnessKind::Pi,
            ordinary_schema::ResolvedHarnessKind::Fixture => harness_contract::HarnessKind::Fixture,
        })
    }
}

impl ProjectInto<ordinary_schema::ModelResolved> for harness_contract::ModelResolved {
    fn project_into(self) -> Result<ordinary_schema::ModelResolved> {
        Ok(ordinary_schema::ModelResolved {
            harness_name: self.harness.project_into()?,
            resolved_harness_kind: self.harness_kind.project_into()?,
            named_model: self.model.project_into()?,
            effort_request: self.effort.project_into()?,
            continuation_handle: self.continuation.project_into()?,
        })
    }
}

impl ProjectInto<harness_contract::ModelResolved> for ordinary_schema::ModelResolved {
    fn project_into(self) -> Result<harness_contract::ModelResolved> {
        Ok(harness_contract::ModelResolved {
            harness: self.harness_name.project_into()?,
            harness_kind: self.resolved_harness_kind.project_into()?,
            model: self.named_model.project_into()?,
            effort: self.effort_request.project_into()?,
            continuation: self.continuation_handle.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ModelUnavailableReason>
    for harness_contract::ModelUnavailableReason
{
    fn project_into(self) -> Result<ordinary_schema::ModelUnavailableReason> {
        Ok(match self {
            harness_contract::ModelUnavailableReason::NoConfiguredHarness => {
                ordinary_schema::ModelUnavailableReason::NoConfiguredHarness
            }
            harness_contract::ModelUnavailableReason::ModelNotKnown => {
                ordinary_schema::ModelUnavailableReason::ModelNotKnown
            }
            harness_contract::ModelUnavailableReason::EffortUnsupported => {
                ordinary_schema::ModelUnavailableReason::EffortUnsupported
            }
            harness_contract::ModelUnavailableReason::CapabilityUnsupported => {
                ordinary_schema::ModelUnavailableReason::CapabilityUnsupported
            }
            harness_contract::ModelUnavailableReason::ProviderUnavailable => {
                ordinary_schema::ModelUnavailableReason::ProviderUnavailable
            }
            harness_contract::ModelUnavailableReason::ContinuationUnavailable => {
                ordinary_schema::ModelUnavailableReason::ContinuationUnavailable
            }
            harness_contract::ModelUnavailableReason::AdapterConfigurationMissing => {
                ordinary_schema::ModelUnavailableReason::AdapterConfigurationMissing
            }
        })
    }
}

impl ProjectInto<harness_contract::ModelUnavailableReason>
    for ordinary_schema::ModelUnavailableReason
{
    fn project_into(self) -> Result<harness_contract::ModelUnavailableReason> {
        Ok(match self {
            ordinary_schema::ModelUnavailableReason::NoConfiguredHarness => {
                harness_contract::ModelUnavailableReason::NoConfiguredHarness
            }
            ordinary_schema::ModelUnavailableReason::ModelNotKnown => {
                harness_contract::ModelUnavailableReason::ModelNotKnown
            }
            ordinary_schema::ModelUnavailableReason::EffortUnsupported => {
                harness_contract::ModelUnavailableReason::EffortUnsupported
            }
            ordinary_schema::ModelUnavailableReason::CapabilityUnsupported => {
                harness_contract::ModelUnavailableReason::CapabilityUnsupported
            }
            ordinary_schema::ModelUnavailableReason::ProviderUnavailable => {
                harness_contract::ModelUnavailableReason::ProviderUnavailable
            }
            ordinary_schema::ModelUnavailableReason::ContinuationUnavailable => {
                harness_contract::ModelUnavailableReason::ContinuationUnavailable
            }
            ordinary_schema::ModelUnavailableReason::AdapterConfigurationMissing => {
                harness_contract::ModelUnavailableReason::AdapterConfigurationMissing
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ModelUnavailable> for harness_contract::ModelUnavailable {
    fn project_into(self) -> Result<ordinary_schema::ModelUnavailable> {
        Ok(ordinary_schema::ModelUnavailable {
            model_resolution_request: self.request.project_into()?,
            model_unavailable_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<harness_contract::ModelUnavailable> for ordinary_schema::ModelUnavailable {
    fn project_into(self) -> Result<harness_contract::ModelUnavailable> {
        Ok(harness_contract::ModelUnavailable {
            request: self.model_resolution_request.project_into()?,
            reason: self.model_unavailable_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ResolvedWorkflowRunRequest>
    for ordinary_contract::ResolvedWorkflowRunRequest
{
    fn project_into(self) -> Result<ordinary_schema::ResolvedWorkflowRunRequest> {
        Ok(ordinary_schema::ResolvedWorkflowRunRequest {
            workflow_run_request: self.workflow_run.project_into()?,
            model_resolution_request: self.model_resolution.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ResolvedWorkflowRunRequest>
    for ordinary_schema::ResolvedWorkflowRunRequest
{
    fn project_into(self) -> Result<ordinary_contract::ResolvedWorkflowRunRequest> {
        Ok(ordinary_contract::ResolvedWorkflowRunRequest {
            workflow_run: self.workflow_run_request.project_into()?,
            model_resolution: self.model_resolution_request.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunObservation>
    for ordinary_contract::WorkflowRunObservation
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunObservation> {
        Ok(ordinary_schema::WorkflowRunObservation::new(
            self.run.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunObservation>
    for ordinary_schema::WorkflowRunObservation
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunObservation> {
        Ok(ordinary_contract::WorkflowRunObservation {
            run: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunObservationToken>
    for ordinary_contract::WorkflowRunObservationToken
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunObservationToken> {
        Ok(ordinary_schema::WorkflowRunObservationToken::new(
            self.run.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunObservationToken>
    for ordinary_schema::WorkflowRunObservationToken
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunObservationToken> {
        Ok(ordinary_contract::WorkflowRunObservationToken {
            run: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunHandle> for ordinary_contract::WorkflowRunHandle {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunHandle> {
        Ok(ordinary_schema::WorkflowRunHandle::new(
            self.run.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunHandle> for ordinary_schema::WorkflowRunHandle {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunHandle> {
        Ok(ordinary_contract::WorkflowRunHandle {
            run: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunAccepted> for ordinary_contract::WorkflowRunAccepted {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunAccepted> {
        Ok(ordinary_schema::WorkflowRunAccepted::new(
            self.handle.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunAccepted> for ordinary_schema::WorkflowRunAccepted {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunAccepted> {
        Ok(ordinary_contract::WorkflowRunAccepted {
            handle: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunResolution>
    for ordinary_contract::WorkflowRunResolution
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunResolution> {
        Ok(ordinary_schema::WorkflowRunResolution {
            workflow_run_handle: self.handle.project_into()?,
            model_resolved: self.resolution.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunResolution>
    for ordinary_schema::WorkflowRunResolution
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunResolution> {
        Ok(ordinary_contract::WorkflowRunResolution {
            handle: self.workflow_run_handle.project_into()?,
            resolution: self.model_resolved.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowResolutionUnavailable>
    for ordinary_contract::WorkflowResolutionUnavailable
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowResolutionUnavailable> {
        Ok(ordinary_schema::WorkflowResolutionUnavailable {
            workflow_run_handle: self.handle.project_into()?,
            resolved_workflow_run_request: self.request.project_into()?,
            model_unavailable: self.unavailable.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowResolutionUnavailable>
    for ordinary_schema::WorkflowResolutionUnavailable
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowResolutionUnavailable> {
        Ok(ordinary_contract::WorkflowResolutionUnavailable {
            handle: self.workflow_run_handle.project_into()?,
            request: self.resolved_workflow_run_request.project_into()?,
            unavailable: self.model_unavailable.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowResolvedReceiptProduced>
    for ordinary_contract::WorkflowResolvedReceiptProduced
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowResolvedReceiptProduced> {
        Ok(ordinary_schema::WorkflowResolvedReceiptProduced {
            workflow_run_resolution: self.run.project_into()?,
            workflow_receipt: self.receipt.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowResolvedReceiptProduced>
    for ordinary_schema::WorkflowResolvedReceiptProduced
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowResolvedReceiptProduced> {
        Ok(ordinary_contract::WorkflowResolvedReceiptProduced {
            run: self.workflow_run_resolution.project_into()?,
            receipt: self.workflow_receipt.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowReceiptProduced>
    for ordinary_contract::WorkflowReceiptProduced
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowReceiptProduced> {
        Ok(ordinary_schema::WorkflowReceiptProduced {
            workflow_run_handle: self.handle.project_into()?,
            workflow_receipt: self.receipt.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowReceiptProduced>
    for ordinary_schema::WorkflowReceiptProduced
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowReceiptProduced> {
        Ok(ordinary_contract::WorkflowReceiptProduced {
            handle: self.workflow_run_handle.project_into()?,
            receipt: self.workflow_receipt.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunLogReported>
    for ordinary_contract::WorkflowRunLogReported
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunLogReported> {
        Ok(ordinary_schema::WorkflowRunLogReported::new(
            self.log.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunLogReported>
    for ordinary_schema::WorkflowRunLogReported
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunLogReported> {
        Ok(ordinary_contract::WorkflowRunLogReported {
            log: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunLog> for ordinary_contract::WorkflowRunLog {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunLog> {
        Ok(ordinary_schema::WorkflowRunLog {
            workflow_run_digest: self.run.project_into()?,
            step_log_vector: self.step_logs.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunLog> for ordinary_schema::WorkflowRunLog {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunLog> {
        Ok(ordinary_contract::WorkflowRunLog {
            run: self.workflow_run_digest.project_into()?,
            step_logs: self.step_log_vector.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::StepLog> for ordinary_contract::StepLog {
    fn project_into(self) -> Result<ordinary_schema::StepLog> {
        Ok(ordinary_schema::StepLog {
            workflow_step_name: self.step.project_into()?,
            model_attestation: self.attestation.project_into()?,
            step_outcome: self.outcome.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::StepLog> for ordinary_schema::StepLog {
    fn project_into(self) -> Result<ordinary_contract::StepLog> {
        Ok(ordinary_contract::StepLog {
            step: self.workflow_step_name.project_into()?,
            attestation: self.model_attestation.project_into()?,
            outcome: self.step_outcome.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ModelAttestation> for ordinary_contract::ModelAttestation {
    fn project_into(self) -> Result<ordinary_schema::ModelAttestation> {
        Ok(ordinary_schema::ModelAttestation {
            provider_name: self.provider.project_into()?,
            model_name: self.model.project_into()?,
            host_name: self.host.project_into()?,
            operation_digest: self.call.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ModelAttestation> for ordinary_schema::ModelAttestation {
    fn project_into(self) -> Result<ordinary_contract::ModelAttestation> {
        Ok(ordinary_contract::ModelAttestation {
            provider: self.provider_name.project_into()?,
            model: self.model_name.project_into()?,
            host: self.host_name.project_into()?,
            call: self.operation_digest.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::StepOutcome> for ordinary_contract::StepOutcome {
    fn project_into(self) -> Result<ordinary_schema::StepOutcome> {
        Ok(match self {
            ordinary_contract::StepOutcome::Produced(decision) => {
                ordinary_schema::StepOutcome::Produced(decision.project_into()?)
            }
            ordinary_contract::StepOutcome::Failed(reason) => {
                ordinary_schema::StepOutcome::Failed(reason.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_contract::StepOutcome> for ordinary_schema::StepOutcome {
    fn project_into(self) -> Result<ordinary_contract::StepOutcome> {
        Ok(match self {
            ordinary_schema::StepOutcome::Produced(decision) => {
                ordinary_contract::StepOutcome::Produced(decision.project_into()?)
            }
            ordinary_schema::StepOutcome::Failed(reason) => {
                ordinary_contract::StepOutcome::Failed(reason.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunSnapshot> for ordinary_contract::WorkflowRunSnapshot {
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunSnapshot> {
        Ok(ordinary_schema::WorkflowRunSnapshot {
            workflow_run_handle: self.handle.project_into()?,
            optional_workflow_run_log: self
                .latest_log
                .map(ProjectInto::project_into)
                .transpose()?,
            optional_workflow_receipt: self.receipt.map(ProjectInto::project_into).transpose()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunSnapshot> for ordinary_schema::WorkflowRunSnapshot {
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunSnapshot> {
        Ok(ordinary_contract::WorkflowRunSnapshot {
            handle: self.workflow_run_handle.project_into()?,
            latest_log: self
                .optional_workflow_run_log
                .map(ProjectInto::project_into)
                .transpose()?,
            receipt: self
                .optional_workflow_receipt
                .map(ProjectInto::project_into)
                .transpose()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunObservationOpened>
    for ordinary_contract::WorkflowRunObservationOpened
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunObservationOpened> {
        Ok(ordinary_schema::WorkflowRunObservationOpened {
            workflow_run_observation_token: self.token.project_into()?,
            workflow_run_snapshot: self.snapshot.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunObservationOpened>
    for ordinary_schema::WorkflowRunObservationOpened
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunObservationOpened> {
        Ok(ordinary_contract::WorkflowRunObservationOpened {
            token: self.workflow_run_observation_token.project_into()?,
            snapshot: self.workflow_run_snapshot.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorkflowRunObservationClosed>
    for ordinary_contract::WorkflowRunObservationClosed
{
    fn project_into(self) -> Result<ordinary_schema::WorkflowRunObservationClosed> {
        Ok(ordinary_schema::WorkflowRunObservationClosed::new(
            self.token.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorkflowRunObservationClosed>
    for ordinary_schema::WorkflowRunObservationClosed
{
    fn project_into(self) -> Result<ordinary_contract::WorkflowRunObservationClosed> {
        Ok(ordinary_contract::WorkflowRunObservationClosed {
            token: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<String> for ordinary_contract::RoleIdentifier {
    fn project_into(self) -> Result<String> {
        Ok(self.as_wire_token().to_string())
    }
}

impl ProjectInto<ordinary_schema::RoleIdentifier> for ordinary_contract::RoleIdentifier {
    fn project_into(self) -> Result<ordinary_schema::RoleIdentifier> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::RoleIdentifier::new(payload))
    }
}

impl ProjectInto<ordinary_contract::RoleIdentifier> for ordinary_schema::RoleIdentifier {
    fn project_into(self) -> Result<ordinary_contract::RoleIdentifier> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_schema::RoleName> for ordinary_contract::RoleIdentifier {
    fn project_into(self) -> Result<ordinary_schema::RoleName> {
        Ok(ordinary_schema::RoleName::new(self.project_into()?))
    }
}

impl ProjectInto<ordinary_contract::RoleIdentifier> for ordinary_schema::RoleName {
    fn project_into(self) -> Result<ordinary_contract::RoleIdentifier> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::RoleIdentifier> for String {
    fn project_into(self) -> Result<ordinary_contract::RoleIdentifier> {
        ordinary_contract::RoleIdentifier::from_wire_token(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::RoleToken {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::RoleToken> for ordinary_contract::RoleToken {
    fn project_into(self) -> Result<ordinary_schema::RoleToken> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::RoleToken::new(payload))
    }
}

impl ProjectInto<ordinary_contract::RoleToken> for ordinary_schema::RoleToken {
    fn project_into(self) -> Result<ordinary_contract::RoleToken> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::RoleToken> for String {
    fn project_into(self) -> Result<ordinary_contract::RoleToken> {
        ordinary_contract::RoleToken::from_text(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::Role> for ordinary_contract::Role {
    fn project_into(self) -> Result<ordinary_schema::Role> {
        Ok(ordinary_schema::Role::new(self.tokens.project_into()?))
    }
}

impl ProjectInto<ordinary_contract::Role> for ordinary_schema::Role {
    fn project_into(self) -> Result<ordinary_contract::Role> {
        ordinary_contract::Role::try_new(self.into_payload().project_into()?)
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::SessionIdentifier {
    fn project_into(self) -> Result<String> {
        Ok(self.as_wire_token().to_string())
    }
}

impl ProjectInto<ordinary_schema::SessionIdentifier> for ordinary_contract::SessionIdentifier {
    fn project_into(self) -> Result<ordinary_schema::SessionIdentifier> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::SessionIdentifier::new(payload))
    }
}

impl ProjectInto<ordinary_contract::SessionIdentifier> for ordinary_schema::SessionIdentifier {
    fn project_into(self) -> Result<ordinary_contract::SessionIdentifier> {
        ordinary_contract::SessionIdentifier::from_camel_case_name(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::LaneDetails {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::LaneDetails> for ordinary_contract::LaneDetails {
    fn project_into(self) -> Result<ordinary_schema::LaneDetails> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::LaneDetails::new(payload))
    }
}

impl ProjectInto<ordinary_contract::LaneDetails> for ordinary_schema::LaneDetails {
    fn project_into(self) -> Result<ordinary_contract::LaneDetails> {
        ordinary_contract::LaneDetails::from_text(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::LaneAuthority> for ordinary_contract::LaneAuthority {
    fn project_into(self) -> Result<ordinary_schema::LaneAuthority> {
        Ok(match self {
            ordinary_contract::LaneAuthority::Structural => {
                ordinary_schema::LaneAuthority::Structural
            }
            ordinary_contract::LaneAuthority::Support => ordinary_schema::LaneAuthority::Support,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneAuthority> for ordinary_schema::LaneAuthority {
    fn project_into(self) -> Result<ordinary_contract::LaneAuthority> {
        Ok(match self {
            ordinary_schema::LaneAuthority::Structural => {
                ordinary_contract::LaneAuthority::Structural
            }
            ordinary_schema::LaneAuthority::Support => ordinary_contract::LaneAuthority::Support,
        })
    }
}

impl ProjectInto<String> for ordinary_contract::LaneIdentifier {
    fn project_into(self) -> Result<String> {
        Ok(self.as_wire_token().to_string())
    }
}

impl ProjectInto<ordinary_schema::LaneIdentifier> for ordinary_contract::LaneIdentifier {
    fn project_into(self) -> Result<ordinary_schema::LaneIdentifier> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::LaneIdentifier::new(payload))
    }
}

impl ProjectInto<ordinary_contract::LaneIdentifier> for ordinary_schema::LaneIdentifier {
    fn project_into(self) -> Result<ordinary_contract::LaneIdentifier> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::LaneIdentifier> for String {
    fn project_into(self) -> Result<ordinary_contract::LaneIdentifier> {
        ordinary_contract::LaneIdentifier::from_wire_token(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::LaneStatus> for ordinary_contract::LaneStatus {
    fn project_into(self) -> Result<ordinary_schema::LaneStatus> {
        Ok(match self {
            ordinary_contract::LaneStatus::Active => ordinary_schema::LaneStatus::Active,
            ordinary_contract::LaneStatus::Released => ordinary_schema::LaneStatus::Released,
            ordinary_contract::LaneStatus::HandoverEnded => {
                ordinary_schema::LaneStatus::HandoverEnded
            }
        })
    }
}

impl ProjectInto<ordinary_contract::LaneStatus> for ordinary_schema::LaneStatus {
    fn project_into(self) -> Result<ordinary_contract::LaneStatus> {
        Ok(match self {
            ordinary_schema::LaneStatus::Active => ordinary_contract::LaneStatus::Active,
            ordinary_schema::LaneStatus::Released => ordinary_contract::LaneStatus::Released,
            ordinary_schema::LaneStatus::HandoverEnded => {
                ordinary_contract::LaneStatus::HandoverEnded
            }
        })
    }
}

impl ProjectInto<ordinary_schema::LaneOwner> for ordinary_contract::LaneOwner {
    fn project_into(self) -> Result<ordinary_schema::LaneOwner> {
        Ok(ordinary_schema::LaneOwner {
            role: self.role.project_into()?,
            lane_authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneOwner> for ordinary_schema::LaneOwner {
    fn project_into(self) -> Result<ordinary_contract::LaneOwner> {
        Ok(ordinary_contract::LaneOwner {
            role: self.role.project_into()?,
            authority: self.lane_authority.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::LaneAssignment> for ordinary_contract::LaneAssignment {
    fn project_into(self) -> Result<ordinary_schema::LaneAssignment> {
        Ok(ordinary_schema::LaneAssignment {
            session_identifier: self.session.project_into()?,
            lane_identifier: self.lane.project_into()?,
            lane_owner: self.owner.project_into()?,
            lane_details: self.details.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneAssignment> for ordinary_schema::LaneAssignment {
    fn project_into(self) -> Result<ordinary_contract::LaneAssignment> {
        Ok(ordinary_contract::LaneAssignment {
            session: self.session_identifier.project_into()?,
            lane: self.lane_identifier.project_into()?,
            owner: self.lane_owner.project_into()?,
            details: self.lane_details.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::LaneRegistration> for ordinary_contract::LaneRegistration {
    fn project_into(self) -> Result<ordinary_schema::LaneRegistration> {
        Ok(ordinary_schema::LaneRegistration {
            lane_assignment: self.assignment.project_into()?,
            timestamp_nanos: self.registered_at.project_into()?,
            lane_status: self.status.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneRegistration> for ordinary_schema::LaneRegistration {
    fn project_into(self) -> Result<ordinary_contract::LaneRegistration> {
        Ok(ordinary_contract::LaneRegistration {
            assignment: self.lane_assignment.project_into()?,
            registered_at: self.timestamp_nanos.project_into()?,
            status: self.lane_status.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::HarnessKind> for ordinary_contract::HarnessKind {
    fn project_into(self) -> Result<ordinary_schema::HarnessKind> {
        Ok(match self {
            ordinary_contract::HarnessKind::Codex => ordinary_schema::HarnessKind::Codex,
            ordinary_contract::HarnessKind::Claude => ordinary_schema::HarnessKind::Claude,
        })
    }
}

impl ProjectInto<ordinary_contract::HarnessKind> for ordinary_schema::HarnessKind {
    fn project_into(self) -> Result<ordinary_contract::HarnessKind> {
        Ok(match self {
            ordinary_schema::HarnessKind::Codex => ordinary_contract::HarnessKind::Codex,
            ordinary_schema::HarnessKind::Claude => ordinary_contract::HarnessKind::Claude,
        })
    }
}

impl ProjectInto<String> for ordinary_contract::WirePath {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::WirePath> for ordinary_contract::WirePath {
    fn project_into(self) -> Result<ordinary_schema::WirePath> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::WirePath::new(payload))
    }
}

impl ProjectInto<ordinary_contract::WirePath> for ordinary_schema::WirePath {
    fn project_into(self) -> Result<ordinary_contract::WirePath> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::WirePath> for String {
    fn project_into(self) -> Result<ordinary_contract::WirePath> {
        ordinary_contract::WirePath::from_absolute_path(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::TaskToken {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::TaskToken> for ordinary_contract::TaskToken {
    fn project_into(self) -> Result<ordinary_schema::TaskToken> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::TaskToken::new(payload))
    }
}

impl ProjectInto<ordinary_contract::TaskToken> for ordinary_schema::TaskToken {
    fn project_into(self) -> Result<ordinary_contract::TaskToken> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::TaskToken> for String {
    fn project_into(self) -> Result<ordinary_contract::TaskToken> {
        ordinary_contract::TaskToken::from_wire_token(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::ScopeReason {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::ScopeReason> for ordinary_contract::ScopeReason {
    fn project_into(self) -> Result<ordinary_schema::ScopeReason> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::ScopeReason::new(payload))
    }
}

impl ProjectInto<ordinary_contract::ScopeReason> for ordinary_schema::ScopeReason {
    fn project_into(self) -> Result<ordinary_contract::ScopeReason> {
        self.into_payload().project_into()
    }
}

impl ProjectInto<ordinary_contract::ScopeReason> for String {
    fn project_into(self) -> Result<ordinary_contract::ScopeReason> {
        ordinary_contract::ScopeReason::from_text(self).map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::ScopeReference> for ordinary_contract::ScopeReference {
    fn project_into(self) -> Result<ordinary_schema::ScopeReference> {
        Ok(match self {
            ordinary_contract::ScopeReference::Path(path) => {
                ordinary_schema::ScopeReference::path(path.project_into()?)
            }
            ordinary_contract::ScopeReference::Task(task) => {
                ordinary_schema::ScopeReference::task(task.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_contract::ScopeReference> for ordinary_schema::ScopeReference {
    fn project_into(self) -> Result<ordinary_contract::ScopeReference> {
        Ok(match self {
            ordinary_schema::ScopeReference::Path(path) => {
                ordinary_contract::ScopeReference::Path(path.project_into()?)
            }
            ordinary_schema::ScopeReference::Task(task) => {
                ordinary_contract::ScopeReference::Task(task.project_into()?)
            }
        })
    }
}

impl ProjectInto<u64> for ordinary_contract::TimestampNanos {
    fn project_into(self) -> Result<u64> {
        Ok(self.value())
    }
}

impl ProjectInto<ordinary_contract::TimestampNanos> for u64 {
    fn project_into(self) -> Result<ordinary_contract::TimestampNanos> {
        Ok(ordinary_contract::TimestampNanos::new(self))
    }
}

impl ProjectInto<ordinary_schema::TimestampNanos> for ordinary_contract::TimestampNanos {
    fn project_into(self) -> Result<ordinary_schema::TimestampNanos> {
        Ok(ordinary_schema::TimestampNanos::new(self.value()))
    }
}

impl ProjectInto<ordinary_contract::TimestampNanos> for ordinary_schema::TimestampNanos {
    fn project_into(self) -> Result<ordinary_contract::TimestampNanos> {
        Ok(ordinary_contract::TimestampNanos::new(self.into_payload()))
    }
}

impl ProjectInto<ordinary_schema::DurationNanos> for ordinary_contract::DurationNanos {
    fn project_into(self) -> Result<ordinary_schema::DurationNanos> {
        Ok(ordinary_schema::DurationNanos::new(self.value()))
    }
}

impl ProjectInto<ordinary_contract::DurationNanos> for ordinary_schema::DurationNanos {
    fn project_into(self) -> Result<ordinary_contract::DurationNanos> {
        Ok(ordinary_contract::DurationNanos::new(self.into_payload()))
    }
}

impl ProjectInto<ordinary_schema::ObservationToken> for ordinary_contract::ObservationToken {
    fn project_into(self) -> Result<ordinary_schema::ObservationToken> {
        Ok(ordinary_schema::ObservationToken::new(self.value()))
    }
}

impl ProjectInto<ordinary_contract::ObservationToken> for ordinary_schema::ObservationToken {
    fn project_into(self) -> Result<ordinary_contract::ObservationToken> {
        Ok(ordinary_contract::ObservationToken::new(
            self.into_payload(),
        ))
    }
}

impl ProjectInto<ordinary_schema::RoleClaim> for ordinary_contract::RoleClaim {
    fn project_into(self) -> Result<ordinary_schema::RoleClaim> {
        Ok(ordinary_schema::RoleClaim {
            role_name: self.role.project_into()?,
            scope_references: self.scopes.project_into()?,
            scope_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleClaim> for ordinary_schema::RoleClaim {
    fn project_into(self) -> Result<ordinary_contract::RoleClaim> {
        Ok(ordinary_contract::RoleClaim {
            role: self.role_name.project_into()?,
            scopes: self.scope_references.project_into()?,
            reason: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleRelease> for ordinary_contract::RoleRelease {
    fn project_into(self) -> Result<ordinary_schema::RoleRelease> {
        Ok(ordinary_schema::RoleRelease::new(self.role.project_into()?))
    }
}

impl ProjectInto<ordinary_contract::RoleRelease> for ordinary_schema::RoleRelease {
    fn project_into(self) -> Result<ordinary_contract::RoleRelease> {
        Ok(ordinary_contract::RoleRelease {
            role: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleHandoff> for ordinary_contract::RoleHandoff {
    fn project_into(self) -> Result<ordinary_schema::RoleHandoff> {
        Ok(ordinary_schema::RoleHandoff {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scope_references: self.scopes.project_into()?,
            scope_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleHandoff> for ordinary_schema::RoleHandoff {
    fn project_into(self) -> Result<ordinary_contract::RoleHandoff> {
        Ok(ordinary_contract::RoleHandoff {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scopes: self.scope_references.project_into()?,
            reason: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::Observation> for ordinary_contract::Observation {
    fn project_into(self) -> Result<ordinary_schema::Observation> {
        Ok(match self {
            ordinary_contract::Observation::Roles => ordinary_schema::Observation::Roles,
            ordinary_contract::Observation::Sessions => ordinary_schema::Observation::Sessions,
            ordinary_contract::Observation::SessionLanes(session) => {
                ordinary_schema::Observation::SessionLanes(session.project_into()?)
            }
            ordinary_contract::Observation::Lanes => ordinary_schema::Observation::Lanes,
            ordinary_contract::Observation::Worktrees => ordinary_schema::Observation::Worktrees,
            ordinary_contract::Observation::Topics => ordinary_schema::Observation::Topics,
            ordinary_contract::Observation::Topic(path) => {
                ordinary_schema::Observation::Topic(path.project_into()?)
            }
            ordinary_contract::Observation::Agents => ordinary_schema::Observation::Agents,
        })
    }
}

impl ProjectInto<ordinary_contract::Observation> for ordinary_schema::Observation {
    fn project_into(self) -> Result<ordinary_contract::Observation> {
        Ok(match self {
            ordinary_schema::Observation::Roles => ordinary_contract::Observation::Roles,
            ordinary_schema::Observation::Sessions => ordinary_contract::Observation::Sessions,
            ordinary_schema::Observation::SessionLanes(session) => {
                ordinary_contract::Observation::SessionLanes(session.project_into()?)
            }
            ordinary_schema::Observation::Lanes => ordinary_contract::Observation::Lanes,
            ordinary_schema::Observation::Worktrees => ordinary_contract::Observation::Worktrees,
            ordinary_schema::Observation::Topics => ordinary_contract::Observation::Topics,
            ordinary_schema::Observation::Topic(path) => {
                ordinary_contract::Observation::Topic(path.project_into()?)
            }
            ordinary_schema::Observation::Agents => ordinary_contract::Observation::Agents,
        })
    }
}

// ─── Worktree newtypes / enums / record (Spirit eh5a) ─────

impl ProjectInto<String> for ordinary_contract::RepositoryName {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::RepositoryName> for ordinary_contract::RepositoryName {
    fn project_into(self) -> Result<ordinary_schema::RepositoryName> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::RepositoryName::new(payload))
    }
}

impl ProjectInto<ordinary_contract::RepositoryName> for ordinary_schema::RepositoryName {
    fn project_into(self) -> Result<ordinary_contract::RepositoryName> {
        ordinary_contract::RepositoryName::from_text(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::BranchName {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::BranchName> for ordinary_contract::BranchName {
    fn project_into(self) -> Result<ordinary_schema::BranchName> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::BranchName::new(payload))
    }
}

impl ProjectInto<ordinary_contract::BranchName> for ordinary_schema::BranchName {
    fn project_into(self) -> Result<ordinary_contract::BranchName> {
        ordinary_contract::BranchName::from_text(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::LaneName {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::LaneName> for ordinary_contract::LaneName {
    fn project_into(self) -> Result<ordinary_schema::LaneName> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::LaneName::new(payload))
    }
}

impl ProjectInto<ordinary_contract::LaneName> for ordinary_schema::LaneName {
    fn project_into(self) -> Result<ordinary_contract::LaneName> {
        ordinary_contract::LaneName::from_text(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<String> for ordinary_contract::PurposeText {
    fn project_into(self) -> Result<String> {
        Ok(self.as_str().to_string())
    }
}

impl ProjectInto<ordinary_schema::PurposeText> for ordinary_contract::PurposeText {
    fn project_into(self) -> Result<ordinary_schema::PurposeText> {
        let payload: String = self.project_into()?;
        Ok(ordinary_schema::PurposeText::new(payload))
    }
}

impl ProjectInto<ordinary_contract::PurposeText> for ordinary_schema::PurposeText {
    fn project_into(self) -> Result<ordinary_contract::PurposeText> {
        ordinary_contract::PurposeText::from_text(self.into_payload())
            .map_err(Error::SignalOrchestrate)
    }
}

impl ProjectInto<ordinary_schema::WorktreeStatus> for ordinary_contract::WorktreeStatus {
    fn project_into(self) -> Result<ordinary_schema::WorktreeStatus> {
        Ok(match self {
            ordinary_contract::WorktreeStatus::Active => ordinary_schema::WorktreeStatus::Active,
            ordinary_contract::WorktreeStatus::Merged => ordinary_schema::WorktreeStatus::Merged,
            ordinary_contract::WorktreeStatus::Archived => {
                ordinary_schema::WorktreeStatus::Archived
            }
            ordinary_contract::WorktreeStatus::Recycled => {
                ordinary_schema::WorktreeStatus::Recycled
            }
            ordinary_contract::WorktreeStatus::Abandoned => {
                ordinary_schema::WorktreeStatus::Abandoned
            }
        })
    }
}

impl ProjectInto<ordinary_contract::WorktreeStatus> for ordinary_schema::WorktreeStatus {
    fn project_into(self) -> Result<ordinary_contract::WorktreeStatus> {
        Ok(match self {
            ordinary_schema::WorktreeStatus::Active => ordinary_contract::WorktreeStatus::Active,
            ordinary_schema::WorktreeStatus::Merged => ordinary_contract::WorktreeStatus::Merged,
            ordinary_schema::WorktreeStatus::Archived => {
                ordinary_contract::WorktreeStatus::Archived
            }
            ordinary_schema::WorktreeStatus::Recycled => {
                ordinary_contract::WorktreeStatus::Recycled
            }
            ordinary_schema::WorktreeStatus::Abandoned => {
                ordinary_contract::WorktreeStatus::Abandoned
            }
        })
    }
}

impl ProjectInto<ordinary_schema::PushedState> for ordinary_contract::PushedState {
    fn project_into(self) -> Result<ordinary_schema::PushedState> {
        Ok(match self {
            ordinary_contract::PushedState::Unpushed => ordinary_schema::PushedState::Unpushed,
            ordinary_contract::PushedState::Pushed => ordinary_schema::PushedState::Pushed,
            ordinary_contract::PushedState::AncestorOfMain => {
                ordinary_schema::PushedState::AncestorOfMain
            }
        })
    }
}

impl ProjectInto<ordinary_contract::PushedState> for ordinary_schema::PushedState {
    fn project_into(self) -> Result<ordinary_contract::PushedState> {
        Ok(match self {
            ordinary_schema::PushedState::Unpushed => ordinary_contract::PushedState::Unpushed,
            ordinary_schema::PushedState::Pushed => ordinary_contract::PushedState::Pushed,
            ordinary_schema::PushedState::AncestorOfMain => {
                ordinary_contract::PushedState::AncestorOfMain
            }
        })
    }
}

impl ProjectInto<ordinary_schema::Worktree> for ordinary_contract::Worktree {
    fn project_into(self) -> Result<ordinary_schema::Worktree> {
        Ok(ordinary_schema::Worktree {
            repository_name: self.repository.project_into()?,
            branch_name: self.branch.project_into()?,
            wire_path: self.path.project_into()?,
            lane_name: self.owning_lane.project_into()?,
            worktree_status: self.status.project_into()?,
            purpose_text: self.purpose.project_into()?,
            timestamp_nanos: self.last_activity.project_into()?,
            pushed_state: self.pushed_state.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::Worktree> for ordinary_schema::Worktree {
    fn project_into(self) -> Result<ordinary_contract::Worktree> {
        Ok(ordinary_contract::Worktree {
            repository: self.repository_name.project_into()?,
            branch: self.branch_name.project_into()?,
            path: self.wire_path.project_into()?,
            owning_lane: self.lane_name.project_into()?,
            status: self.worktree_status.project_into()?,
            purpose: self.purpose_text.project_into()?,
            last_activity: self.timestamp_nanos.project_into()?,
            pushed_state: self.pushed_state.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreeConclusion> for ordinary_contract::WorktreeConclusion {
    fn project_into(self) -> Result<ordinary_schema::WorktreeConclusion> {
        Ok(match self {
            ordinary_contract::WorktreeConclusion::Merged => {
                ordinary_schema::WorktreeConclusion::Merged
            }
            ordinary_contract::WorktreeConclusion::Rejected => {
                ordinary_schema::WorktreeConclusion::Rejected
            }
        })
    }
}

impl ProjectInto<ordinary_contract::WorktreeConclusion> for ordinary_schema::WorktreeConclusion {
    fn project_into(self) -> Result<ordinary_contract::WorktreeConclusion> {
        Ok(match self {
            ordinary_schema::WorktreeConclusion::Merged => {
                ordinary_contract::WorktreeConclusion::Merged
            }
            ordinary_schema::WorktreeConclusion::Rejected => {
                ordinary_contract::WorktreeConclusion::Rejected
            }
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreeRequest> for ordinary_contract::WorktreeRequest {
    fn project_into(self) -> Result<ordinary_schema::WorktreeRequest> {
        Ok(ordinary_schema::WorktreeRequest {
            repository_name: self.repository.project_into()?,
            branch_name: self.branch.project_into()?,
            lane_name: self.owning_lane.project_into()?,
            purpose_text: self.purpose.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorktreeRequest> for ordinary_schema::WorktreeRequest {
    fn project_into(self) -> Result<ordinary_contract::WorktreeRequest> {
        Ok(ordinary_contract::WorktreeRequest {
            repository: self.repository_name.project_into()?,
            branch: self.branch_name.project_into()?,
            owning_lane: self.lane_name.project_into()?,
            purpose: self.purpose_text.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreeConclusionRequest>
    for ordinary_contract::WorktreeConclusionRequest
{
    fn project_into(self) -> Result<ordinary_schema::WorktreeConclusionRequest> {
        Ok(ordinary_schema::WorktreeConclusionRequest {
            lane_name: self.owning_lane.project_into()?,
            worktree_conclusion: self.disposition.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::WorktreeConclusionRequest>
    for ordinary_schema::WorktreeConclusionRequest
{
    fn project_into(self) -> Result<ordinary_contract::WorktreeConclusionRequest> {
        Ok(ordinary_contract::WorktreeConclusionRequest {
            owning_lane: self.lane_name.project_into()?,
            disposition: self.worktree_conclusion.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreeRequestRejection>
    for ordinary_contract::WorktreeRequestRejection
{
    fn project_into(self) -> Result<ordinary_schema::WorktreeRequestRejection> {
        Ok(match self {
            ordinary_contract::WorktreeRequestRejection::RepositoryNotFound => {
                ordinary_schema::WorktreeRequestRejection::RepositoryNotFound
            }
            ordinary_contract::WorktreeRequestRejection::WorktreeAlreadyExists => {
                ordinary_schema::WorktreeRequestRejection::WorktreeAlreadyExists
            }
        })
    }
}

impl ProjectInto<ordinary_contract::WorktreeRequestRejection>
    for ordinary_schema::WorktreeRequestRejection
{
    fn project_into(self) -> Result<ordinary_contract::WorktreeRequestRejection> {
        Ok(match self {
            ordinary_schema::WorktreeRequestRejection::RepositoryNotFound => {
                ordinary_contract::WorktreeRequestRejection::RepositoryNotFound
            }
            ordinary_schema::WorktreeRequestRejection::WorktreeAlreadyExists => {
                ordinary_contract::WorktreeRequestRejection::WorktreeAlreadyExists
            }
        })
    }
}

impl ProjectInto<ordinary_schema::TeardownRefusal> for ordinary_contract::TeardownRefusal {
    fn project_into(self) -> Result<ordinary_schema::TeardownRefusal> {
        Ok(match self {
            ordinary_contract::TeardownRefusal::UnmergedWorkPresent => {
                ordinary_schema::TeardownRefusal::UnmergedWorkPresent
            }
        })
    }
}

impl ProjectInto<ordinary_contract::TeardownRefusal> for ordinary_schema::TeardownRefusal {
    fn project_into(self) -> Result<ordinary_contract::TeardownRefusal> {
        Ok(match self {
            ordinary_schema::TeardownRefusal::UnmergedWorkPresent => {
                ordinary_contract::TeardownRefusal::UnmergedWorkPresent
            }
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreeTeardownRefused>
    for ordinary_contract::WorktreeTeardownRefused
{
    fn project_into(self) -> Result<ordinary_schema::WorktreeTeardownRefused> {
        Ok(ordinary_schema::WorktreeTeardownRefused {
            worktree: self.worktree.project_into()?,
            teardown_refusal: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::WorktreesObserved> for ordinary_contract::WorktreesObserved {
    fn project_into(self) -> Result<ordinary_schema::WorktreesObserved> {
        Ok(ordinary_schema::WorktreesObserved::new(
            self.worktrees.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::WorktreesObserved> for ordinary_schema::WorktreesObserved {
    fn project_into(self) -> Result<ordinary_contract::WorktreesObserved> {
        Ok(ordinary_contract::WorktreesObserved {
            worktrees: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivitySubmission> for ordinary_contract::ActivitySubmission {
    fn project_into(self) -> Result<ordinary_schema::ActivitySubmission> {
        Ok(ordinary_schema::ActivitySubmission {
            role_name: self.role.project_into()?,
            scope_reference: self.scope.project_into()?,
            scope_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ActivitySubmission> for ordinary_schema::ActivitySubmission {
    fn project_into(self) -> Result<ordinary_contract::ActivitySubmission> {
        Ok(ordinary_contract::ActivitySubmission {
            role: self.role_name.project_into()?,
            scope: self.scope_reference.project_into()?,
            reason: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivityQuery> for ordinary_contract::ActivityQuery {
    fn project_into(self) -> Result<ordinary_schema::ActivityQuery> {
        Ok(ordinary_schema::ActivityQuery {
            integer: u64::from(self.limit),
            activity_filters: self.filters.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ActivityQuery> for ordinary_schema::ActivityQuery {
    fn project_into(self) -> Result<ordinary_contract::ActivityQuery> {
        Ok(ordinary_contract::ActivityQuery {
            limit: u32::try_from(self.integer).map_err(|error| Error::SchemaBridge {
                message: format!("activity query limit does not fit u32: {error}"),
            })?,
            filters: self.activity_filters.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivityFilter> for ordinary_contract::ActivityFilter {
    fn project_into(self) -> Result<ordinary_schema::ActivityFilter> {
        Ok(match self {
            ordinary_contract::ActivityFilter::RoleFilter(role) => {
                ordinary_schema::ActivityFilter::role_filter(role.project_into()?)
            }
            ordinary_contract::ActivityFilter::PathPrefix(path) => {
                ordinary_schema::ActivityFilter::path_prefix(path.project_into()?)
            }
            ordinary_contract::ActivityFilter::TaskToken(task) => {
                ordinary_schema::ActivityFilter::task_token(task.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_contract::ActivityFilter> for ordinary_schema::ActivityFilter {
    fn project_into(self) -> Result<ordinary_contract::ActivityFilter> {
        Ok(match self {
            ordinary_schema::ActivityFilter::RoleFilter(role) => {
                ordinary_contract::ActivityFilter::RoleFilter(role.project_into()?)
            }
            ordinary_schema::ActivityFilter::PathPrefix(path) => {
                ordinary_contract::ActivityFilter::PathPrefix(path.project_into()?)
            }
            ordinary_schema::ActivityFilter::TaskToken(task) => {
                ordinary_contract::ActivityFilter::TaskToken(task.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ObservationSubscription>
    for ordinary_contract::ObservationSubscription
{
    fn project_into(self) -> Result<ordinary_schema::ObservationSubscription> {
        Ok(ordinary_schema::ObservationSubscription {
            include_operations: self.include_operations,
            include_effects: self.include_effects,
        })
    }
}

impl ProjectInto<ordinary_contract::ObservationSubscription>
    for ordinary_schema::ObservationSubscription
{
    fn project_into(self) -> Result<ordinary_contract::ObservationSubscription> {
        Ok(ordinary_contract::ObservationSubscription {
            include_operations: self.include_operations,
            include_effects: self.include_effects,
        })
    }
}

impl ProjectInto<ordinary_schema::Input> for ordinary_contract::OrchestrateRequest {
    fn project_into(self) -> Result<ordinary_schema::Input> {
        Ok(match self {
            ordinary_contract::OrchestrateRequest::Claim(payload) => {
                ordinary_schema::Input::claim(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Release(payload) => {
                ordinary_schema::Input::Release(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Handoff(payload) => {
                ordinary_schema::Input::handoff(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Observe(payload) => {
                ordinary_schema::Input::observe(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Submit(payload) => {
                ordinary_schema::Input::submit(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Query(payload) => {
                ordinary_schema::Input::query(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::RunWorkflow(payload) => {
                ordinary_schema::Input::run_workflow(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::RunResolvedWorkflow(payload) => {
                ordinary_schema::Input::run_resolved_workflow(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::ObserveWorkflowRun(payload) => {
                ordinary_schema::Input::observe_workflow_run(payload.run.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::WorkflowRunObservationRetraction(payload) => {
                ordinary_schema::Input::workflow_run_observation_retraction(
                    payload.run.project_into()?,
                )
            }
            ordinary_contract::OrchestrateRequest::Watch(payload) => {
                ordinary_schema::Input::watch(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Unwatch(payload) => {
                ordinary_schema::Input::unwatch(payload.value())
            }
            ordinary_contract::OrchestrateRequest::RegisterAgent(payload) => {
                ordinary_schema::Input::register_agent(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::RequestWorktree(payload) => {
                ordinary_schema::Input::request_worktree(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::ConcludeWorktree(payload) => {
                ordinary_schema::Input::conclude_worktree(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestrateRequest> for ordinary_schema::Input {
    fn project_into(self) -> Result<ordinary_contract::OrchestrateRequest> {
        Ok(match self {
            ordinary_schema::Input::Claim(payload) => {
                ordinary_contract::OrchestrateRequest::Claim(payload.project_into()?)
            }
            ordinary_schema::Input::Release(payload) => {
                ordinary_contract::OrchestrateRequest::Release(payload.project_into()?)
            }
            ordinary_schema::Input::Handoff(payload) => {
                ordinary_contract::OrchestrateRequest::Handoff(payload.project_into()?)
            }
            ordinary_schema::Input::Observe(payload) => {
                ordinary_contract::OrchestrateRequest::Observe(payload.project_into()?)
            }
            ordinary_schema::Input::Submit(payload) => {
                ordinary_contract::OrchestrateRequest::Submit(payload.project_into()?)
            }
            ordinary_schema::Input::Query(payload) => {
                ordinary_contract::OrchestrateRequest::Query(payload.project_into()?)
            }
            ordinary_schema::Input::RunWorkflow(payload) => {
                ordinary_contract::OrchestrateRequest::RunWorkflow(payload.project_into()?)
            }
            ordinary_schema::Input::RunResolvedWorkflow(payload) => {
                ordinary_contract::OrchestrateRequest::RunResolvedWorkflow(payload.project_into()?)
            }
            ordinary_schema::Input::ObserveWorkflowRun(payload) => {
                ordinary_contract::OrchestrateRequest::ObserveWorkflowRun(payload.project_into()?)
            }
            ordinary_schema::Input::WorkflowRunObservationRetraction(payload) => {
                ordinary_contract::OrchestrateRequest::WorkflowRunObservationRetraction(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Input::Watch(payload) => {
                ordinary_contract::OrchestrateRequest::Watch(payload.project_into()?)
            }
            ordinary_schema::Input::Unwatch(payload) => {
                ordinary_contract::OrchestrateRequest::Unwatch(payload.project_into()?)
            }
            ordinary_schema::Input::RegisterAgent(payload) => {
                ordinary_contract::OrchestrateRequest::RegisterAgent(payload.project_into()?)
            }
            ordinary_schema::Input::RequestWorktree(payload) => {
                ordinary_contract::OrchestrateRequest::RequestWorktree(payload.project_into()?)
            }
            ordinary_schema::Input::ConcludeWorktree(payload) => {
                ordinary_contract::OrchestrateRequest::ConcludeWorktree(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimAcceptance> for ordinary_contract::ClaimAcceptance {
    fn project_into(self) -> Result<ordinary_schema::ClaimAcceptance> {
        Ok(ordinary_schema::ClaimAcceptance {
            role_name: self.role.project_into()?,
            scope_references: self.scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimAcceptance> for ordinary_schema::ClaimAcceptance {
    fn project_into(self) -> Result<ordinary_contract::ClaimAcceptance> {
        Ok(ordinary_contract::ClaimAcceptance {
            role: self.role_name.project_into()?,
            scopes: self.scope_references.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ScopeConflict> for ordinary_contract::ScopeConflict {
    fn project_into(self) -> Result<ordinary_schema::ScopeConflict> {
        Ok(ordinary_schema::ScopeConflict {
            scope_reference: self.scope.project_into()?,
            role_name: self.held_by.project_into()?,
            scope_reason: self.held_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ScopeConflict> for ordinary_schema::ScopeConflict {
    fn project_into(self) -> Result<ordinary_contract::ScopeConflict> {
        Ok(ordinary_contract::ScopeConflict {
            scope: self.scope_reference.project_into()?,
            held_by: self.role_name.project_into()?,
            held_reason: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimRejection> for ordinary_contract::ClaimRejection {
    fn project_into(self) -> Result<ordinary_schema::ClaimRejection> {
        Ok(ordinary_schema::ClaimRejection {
            role_name: self.role.project_into()?,
            scope_conflicts: self.conflicts.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimRejection> for ordinary_schema::ClaimRejection {
    fn project_into(self) -> Result<ordinary_contract::ClaimRejection> {
        Ok(ordinary_contract::ClaimRejection {
            role: self.role_name.project_into()?,
            conflicts: self.scope_conflicts.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ReleaseAcknowledgment>
    for ordinary_contract::ReleaseAcknowledgment
{
    fn project_into(self) -> Result<ordinary_schema::ReleaseAcknowledgment> {
        Ok(ordinary_schema::ReleaseAcknowledgment {
            role_name: self.role.project_into()?,
            scope_references: self.released_scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ReleaseAcknowledgment>
    for ordinary_schema::ReleaseAcknowledgment
{
    fn project_into(self) -> Result<ordinary_contract::ReleaseAcknowledgment> {
        Ok(ordinary_contract::ReleaseAcknowledgment {
            role: self.role_name.project_into()?,
            released_scopes: self.scope_references.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::HandoffAcceptance> for ordinary_contract::HandoffAcceptance {
    fn project_into(self) -> Result<ordinary_schema::HandoffAcceptance> {
        Ok(ordinary_schema::HandoffAcceptance {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scope_references: self.scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::HandoffAcceptance> for ordinary_schema::HandoffAcceptance {
    fn project_into(self) -> Result<ordinary_contract::HandoffAcceptance> {
        Ok(ordinary_contract::HandoffAcceptance {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scopes: self.scope_references.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::HandoffRejectionReason>
    for ordinary_contract::HandoffRejectionReason
{
    fn project_into(self) -> Result<ordinary_schema::HandoffRejectionReason> {
        Ok(match self {
            ordinary_contract::HandoffRejectionReason::SourceRoleDoesNotHold => {
                ordinary_schema::HandoffRejectionReason::SourceRoleDoesNotHold
            }
            ordinary_contract::HandoffRejectionReason::TargetRoleConflict(conflicts) => {
                ordinary_schema::HandoffRejectionReason::target_role_conflict(
                    conflicts.project_into()?,
                )
            }
        })
    }
}

impl ProjectInto<ordinary_contract::HandoffRejectionReason>
    for ordinary_schema::HandoffRejectionReason
{
    fn project_into(self) -> Result<ordinary_contract::HandoffRejectionReason> {
        Ok(match self {
            ordinary_schema::HandoffRejectionReason::SourceRoleDoesNotHold => {
                ordinary_contract::HandoffRejectionReason::SourceRoleDoesNotHold
            }
            ordinary_schema::HandoffRejectionReason::TargetRoleConflict(conflicts) => {
                ordinary_contract::HandoffRejectionReason::TargetRoleConflict(
                    conflicts.project_into()?,
                )
            }
        })
    }
}

impl ProjectInto<ordinary_schema::HandoffRejection> for ordinary_contract::HandoffRejection {
    fn project_into(self) -> Result<ordinary_schema::HandoffRejection> {
        Ok(ordinary_schema::HandoffRejection {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            handoff_rejection_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::HandoffRejection> for ordinary_schema::HandoffRejection {
    fn project_into(self) -> Result<ordinary_contract::HandoffRejection> {
        Ok(ordinary_contract::HandoffRejection {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            reason: self.handoff_rejection_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimEntry> for ordinary_contract::ClaimEntry {
    fn project_into(self) -> Result<ordinary_schema::ClaimEntry> {
        Ok(ordinary_schema::ClaimEntry {
            scope_reference: self.scope.project_into()?,
            scope_reason: self.reason.project_into()?,
            timestamp_nanos: self.claimed_at.project_into()?,
            duration_nanos: self.age.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimEntry> for ordinary_schema::ClaimEntry {
    fn project_into(self) -> Result<ordinary_contract::ClaimEntry> {
        Ok(ordinary_contract::ClaimEntry {
            scope: self.scope_reference.project_into()?,
            reason: self.scope_reason.project_into()?,
            claimed_at: self.timestamp_nanos.project_into()?,
            age: self.duration_nanos.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::Activity> for ordinary_contract::Activity {
    fn project_into(self) -> Result<ordinary_schema::Activity> {
        Ok(ordinary_schema::Activity {
            role_name: self.role.project_into()?,
            scope_reference: self.scope.project_into()?,
            scope_reason: self.reason.project_into()?,
            timestamp_nanos: self.stamped_at.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::Activity> for ordinary_schema::Activity {
    fn project_into(self) -> Result<ordinary_contract::Activity> {
        Ok(ordinary_contract::Activity {
            role: self.role_name.project_into()?,
            scope: self.scope_reference.project_into()?,
            reason: self.scope_reason.project_into()?,
            stamped_at: self.timestamp_nanos.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleStatus> for ordinary_contract::RoleStatus {
    fn project_into(self) -> Result<ordinary_schema::RoleStatus> {
        Ok(ordinary_schema::RoleStatus {
            role_name: self.role.project_into()?,
            harness_kind: self.harness.project_into()?,
            claim_entries: self.claims.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleStatus> for ordinary_schema::RoleStatus {
    fn project_into(self) -> Result<ordinary_contract::RoleStatus> {
        Ok(ordinary_contract::RoleStatus {
            role: self.role_name.project_into()?,
            harness: self.harness_kind.project_into()?,
            claims: self.claim_entries.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleSnapshot> for ordinary_contract::RoleSnapshot {
    fn project_into(self) -> Result<ordinary_schema::RoleSnapshot> {
        Ok(ordinary_schema::RoleSnapshot {
            role_statuses: self.roles.project_into()?,
            activities: self.recent_activity.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleSnapshot> for ordinary_schema::RoleSnapshot {
    fn project_into(self) -> Result<ordinary_contract::RoleSnapshot> {
        Ok(ordinary_contract::RoleSnapshot {
            roles: self.role_statuses.project_into()?,
            recent_activity: self.activities.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::SessionProjection> for ordinary_contract::SessionProjection {
    fn project_into(self) -> Result<ordinary_schema::SessionProjection> {
        Ok(ordinary_schema::SessionProjection {
            session_identifier: self.session.project_into()?,
            integer: self.active_lanes,
        })
    }
}

impl ProjectInto<ordinary_contract::SessionProjection> for ordinary_schema::SessionProjection {
    fn project_into(self) -> Result<ordinary_contract::SessionProjection> {
        Ok(ordinary_contract::SessionProjection {
            session: self.session_identifier.project_into()?,
            active_lanes: self.integer,
        })
    }
}

impl ProjectInto<ordinary_schema::SessionsObserved> for ordinary_contract::SessionsObserved {
    fn project_into(self) -> Result<ordinary_schema::SessionsObserved> {
        Ok(ordinary_schema::SessionsObserved::new(
            self.sessions.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::SessionsObserved> for ordinary_schema::SessionsObserved {
    fn project_into(self) -> Result<ordinary_contract::SessionsObserved> {
        Ok(ordinary_contract::SessionsObserved {
            sessions: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::LaneResourceClaim> for ordinary_contract::LaneResourceClaim {
    fn project_into(self) -> Result<ordinary_schema::LaneResourceClaim> {
        Ok(ordinary_schema::LaneResourceClaim {
            scope_reference: self.scope.project_into()?,
            scope_reason: self.reason.project_into()?,
            timestamp_nanos: self.claimed_at.project_into()?,
            duration_nanos: self.age.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneResourceClaim> for ordinary_schema::LaneResourceClaim {
    fn project_into(self) -> Result<ordinary_contract::LaneResourceClaim> {
        Ok(ordinary_contract::LaneResourceClaim {
            scope: self.scope_reference.project_into()?,
            reason: self.scope_reason.project_into()?,
            claimed_at: self.timestamp_nanos.project_into()?,
            age: self.duration_nanos.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::LaneProjection> for ordinary_contract::LaneProjection {
    fn project_into(self) -> Result<ordinary_schema::LaneProjection> {
        Ok(ordinary_schema::LaneProjection {
            lane_registration: self.registration.project_into()?,
            lane_resource_claims: self.resource_claims.project_into()?,
            timestamp_nanos: self.observed_at.project_into()?,
            duration_nanos: self.age.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneProjection> for ordinary_schema::LaneProjection {
    fn project_into(self) -> Result<ordinary_contract::LaneProjection> {
        Ok(ordinary_contract::LaneProjection {
            registration: self.lane_registration.project_into()?,
            resource_claims: self.lane_resource_claims.project_into()?,
            observed_at: self.timestamp_nanos.project_into()?,
            age: self.duration_nanos.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::LanesObserved> for ordinary_contract::LanesObserved {
    fn project_into(self) -> Result<ordinary_schema::LanesObserved> {
        Ok(ordinary_schema::LanesObserved::new(
            self.lanes.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::LanesObserved> for ordinary_schema::LanesObserved {
    fn project_into(self) -> Result<ordinary_contract::LanesObserved> {
        Ok(ordinary_contract::LanesObserved {
            lanes: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivityAcknowledgment>
    for ordinary_contract::ActivityAcknowledgment
{
    fn project_into(self) -> Result<ordinary_schema::ActivityAcknowledgment> {
        Ok(ordinary_schema::ActivityAcknowledgment::new(self.slot))
    }
}

impl ProjectInto<ordinary_contract::ActivityAcknowledgment>
    for ordinary_schema::ActivityAcknowledgment
{
    fn project_into(self) -> Result<ordinary_contract::ActivityAcknowledgment> {
        Ok(ordinary_contract::ActivityAcknowledgment {
            slot: self.into_payload(),
        })
    }
}

impl ProjectInto<ordinary_schema::ActivityList> for ordinary_contract::ActivityList {
    fn project_into(self) -> Result<ordinary_schema::ActivityList> {
        Ok(ordinary_schema::ActivityList::new(
            self.records.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::ActivityList> for ordinary_schema::ActivityList {
    fn project_into(self) -> Result<ordinary_contract::ActivityList> {
        Ok(ordinary_contract::ActivityList {
            records: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::DownstreamComponent> for ordinary_contract::DownstreamComponent {
    fn project_into(self) -> Result<ordinary_schema::DownstreamComponent> {
        Ok(match self {
            ordinary_contract::DownstreamComponent::Router => {
                ordinary_schema::DownstreamComponent::Router
            }
            ordinary_contract::DownstreamComponent::Harness => {
                ordinary_schema::DownstreamComponent::Harness
            }
            ordinary_contract::DownstreamComponent::Terminal => {
                ordinary_schema::DownstreamComponent::Terminal
            }
            ordinary_contract::DownstreamComponent::Message => {
                ordinary_schema::DownstreamComponent::Message
            }
            ordinary_contract::DownstreamComponent::Mind => {
                ordinary_schema::DownstreamComponent::Mind
            }
            ordinary_contract::DownstreamComponent::System => {
                ordinary_schema::DownstreamComponent::System
            }
            ordinary_contract::DownstreamComponent::Introspect => {
                ordinary_schema::DownstreamComponent::Introspect
            }
        })
    }
}

impl ProjectInto<ordinary_contract::DownstreamComponent> for ordinary_schema::DownstreamComponent {
    fn project_into(self) -> Result<ordinary_contract::DownstreamComponent> {
        Ok(match self {
            ordinary_schema::DownstreamComponent::Router => {
                ordinary_contract::DownstreamComponent::Router
            }
            ordinary_schema::DownstreamComponent::Harness => {
                ordinary_contract::DownstreamComponent::Harness
            }
            ordinary_schema::DownstreamComponent::Terminal => {
                ordinary_contract::DownstreamComponent::Terminal
            }
            ordinary_schema::DownstreamComponent::Message => {
                ordinary_contract::DownstreamComponent::Message
            }
            ordinary_schema::DownstreamComponent::Mind => {
                ordinary_contract::DownstreamComponent::Mind
            }
            ordinary_schema::DownstreamComponent::System => {
                ordinary_contract::DownstreamComponent::System
            }
            ordinary_schema::DownstreamComponent::Introspect => {
                ordinary_contract::DownstreamComponent::Introspect
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ApplicationFailureReason>
    for ordinary_contract::ApplicationFailureReason
{
    fn project_into(self) -> Result<ordinary_schema::ApplicationFailureReason> {
        Ok(match self {
            ordinary_contract::ApplicationFailureReason::Unreachable => {
                ordinary_schema::ApplicationFailureReason::Unreachable
            }
            ordinary_contract::ApplicationFailureReason::Rejected => {
                ordinary_schema::ApplicationFailureReason::Rejected
            }
            ordinary_contract::ApplicationFailureReason::Unimplemented => {
                ordinary_schema::ApplicationFailureReason::Unimplemented
            }
            ordinary_contract::ApplicationFailureReason::TimedOut => {
                ordinary_schema::ApplicationFailureReason::TimedOut
            }
            ordinary_contract::ApplicationFailureReason::Unknown => {
                ordinary_schema::ApplicationFailureReason::Unknown
            }
        })
    }
}

impl ProjectInto<ordinary_contract::ApplicationFailureReason>
    for ordinary_schema::ApplicationFailureReason
{
    fn project_into(self) -> Result<ordinary_contract::ApplicationFailureReason> {
        Ok(match self {
            ordinary_schema::ApplicationFailureReason::Unreachable => {
                ordinary_contract::ApplicationFailureReason::Unreachable
            }
            ordinary_schema::ApplicationFailureReason::Rejected => {
                ordinary_contract::ApplicationFailureReason::Rejected
            }
            ordinary_schema::ApplicationFailureReason::Unimplemented => {
                ordinary_contract::ApplicationFailureReason::Unimplemented
            }
            ordinary_schema::ApplicationFailureReason::TimedOut => {
                ordinary_contract::ApplicationFailureReason::TimedOut
            }
            ordinary_schema::ApplicationFailureReason::Unknown => {
                ordinary_contract::ApplicationFailureReason::Unknown
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ApplicationSuccess> for ordinary_contract::ApplicationSuccess {
    fn project_into(self) -> Result<ordinary_schema::ApplicationSuccess> {
        Ok(ordinary_schema::ApplicationSuccess {
            downstream_component: self.component.project_into()?,
            scope_reason: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ApplicationSuccess> for ordinary_schema::ApplicationSuccess {
    fn project_into(self) -> Result<ordinary_contract::ApplicationSuccess> {
        Ok(ordinary_contract::ApplicationSuccess {
            component: self.downstream_component.project_into()?,
            detail: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ApplicationFailure> for ordinary_contract::ApplicationFailure {
    fn project_into(self) -> Result<ordinary_schema::ApplicationFailure> {
        Ok(ordinary_schema::ApplicationFailure {
            downstream_component: self.component.project_into()?,
            application_failure_reason: self.reason.project_into()?,
            scope_reason: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ApplicationFailure> for ordinary_schema::ApplicationFailure {
    fn project_into(self) -> Result<ordinary_contract::ApplicationFailure> {
        Ok(ordinary_contract::ApplicationFailure {
            component: self.downstream_component.project_into()?,
            reason: self.application_failure_reason.project_into()?,
            detail: self.scope_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::PartialApplied> for ordinary_contract::PartialApplied {
    fn project_into(self) -> Result<ordinary_schema::PartialApplied> {
        Ok(ordinary_schema::PartialApplied {
            application_successes: self.succeeded.project_into()?,
            application_failures: self.failed.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::PartialApplied> for ordinary_schema::PartialApplied {
    fn project_into(self) -> Result<ordinary_contract::PartialApplied> {
        Ok(ordinary_contract::PartialApplied {
            succeeded: self.application_successes.project_into()?,
            failed: self.application_failures.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ObservationOpened> for ordinary_contract::ObservationOpened {
    fn project_into(self) -> Result<ordinary_schema::ObservationOpened> {
        Ok(ordinary_schema::ObservationOpened::new(
            self.token.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::ObservationOpened> for ordinary_schema::ObservationOpened {
    fn project_into(self) -> Result<ordinary_contract::ObservationOpened> {
        Ok(ordinary_contract::ObservationOpened {
            token: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ObservationClosed> for ordinary_contract::ObservationClosed {
    fn project_into(self) -> Result<ordinary_schema::ObservationClosed> {
        Ok(ordinary_schema::ObservationClosed::new(
            self.token.project_into()?,
        ))
    }
}

impl ProjectInto<ordinary_contract::ObservationClosed> for ordinary_schema::ObservationClosed {
    fn project_into(self) -> Result<ordinary_contract::ObservationClosed> {
        Ok(ordinary_contract::ObservationClosed {
            token: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::Output> for ordinary_contract::OrchestrateReply {
    fn project_into(self) -> Result<ordinary_schema::Output> {
        Ok(match self {
            ordinary_contract::OrchestrateReply::ClaimAcceptance(payload) => {
                ordinary_schema::Output::claim_acceptance(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ClaimRejection(payload) => {
                ordinary_schema::Output::claim_rejection(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ReleaseAcknowledgment(payload) => {
                ordinary_schema::Output::release_acknowledgment(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::HandoffAcceptance(payload) => {
                ordinary_schema::Output::handoff_acceptance(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::HandoffRejection(payload) => {
                ordinary_schema::Output::handoff_rejection(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::RoleSnapshot(payload) => {
                ordinary_schema::Output::role_snapshot(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::SessionsObserved(payload) => {
                ordinary_schema::Output::SessionsObserved(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::LanesObserved(payload) => {
                ordinary_schema::Output::LanesObserved(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorktreesObserved(payload) => {
                ordinary_schema::Output::WorktreesObserved(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ActivityAcknowledgment(payload) => {
                ordinary_schema::Output::ActivityAcknowledgment(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ActivityList(payload) => {
                ordinary_schema::Output::ActivityList(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowRunAccepted(payload) => {
                ordinary_schema::Output::workflow_run_accepted(payload.handle.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowResolutionAccepted(payload) => {
                ordinary_schema::Output::workflow_resolution_accepted(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowResolutionUnavailable(payload) => {
                ordinary_schema::Output::workflow_resolution_unavailable(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowReceiptProduced(payload) => {
                ordinary_schema::Output::workflow_receipt_produced(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowResolvedReceiptProduced(payload) => {
                ordinary_schema::Output::workflow_resolved_receipt_produced(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowRunLogReported(payload) => {
                ordinary_schema::Output::workflow_run_log_reported(payload.log.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowRunObservationOpened(payload) => {
                ordinary_schema::Output::workflow_run_observation_opened(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorkflowRunObservationClosed(payload) => {
                ordinary_schema::Output::workflow_run_observation_closed(
                    payload.token.project_into()?,
                )
            }
            ordinary_contract::OrchestrateReply::PartialApplied(payload) => {
                ordinary_schema::Output::partial_applied(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ObservationOpened(payload) => {
                ordinary_schema::Output::ObservationOpened(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ObservationClosed(payload) => {
                ordinary_schema::Output::ObservationClosed(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::AgentRegistered(payload) => {
                ordinary_schema::Output::agent_registered(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::AgentRegistrationRejected(payload) => {
                ordinary_schema::Output::agent_registration_rejected(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::TopicTree(payload) => {
                ordinary_schema::Output::TopicTree(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::TopicDetail(payload) => {
                ordinary_schema::Output::TopicDetail(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::AgentDirectory(payload) => {
                ordinary_schema::Output::AgentDirectory(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorktreeScaffolded(payload) => {
                ordinary_schema::Output::worktree_scaffolded(payload.worktree.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorktreeRequestRejected(payload) => {
                ordinary_schema::Output::worktree_request_rejected(payload.reason.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorktreeConcluded(payload) => {
                ordinary_schema::Output::worktree_concluded(payload.worktree.project_into()?)
            }
            ordinary_contract::OrchestrateReply::WorktreeTeardownRefused(payload) => {
                ordinary_schema::Output::worktree_teardown_refused(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestrateReply> for ordinary_schema::Output {
    fn project_into(self) -> Result<ordinary_contract::OrchestrateReply> {
        Ok(match self {
            ordinary_schema::Output::ClaimAcceptance(payload) => {
                ordinary_contract::OrchestrateReply::ClaimAcceptance(payload.project_into()?)
            }
            ordinary_schema::Output::ClaimRejection(payload) => {
                ordinary_contract::OrchestrateReply::ClaimRejection(payload.project_into()?)
            }
            ordinary_schema::Output::ReleaseAcknowledgment(payload) => {
                ordinary_contract::OrchestrateReply::ReleaseAcknowledgment(payload.project_into()?)
            }
            ordinary_schema::Output::HandoffAcceptance(payload) => {
                ordinary_contract::OrchestrateReply::HandoffAcceptance(payload.project_into()?)
            }
            ordinary_schema::Output::HandoffRejection(payload) => {
                ordinary_contract::OrchestrateReply::HandoffRejection(payload.project_into()?)
            }
            ordinary_schema::Output::RoleSnapshot(payload) => {
                ordinary_contract::OrchestrateReply::RoleSnapshot(payload.project_into()?)
            }
            ordinary_schema::Output::SessionsObserved(payload) => {
                ordinary_contract::OrchestrateReply::SessionsObserved(payload.project_into()?)
            }
            ordinary_schema::Output::LanesObserved(payload) => {
                ordinary_contract::OrchestrateReply::LanesObserved(payload.project_into()?)
            }
            ordinary_schema::Output::WorktreesObserved(payload) => {
                ordinary_contract::OrchestrateReply::WorktreesObserved(payload.project_into()?)
            }
            ordinary_schema::Output::ActivityAcknowledgment(payload) => {
                ordinary_contract::OrchestrateReply::ActivityAcknowledgment(payload.project_into()?)
            }
            ordinary_schema::Output::ActivityList(payload) => {
                ordinary_contract::OrchestrateReply::ActivityList(payload.project_into()?)
            }
            ordinary_schema::Output::WorkflowRunAccepted(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowRunAccepted(payload.project_into()?)
            }
            ordinary_schema::Output::WorkflowResolutionAccepted(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowResolutionAccepted(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::WorkflowResolutionUnavailable(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowResolutionUnavailable(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::WorkflowReceiptProduced(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowReceiptProduced(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::WorkflowResolvedReceiptProduced(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowResolvedReceiptProduced(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::WorkflowRunLogReported(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowRunLogReported(payload.project_into()?)
            }
            ordinary_schema::Output::WorkflowRunObservationOpened(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowRunObservationOpened(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::WorkflowRunObservationClosed(payload) => {
                ordinary_contract::OrchestrateReply::WorkflowRunObservationClosed(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::PartialApplied(payload) => {
                ordinary_contract::OrchestrateReply::PartialApplied(payload.project_into()?)
            }
            ordinary_schema::Output::ObservationOpened(payload) => {
                ordinary_contract::OrchestrateReply::ObservationOpened(payload.project_into()?)
            }
            ordinary_schema::Output::ObservationClosed(payload) => {
                ordinary_contract::OrchestrateReply::ObservationClosed(payload.project_into()?)
            }
            ordinary_schema::Output::AgentRegistered(payload) => {
                ordinary_contract::OrchestrateReply::AgentRegistered(payload.project_into()?)
            }
            ordinary_schema::Output::AgentRegistrationRejected(payload) => {
                ordinary_contract::OrchestrateReply::AgentRegistrationRejected(
                    payload.project_into()?,
                )
            }
            ordinary_schema::Output::TopicTree(payload) => {
                ordinary_contract::OrchestrateReply::TopicTree(payload.project_into()?)
            }
            ordinary_schema::Output::TopicDetail(payload) => {
                ordinary_contract::OrchestrateReply::TopicDetail(payload.project_into()?)
            }
            ordinary_schema::Output::AgentDirectory(payload) => {
                ordinary_contract::OrchestrateReply::AgentDirectory(payload.project_into()?)
            }
            ordinary_schema::Output::WorktreeScaffolded(payload) => {
                ordinary_contract::OrchestrateReply::WorktreeScaffolded(
                    ordinary_contract::WorktreeScaffolded {
                        worktree: payload.into_payload().project_into()?,
                    },
                )
            }
            ordinary_schema::Output::WorktreeRequestRejected(payload) => {
                ordinary_contract::OrchestrateReply::WorktreeRequestRejected(
                    ordinary_contract::WorktreeRequestRejected {
                        reason: payload.into_payload().project_into()?,
                    },
                )
            }
            ordinary_schema::Output::WorktreeConcluded(payload) => {
                ordinary_contract::OrchestrateReply::WorktreeConcluded(
                    ordinary_contract::WorktreeConcluded {
                        worktree: payload.into_payload().project_into()?,
                    },
                )
            }
            ordinary_schema::Output::WorktreeTeardownRefused(payload) => {
                ordinary_contract::OrchestrateReply::WorktreeTeardownRefused(
                    ordinary_contract::WorktreeTeardownRefused {
                        worktree: payload.worktree.project_into()?,
                        reason: payload.teardown_refusal.project_into()?,
                    },
                )
            }
        })
    }
}

impl ProjectInto<meta_schema::CreateRoleOrder> for meta_contract::CreateRoleOrder {
    fn project_into(self) -> Result<meta_schema::CreateRoleOrder> {
        Ok(meta_schema::CreateRoleOrder {
            role_identifier: self.role.project_into()?,
            harness_kind: self.harness.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::CreateRoleOrder> for meta_schema::CreateRoleOrder {
    fn project_into(self) -> Result<meta_contract::CreateRoleOrder> {
        Ok(meta_contract::CreateRoleOrder {
            role: self.role_identifier.project_into()?,
            harness: self.harness_kind.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RetireRoleOrder> for meta_contract::RetireRoleOrder {
    fn project_into(self) -> Result<meta_schema::RetireRoleOrder> {
        Ok(meta_schema::RetireRoleOrder::new(self.role.project_into()?))
    }
}

impl ProjectInto<meta_contract::RetireRoleOrder> for meta_schema::RetireRoleOrder {
    fn project_into(self) -> Result<meta_contract::RetireRoleOrder> {
        Ok(meta_contract::RetireRoleOrder {
            role: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::Retirement> for meta_contract::Retirement {
    fn project_into(self) -> Result<meta_schema::Retirement> {
        Ok(match self {
            meta_contract::Retirement::Role(order) => {
                meta_schema::Retirement::Role(order.project_into()?)
            }
            meta_contract::Retirement::Lane(lane) => {
                meta_schema::Retirement::lane(lane.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_contract::Retirement> for meta_schema::Retirement {
    fn project_into(self) -> Result<meta_contract::Retirement> {
        Ok(match self {
            meta_schema::Retirement::Role(order) => {
                meta_contract::Retirement::Role(order.project_into()?)
            }
            meta_schema::Retirement::Lane(lane) => {
                meta_contract::Retirement::Lane(lane.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_schema::RefreshRepositoryIndexOrder>
    for meta_contract::RefreshRepositoryIndexOrder
{
    fn project_into(self) -> Result<meta_schema::RefreshRepositoryIndexOrder> {
        Ok(meta_schema::RefreshRepositoryIndexOrder {})
    }
}

impl ProjectInto<meta_contract::RefreshRepositoryIndexOrder>
    for meta_schema::RefreshRepositoryIndexOrder
{
    fn project_into(self) -> Result<meta_contract::RefreshRepositoryIndexOrder> {
        Ok(meta_contract::RefreshRepositoryIndexOrder {})
    }
}

impl ProjectInto<meta_schema::RegisterWorktree> for meta_contract::RegisterWorktree {
    fn project_into(self) -> Result<meta_schema::RegisterWorktree> {
        Ok(meta_schema::RegisterWorktree::new(
            self.worktree.project_into()?,
        ))
    }
}

impl ProjectInto<meta_contract::RegisterWorktree> for meta_schema::RegisterWorktree {
    fn project_into(self) -> Result<meta_contract::RegisterWorktree> {
        Ok(meta_contract::RegisterWorktree {
            worktree: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RefreshWorktreeIndexOrder>
    for meta_contract::RefreshWorktreeIndexOrder
{
    fn project_into(self) -> Result<meta_schema::RefreshWorktreeIndexOrder> {
        Ok(meta_schema::RefreshWorktreeIndexOrder {})
    }
}

impl ProjectInto<meta_contract::RefreshWorktreeIndexOrder>
    for meta_schema::RefreshWorktreeIndexOrder
{
    fn project_into(self) -> Result<meta_contract::RefreshWorktreeIndexOrder> {
        Ok(meta_contract::RefreshWorktreeIndexOrder {})
    }
}

impl ProjectInto<meta_schema::WorktreeRegistered> for meta_contract::WorktreeRegistered {
    fn project_into(self) -> Result<meta_schema::WorktreeRegistered> {
        Ok(meta_schema::WorktreeRegistered::new(
            self.worktree.project_into()?,
        ))
    }
}

impl ProjectInto<meta_contract::WorktreeRegistered> for meta_schema::WorktreeRegistered {
    fn project_into(self) -> Result<meta_contract::WorktreeRegistered> {
        Ok(meta_contract::WorktreeRegistered {
            worktree: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::WorktreeIndexRefreshed> for meta_contract::WorktreeIndexRefreshed {
    fn project_into(self) -> Result<meta_schema::WorktreeIndexRefreshed> {
        Ok(meta_schema::WorktreeIndexRefreshed::new(u64::from(
            self.worktrees(),
        )))
    }
}

impl ProjectInto<meta_contract::WorktreeIndexRefreshed> for meta_schema::WorktreeIndexRefreshed {
    fn project_into(self) -> Result<meta_contract::WorktreeIndexRefreshed> {
        Ok(meta_contract::WorktreeIndexRefreshed::new(
            u32::try_from(self.into_payload()).map_err(|error| Error::SchemaBridge {
                message: format!("worktree count does not fit u32: {error}"),
            })?,
        ))
    }
}

impl ProjectInto<meta_schema::ArchiveWorktreeOrder> for meta_contract::ArchiveWorktreeOrder {
    fn project_into(self) -> Result<meta_schema::ArchiveWorktreeOrder> {
        Ok(meta_schema::ArchiveWorktreeOrder::new(
            self.path.project_into()?,
        ))
    }
}

impl ProjectInto<meta_contract::ArchiveWorktreeOrder> for meta_schema::ArchiveWorktreeOrder {
    fn project_into(self) -> Result<meta_contract::ArchiveWorktreeOrder> {
        Ok(meta_contract::ArchiveWorktreeOrder {
            path: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::WorktreeArchived> for meta_contract::WorktreeArchived {
    fn project_into(self) -> Result<meta_schema::WorktreeArchived> {
        Ok(meta_schema::WorktreeArchived::new(
            self.worktree.project_into()?,
        ))
    }
}

impl ProjectInto<meta_contract::WorktreeArchived> for meta_schema::WorktreeArchived {
    fn project_into(self) -> Result<meta_contract::WorktreeArchived> {
        Ok(meta_contract::WorktreeArchived {
            worktree: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneRegistrationMode> for meta_contract::LaneRegistrationMode {
    fn project_into(self) -> Result<meta_schema::LaneRegistrationMode> {
        Ok(match self {
            meta_contract::LaneRegistrationMode::Fresh => meta_schema::LaneRegistrationMode::Fresh,
            meta_contract::LaneRegistrationMode::Recovery => {
                meta_schema::LaneRegistrationMode::Recovery
            }
        })
    }
}

impl ProjectInto<meta_contract::LaneRegistrationMode> for meta_schema::LaneRegistrationMode {
    fn project_into(self) -> Result<meta_contract::LaneRegistrationMode> {
        Ok(match self {
            meta_schema::LaneRegistrationMode::Fresh => meta_contract::LaneRegistrationMode::Fresh,
            meta_schema::LaneRegistrationMode::Recovery => {
                meta_contract::LaneRegistrationMode::Recovery
            }
        })
    }
}

impl ProjectInto<meta_schema::LaneRegistrationRequest> for meta_contract::LaneRegistrationRequest {
    fn project_into(self) -> Result<meta_schema::LaneRegistrationRequest> {
        Ok(meta_schema::LaneRegistrationRequest {
            lane_assignment: self.assignment.project_into()?,
            lane_registration_mode: self.mode.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneRegistrationRequest> for meta_schema::LaneRegistrationRequest {
    fn project_into(self) -> Result<meta_contract::LaneRegistrationRequest> {
        Ok(meta_contract::LaneRegistrationRequest {
            assignment: self.lane_assignment.project_into()?,
            mode: self.lane_registration_mode.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneUnregistrationRequest>
    for meta_contract::LaneUnregistrationRequest
{
    fn project_into(self) -> Result<meta_schema::LaneUnregistrationRequest> {
        Ok(meta_schema::LaneUnregistrationRequest {
            session_identifier: self.session.project_into()?,
            lane_identifier: self.lane.project_into()?,
            lane_details: self.details.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneUnregistrationRequest>
    for meta_schema::LaneUnregistrationRequest
{
    fn project_into(self) -> Result<meta_contract::LaneUnregistrationRequest> {
        Ok(meta_contract::LaneUnregistrationRequest {
            session: self.session_identifier.project_into()?,
            lane: self.lane_identifier.project_into()?,
            details: self.lane_details.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::SessionClearRequest> for meta_contract::SessionClearRequest {
    fn project_into(self) -> Result<meta_schema::SessionClearRequest> {
        Ok(meta_schema::SessionClearRequest {
            session_identifier: self.session.project_into()?,
            lane_details: self.details.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::SessionClearRequest> for meta_schema::SessionClearRequest {
    fn project_into(self) -> Result<meta_contract::SessionClearRequest> {
        Ok(meta_contract::SessionClearRequest {
            session: self.session_identifier.project_into()?,
            details: self.lane_details.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneAuthorityChange> for meta_contract::LaneAuthorityChange {
    fn project_into(self) -> Result<meta_schema::LaneAuthorityChange> {
        Ok(meta_schema::LaneAuthorityChange {
            lane_identifier: self.lane.project_into()?,
            lane_authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneAuthorityChange> for meta_schema::LaneAuthorityChange {
    fn project_into(self) -> Result<meta_contract::LaneAuthorityChange> {
        Ok(meta_contract::LaneAuthorityChange {
            lane: self.lane_identifier.project_into()?,
            authority: self.lane_authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::Input> for meta_contract::MetaOrchestrateRequest {
    fn project_into(self) -> Result<meta_schema::Input> {
        Ok(match self {
            meta_contract::MetaOrchestrateRequest::Create(payload) => {
                meta_schema::Input::create(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::Retire(payload) => {
                meta_schema::Input::retire(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::Refresh(payload) => {
                meta_schema::Input::refresh(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::Register(payload) => {
                meta_schema::Input::register(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::Unregister(payload) => {
                meta_schema::Input::unregister(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::ClearSession(payload) => {
                meta_schema::Input::clear_session(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::SetAuthority(payload) => {
                meta_schema::Input::set_authority(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::RegisterWorktree(payload) => {
                meta_schema::Input::register_worktree(payload.worktree.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::RefreshWorktreeIndex(payload) => {
                meta_schema::Input::refresh_worktree_index(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateRequest::ArchiveWorktree(payload) => {
                meta_schema::Input::archive_worktree(payload.path.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_contract::MetaOrchestrateRequest> for meta_schema::Input {
    fn project_into(self) -> Result<meta_contract::MetaOrchestrateRequest> {
        Ok(match self {
            meta_schema::Input::Create(payload) => {
                meta_contract::MetaOrchestrateRequest::Create(payload.project_into()?)
            }
            meta_schema::Input::Retire(payload) => {
                meta_contract::MetaOrchestrateRequest::Retire(payload.project_into()?)
            }
            meta_schema::Input::Refresh(payload) => {
                meta_contract::MetaOrchestrateRequest::Refresh(payload.project_into()?)
            }
            meta_schema::Input::Register(payload) => {
                meta_contract::MetaOrchestrateRequest::Register(payload.project_into()?)
            }
            meta_schema::Input::Unregister(payload) => {
                meta_contract::MetaOrchestrateRequest::Unregister(payload.project_into()?)
            }
            meta_schema::Input::ClearSession(payload) => {
                meta_contract::MetaOrchestrateRequest::ClearSession(payload.project_into()?)
            }
            meta_schema::Input::SetAuthority(payload) => {
                meta_contract::MetaOrchestrateRequest::SetAuthority(payload.project_into()?)
            }
            meta_schema::Input::RegisterWorktree(payload) => {
                meta_contract::MetaOrchestrateRequest::RegisterWorktree(payload.project_into()?)
            }
            meta_schema::Input::RefreshWorktreeIndex(payload) => {
                meta_contract::MetaOrchestrateRequest::RefreshWorktreeIndex(payload.project_into()?)
            }
            meta_schema::Input::ArchiveWorktree(payload) => {
                meta_contract::MetaOrchestrateRequest::ArchiveWorktree(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_schema::RoleCreated> for meta_contract::RoleCreated {
    fn project_into(self) -> Result<meta_schema::RoleCreated> {
        Ok(meta_schema::RoleCreated {
            role_identifier: self.role.project_into()?,
            harness_kind: self.harness.project_into()?,
            report_repository_path: self.report_repository_path.project_into()?,
            report_lane_path: self.report_lane_path.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::RoleCreated> for meta_schema::RoleCreated {
    fn project_into(self) -> Result<meta_contract::RoleCreated> {
        Ok(meta_contract::RoleCreated {
            role: self.role_identifier.project_into()?,
            harness: self.harness_kind.project_into()?,
            report_repository_path: self.report_repository_path.project_into()?,
            report_lane_path: self.report_lane_path.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RoleRetired> for meta_contract::RoleRetired {
    fn project_into(self) -> Result<meta_schema::RoleRetired> {
        Ok(meta_schema::RoleRetired::new(self.role.project_into()?))
    }
}

impl ProjectInto<meta_contract::RoleRetired> for meta_schema::RoleRetired {
    fn project_into(self) -> Result<meta_contract::RoleRetired> {
        Ok(meta_contract::RoleRetired {
            role: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RoleCreationRejectionReason>
    for meta_contract::RoleCreationRejectionReason
{
    fn project_into(self) -> Result<meta_schema::RoleCreationRejectionReason> {
        Ok(match self {
            meta_contract::RoleCreationRejectionReason::RoleAlreadyExists => {
                meta_schema::RoleCreationRejectionReason::RoleAlreadyExists
            }
            meta_contract::RoleCreationRejectionReason::ReportRepositoryAlreadyExists => {
                meta_schema::RoleCreationRejectionReason::ReportRepositoryAlreadyExists
            }
            meta_contract::RoleCreationRejectionReason::ReportLaneAlreadyExists => {
                meta_schema::RoleCreationRejectionReason::ReportLaneAlreadyExists
            }
        })
    }
}

impl ProjectInto<meta_contract::RoleCreationRejectionReason>
    for meta_schema::RoleCreationRejectionReason
{
    fn project_into(self) -> Result<meta_contract::RoleCreationRejectionReason> {
        Ok(match self {
            meta_schema::RoleCreationRejectionReason::RoleAlreadyExists => {
                meta_contract::RoleCreationRejectionReason::RoleAlreadyExists
            }
            meta_schema::RoleCreationRejectionReason::ReportRepositoryAlreadyExists => {
                meta_contract::RoleCreationRejectionReason::ReportRepositoryAlreadyExists
            }
            meta_schema::RoleCreationRejectionReason::ReportLaneAlreadyExists => {
                meta_contract::RoleCreationRejectionReason::ReportLaneAlreadyExists
            }
        })
    }
}

impl ProjectInto<meta_schema::RoleCreationRejected> for meta_contract::RoleCreationRejected {
    fn project_into(self) -> Result<meta_schema::RoleCreationRejected> {
        Ok(meta_schema::RoleCreationRejected {
            role_identifier: self.role.project_into()?,
            role_creation_rejection_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::RoleCreationRejected> for meta_schema::RoleCreationRejected {
    fn project_into(self) -> Result<meta_contract::RoleCreationRejected> {
        Ok(meta_contract::RoleCreationRejected {
            role: self.role_identifier.project_into()?,
            reason: self.role_creation_rejection_reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RepositoryIndexRefreshed>
    for meta_contract::RepositoryIndexRefreshed
{
    fn project_into(self) -> Result<meta_schema::RepositoryIndexRefreshed> {
        Ok(meta_schema::RepositoryIndexRefreshed::new(u64::from(
            self.repositories(),
        )))
    }
}

impl ProjectInto<meta_contract::RepositoryIndexRefreshed>
    for meta_schema::RepositoryIndexRefreshed
{
    fn project_into(self) -> Result<meta_contract::RepositoryIndexRefreshed> {
        Ok(meta_contract::RepositoryIndexRefreshed::new(
            u32::try_from(self.into_payload()).map_err(|error| Error::SchemaBridge {
                message: format!("repository count does not fit u32: {error}"),
            })?,
        ))
    }
}

impl ProjectInto<meta_schema::LaneRegistered> for meta_contract::LaneRegistered {
    fn project_into(self) -> Result<meta_schema::LaneRegistered> {
        Ok(meta_schema::LaneRegistered::new(
            self.registration.project_into()?,
        ))
    }
}

impl ProjectInto<meta_contract::LaneRegistered> for meta_schema::LaneRegistered {
    fn project_into(self) -> Result<meta_contract::LaneRegistered> {
        Ok(meta_contract::LaneRegistered {
            registration: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneAlreadyRegisteredResolution>
    for meta_contract::LaneAlreadyRegisteredResolution
{
    fn project_into(self) -> Result<meta_schema::LaneAlreadyRegisteredResolution> {
        Ok(match self {
            meta_contract::LaneAlreadyRegisteredResolution::FreshConflict => {
                meta_schema::LaneAlreadyRegisteredResolution::FreshConflict
            }
            meta_contract::LaneAlreadyRegisteredResolution::RecoveryInherited => {
                meta_schema::LaneAlreadyRegisteredResolution::RecoveryInherited
            }
        })
    }
}

impl ProjectInto<meta_contract::LaneAlreadyRegisteredResolution>
    for meta_schema::LaneAlreadyRegisteredResolution
{
    fn project_into(self) -> Result<meta_contract::LaneAlreadyRegisteredResolution> {
        Ok(match self {
            meta_schema::LaneAlreadyRegisteredResolution::FreshConflict => {
                meta_contract::LaneAlreadyRegisteredResolution::FreshConflict
            }
            meta_schema::LaneAlreadyRegisteredResolution::RecoveryInherited => {
                meta_contract::LaneAlreadyRegisteredResolution::RecoveryInherited
            }
        })
    }
}

impl ProjectInto<meta_schema::LaneAlreadyRegistered> for meta_contract::LaneAlreadyRegistered {
    fn project_into(self) -> Result<meta_schema::LaneAlreadyRegistered> {
        Ok(meta_schema::LaneAlreadyRegistered {
            lane_registration_request: self.requested.project_into()?,
            lane_projection: self.active.project_into()?,
            lane_already_registered_resolution: self.resolution.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneAlreadyRegistered> for meta_schema::LaneAlreadyRegistered {
    fn project_into(self) -> Result<meta_contract::LaneAlreadyRegistered> {
        Ok(meta_contract::LaneAlreadyRegistered {
            requested: self.lane_registration_request.project_into()?,
            active: self.lane_projection.project_into()?,
            resolution: self.lane_already_registered_resolution.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneUnregistered> for meta_contract::LaneUnregistered {
    fn project_into(self) -> Result<meta_schema::LaneUnregistered> {
        Ok(meta_schema::LaneUnregistered {
            session_identifier: self.session.project_into()?,
            lane_identifier: self.lane.project_into()?,
            timestamp_nanos: self.ended_at.project_into()?,
            lane_details: self.details.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneUnregistered> for meta_schema::LaneUnregistered {
    fn project_into(self) -> Result<meta_contract::LaneUnregistered> {
        Ok(meta_contract::LaneUnregistered {
            session: self.session_identifier.project_into()?,
            lane: self.lane_identifier.project_into()?,
            ended_at: self.timestamp_nanos.project_into()?,
            details: self.lane_details.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::SessionCleared> for meta_contract::SessionCleared {
    fn project_into(self) -> Result<meta_schema::SessionCleared> {
        Ok(meta_schema::SessionCleared {
            session_identifier: self.session.project_into()?,
            integer: u64::from(self.cleared_lanes),
            timestamp_nanos: self.ended_at.project_into()?,
            lane_details: self.details.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::SessionCleared> for meta_schema::SessionCleared {
    fn project_into(self) -> Result<meta_contract::SessionCleared> {
        Ok(meta_contract::SessionCleared {
            session: self.session_identifier.project_into()?,
            cleared_lanes: u32::try_from(self.integer).map_err(|error| Error::SchemaBridge {
                message: format!("cleared lane count does not fit u32: {error}"),
            })?,
            ended_at: self.timestamp_nanos.project_into()?,
            details: self.lane_details.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneRetired> for meta_contract::LaneRetired {
    fn project_into(self) -> Result<meta_schema::LaneRetired> {
        Ok(meta_schema::LaneRetired::new(self.lane.project_into()?))
    }
}

impl ProjectInto<meta_contract::LaneRetired> for meta_schema::LaneRetired {
    fn project_into(self) -> Result<meta_contract::LaneRetired> {
        Ok(meta_contract::LaneRetired {
            lane: self.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneAuthoritySet> for meta_contract::LaneAuthoritySet {
    fn project_into(self) -> Result<meta_schema::LaneAuthoritySet> {
        Ok(meta_schema::LaneAuthoritySet {
            lane_identifier: self.lane.project_into()?,
            lane_authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneAuthoritySet> for meta_schema::LaneAuthoritySet {
    fn project_into(self) -> Result<meta_contract::LaneAuthoritySet> {
        Ok(meta_contract::LaneAuthoritySet {
            lane: self.lane_identifier.project_into()?,
            authority: self.lane_authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::MetaOperationKind> for meta_contract::MetaOperationKind {
    fn project_into(self) -> Result<meta_schema::MetaOperationKind> {
        Ok(match self {
            meta_contract::MetaOperationKind::Create => meta_schema::MetaOperationKind::Create,
            meta_contract::MetaOperationKind::Retire => meta_schema::MetaOperationKind::Retire,
            meta_contract::MetaOperationKind::Refresh => meta_schema::MetaOperationKind::Refresh,
            meta_contract::MetaOperationKind::Register => meta_schema::MetaOperationKind::Register,
            meta_contract::MetaOperationKind::Unregister => {
                meta_schema::MetaOperationKind::Unregister
            }
            meta_contract::MetaOperationKind::ClearSession => {
                meta_schema::MetaOperationKind::ClearSession
            }
            meta_contract::MetaOperationKind::SetAuthority => {
                meta_schema::MetaOperationKind::SetAuthority
            }
            meta_contract::MetaOperationKind::RegisterWorktree => {
                meta_schema::MetaOperationKind::RegisterWorktree
            }
            meta_contract::MetaOperationKind::RefreshWorktreeIndex => {
                meta_schema::MetaOperationKind::RefreshWorktreeIndex
            }
            meta_contract::MetaOperationKind::ArchiveWorktree => {
                meta_schema::MetaOperationKind::ArchiveWorktree
            }
        })
    }
}

impl ProjectInto<meta_contract::MetaOperationKind> for meta_schema::MetaOperationKind {
    fn project_into(self) -> Result<meta_contract::MetaOperationKind> {
        Ok(match self {
            meta_schema::MetaOperationKind::Create => meta_contract::MetaOperationKind::Create,
            meta_schema::MetaOperationKind::Retire => meta_contract::MetaOperationKind::Retire,
            meta_schema::MetaOperationKind::Refresh => meta_contract::MetaOperationKind::Refresh,
            meta_schema::MetaOperationKind::Register => meta_contract::MetaOperationKind::Register,
            meta_schema::MetaOperationKind::Unregister => {
                meta_contract::MetaOperationKind::Unregister
            }
            meta_schema::MetaOperationKind::ClearSession => {
                meta_contract::MetaOperationKind::ClearSession
            }
            meta_schema::MetaOperationKind::SetAuthority => {
                meta_contract::MetaOperationKind::SetAuthority
            }
            meta_schema::MetaOperationKind::RegisterWorktree => {
                meta_contract::MetaOperationKind::RegisterWorktree
            }
            meta_schema::MetaOperationKind::RefreshWorktreeIndex => {
                meta_contract::MetaOperationKind::RefreshWorktreeIndex
            }
            meta_schema::MetaOperationKind::ArchiveWorktree => {
                meta_contract::MetaOperationKind::ArchiveWorktree
            }
        })
    }
}

impl ProjectInto<meta_schema::MetaOrchestrateUnimplementedReason>
    for meta_contract::MetaOrchestrateUnimplementedReason
{
    fn project_into(self) -> Result<meta_schema::MetaOrchestrateUnimplementedReason> {
        Ok(match self {
            meta_contract::MetaOrchestrateUnimplementedReason::NotBuiltYet => {
                meta_schema::MetaOrchestrateUnimplementedReason::NotBuiltYet
            }
            meta_contract::MetaOrchestrateUnimplementedReason::DependencyNotReady => {
                meta_schema::MetaOrchestrateUnimplementedReason::DependencyNotReady
            }
        })
    }
}

impl ProjectInto<meta_contract::MetaOrchestrateUnimplementedReason>
    for meta_schema::MetaOrchestrateUnimplementedReason
{
    fn project_into(self) -> Result<meta_contract::MetaOrchestrateUnimplementedReason> {
        Ok(match self {
            meta_schema::MetaOrchestrateUnimplementedReason::NotBuiltYet => {
                meta_contract::MetaOrchestrateUnimplementedReason::NotBuiltYet
            }
            meta_schema::MetaOrchestrateUnimplementedReason::DependencyNotReady => {
                meta_contract::MetaOrchestrateUnimplementedReason::DependencyNotReady
            }
        })
    }
}

impl ProjectInto<meta_schema::MetaOrchestrateRequestUnimplemented>
    for meta_contract::MetaOrchestrateRequestUnimplemented
{
    fn project_into(self) -> Result<meta_schema::MetaOrchestrateRequestUnimplemented> {
        Ok(meta_schema::MetaOrchestrateRequestUnimplemented {
            meta_operation_kind: self.operation.project_into()?,
            meta_orchestrate_unimplemented_reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::MetaOrchestrateRequestUnimplemented>
    for meta_schema::MetaOrchestrateRequestUnimplemented
{
    fn project_into(self) -> Result<meta_contract::MetaOrchestrateRequestUnimplemented> {
        Ok(meta_contract::MetaOrchestrateRequestUnimplemented {
            operation: self.meta_operation_kind.project_into()?,
            reason: self.meta_orchestrate_unimplemented_reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::Output> for meta_contract::MetaOrchestrateReply {
    fn project_into(self) -> Result<meta_schema::Output> {
        Ok(match self {
            meta_contract::MetaOrchestrateReply::RoleCreated(payload) => {
                meta_schema::Output::role_created(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::RoleRetired(payload) => {
                meta_schema::Output::RoleRetired(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::RoleCreationRejected(payload) => {
                meta_schema::Output::role_creation_rejected(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::RepositoryIndexRefreshed(payload) => {
                meta_schema::Output::RepositoryIndexRefreshed(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneRegistered(payload) => {
                meta_schema::Output::LaneRegistered(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneAlreadyRegistered(payload) => {
                meta_schema::Output::LaneAlreadyRegistered(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneUnregistered(payload) => {
                meta_schema::Output::LaneUnregistered(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::SessionCleared(payload) => {
                meta_schema::Output::SessionCleared(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneRetired(payload) => {
                meta_schema::Output::LaneRetired(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneAuthoritySet(payload) => {
                meta_schema::Output::lane_authority_set(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::WorktreeRegistered(payload) => {
                meta_schema::Output::worktree_registered(payload.worktree.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::WorktreeIndexRefreshed(payload) => {
                meta_schema::Output::worktree_index_refreshed(u64::from(payload.worktrees()))
            }
            meta_contract::MetaOrchestrateReply::WorktreeArchived(payload) => {
                meta_schema::Output::worktree_archived(payload.worktree.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::PartialApplied(payload) => {
                meta_schema::Output::partial_applied(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::MetaOrchestrateRequestUnimplemented(payload) => {
                meta_schema::Output::meta_orchestrate_request_unimplemented(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_contract::MetaOrchestrateReply> for meta_schema::Output {
    fn project_into(self) -> Result<meta_contract::MetaOrchestrateReply> {
        Ok(match self {
            meta_schema::Output::RoleCreated(payload) => {
                meta_contract::MetaOrchestrateReply::RoleCreated(payload.project_into()?)
            }
            meta_schema::Output::RoleRetired(payload) => {
                meta_contract::MetaOrchestrateReply::RoleRetired(payload.project_into()?)
            }
            meta_schema::Output::RoleCreationRejected(payload) => {
                meta_contract::MetaOrchestrateReply::RoleCreationRejected(payload.project_into()?)
            }
            meta_schema::Output::RepositoryIndexRefreshed(payload) => {
                meta_contract::MetaOrchestrateReply::RepositoryIndexRefreshed(
                    payload.project_into()?,
                )
            }
            meta_schema::Output::LaneRegistered(payload) => {
                meta_contract::MetaOrchestrateReply::LaneRegistered(payload.project_into()?)
            }
            meta_schema::Output::LaneAlreadyRegistered(payload) => {
                meta_contract::MetaOrchestrateReply::LaneAlreadyRegistered(payload.project_into()?)
            }
            meta_schema::Output::LaneUnregistered(payload) => {
                meta_contract::MetaOrchestrateReply::LaneUnregistered(payload.project_into()?)
            }
            meta_schema::Output::SessionCleared(payload) => {
                meta_contract::MetaOrchestrateReply::SessionCleared(payload.project_into()?)
            }
            meta_schema::Output::LaneRetired(payload) => {
                meta_contract::MetaOrchestrateReply::LaneRetired(payload.project_into()?)
            }
            meta_schema::Output::LaneAuthoritySet(payload) => {
                meta_contract::MetaOrchestrateReply::LaneAuthoritySet(payload.project_into()?)
            }
            meta_schema::Output::WorktreeRegistered(payload) => {
                meta_contract::MetaOrchestrateReply::WorktreeRegistered(payload.project_into()?)
            }
            meta_schema::Output::WorktreeIndexRefreshed(payload) => {
                meta_contract::MetaOrchestrateReply::WorktreeIndexRefreshed(payload.project_into()?)
            }
            meta_schema::Output::WorktreeArchived(payload) => {
                meta_contract::MetaOrchestrateReply::WorktreeArchived(payload.project_into()?)
            }
            meta_schema::Output::PartialApplied(payload) => {
                meta_contract::MetaOrchestrateReply::PartialApplied(payload.project_into()?)
            }
            meta_schema::Output::MetaOrchestrateRequestUnimplemented(payload) => {
                meta_contract::MetaOrchestrateReply::MetaOrchestrateRequestUnimplemented(
                    payload.project_into()?,
                )
            }
        })
    }
}

impl ProjectInto<ordinary_schema::OrchestratorAgentIdentifier>
    for ordinary_contract::OrchestratorAgentIdentifier
{
    fn project_into(self) -> Result<ordinary_schema::OrchestratorAgentIdentifier> {
        Ok(ordinary_schema::OrchestratorAgentIdentifier::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::OrchestratorAgentIdentifier>
    for ordinary_schema::OrchestratorAgentIdentifier
{
    fn project_into(self) -> Result<ordinary_contract::OrchestratorAgentIdentifier> {
        ordinary_contract::OrchestratorAgentIdentifier::from_wire_token(self.into_payload())
            .map_err(|error| Error::SchemaBridge {
                message: error.to_string(),
            })
    }
}

impl ProjectInto<ordinary_schema::OrchestratorTopicPath>
    for ordinary_contract::OrchestratorTopicPath
{
    fn project_into(self) -> Result<ordinary_schema::OrchestratorTopicPath> {
        Ok(ordinary_schema::OrchestratorTopicPath::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::OrchestratorTopicPath>
    for ordinary_schema::OrchestratorTopicPath
{
    fn project_into(self) -> Result<ordinary_contract::OrchestratorTopicPath> {
        ordinary_contract::OrchestratorTopicPath::from_wire_token(self.into_payload()).map_err(
            |error| Error::SchemaBridge {
                message: error.to_string(),
            },
        )
    }
}

impl ProjectInto<ordinary_schema::TopicName> for ordinary_contract::TopicName {
    fn project_into(self) -> Result<ordinary_schema::TopicName> {
        Ok(ordinary_schema::TopicName::new(self.as_str().to_string()))
    }
}

impl ProjectInto<ordinary_contract::TopicName> for ordinary_schema::TopicName {
    fn project_into(self) -> Result<ordinary_contract::TopicName> {
        ordinary_contract::TopicName::from_text(self.into_payload()).map_err(|error| {
            Error::SchemaBridge {
                message: error.to_string(),
            }
        })
    }
}

impl ProjectInto<ordinary_schema::MissionDescription> for ordinary_contract::MissionDescription {
    fn project_into(self) -> Result<ordinary_schema::MissionDescription> {
        Ok(ordinary_schema::MissionDescription::new(
            self.as_str().to_string(),
        ))
    }
}

impl ProjectInto<ordinary_contract::MissionDescription> for ordinary_schema::MissionDescription {
    fn project_into(self) -> Result<ordinary_contract::MissionDescription> {
        ordinary_contract::MissionDescription::from_text(self.into_payload()).map_err(|error| {
            Error::SchemaBridge {
                message: error.to_string(),
            }
        })
    }
}

impl ProjectInto<ordinary_schema::OrchestratorTopic> for ordinary_contract::OrchestratorTopic {
    fn project_into(self) -> Result<ordinary_schema::OrchestratorTopic> {
        Ok(ordinary_schema::OrchestratorTopic {
            orchestrator_topic_path: self.path.project_into()?,
            topic_name: self.name.project_into()?,
            optional_orchestrator_topic_path: self
                .parent
                .map(ProjectInto::project_into)
                .transpose()?,
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestratorTopic> for ordinary_schema::OrchestratorTopic {
    fn project_into(self) -> Result<ordinary_contract::OrchestratorTopic> {
        Ok(ordinary_contract::OrchestratorTopic {
            path: self.orchestrator_topic_path.project_into()?,
            name: self.topic_name.project_into()?,
            parent: self
                .optional_orchestrator_topic_path
                .map(ProjectInto::project_into)
                .transpose()?,
        })
    }
}

impl ProjectInto<ordinary_schema::OrchestratorAgentStatus>
    for ordinary_contract::OrchestratorAgentStatus
{
    fn project_into(self) -> Result<ordinary_schema::OrchestratorAgentStatus> {
        Ok(match self {
            ordinary_contract::OrchestratorAgentStatus::Active => {
                ordinary_schema::OrchestratorAgentStatus::Active
            }
            ordinary_contract::OrchestratorAgentStatus::Retired => {
                ordinary_schema::OrchestratorAgentStatus::Retired
            }
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestratorAgentStatus>
    for ordinary_schema::OrchestratorAgentStatus
{
    fn project_into(self) -> Result<ordinary_contract::OrchestratorAgentStatus> {
        Ok(match self {
            ordinary_schema::OrchestratorAgentStatus::Active => {
                ordinary_contract::OrchestratorAgentStatus::Active
            }
            ordinary_schema::OrchestratorAgentStatus::Retired => {
                ordinary_contract::OrchestratorAgentStatus::Retired
            }
        })
    }
}

impl ProjectInto<ordinary_schema::TopicAssignmentSource>
    for ordinary_contract::TopicAssignmentSource
{
    fn project_into(self) -> Result<ordinary_schema::TopicAssignmentSource> {
        Ok(match self {
            ordinary_contract::TopicAssignmentSource::Judge => {
                ordinary_schema::TopicAssignmentSource::Judge
            }
            ordinary_contract::TopicAssignmentSource::Explicit => {
                ordinary_schema::TopicAssignmentSource::Explicit
            }
        })
    }
}

impl ProjectInto<ordinary_contract::TopicAssignmentSource>
    for ordinary_schema::TopicAssignmentSource
{
    fn project_into(self) -> Result<ordinary_contract::TopicAssignmentSource> {
        Ok(match self {
            ordinary_schema::TopicAssignmentSource::Judge => {
                ordinary_contract::TopicAssignmentSource::Judge
            }
            ordinary_schema::TopicAssignmentSource::Explicit => {
                ordinary_contract::TopicAssignmentSource::Explicit
            }
        })
    }
}

impl ProjectInto<ordinary_schema::TopicSelection> for ordinary_contract::TopicSelection {
    fn project_into(self) -> Result<ordinary_schema::TopicSelection> {
        Ok(match self {
            ordinary_contract::TopicSelection::Automatic => {
                ordinary_schema::TopicSelection::Automatic
            }
            ordinary_contract::TopicSelection::Explicit(paths) => {
                ordinary_schema::TopicSelection::Explicit(
                    ordinary_schema::OrchestratorTopicPaths::new(paths.project_into()?),
                )
            }
        })
    }
}

impl ProjectInto<ordinary_contract::TopicSelection> for ordinary_schema::TopicSelection {
    fn project_into(self) -> Result<ordinary_contract::TopicSelection> {
        Ok(match self {
            ordinary_schema::TopicSelection::Automatic => {
                ordinary_contract::TopicSelection::Automatic
            }
            ordinary_schema::TopicSelection::Explicit(paths) => {
                ordinary_contract::TopicSelection::Explicit(paths.into_payload().project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::OrchestratorAgentRegistration>
    for ordinary_contract::OrchestratorAgentRegistration
{
    fn project_into(self) -> Result<ordinary_schema::OrchestratorAgentRegistration> {
        Ok(ordinary_schema::OrchestratorAgentRegistration {
            session_identifier: self.session.project_into()?,
            mission_description: self.mission.project_into()?,
            harness_kind: self.harness.project_into()?,
            topic_selection: self.topic_selection.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestratorAgentRegistration>
    for ordinary_schema::OrchestratorAgentRegistration
{
    fn project_into(self) -> Result<ordinary_contract::OrchestratorAgentRegistration> {
        Ok(ordinary_contract::OrchestratorAgentRegistration {
            session: self.session_identifier.project_into()?,
            mission: self.mission_description.project_into()?,
            harness: self.harness_kind.project_into()?,
            topic_selection: self.topic_selection.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::AgentRegistrationRejectionReason>
    for ordinary_contract::AgentRegistrationRejectionReason
{
    fn project_into(self) -> Result<ordinary_schema::AgentRegistrationRejectionReason> {
        Ok(match self {
            ordinary_contract::AgentRegistrationRejectionReason::MissionEmpty => {
                ordinary_schema::AgentRegistrationRejectionReason::MissionEmpty
            }
            ordinary_contract::AgentRegistrationRejectionReason::MissionTooVague => {
                ordinary_schema::AgentRegistrationRejectionReason::MissionTooVague
            }
            ordinary_contract::AgentRegistrationRejectionReason::UnknownTopic => {
                ordinary_schema::AgentRegistrationRejectionReason::UnknownTopic
            }
            ordinary_contract::AgentRegistrationRejectionReason::JudgeUnavailable => {
                ordinary_schema::AgentRegistrationRejectionReason::JudgeUnavailable
            }
            ordinary_contract::AgentRegistrationRejectionReason::JudgeMalformed => {
                ordinary_schema::AgentRegistrationRejectionReason::JudgeMalformed
            }
            ordinary_contract::AgentRegistrationRejectionReason::JudgeTimedOut => {
                ordinary_schema::AgentRegistrationRejectionReason::JudgeTimedOut
            }
        })
    }
}

impl ProjectInto<ordinary_contract::AgentRegistrationRejectionReason>
    for ordinary_schema::AgentRegistrationRejectionReason
{
    fn project_into(self) -> Result<ordinary_contract::AgentRegistrationRejectionReason> {
        Ok(match self {
            ordinary_schema::AgentRegistrationRejectionReason::MissionEmpty => {
                ordinary_contract::AgentRegistrationRejectionReason::MissionEmpty
            }
            ordinary_schema::AgentRegistrationRejectionReason::MissionTooVague => {
                ordinary_contract::AgentRegistrationRejectionReason::MissionTooVague
            }
            ordinary_schema::AgentRegistrationRejectionReason::UnknownTopic => {
                ordinary_contract::AgentRegistrationRejectionReason::UnknownTopic
            }
            ordinary_schema::AgentRegistrationRejectionReason::JudgeUnavailable => {
                ordinary_contract::AgentRegistrationRejectionReason::JudgeUnavailable
            }
            ordinary_schema::AgentRegistrationRejectionReason::JudgeMalformed => {
                ordinary_contract::AgentRegistrationRejectionReason::JudgeMalformed
            }
            ordinary_schema::AgentRegistrationRejectionReason::JudgeTimedOut => {
                ordinary_contract::AgentRegistrationRejectionReason::JudgeTimedOut
            }
        })
    }
}

impl ProjectInto<ordinary_schema::AgentRegistered> for ordinary_contract::AgentRegistered {
    fn project_into(self) -> Result<ordinary_schema::AgentRegistered> {
        Ok(ordinary_schema::AgentRegistered {
            orchestrator_agent_identifier: self.agent_identifier.project_into()?,
            orchestrator_topics: ordinary_schema::OrchestratorTopics::new(
                self.assigned_topics.project_into()?,
            ),
            topic_assignment_source: self.assignment_source.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::AgentRegistered> for ordinary_schema::AgentRegistered {
    fn project_into(self) -> Result<ordinary_contract::AgentRegistered> {
        Ok(ordinary_contract::AgentRegistered {
            agent_identifier: self.orchestrator_agent_identifier.project_into()?,
            assigned_topics: self.orchestrator_topics.into_payload().project_into()?,
            assignment_source: self.topic_assignment_source.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::AgentRegistrationRejected>
    for ordinary_contract::AgentRegistrationRejected
{
    fn project_into(self) -> Result<ordinary_schema::AgentRegistrationRejected> {
        Ok(ordinary_schema::AgentRegistrationRejected {
            agent_registration_rejection_reason: self.reason.project_into()?,
            orchestrator_topics: ordinary_schema::OrchestratorTopics::new(
                self.available_topics.project_into()?,
            ),
        })
    }
}

impl ProjectInto<ordinary_contract::AgentRegistrationRejected>
    for ordinary_schema::AgentRegistrationRejected
{
    fn project_into(self) -> Result<ordinary_contract::AgentRegistrationRejected> {
        Ok(ordinary_contract::AgentRegistrationRejected {
            reason: self.agent_registration_rejection_reason.project_into()?,
            available_topics: self.orchestrator_topics.into_payload().project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::TopicTree> for ordinary_contract::TopicTree {
    fn project_into(self) -> Result<ordinary_schema::TopicTree> {
        Ok(ordinary_schema::TopicTree::new(
            ordinary_schema::OrchestratorTopics::new(self.topics.project_into()?),
        ))
    }
}

impl ProjectInto<ordinary_contract::TopicTree> for ordinary_schema::TopicTree {
    fn project_into(self) -> Result<ordinary_contract::TopicTree> {
        Ok(ordinary_contract::TopicTree {
            topics: self.into_payload().into_payload().project_into()?,
        })
    }
}

impl OrchestrateSemaEngine<'_> {
    fn orchestrator_topics(&self) -> Result<Vec<ordinary_contract::OrchestratorTopic>> {
        Ok(self
            .service
            .tables()
            .orchestrator_topic_records()?
            .into_iter()
            .map(|topic| ordinary_contract::OrchestratorTopic {
                path: topic.path,
                name: topic.name,
                parent: topic.parent,
            })
            .collect())
    }

    fn register_orchestrator_agent(
        &mut self,
        registration: ordinary_contract::OrchestratorAgentRegistration,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        // Automatic seating defers to the topic judge, which is shelved this
        // phase: it fails closed with `JudgeUnavailable`, carrying the current
        // topic list so the caller can retry with an explicit selection. There
        // is no catch-all fallback seat.
        let selected_paths = match registration.topic_selection {
            ordinary_contract::TopicSelection::Automatic => {
                return Ok(ordinary_contract::OrchestrateReply::AgentRegistrationRejected(
                    ordinary_contract::AgentRegistrationRejected {
                        reason: ordinary_contract::AgentRegistrationRejectionReason::JudgeUnavailable,
                        available_topics: self.orchestrator_topics()?,
                    },
                ));
            }
            ordinary_contract::TopicSelection::Explicit(paths) => paths,
        };
        // Explicit registration lets the agent author its own topics: every
        // topic implied by a selected path is created (parents first), an
        // existing topic is joined rather than duplicated, and the agent is
        // seated on the leaf it named. `UnknownTopic` is therefore unreachable
        // from this path — the reason is reserved for the future judge path
        // that may validate a reuse-topic the model named — so no selection is
        // rejected for naming an absent topic.
        let agent = self.service.tables().register_orchestrator_agent(
            registration.session,
            registration.mission,
            registration.harness,
        )?;
        let mut assigned_topics = Vec::new();
        for path in selected_paths {
            let mut seated_leaf = None;
            for topic in path.lineage()? {
                seated_leaf = Some(self.service.tables().ensure_orchestrator_topic(
                    topic.path,
                    topic.name,
                    topic.parent,
                )?);
            }
            if let Some(leaf) = seated_leaf {
                self.service
                    .tables()
                    .seat_agent_on_topic(agent.agent_identifier.clone(), leaf.path.clone())?;
                assigned_topics.push(leaf.into_orchestrator_topic());
            }
        }
        self.discover_agent_reachability(&agent.agent_identifier)?;
        Ok(ordinary_contract::OrchestrateReply::AgentRegistered(
            ordinary_contract::AgentRegistered {
                agent_identifier: agent.agent_identifier,
                assigned_topics,
                assignment_source: ordinary_contract::TopicAssignmentSource::Explicit,
            },
        ))
    }

    /// Discover and persist the registering agent's reachability from the peer's
    /// kernel-vouched pid: walk the caller's `/proc` ancestry and match it
    /// against the terminal-cell session index, attaching the endpoint on a
    /// match. A direct contract-level caller supplies no pid, and no match
    /// leaves the agent registered without reachability — its identity and
    /// topics are valid regardless, and delivery parks until an endpoint exists.
    fn discover_agent_reachability(
        &mut self,
        agent_identifier: &ordinary_contract::OrchestratorAgentIdentifier,
    ) -> Result<()> {
        let Some(caller_process_id) = self.service.take_pending_caller_process_id() else {
            return Ok(());
        };
        let Some(reachability) =
            AgentReachabilityDiscovery::from_process_environment().discover(caller_process_id)
        else {
            return Ok(());
        };
        self.service
            .tables()
            .attach_agent_reachability(agent_identifier, reachability.clone())?;
        self.propagate_registration_to_router(agent_identifier, &reachability)?;
        Ok(())
    }

    /// Propagate a discovered registration to the router so the minted identity
    /// becomes a live delivery target. The router is a co-resident peer, so this
    /// is best-effort: a router that is unreachable or that refuses the
    /// registration is recorded as a divergence (the router leg of the
    /// registration did not apply), never a failure of the agent's own
    /// registration. When no router socket is configured, propagation is skipped
    /// with no divergence.
    fn propagate_registration_to_router(
        &self,
        agent_identifier: &ordinary_contract::OrchestratorAgentIdentifier,
        reachability: &StoredAgentReachability,
    ) -> Result<()> {
        let Some(socket_path) = self.service.router_registration_endpoint() else {
            return Ok(());
        };
        match RouterActorRegistration::new(socket_path.to_path_buf())
            .register(agent_identifier, reachability)
        {
            Ok(_disposition) => Ok(()),
            Err(degradation) => {
                self.record_router_registration_divergence(agent_identifier, degradation)
            }
        }
    }

    /// Record a router-registration degradation as a divergence: the router
    /// downstream leg failed while the agent's own registration succeeded. An
    /// unreachable router maps to `Unreachable`; a router refusal maps to
    /// `Rejected` carrying the typed reason.
    fn record_router_registration_divergence(
        &self,
        agent_identifier: &ordinary_contract::OrchestratorAgentIdentifier,
        degradation: RouterRegistrationDegradation,
    ) -> Result<()> {
        let (reason, detail) = match degradation {
            RouterRegistrationDegradation::Unreachable(detail) => (
                ordinary_contract::ApplicationFailureReason::Unreachable,
                format!(
                    "router registration for agent {} degraded: {detail}",
                    agent_identifier.as_str()
                ),
            ),
            RouterRegistrationDegradation::Rejected(refusal) => (
                ordinary_contract::ApplicationFailureReason::Rejected,
                format!(
                    "router refused registration for agent {}: {}",
                    agent_identifier.as_str(),
                    Self::router_refusal_detail(refusal)
                ),
            ),
        };
        let failure = ordinary_contract::ApplicationFailure {
            component: ordinary_contract::DownstreamComponent::Router,
            reason,
            detail: ordinary_contract::ScopeReason::from_text(detail)?,
        };
        self.service
            .tables()
            .append_divergence(ordinary_contract::PartialApplied {
                succeeded: Vec::new(),
                failed: vec![failure],
            })?;
        Ok(())
    }

    fn router_refusal_detail(
        refusal: signal_router::ActorRegistrationRefusalReason,
    ) -> &'static str {
        match refusal {
            signal_router::ActorRegistrationRefusalReason::ProcessIdentifierOutOfRange => {
                "process identifier out of range"
            }
            signal_router::ActorRegistrationRefusalReason::RemoteRouterEndpointNotLocal => {
                "remote-router endpoint is not a local delivery target"
            }
        }
    }

    fn observe_orchestrator_topic(
        &self,
        path: ordinary_contract::OrchestratorTopicPath,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let topics = self.orchestrator_topics()?;
        let topic = topics
            .into_iter()
            .find(|topic| topic.path == path)
            .ok_or_else(|| Error::SchemaBridge {
                message: "requested orchestrator topic is absent".to_string(),
            })?;
        let member_agent_identifiers = self
            .service
            .tables()
            .topic_member_identifiers(&topic.path)?;
        Ok(ordinary_contract::OrchestrateReply::TopicDetail(
            ordinary_contract::TopicDetail {
                topic,
                member_agent_identifiers,
            },
        ))
    }

    fn observe_orchestrator_agents(&self) -> Result<ordinary_contract::OrchestrateReply> {
        let agents = self
            .service
            .tables()
            .orchestrator_agent_records()?
            .into_iter()
            .map(|agent| {
                let topics = self
                    .service
                    .tables()
                    .agent_topic_paths(&agent.agent_identifier)?;
                Ok(ordinary_contract::OrchestratorAgentSummary {
                    agent_identifier: agent.agent_identifier,
                    mission: agent.mission,
                    topics,
                    status: agent.status,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ordinary_contract::OrchestrateReply::AgentDirectory(
            ordinary_contract::AgentDirectory { agents },
        ))
    }
}

impl ProjectInto<ordinary_schema::OrchestratorAgentSummary>
    for ordinary_contract::OrchestratorAgentSummary
{
    fn project_into(self) -> Result<ordinary_schema::OrchestratorAgentSummary> {
        Ok(ordinary_schema::OrchestratorAgentSummary {
            orchestrator_agent_identifier: self.agent_identifier.project_into()?,
            mission_description: self.mission.project_into()?,
            orchestrator_topic_paths: ordinary_schema::OrchestratorTopicPaths::new(
                self.topics.project_into()?,
            ),
            orchestrator_agent_status: self.status.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::OrchestratorAgentSummary>
    for ordinary_schema::OrchestratorAgentSummary
{
    fn project_into(self) -> Result<ordinary_contract::OrchestratorAgentSummary> {
        Ok(ordinary_contract::OrchestratorAgentSummary {
            agent_identifier: self.orchestrator_agent_identifier.project_into()?,
            mission: self.mission_description.project_into()?,
            topics: self
                .orchestrator_topic_paths
                .into_payload()
                .project_into()?,
            status: self.orchestrator_agent_status.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::TopicDetail> for ordinary_contract::TopicDetail {
    fn project_into(self) -> Result<ordinary_schema::TopicDetail> {
        Ok(ordinary_schema::TopicDetail {
            orchestrator_topic: self.topic.project_into()?,
            orchestrator_agent_identifiers: ordinary_schema::OrchestratorAgentIdentifiers::new(
                self.member_agent_identifiers.project_into()?,
            ),
        })
    }
}

impl ProjectInto<ordinary_contract::TopicDetail> for ordinary_schema::TopicDetail {
    fn project_into(self) -> Result<ordinary_contract::TopicDetail> {
        Ok(ordinary_contract::TopicDetail {
            topic: self.orchestrator_topic.project_into()?,
            member_agent_identifiers: self
                .orchestrator_agent_identifiers
                .into_payload()
                .project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::AgentDirectory> for ordinary_contract::AgentDirectory {
    fn project_into(self) -> Result<ordinary_schema::AgentDirectory> {
        Ok(ordinary_schema::AgentDirectory::new(
            ordinary_schema::OrchestratorAgentSummaries::new(self.agents.project_into()?),
        ))
    }
}

impl ProjectInto<ordinary_contract::AgentDirectory> for ordinary_schema::AgentDirectory {
    fn project_into(self) -> Result<ordinary_contract::AgentDirectory> {
        Ok(ordinary_contract::AgentDirectory {
            agents: self.into_payload().into_payload().project_into()?,
        })
    }
}
