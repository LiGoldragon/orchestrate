use meta_signal_orchestrate as meta_contract;
use signal_frame::{
    BatchFailureReason, CommitStatus, NonEmpty, Reply, RetryClassification, SubReply,
};
use signal_orchestrate as ordinary_contract;

use crate::{
    ActivityLedger, ClaimLedger, Error, HarnessModelResolver, LaneRegistry,
    MessengerRegistrationDegradation, MessengerRegistryPush, MetaHarnessResolver,
    OrchestrateService, OrchestratorAgentStatus, RepositoryRegistry, Result, RoleRegistry,
    StoredTriageRejectionReason, StoredTriageVerdict, WorkflowRunner, WorktreeRegistry,
};

/// The Dotos delivery note an orchestrator message becomes on its messenger
/// hop: the semantic sender rides here because the messenger's own
/// provenance names the transport peer (this daemon), not the sending agent.
#[derive(dotos::DotosEncode, Debug, Clone, PartialEq, Eq)]
struct OrchestratorMessageDeliveryNote {
    sender: String,
    kind: signal_orchestrator_message::OrchestratorMessageKind,
    subject: String,
    content: String,
}

/// Direct contract execution over the sema-backed orchestration ledgers.
///
/// The public Signal contracts arrive through the Rust binding generated from
/// their canonical Ethos declarations. The runtime executes those values
/// directly, with no second vocabulary or projection bridge.
pub struct OrchestratorExecution<'service> {
    service: &'service mut OrchestrateService,
}

impl<'service> OrchestratorExecution<'service> {
    pub fn new(service: &'service mut OrchestrateService) -> Self {
        Self { service }
    }

    pub fn execute_ordinary(
        mut self,
        request: signal_frame::Request<ordinary_contract::OrchestrateRequest>,
    ) -> (Reply<ordinary_contract::OrchestrateReply>, Option<Error>) {
        let operation_count = request.payloads().len();
        if operation_count != 1 {
            let error = Error::UnsupportedAtomicBatch { operation_count };
            return (Self::batch_aborted_reply(), Some(error));
        }
        match self.apply_ordinary_request(request.payloads.into_head()) {
            Ok(reply) => (
                Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
                None,
            ),
            Err(error) => (Self::batch_aborted_reply(), Some(error)),
        }
    }

    pub fn execute_meta(
        mut self,
        request: signal_frame::Request<meta_contract::MetaOrchestrateRequest>,
    ) -> (Reply<meta_contract::MetaOrchestrateReply>, Option<Error>) {
        let operation_count = request.payloads().len();
        if operation_count != 1 {
            let error = Error::UnsupportedAtomicBatch { operation_count };
            return (Self::batch_aborted_reply(), Some(error));
        }
        match self.apply_meta_request(request.payloads.into_head()) {
            Ok(reply) => (
                Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
                None,
            ),
            Err(error) => (Self::batch_aborted_reply(), Some(error)),
        }
    }

    fn batch_aborted_reply<Payload>() -> Reply<Payload> {
        Reply::batch_aborted(
            BatchFailureReason::EngineRejected,
            RetryClassification::NotRetryable,
            CommitStatus::NotCommitted,
            NonEmpty::single(SubReply::Invalidated),
        )
    }

    fn observe_ordinary_request(
        &self,
        observation: ordinary_contract::Observation,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        match observation {
            ordinary_contract::Observation::Roles => {
                ClaimLedger::new(self.service.tables()).observe()
            }
            ordinary_contract::Observation::Sessions => {
                LaneRegistry::new(self.service.tables()).observe_sessions()
            }
            ordinary_contract::Observation::SessionLanes(session) => {
                LaneRegistry::new(self.service.tables()).observe_session(session)
            }
            ordinary_contract::Observation::Lanes => {
                LaneRegistry::new(self.service.tables()).observe()
            }
            ordinary_contract::Observation::Worktrees => {
                WorktreeRegistry::new(self.service.tables()).observe()
            }
            ordinary_contract::Observation::Repositories => {
                RepositoryRegistry::new(self.service.tables()).observe()
            }
            ordinary_contract::Observation::Topics => Ok(
                ordinary_contract::OrchestrateReply::TopicTree(ordinary_contract::TopicTree {
                    topics: self.orchestrator_topics()?,
                }),
            ),
            ordinary_contract::Observation::Topic(path) => self.observe_orchestrator_topic(path),
            ordinary_contract::Observation::Agents => self.observe_orchestrator_agents(),
        }
    }

    fn apply_ordinary_request(
        &mut self,
        request: ordinary_contract::OrchestrateRequest,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let reply = match request {
            ordinary_contract::OrchestrateRequest::Claim(claim) => {
                ClaimLedger::new(self.service.tables()).apply_claim(claim)?
            }
            ordinary_contract::OrchestrateRequest::Release(release) => {
                ClaimLedger::new(self.service.tables()).apply_release(release)?
            }
            ordinary_contract::OrchestrateRequest::Handoff(handoff) => {
                ClaimLedger::new(self.service.tables()).apply_handoff(handoff)?
            }
            ordinary_contract::OrchestrateRequest::Observe(observation) => {
                self.observe_ordinary_request(observation)?
            }
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
                WorktreeRegistry::new(self.service.tables()).request(order)?
            }
            ordinary_contract::OrchestrateRequest::ConcludeWorktree(order) => {
                WorktreeRegistry::new(self.service.tables()).conclude(order)?
            }
            ordinary_contract::OrchestrateRequest::MintAgentIdentity(request) => {
                self.mint_agent_identity(request)?
            }
            ordinary_contract::OrchestrateRequest::LaunchAgent(request) => {
                self.launch_agent(request, &MetaHarnessResolver::from_process())?
            }
            ordinary_contract::OrchestrateRequest::SendOrchestratorMessage(submission) => {
                self.send_orchestrator_message(submission)?
            }
        };
        Ok(reply)
    }

    fn apply_meta_request(
        &mut self,
        request: meta_contract::MetaOrchestrateRequest,
    ) -> Result<meta_contract::MetaOrchestrateReply> {
        let reply = match request {
            meta_contract::MetaOrchestrateRequest::Create(order) => {
                RoleRegistry::new(self.service.tables(), self.service.layout())
                    .create_role(order)?
            }
            meta_contract::MetaOrchestrateRequest::Retire(meta_contract::Retirement::Role(
                order,
            )) => RoleRegistry::new(self.service.tables(), self.service.layout())
                .retire_role(order)?,
            meta_contract::MetaOrchestrateRequest::Retire(meta_contract::Retirement::Lane(
                lane,
            )) => LaneRegistry::new(self.service.tables()).retire(lane)?,
            meta_contract::MetaOrchestrateRequest::Refresh(_order) => {
                RepositoryRegistry::new(self.service.tables()).refresh()?
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
                WorktreeRegistry::new(self.service.tables()).register(order)?
            }
            meta_contract::MetaOrchestrateRequest::RefreshWorktreeIndex(_order) => {
                WorktreeRegistry::new(self.service.tables()).refresh()?
            }
            meta_contract::MetaOrchestrateRequest::ArchiveWorktree(order) => {
                WorktreeRegistry::new(self.service.tables()).archive(order)?
            }
            meta_contract::MetaOrchestrateRequest::ForceRemoveRegistryRow(_) => {
                meta_contract::MetaOrchestrateReply::MetaOrchestrateRequestUnimplemented(
                    meta_contract::MetaOrchestrateRequestUnimplemented {
                        operation: meta_contract::MetaOperationKind::ForceRemoveRegistryRow,
                        reason: meta_contract::MetaOrchestrateUnimplementedReason::NotBuiltYet,
                    },
                )
            }
        };
        Ok(reply)
    }
}

impl OrchestratorExecution<'_> {
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
            registration.minted_identity,
        )?;
        self.propagate_identity_to_messenger(&agent.agent_identifier)?;
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
        Ok(ordinary_contract::OrchestrateReply::AgentRegistered(
            ordinary_contract::AgentRegistered {
                agent_identifier: agent.agent_identifier,
                assigned_topics,
                assignment_source: ordinary_contract::TopicAssignmentSource::Explicit,
            },
        ))
    }

    /// Allocate an agent identity ahead of launch: the orchestrator is the
    /// mint (psyche-ruled 2026-07-17). The reservation is seated `Allocated`
    /// in orchestrate's registry, pushed into the messenger's durable
    /// registry, and returned so the launcher can hand it to the process in
    /// its initial prompt. Registration with the pre-minted identity later
    /// binds it `Active`.
    /// Triage and route one orchestrator message (packet 3.4). The judge is
    /// shelved: a message to a registered agent routes as-is; an escalation
    /// (recipient `Orchestrator`) has no coordinator seat to land on and
    /// refuses typed `MissingCoordinator`. Every triage decision — routed or
    /// rejected — is committed to the bounded audit table first; the
    /// messenger hop is best-effort and degrades in the reply, never
    /// retroactively failing the triage.
    fn send_orchestrator_message(
        &mut self,
        submission: ordinary_contract::OrchestratorMessageSubmission,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let sender = submission.sender.clone();
        let stored_kind = submission.message.kind.clone();
        if self
            .service
            .tables()
            .orchestrator_agent_record(&sender)?
            .is_none()
        {
            self.service.tables().append_orchestrator_triage_record(
                sender,
                stored_kind,
                StoredTriageVerdict::Reject {
                    reason: StoredTriageRejectionReason::SenderNotRegistered,
                },
            )?;
            return Ok(Self::orchestrator_message_rejected(
                ordinary_contract::OrchestratorMessageRejection::SenderNotRegistered,
            ));
        }
        match submission.recipient {
            ordinary_contract::OrchestratorMessageRecipient::Orchestrator => {
                self.service.tables().append_orchestrator_triage_record(
                    sender,
                    stored_kind,
                    StoredTriageVerdict::Escalate,
                )?;
                Ok(Self::orchestrator_message_rejected(
                    ordinary_contract::OrchestratorMessageRejection::MissingCoordinator,
                ))
            }
            ordinary_contract::OrchestratorMessageRecipient::Agent(recipient) => {
                if self
                    .service
                    .tables()
                    .orchestrator_agent_record(&recipient)?
                    .is_none()
                {
                    self.service.tables().append_orchestrator_triage_record(
                        sender,
                        stored_kind,
                        StoredTriageVerdict::Reject {
                            reason: StoredTriageRejectionReason::NoEligibleRecipient,
                        },
                    )?;
                    return Ok(Self::orchestrator_message_rejected(
                        ordinary_contract::OrchestratorMessageRejection::NoEligibleRecipient,
                    ));
                }
                let record = self.service.tables().append_orchestrator_triage_record(
                    sender.clone(),
                    stored_kind,
                    StoredTriageVerdict::Route {
                        recipients: vec![recipient.clone()],
                        retyped: None,
                    },
                )?;
                let messenger_delivery_state =
                    self.submit_routed_message(&sender, &recipient, &submission.message);
                Ok(
                    ordinary_contract::OrchestrateReply::OrchestratorMessageRouted(
                        ordinary_contract::OrchestratorMessageRouted {
                            triage_slot: record.slot,
                            recipients: vec![recipient],
                            messenger_delivery_state,
                        },
                    ),
                )
            }
        }
    }

    fn orchestrator_message_rejected(
        rejection: ordinary_contract::OrchestratorMessageRejection,
    ) -> ordinary_contract::OrchestrateReply {
        ordinary_contract::OrchestrateReply::OrchestratorMessageRejected(
            ordinary_contract::OrchestratorMessageRejected { rejection },
        )
    }

    /// The best-effort messenger hop for a routed message. No configured
    /// messenger socket is a named degradation (not silence); transport and
    /// refusal degradations carry the push client's detail.
    fn submit_routed_message(
        &self,
        sender: &ordinary_contract::OrchestratorAgentIdentifier,
        recipient: &ordinary_contract::OrchestratorAgentIdentifier,
        message: &signal_orchestrator_message::OrchestratorMessage,
    ) -> ordinary_contract::MessengerDeliveryState {
        let Some(socket_path) = self.service.messenger_registration_endpoint() else {
            return ordinary_contract::MessengerDeliveryState::Degraded(
                ordinary_contract::MessengerDegradationDetail::new(
                    "no messenger socket configured".to_string(),
                ),
            );
        };
        let note = OrchestratorMessageDeliveryNote {
            sender: sender.as_str().to_string(),
            kind: message.kind.clone(),
            subject: message.subject.as_str().to_string(),
            content: message.content.as_str().to_string(),
        };
        match MessengerRegistryPush::new(socket_path.to_path_buf())
            .submit_message(recipient, dotos::DotosEncode::to_dotos(&note))
        {
            Ok(()) => ordinary_contract::MessengerDeliveryState::Submitted,
            Err(MessengerRegistrationDegradation::Unreachable(detail)) => {
                ordinary_contract::MessengerDeliveryState::Degraded(
                    ordinary_contract::MessengerDegradationDetail::new(format!(
                        "messenger unreachable: {detail}"
                    )),
                )
            }
            Err(MessengerRegistrationDegradation::Rejected(detail)) => {
                ordinary_contract::MessengerDeliveryState::Degraded(
                    ordinary_contract::MessengerDegradationDetail::new(format!(
                        "messenger refused: {detail}"
                    )),
                )
            }
        }
    }

    fn mint_agent_identity(
        &mut self,
        request: ordinary_contract::AgentIdentityMintRequest,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let agent = self.service.tables().allocate_orchestrator_agent(
            request.session,
            request.mission,
            request.harness,
        )?;
        self.propagate_identity_to_messenger(&agent.agent_identifier)?;
        Ok(ordinary_contract::OrchestrateReply::AgentIdentityMinted(
            ordinary_contract::AgentIdentityMinted {
                agent_identifier: agent.agent_identifier,
            },
        ))
    }

    /// Launch a previously allocated agent through the harness component
    /// (psyche-ruled §0d Q2: the orchestrator mints, then launches through
    /// the harness). The pre-minted identity rides the initial prompt. A
    /// launch reply carrying a terminal-cell session directory seeds the
    /// row's reachability — and the messenger endpoint push — before the
    /// launched process ever registers.
    fn launch_agent<Commander: HarnessModelResolver>(
        &mut self,
        request: ordinary_contract::AgentLaunchRequest,
        commander: &Commander,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let Some(agent) = self
            .service
            .tables()
            .orchestrator_agent_record(&request.agent_identifier)?
        else {
            return Ok(Self::agent_launch_refused(
                request.agent_identifier,
                ordinary_contract::AgentLaunchRefusalReason::UnknownAgent,
                "no agent registry row holds this identity".to_string(),
            ));
        };
        if agent.status != OrchestratorAgentStatus::Allocated {
            return Ok(Self::agent_launch_refused(
                request.agent_identifier,
                ordinary_contract::AgentLaunchRefusalReason::AgentNotAllocated,
                format!("agent status is {:?}, launch needs Allocated", agent.status),
            ));
        }
        let launch = meta_signal_harness::SessionLaunchRequest {
            harness_kind: match agent.harness {
                ordinary_contract::HarnessKind::Claude => meta_signal_harness::HarnessKind::Claude,
                ordinary_contract::HarnessKind::Codex => meta_signal_harness::HarnessKind::Codex,
            },
            agent_identity: meta_signal_harness::AgentIdentityToken::new(
                agent.agent_identifier.as_str(),
            ),
            initial_prompt: meta_signal_harness::InitialPrompt::new(format!(
                "You are agent {}. {}",
                agent.agent_identifier.as_str(),
                agent.mission.as_str()
            )),
            continuation: meta_signal_harness::ContinuationRequest::Fresh,
        };
        let reply = match commander.launch_session(launch) {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(Self::agent_launch_refused(
                    request.agent_identifier,
                    ordinary_contract::AgentLaunchRefusalReason::HarnessUnreachable,
                    error.to_string(),
                ));
            }
        };
        match reply {
            meta_signal_harness::MetaHarnessReply::SessionLaunched(launched) => {
                Ok(ordinary_contract::OrchestrateReply::AgentLaunched(
                    ordinary_contract::AgentLaunched {
                        agent_identifier: request.agent_identifier,
                        child_process_id: launched.child_process_id,
                        session_directory: launched.session_directory.and_then(|directory| {
                            ordinary_contract::WirePath::from_absolute_path(directory.as_str()).ok()
                        }),
                    },
                ))
            }
            meta_signal_harness::MetaHarnessReply::SessionLaunchRefused(refused) => {
                Ok(Self::agent_launch_refused(
                    request.agent_identifier,
                    ordinary_contract::AgentLaunchRefusalReason::HarnessRefused,
                    format!("{:?}: {}", refused.reason, refused.detail),
                ))
            }
            other => Ok(Self::agent_launch_refused(
                request.agent_identifier,
                ordinary_contract::AgentLaunchRefusalReason::HarnessRefused,
                format!("unexpected harness reply: {other:?}"),
            )),
        }
    }

    fn agent_launch_refused(
        agent_identifier: ordinary_contract::OrchestratorAgentIdentifier,
        reason: ordinary_contract::AgentLaunchRefusalReason,
        detail: String,
    ) -> ordinary_contract::OrchestrateReply {
        ordinary_contract::OrchestrateReply::AgentLaunchRefused(
            ordinary_contract::AgentLaunchRefused {
                agent_identifier,
                reason,
                detail,
            },
        )
    }

    /// Push a minted or registered identity into the messenger's durable
    /// registry so the messenger holds the consumer view of identity. The
    /// messenger is a co-resident peer, so this is best-effort: an
    /// unreachable or refusing messenger is recorded as a divergence, never a
    /// failure of the mint or registration itself. When no messenger socket
    /// is configured, the push is skipped with no divergence.
    fn propagate_identity_to_messenger(
        &self,
        agent_identifier: &ordinary_contract::OrchestratorAgentIdentifier,
    ) -> Result<()> {
        let Some(socket_path) = self.service.messenger_registration_endpoint() else {
            return Ok(());
        };
        match MessengerRegistryPush::new(socket_path.to_path_buf()).seat_identity(agent_identifier)
        {
            Ok(()) => Ok(()),
            Err(degradation) => {
                self.record_messenger_push_divergence(agent_identifier, degradation)
            }
        }
    }

    /// Record a messenger-push degradation as a divergence: the messenger
    /// downstream leg failed while the identity operation itself succeeded.
    fn record_messenger_push_divergence(
        &self,
        agent_identifier: &ordinary_contract::OrchestratorAgentIdentifier,
        degradation: MessengerRegistrationDegradation,
    ) -> Result<()> {
        let (reason, detail) = match degradation {
            MessengerRegistrationDegradation::Unreachable(detail) => (
                ordinary_contract::ApplicationFailureReason::Unreachable,
                format!(
                    "messenger registry push for agent {} degraded: {detail}",
                    agent_identifier.as_str()
                ),
            ),
            MessengerRegistrationDegradation::Rejected(detail) => (
                ordinary_contract::ApplicationFailureReason::Rejected,
                format!(
                    "messenger refused registry push for agent {}: {detail}",
                    agent_identifier.as_str()
                ),
            ),
        };
        let failure = ordinary_contract::ApplicationFailure {
            component: ordinary_contract::DownstreamComponent::Message,
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

    fn observe_orchestrator_topic(
        &self,
        path: ordinary_contract::OrchestratorTopicPath,
    ) -> Result<ordinary_contract::OrchestrateReply> {
        let topics = self.orchestrator_topics()?;
        let topic = topics
            .into_iter()
            .find(|topic| topic.path == path)
            .ok_or_else(|| Error::OrchestratorTopicNotFound {
                path: path.as_str().to_owned(),
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
