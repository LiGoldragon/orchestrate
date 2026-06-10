use meta_signal_orchestrate as meta_contract;
use meta_signal_orchestrate::schema::lib as meta_schema;
use signal_frame::{
    BatchFailureReason, CommitStatus, NonEmpty, Reply, RetryClassification, SubReply,
};
use signal_orchestrate as ordinary_contract;
use signal_orchestrate::schema::lib as ordinary_schema;

use crate::schema::{nexus as nexus_schema, sema as sema_schema};
use crate::{
    ActivityLedger, ClaimLedger, Error, LaneRegistry, OrchestrateService, RepositoryRegistry,
    Result, RoleRegistry,
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
        let signal_input = nexus_schema::SignalInput::ordinary_input(input);
        match OrchestrateRequestExecution::drive_nexus(self, signal_input).await? {
            nexus_schema::SignalOutput::OrdinaryOutput(output) => Ok(output),
            nexus_schema::SignalOutput::MetaOutput(_) => Err(Error::NexusReplyTierMismatch {
                expected: "ordinary",
                actual: "meta",
            }),
        }
    }

    /// Drive one meta `Input` (the owner-only policy root) through the nexus
    /// runner and return the meta schema `Output` root.
    pub async fn handle_signal_meta_input(
        &mut self,
        input: meta_schema::Input,
    ) -> Result<meta_schema::Output> {
        let signal_input = nexus_schema::SignalInput::meta_input(input);
        match MetaRequestExecution::drive_nexus(self, signal_input).await? {
            nexus_schema::SignalOutput::MetaOutput(output) => Ok(output),
            nexus_schema::SignalOutput::OrdinaryOutput(_) => Err(Error::NexusReplyTierMismatch {
                expected: "meta",
                actual: "ordinary",
            }),
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
                ordinary_contract::Observation::Lanes,
            ) => LaneRegistry::new(self.service.tables()).observe()?,
            ordinary_contract::OrchestrateRequest::Submit(submission) => {
                ActivityLedger::new(self.service.tables()).submit(submission)?
            }
            ordinary_contract::OrchestrateRequest::Query(query) => {
                ActivityLedger::new(self.service.tables()).query(query)?
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
        };
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
            meta_contract::MetaOrchestrateRequest::SetAuthority(change) => {
                LaneRegistry::new(self.service.tables()).set_authority(change)?
            }
        };
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

    fn partial_applied(self) -> ordinary_schema::PartialApplied {
        ordinary_schema::PartialApplied {
            succeeded: Vec::new(),
            failed: vec![ordinary_schema::ApplicationFailure {
                component: ordinary_schema::DownstreamComponent::System,
                reason: ordinary_schema::ApplicationFailureReason::Unknown,
                detail: self.detail,
            }],
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

impl ProjectInto<ordinary_schema::LaneRegistration> for ordinary_contract::LaneRegistration {
    fn project_into(self) -> Result<ordinary_schema::LaneRegistration> {
        Ok(ordinary_schema::LaneRegistration {
            lane: self.lane.project_into()?,
            role: self.role.project_into()?,
            authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::LaneRegistration> for ordinary_schema::LaneRegistration {
    fn project_into(self) -> Result<ordinary_contract::LaneRegistration> {
        Ok(ordinary_contract::LaneRegistration {
            lane: self.lane.project_into()?,
            role: self.role.project_into()?,
            authority: self.authority.project_into()?,
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
            role: self.role.project_into()?,
            scopes: self.scopes.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleClaim> for ordinary_schema::RoleClaim {
    fn project_into(self) -> Result<ordinary_contract::RoleClaim> {
        Ok(ordinary_contract::RoleClaim {
            role: self.role.project_into()?,
            scopes: self.scopes.project_into()?,
            reason: self.reason.project_into()?,
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
            scopes: self.scopes.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleHandoff> for ordinary_schema::RoleHandoff {
    fn project_into(self) -> Result<ordinary_contract::RoleHandoff> {
        Ok(ordinary_contract::RoleHandoff {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scopes: self.scopes.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::Observation> for ordinary_contract::Observation {
    fn project_into(self) -> Result<ordinary_schema::Observation> {
        Ok(match self {
            ordinary_contract::Observation::Roles => ordinary_schema::Observation::Roles,
            ordinary_contract::Observation::Lanes => ordinary_schema::Observation::Lanes,
        })
    }
}

impl ProjectInto<ordinary_contract::Observation> for ordinary_schema::Observation {
    fn project_into(self) -> Result<ordinary_contract::Observation> {
        Ok(match self {
            ordinary_schema::Observation::Roles => ordinary_contract::Observation::Roles,
            ordinary_schema::Observation::Lanes => ordinary_contract::Observation::Lanes,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivitySubmission> for ordinary_contract::ActivitySubmission {
    fn project_into(self) -> Result<ordinary_schema::ActivitySubmission> {
        Ok(ordinary_schema::ActivitySubmission {
            role: self.role.project_into()?,
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ActivitySubmission> for ordinary_schema::ActivitySubmission {
    fn project_into(self) -> Result<ordinary_contract::ActivitySubmission> {
        Ok(ordinary_contract::ActivitySubmission {
            role: self.role.project_into()?,
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ActivityQuery> for ordinary_contract::ActivityQuery {
    fn project_into(self) -> Result<ordinary_schema::ActivityQuery> {
        Ok(ordinary_schema::ActivityQuery {
            limit: u64::from(self.limit),
            filters: self.filters.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ActivityQuery> for ordinary_schema::ActivityQuery {
    fn project_into(self) -> Result<ordinary_contract::ActivityQuery> {
        Ok(ordinary_contract::ActivityQuery {
            limit: u32::try_from(self.limit).map_err(|error| Error::SchemaBridge {
                message: format!("activity query limit does not fit u32: {error}"),
            })?,
            filters: self.filters.project_into()?,
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
            ordinary_contract::OrchestrateRequest::Watch(payload) => {
                ordinary_schema::Input::watch(payload.project_into()?)
            }
            ordinary_contract::OrchestrateRequest::Unwatch(payload) => {
                ordinary_schema::Input::unwatch(payload.value())
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
            ordinary_schema::Input::Watch(payload) => {
                ordinary_contract::OrchestrateRequest::Watch(payload.project_into()?)
            }
            ordinary_schema::Input::Unwatch(payload) => {
                ordinary_contract::OrchestrateRequest::Unwatch(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimAcceptance> for ordinary_contract::ClaimAcceptance {
    fn project_into(self) -> Result<ordinary_schema::ClaimAcceptance> {
        Ok(ordinary_schema::ClaimAcceptance {
            role: self.role.project_into()?,
            scopes: self.scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimAcceptance> for ordinary_schema::ClaimAcceptance {
    fn project_into(self) -> Result<ordinary_contract::ClaimAcceptance> {
        Ok(ordinary_contract::ClaimAcceptance {
            role: self.role.project_into()?,
            scopes: self.scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ScopeConflict> for ordinary_contract::ScopeConflict {
    fn project_into(self) -> Result<ordinary_schema::ScopeConflict> {
        Ok(ordinary_schema::ScopeConflict {
            scope: self.scope.project_into()?,
            held_by: self.held_by.project_into()?,
            held_reason: self.held_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ScopeConflict> for ordinary_schema::ScopeConflict {
    fn project_into(self) -> Result<ordinary_contract::ScopeConflict> {
        Ok(ordinary_contract::ScopeConflict {
            scope: self.scope.project_into()?,
            held_by: self.held_by.project_into()?,
            held_reason: self.held_reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimRejection> for ordinary_contract::ClaimRejection {
    fn project_into(self) -> Result<ordinary_schema::ClaimRejection> {
        Ok(ordinary_schema::ClaimRejection {
            role: self.role.project_into()?,
            conflicts: self.conflicts.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimRejection> for ordinary_schema::ClaimRejection {
    fn project_into(self) -> Result<ordinary_contract::ClaimRejection> {
        Ok(ordinary_contract::ClaimRejection {
            role: self.role.project_into()?,
            conflicts: self.conflicts.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ReleaseAcknowledgment>
    for ordinary_contract::ReleaseAcknowledgment
{
    fn project_into(self) -> Result<ordinary_schema::ReleaseAcknowledgment> {
        Ok(ordinary_schema::ReleaseAcknowledgment {
            role: self.role.project_into()?,
            released_scopes: self.released_scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ReleaseAcknowledgment>
    for ordinary_schema::ReleaseAcknowledgment
{
    fn project_into(self) -> Result<ordinary_contract::ReleaseAcknowledgment> {
        Ok(ordinary_contract::ReleaseAcknowledgment {
            role: self.role.project_into()?,
            released_scopes: self.released_scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::HandoffAcceptance> for ordinary_contract::HandoffAcceptance {
    fn project_into(self) -> Result<ordinary_schema::HandoffAcceptance> {
        Ok(ordinary_schema::HandoffAcceptance {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scopes: self.scopes.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::HandoffAcceptance> for ordinary_schema::HandoffAcceptance {
    fn project_into(self) -> Result<ordinary_contract::HandoffAcceptance> {
        Ok(ordinary_contract::HandoffAcceptance {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            scopes: self.scopes.project_into()?,
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
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::HandoffRejection> for ordinary_schema::HandoffRejection {
    fn project_into(self) -> Result<ordinary_contract::HandoffRejection> {
        Ok(ordinary_contract::HandoffRejection {
            from: self.from.project_into()?,
            to: self.to.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ClaimEntry> for ordinary_contract::ClaimEntry {
    fn project_into(self) -> Result<ordinary_schema::ClaimEntry> {
        Ok(ordinary_schema::ClaimEntry {
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ClaimEntry> for ordinary_schema::ClaimEntry {
    fn project_into(self) -> Result<ordinary_contract::ClaimEntry> {
        Ok(ordinary_contract::ClaimEntry {
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::Activity> for ordinary_contract::Activity {
    fn project_into(self) -> Result<ordinary_schema::Activity> {
        Ok(ordinary_schema::Activity {
            role: self.role.project_into()?,
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
            stamped_at: self.stamped_at.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::Activity> for ordinary_schema::Activity {
    fn project_into(self) -> Result<ordinary_contract::Activity> {
        Ok(ordinary_contract::Activity {
            role: self.role.project_into()?,
            scope: self.scope.project_into()?,
            reason: self.reason.project_into()?,
            stamped_at: self.stamped_at.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleStatus> for ordinary_contract::RoleStatus {
    fn project_into(self) -> Result<ordinary_schema::RoleStatus> {
        Ok(ordinary_schema::RoleStatus {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
            claims: self.claims.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleStatus> for ordinary_schema::RoleStatus {
    fn project_into(self) -> Result<ordinary_contract::RoleStatus> {
        Ok(ordinary_contract::RoleStatus {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
            claims: self.claims.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::RoleSnapshot> for ordinary_contract::RoleSnapshot {
    fn project_into(self) -> Result<ordinary_schema::RoleSnapshot> {
        Ok(ordinary_schema::RoleSnapshot {
            roles: self.roles.project_into()?,
            recent_activity: self.recent_activity.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::RoleSnapshot> for ordinary_schema::RoleSnapshot {
    fn project_into(self) -> Result<ordinary_contract::RoleSnapshot> {
        Ok(ordinary_contract::RoleSnapshot {
            roles: self.roles.project_into()?,
            recent_activity: self.recent_activity.project_into()?,
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
            component: self.component.project_into()?,
            detail: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ApplicationSuccess> for ordinary_schema::ApplicationSuccess {
    fn project_into(self) -> Result<ordinary_contract::ApplicationSuccess> {
        Ok(ordinary_contract::ApplicationSuccess {
            component: self.component.project_into()?,
            detail: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::ApplicationFailure> for ordinary_contract::ApplicationFailure {
    fn project_into(self) -> Result<ordinary_schema::ApplicationFailure> {
        Ok(ordinary_schema::ApplicationFailure {
            component: self.component.project_into()?,
            reason: self.reason.project_into()?,
            detail: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::ApplicationFailure> for ordinary_schema::ApplicationFailure {
    fn project_into(self) -> Result<ordinary_contract::ApplicationFailure> {
        Ok(ordinary_contract::ApplicationFailure {
            component: self.component.project_into()?,
            reason: self.reason.project_into()?,
            detail: self.detail.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_schema::PartialApplied> for ordinary_contract::PartialApplied {
    fn project_into(self) -> Result<ordinary_schema::PartialApplied> {
        Ok(ordinary_schema::PartialApplied {
            succeeded: self.succeeded.project_into()?,
            failed: self.failed.project_into()?,
        })
    }
}

impl ProjectInto<ordinary_contract::PartialApplied> for ordinary_schema::PartialApplied {
    fn project_into(self) -> Result<ordinary_contract::PartialApplied> {
        Ok(ordinary_contract::PartialApplied {
            succeeded: self.succeeded.project_into()?,
            failed: self.failed.project_into()?,
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
            ordinary_contract::OrchestrateReply::LanesObserved(payload) => {
                ordinary_schema::Output::LanesObserved(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ActivityAcknowledgment(payload) => {
                ordinary_schema::Output::ActivityAcknowledgment(payload.project_into()?)
            }
            ordinary_contract::OrchestrateReply::ActivityList(payload) => {
                ordinary_schema::Output::ActivityList(payload.project_into()?)
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
            ordinary_schema::Output::LanesObserved(payload) => {
                ordinary_contract::OrchestrateReply::LanesObserved(payload.project_into()?)
            }
            ordinary_schema::Output::ActivityAcknowledgment(payload) => {
                ordinary_contract::OrchestrateReply::ActivityAcknowledgment(payload.project_into()?)
            }
            ordinary_schema::Output::ActivityList(payload) => {
                ordinary_contract::OrchestrateReply::ActivityList(payload.project_into()?)
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
        })
    }
}

impl ProjectInto<meta_schema::CreateRoleOrder> for meta_contract::CreateRoleOrder {
    fn project_into(self) -> Result<meta_schema::CreateRoleOrder> {
        Ok(meta_schema::CreateRoleOrder {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::CreateRoleOrder> for meta_schema::CreateRoleOrder {
    fn project_into(self) -> Result<meta_contract::CreateRoleOrder> {
        Ok(meta_contract::CreateRoleOrder {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
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

impl ProjectInto<meta_schema::LaneRegistrationRequest> for meta_contract::LaneRegistrationRequest {
    fn project_into(self) -> Result<meta_schema::LaneRegistrationRequest> {
        Ok(meta_schema::LaneRegistrationRequest {
            role: self.role.project_into()?,
            authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneRegistrationRequest> for meta_schema::LaneRegistrationRequest {
    fn project_into(self) -> Result<meta_contract::LaneRegistrationRequest> {
        Ok(meta_contract::LaneRegistrationRequest {
            role: self.role.project_into()?,
            authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::LaneAuthorityChange> for meta_contract::LaneAuthorityChange {
    fn project_into(self) -> Result<meta_schema::LaneAuthorityChange> {
        Ok(meta_schema::LaneAuthorityChange {
            lane: self.lane.project_into()?,
            authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneAuthorityChange> for meta_schema::LaneAuthorityChange {
    fn project_into(self) -> Result<meta_contract::LaneAuthorityChange> {
        Ok(meta_contract::LaneAuthorityChange {
            lane: self.lane.project_into()?,
            authority: self.authority.project_into()?,
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
            meta_contract::MetaOrchestrateRequest::SetAuthority(payload) => {
                meta_schema::Input::set_authority(payload.project_into()?)
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
            meta_schema::Input::SetAuthority(payload) => {
                meta_contract::MetaOrchestrateRequest::SetAuthority(payload.project_into()?)
            }
        })
    }
}

impl ProjectInto<meta_schema::RoleCreated> for meta_contract::RoleCreated {
    fn project_into(self) -> Result<meta_schema::RoleCreated> {
        Ok(meta_schema::RoleCreated {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
            report_repository_path: self.report_repository_path.project_into()?,
            report_lane_path: self.report_lane_path.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::RoleCreated> for meta_schema::RoleCreated {
    fn project_into(self) -> Result<meta_contract::RoleCreated> {
        Ok(meta_contract::RoleCreated {
            role: self.role.project_into()?,
            harness: self.harness.project_into()?,
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
            role: self.role.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::RoleCreationRejected> for meta_schema::RoleCreationRejected {
    fn project_into(self) -> Result<meta_contract::RoleCreationRejected> {
        Ok(meta_contract::RoleCreationRejected {
            role: self.role.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_schema::RepositoryIndexRefreshed>
    for meta_contract::RepositoryIndexRefreshed
{
    fn project_into(self) -> Result<meta_schema::RepositoryIndexRefreshed> {
        Ok(meta_schema::RepositoryIndexRefreshed::new(u64::from(
            self.repositories,
        )))
    }
}

impl ProjectInto<meta_contract::RepositoryIndexRefreshed>
    for meta_schema::RepositoryIndexRefreshed
{
    fn project_into(self) -> Result<meta_contract::RepositoryIndexRefreshed> {
        Ok(meta_contract::RepositoryIndexRefreshed {
            repositories: u32::try_from(self.into_payload()).map_err(|error| {
                Error::SchemaBridge {
                    message: format!("repository count does not fit u32: {error}"),
                }
            })?,
        })
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
            lane: self.lane.project_into()?,
            authority: self.authority.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::LaneAuthoritySet> for meta_schema::LaneAuthoritySet {
    fn project_into(self) -> Result<meta_contract::LaneAuthoritySet> {
        Ok(meta_contract::LaneAuthoritySet {
            lane: self.lane.project_into()?,
            authority: self.authority.project_into()?,
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
            meta_contract::MetaOperationKind::SetAuthority => {
                meta_schema::MetaOperationKind::SetAuthority
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
            meta_schema::MetaOperationKind::SetAuthority => {
                meta_contract::MetaOperationKind::SetAuthority
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
            operation: self.operation.project_into()?,
            reason: self.reason.project_into()?,
        })
    }
}

impl ProjectInto<meta_contract::MetaOrchestrateRequestUnimplemented>
    for meta_schema::MetaOrchestrateRequestUnimplemented
{
    fn project_into(self) -> Result<meta_contract::MetaOrchestrateRequestUnimplemented> {
        Ok(meta_contract::MetaOrchestrateRequestUnimplemented {
            operation: self.operation.project_into()?,
            reason: self.reason.project_into()?,
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
            meta_contract::MetaOrchestrateReply::LaneRetired(payload) => {
                meta_schema::Output::LaneRetired(payload.project_into()?)
            }
            meta_contract::MetaOrchestrateReply::LaneAuthoritySet(payload) => {
                meta_schema::Output::lane_authority_set(payload.project_into()?)
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
            meta_schema::Output::LaneRetired(payload) => {
                meta_contract::MetaOrchestrateReply::LaneRetired(payload.project_into()?)
            }
            meta_schema::Output::LaneAuthoritySet(payload) => {
                meta_contract::MetaOrchestrateReply::LaneAuthoritySet(payload.project_into()?)
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
