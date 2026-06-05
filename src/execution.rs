use meta_signal_orchestrate::{MetaOrchestrateReply, MetaOrchestrateRequest, Retirement};
use signal_executor::{
    BatchEffects, BatchPlan, CommandEffect, CommandExecutor, Lowering as LoweringTrait,
    OperationEffects, OperationPlan,
};
use signal_frame::{BatchFailureReason, CommitStatus, NonEmpty, RetryClassification};
use signal_orchestrate::{Observation, OrchestrateReply, OrchestrateRequest};
use signal_sema::{SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

use crate::{
    ActivityLedger, ClaimLedger, Error, LaneRegistry, OrchestrateService, RepositoryRegistry,
    Result, RoleRegistry,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrdinaryCommand {
    Claim(signal_orchestrate::RoleClaim),
    Release(signal_orchestrate::RoleRelease),
    Handoff(signal_orchestrate::RoleHandoff),
    Observe(Observation),
    Submit(signal_orchestrate::ActivitySubmission),
    Query(signal_orchestrate::ActivityQuery),
    Watch(signal_orchestrate::ObservationSubscription),
    Unwatch(signal_orchestrate::ObservationToken),
}

impl ToSemaOperation for OrdinaryCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::Claim(_) | Self::Submit(_) => SemaOperation::Assert,
            Self::Release(_) | Self::Unwatch(_) => SemaOperation::Retract,
            Self::Handoff(_) => SemaOperation::Mutate,
            Self::Observe(_) | Self::Query(_) => SemaOperation::Match,
            Self::Watch(_) => SemaOperation::Subscribe,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrdinaryEffect {
    Reply(OrchestrateReply),
}

impl OrdinaryEffect {
    fn into_reply(self) -> OrchestrateReply {
        match self {
            Self::Reply(reply) => reply,
        }
    }
}

impl ToSemaOutcome for OrdinaryEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::Reply(OrchestrateReply::ClaimAcceptance(_))
            | Self::Reply(OrchestrateReply::ActivityAcknowledgment(_)) => SemaOutcome::Asserted,
            Self::Reply(OrchestrateReply::ReleaseAcknowledgment(_))
            | Self::Reply(OrchestrateReply::ObservationClosed(_)) => SemaOutcome::Retracted,
            Self::Reply(OrchestrateReply::HandoffAcceptance(_))
            | Self::Reply(OrchestrateReply::PartialApplied(_)) => SemaOutcome::Mutated,
            Self::Reply(OrchestrateReply::RoleSnapshot(_))
            | Self::Reply(OrchestrateReply::LanesObserved(_))
            | Self::Reply(OrchestrateReply::ActivityList(_)) => SemaOutcome::Matched,
            Self::Reply(OrchestrateReply::ObservationOpened(_)) => SemaOutcome::Subscribed,
            Self::Reply(OrchestrateReply::ClaimRejection(_))
            | Self::Reply(OrchestrateReply::HandoffRejection(_)) => SemaOutcome::NoChange,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrdinaryLowering;

impl LoweringTrait for OrdinaryLowering {
    type Operation = OrchestrateRequest;
    type Reply = OrchestrateReply;
    type Command = OrdinaryCommand;
    type ComponentEffect = OrdinaryEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> std::result::Result<OperationPlan<Self::Command>, Self::Reply> {
        let command = match operation {
            OrchestrateRequest::Claim(payload) => OrdinaryCommand::Claim(payload.clone()),
            OrchestrateRequest::Release(payload) => OrdinaryCommand::Release(payload.clone()),
            OrchestrateRequest::Handoff(payload) => OrdinaryCommand::Handoff(payload.clone()),
            OrchestrateRequest::Observe(payload) => OrdinaryCommand::Observe(*payload),
            OrchestrateRequest::Submit(payload) => OrdinaryCommand::Submit(payload.clone()),
            OrchestrateRequest::Query(payload) => OrdinaryCommand::Query(payload.clone()),
            OrchestrateRequest::Watch(payload) => OrdinaryCommand::Watch(payload.clone()),
            OrchestrateRequest::Unwatch(payload) => OrdinaryCommand::Unwatch(*payload),
        };
        Ok(OperationPlan::single(command))
    }

    fn reply_from_effects(
        &self,
        _operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        effects
            .component_effects()
            .last()
            .expect("orchestrate ordinary operation effects are non-empty")
            .clone()
            .into_reply()
    }
}

pub struct OrdinaryCommandExecutor<'service> {
    service: &'service OrchestrateService,
}

impl<'service> OrdinaryCommandExecutor<'service> {
    pub fn new(service: &'service OrchestrateService) -> Self {
        Self { service }
    }

    fn execute_command(
        &self,
        command: OrdinaryCommand,
    ) -> Result<CommandEffect<OrdinaryCommand, OrdinaryEffect>> {
        let reply = match command.clone() {
            OrdinaryCommand::Claim(claim) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_claim(claim)?;
                self.service.project_locks()?;
                reply
            }
            OrdinaryCommand::Release(release) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_release(release)?;
                self.service.project_locks()?;
                reply
            }
            OrdinaryCommand::Handoff(handoff) => {
                let reply = ClaimLedger::new(self.service.tables()).apply_handoff(handoff)?;
                self.service.project_locks()?;
                reply
            }
            OrdinaryCommand::Observe(Observation::Roles) => {
                ClaimLedger::new(self.service.tables()).observe()?
            }
            OrdinaryCommand::Observe(Observation::Lanes) => {
                LaneRegistry::new(self.service.tables()).observe()?
            }
            OrdinaryCommand::Submit(submission) => {
                ActivityLedger::new(self.service.tables()).submit(submission)?
            }
            OrdinaryCommand::Query(query) => {
                ActivityLedger::new(self.service.tables()).query(query)?
            }
            OrdinaryCommand::Watch(_subscription) => {
                OrchestrateReply::ObservationOpened(signal_orchestrate::ObservationOpened {
                    token: self.service.next_observation_token()?,
                })
            }
            OrdinaryCommand::Unwatch(token) => {
                OrchestrateReply::ObservationClosed(signal_orchestrate::ObservationClosed { token })
            }
        };
        Ok(CommandEffect::new(command, OrdinaryEffect::Reply(reply)))
    }
    fn execute_batch(
        &mut self,
        plan: BatchPlan<OrdinaryCommand>,
    ) -> Result<BatchEffects<OrdinaryCommand, OrdinaryEffect>> {
        let _sequence = self.service.lock_sequence()?;
        if plan.operations().len() != 1 {
            return Err(Error::UnsupportedAtomicBatch {
                operation_count: plan.operations().len(),
            });
        }
        let operation = plan.into_operations().into_head();
        let command = single_command(operation)?;
        let effect = self.execute_command(command)?;
        Ok(BatchEffects::single(OperationEffects::new(
            NonEmpty::single(effect),
        )))
    }
}

impl CommandExecutor for OrdinaryCommandExecutor<'_> {
    type Command = OrdinaryCommand;
    type ComponentEffect = OrdinaryEffect;
    type Error = Error;

    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl std::future::Future<
        Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>>,
    > + Send
    + '_ {
        std::future::ready(self.execute_batch(plan))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaCommand {
    Create(meta_signal_orchestrate::CreateRoleOrder),
    Retire(Retirement),
    Refresh(meta_signal_orchestrate::RefreshRepositoryIndexOrder),
    Register(meta_signal_orchestrate::LaneRegistrationRequest),
    SetAuthority(meta_signal_orchestrate::LaneAuthorityChange),
}

impl ToSemaOperation for MetaCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::Create(_) | Self::Refresh(_) | Self::Register(_) | Self::SetAuthority(_) => {
                SemaOperation::Mutate
            }
            Self::Retire(_) => SemaOperation::Retract,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaEffect {
    Reply(MetaOrchestrateReply),
}

impl MetaEffect {
    fn into_reply(self) -> MetaOrchestrateReply {
        match self {
            Self::Reply(reply) => reply,
        }
    }
}

impl ToSemaOutcome for MetaEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::Reply(MetaOrchestrateReply::RoleCreated(_))
            | Self::Reply(MetaOrchestrateReply::RepositoryIndexRefreshed(_))
            | Self::Reply(MetaOrchestrateReply::LaneRegistered(_))
            | Self::Reply(MetaOrchestrateReply::LaneAuthoritySet(_))
            | Self::Reply(MetaOrchestrateReply::PartialApplied(_)) => SemaOutcome::Mutated,
            Self::Reply(MetaOrchestrateReply::RoleRetired(_))
            | Self::Reply(MetaOrchestrateReply::LaneRetired(_)) => SemaOutcome::Retracted,
            Self::Reply(MetaOrchestrateReply::RoleCreationRejected(_))
            | Self::Reply(MetaOrchestrateReply::MetaOrchestrateRequestUnimplemented(_)) => {
                SemaOutcome::NoChange
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaLowering;

impl LoweringTrait for MetaLowering {
    type Operation = MetaOrchestrateRequest;
    type Reply = MetaOrchestrateReply;
    type Command = MetaCommand;
    type ComponentEffect = MetaEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> std::result::Result<OperationPlan<Self::Command>, Self::Reply> {
        let command = match operation {
            MetaOrchestrateRequest::Create(payload) => MetaCommand::Create(payload.clone()),
            MetaOrchestrateRequest::Retire(payload) => MetaCommand::Retire(payload.clone()),
            MetaOrchestrateRequest::Refresh(payload) => MetaCommand::Refresh(payload.clone()),
            MetaOrchestrateRequest::Register(payload) => MetaCommand::Register(payload.clone()),
            MetaOrchestrateRequest::SetAuthority(payload) => {
                MetaCommand::SetAuthority(payload.clone())
            }
        };
        Ok(OperationPlan::single(command))
    }

    fn reply_from_effects(
        &self,
        _operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        effects
            .component_effects()
            .last()
            .expect("orchestrate meta operation effects are non-empty")
            .clone()
            .into_reply()
    }
}

pub struct MetaCommandExecutor<'service> {
    service: &'service OrchestrateService,
}

impl<'service> MetaCommandExecutor<'service> {
    pub fn new(service: &'service OrchestrateService) -> Self {
        Self { service }
    }

    fn execute_command(
        &self,
        command: MetaCommand,
    ) -> Result<CommandEffect<MetaCommand, MetaEffect>> {
        let reply = match command.clone() {
            MetaCommand::Create(order) => {
                let reply = RoleRegistry::new(self.service.tables(), self.service.layout())
                    .create_role(order)?;
                self.service.project_locks()?;
                reply
            }
            MetaCommand::Retire(Retirement::Role(order)) => {
                let reply = RoleRegistry::new(self.service.tables(), self.service.layout())
                    .retire_role(order)?;
                self.service.project_locks()?;
                reply
            }
            MetaCommand::Retire(Retirement::Lane(lane)) => {
                LaneRegistry::new(self.service.tables()).retire(lane)?
            }
            MetaCommand::Refresh(_order) => {
                RepositoryRegistry::new(self.service.tables(), self.service.layout()).refresh()?
            }
            MetaCommand::Register(request) => {
                LaneRegistry::new(self.service.tables()).register(request)?
            }
            MetaCommand::SetAuthority(change) => {
                LaneRegistry::new(self.service.tables()).set_authority(change)?
            }
        };
        Ok(CommandEffect::new(command, MetaEffect::Reply(reply)))
    }
    fn execute_batch(
        &mut self,
        plan: BatchPlan<MetaCommand>,
    ) -> Result<BatchEffects<MetaCommand, MetaEffect>> {
        let _sequence = self.service.lock_sequence()?;
        if plan.operations().len() != 1 {
            return Err(Error::UnsupportedAtomicBatch {
                operation_count: plan.operations().len(),
            });
        }
        let operation = plan.into_operations().into_head();
        let command = single_command(operation)?;
        let effect = self.execute_command(command)?;
        Ok(BatchEffects::single(OperationEffects::new(
            NonEmpty::single(effect),
        )))
    }
}

impl CommandExecutor for MetaCommandExecutor<'_> {
    type Command = MetaCommand;
    type ComponentEffect = MetaEffect;
    type Error = Error;

    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl std::future::Future<
        Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>>,
    > + Send
    + '_ {
        std::future::ready(self.execute_batch(plan))
    }
}

fn single_command<Command>(plan: OperationPlan<Command>) -> Result<Command> {
    let commands = plan.into_commands();
    let command_count = commands.len();
    if command_count != 1 {
        return Err(Error::UnsupportedAtomicOperationPlan { command_count });
    }
    Ok(commands.into_head())
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
