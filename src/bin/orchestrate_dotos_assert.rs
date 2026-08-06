use std::{env, process::ExitCode};

use dotos::DotosSource;
use meta_signal_orchestrate::MetaOrchestrateReply;
use signal_orchestrate::{
    HandoffRejectionReason, MessengerDeliveryState, ModelUnavailableReason, OrchestrateReply,
    OrchestratorMessageRejection, RoleIdentifier, ScopeConflict, ScopeReference, WorktreeStatus,
};

fn main() -> ExitCode {
    match ScenarioDotosAssertion::from_process_arguments().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-dotos-assert: {error}");
            ExitCode::FAILURE
        }
    }
}

struct ScenarioDotosAssertion {
    tier: String,
    expected_route: String,
    reply: String,
}

impl ScenarioDotosAssertion {
    fn from_process_arguments() -> Self {
        let mut arguments = env::args().skip(1);
        Self {
            tier: arguments.next().unwrap_or_default(),
            expected_route: arguments.next().unwrap_or_default(),
            reply: arguments.next().unwrap_or_default(),
        }
    }

    fn run(self) -> Result<(), String> {
        if self.tier == "ordinary-identifier" {
            return self.print_ordinary_identifier();
        }
        self.verify()
    }

    fn print_ordinary_identifier(self) -> Result<(), String> {
        let output = DotosSource::new(&self.reply)
            .parse::<OrchestrateReply>()
            .map_err(|error| error.to_string())?;
        let identifier = match output {
            OrchestrateReply::AgentRegistered(registered) => registered.agent_identifier,
            OrchestrateReply::AgentIdentityMinted(minted) => minted.agent_identifier,
            other => {
                return Err(format!(
                    "expected an agent identifier output, got {:?}",
                    reply_route(&other)
                ));
            }
        };
        println!("{}", identifier.as_str());
        Ok(())
    }

    fn verify(self) -> Result<(), String> {
        if self.tier == "ordinary" {
            let output = DotosSource::new(&self.reply)
                .parse::<OrchestrateReply>()
                .map_err(|error| error.to_string())?;
            return match self.expected_route.as_str() {
                "ClaimRejection:NestedScope" => match output {
                    OrchestrateReply::ClaimRejection(rejection)
                        if role_is(&rejection.role, "beta")
                            && exact_conflict(
                                &rejection.conflicts,
                                "/scenario/shared/file",
                                "alpha",
                                "first claim",
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected nested claim conflict, got {other:?}")),
                },
                "HandoffAcceptance:AlphaToBeta" => match output {
                    OrchestrateReply::HandoffAcceptance(acceptance)
                        if role_is(&acceptance.from, "alpha")
                            && role_is(&acceptance.to, "beta")
                            && exact_path(&acceptance.scopes, "/scenario/shared") =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected alpha-to-beta handoff, got {other:?}")),
                },
                "HandoffRejection:SourceRoleDoesNotHold" => match output {
                    OrchestrateReply::HandoffRejection(rejection)
                        if role_is(&rejection.from, "gamma")
                            && role_is(&rejection.to, "beta")
                            && matches!(
                                rejection.reason,
                                HandoffRejectionReason::SourceRoleDoesNotHold
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected missing-source handoff refusal, got {other:?}"
                    )),
                },
                "HandoffRejection:TargetRoleConflict" => match output {
                    OrchestrateReply::HandoffRejection(rejection)
                        if role_is(&rejection.from, "mirror-source")
                            && role_is(&rejection.to, "mirror-target")
                            && matches!(
                                rejection.reason,
                                HandoffRejectionReason::TargetRoleConflict(ref conflicts)
                                    if exact_conflict(
                                        conflicts,
                                        "/scenario/mirror-handoff",
                                        "mirror-conflict",
                                        "mirrored conflict",
                                    )
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected target-conflict handoff refusal, got {other:?}"
                    )),
                },
                "ActivityAcknowledgment:FirstSlot" => match output {
                    OrchestrateReply::ActivityAcknowledgment(acknowledgement)
                        if acknowledgement.slot == 0 =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected first activity slot, got {other:?}")),
                },
                "ActivityList:ScenarioActivity" => match output {
                    OrchestrateReply::ActivityList(list)
                        if list.records.iter().any(|activity| {
                            role_is(&activity.role, "alpha")
                                && scope_is_task(&activity.scope, "scenario-task")
                                && activity.reason.as_str() == "record activity"
                        }) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected scenario activity record, got {other:?}")),
                },
                "WorkflowResolutionAccepted:FakeHarness" => match output {
                    OrchestrateReply::WorkflowResolutionAccepted(resolution)
                        if resolution.resolution.model.as_str() == "stateful-scenario-model" =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected fake-harness resolution, got {other:?}")),
                },
                "WorkflowResolutionUnavailable:FakeHarness" => match output {
                    OrchestrateReply::WorkflowResolutionUnavailable(unavailable)
                        if matches!(
                            unavailable.unavailable.reason,
                            ModelUnavailableReason::ModelNotKnown
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected fake-harness unavailability, got {other:?}"
                    )),
                },
                "WorktreeRequestRejected:RepositoryNotFound" => match output {
                    OrchestrateReply::WorktreeRequestRejected(rejection)
                        if matches!(
                            rejection.reason,
                            signal_orchestrate::WorktreeRequestRejection::RepositoryNotFound
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected repository-not-found refusal, got {other:?}"
                    )),
                },
                "WorktreeConcluded:Archived" => match output {
                    OrchestrateReply::WorktreeConcluded(conclusion)
                        if conclusion.worktree.status == WorktreeStatus::Archived =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected archived conclusion, got {other:?}")),
                },
                "WorktreeConcluded:Merged" => match output {
                    OrchestrateReply::WorktreeConcluded(conclusion)
                        if conclusion.worktree.status == WorktreeStatus::Merged =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected merged conclusion, got {other:?}")),
                },
                "OrchestratorMessageRouted:FirstReceipt" => match output {
                    OrchestrateReply::OrchestratorMessageRouted(routed)
                        if routed.triage_slot == 0
                            && routed.recipients.len() == 1
                            && matches!(
                                routed.messenger_delivery_state,
                                MessengerDeliveryState::Degraded(_)
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected first routed triage receipt, got {other:?}"
                    )),
                },
                "OrchestratorMessageRejected:MissingCoordinator" => match output {
                    OrchestrateReply::OrchestratorMessageRejected(rejection)
                        if matches!(
                            rejection.rejection,
                            OrchestratorMessageRejection::MissingCoordinator
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected missing-coordinator refusal, got {other:?}"
                    )),
                },
                "LanesObserved:MirrorRestored" => match output {
                    OrchestrateReply::LanesObserved(lanes) if mirrored_lanes_restored(&lanes) => {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected restored mirror lanes and claim, got {other:?}"
                    )),
                },
                "AgentLaunchRefused:UnknownAgent" => match output {
                    OrchestrateReply::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.reason,
                            signal_orchestrate::AgentLaunchRefusalReason::UnknownAgent
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected UnknownAgent refusal, got {}",
                        reply_route(&other)
                    )),
                },
                "AgentLaunchRefused:HarnessUnreachable" => match output {
                    OrchestrateReply::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.reason,
                            signal_orchestrate::AgentLaunchRefusalReason::HarnessUnreachable
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected HarnessUnreachable refusal, got {}",
                        reply_route(&other)
                    )),
                },
                "AgentRegistrationRejected:JudgeUnavailable" => match output {
                    OrchestrateReply::AgentRegistrationRejected(refusal)
                        if matches!(
                            refusal.reason,
                            signal_orchestrate::AgentRegistrationRejectionReason::JudgeUnavailable
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!(
                        "expected JudgeUnavailable refusal, got {}",
                        reply_route(&other)
                    )),
                },
                _ => self.verify_ordinary_route(output),
            };
        }
        let meta_output = DotosSource::new(&self.reply)
            .parse::<MetaOrchestrateReply>()
            .map_err(|error| error.to_string())?;
        match self.expected_route.as_str() {
            "WorktreeArchived:Archived" => match meta_output {
                MetaOrchestrateReply::WorktreeArchived(archived)
                    if archived.worktree.status == WorktreeStatus::Archived =>
                {
                    Ok(())
                }
                other => Err(format!("expected archived worktree row, got {other:?}")),
            },
            "RoleCreationRejected:RoleAlreadyExists" => match meta_output {
                MetaOrchestrateReply::RoleCreationRejected(refusal)
                    if matches!(
                        refusal.reason,
                        meta_signal_orchestrate::RoleCreationRejectionReason::RoleAlreadyExists
                    ) =>
                {
                    Ok(())
                }
                other => Err(format!(
                    "expected RoleAlreadyExists, got {}",
                    reply_route(&other)
                )),
            },
            "LaneAlreadyRegistered:FreshConflict" => match meta_output {
                MetaOrchestrateReply::LaneAlreadyRegistered(reply)
                    if matches!(
                        reply.resolution,
                        meta_signal_orchestrate::LaneAlreadyRegisteredResolution::FreshConflict
                    ) =>
                {
                    Ok(())
                }
                other => Err(format!(
                    "expected FreshConflict, got {}",
                    reply_route(&other)
                )),
            },
            "LaneAlreadyRegistered:RecoveryInherited" => match meta_output {
                MetaOrchestrateReply::LaneAlreadyRegistered(reply)
                    if matches!(
                        reply.resolution,
                        meta_signal_orchestrate::LaneAlreadyRegisteredResolution::RecoveryInherited
                    ) =>
                {
                    Ok(())
                }
                other => Err(format!(
                    "expected RecoveryInherited, got {}",
                    reply_route(&other)
                )),
            },
            _ => self.verify_meta_route(meta_output),
        }
    }

    fn verify_meta_route(self, output: MetaOrchestrateReply) -> Result<(), String> {
        let actual_route = reply_route(&output);
        if actual_route == self.expected_route {
            Ok(())
        } else {
            Err(format!(
                "expected {} route {}, got {}",
                self.tier, self.expected_route, actual_route
            ))
        }
    }

    fn verify_ordinary_route(self, output: OrchestrateReply) -> Result<(), String> {
        let actual_route = reply_route(&output);
        if actual_route == self.expected_route {
            Ok(())
        } else {
            Err(format!(
                "expected ordinary route {}, got {}",
                self.expected_route, actual_route
            ))
        }
    }
}

fn role_is(role: &RoleIdentifier, expected: &str) -> bool {
    role.as_str() == expected
}

fn exact_path(scopes: &[ScopeReference], expected: &str) -> bool {
    matches!(
        scopes,
        [ScopeReference::Path(path)] if path.as_str() == expected
    )
}

fn scope_is_task(reference: &ScopeReference, expected: &str) -> bool {
    matches!(reference, ScopeReference::Task(token) if token.as_str() == expected)
}

fn exact_conflict(conflicts: &[ScopeConflict], scope: &str, holder: &str, reason: &str) -> bool {
    matches!(
        conflicts,
        [conflict]
            if matches!(
                &conflict.scope,
                ScopeReference::Path(path) if path.as_str() == scope
            )
                && role_is(&conflict.held_by, holder)
                && conflict.held_reason.as_str() == reason
    )
}

fn mirrored_lanes_restored(lanes: &signal_orchestrate::LanesObserved) -> bool {
    let projections = &lanes.lanes;
    ["mirror-source", "mirror-target", "mirror-conflict"]
        .iter()
        .all(|expected| {
            projections.iter().any(|projection| {
                projection.registration.status == signal_orchestrate::LaneStatus::Active
                    && projection.registration.assignment.lane.as_str() == *expected
            })
        })
        && projections.iter().any(|projection| {
            projection.registration.assignment.lane.as_str() == "mirror-source"
                && projection.resource_claims.iter().any(|claim| {
                    matches!(
                        &claim.scope,
                        ScopeReference::Path(path) if path.as_str() == "/scenario/mirror-retained"
                    ) && claim.reason.as_str() == "known mirrored claim"
                })
        })
}

fn reply_route(reply: &impl std::fmt::Debug) -> String {
    format!("{reply:?}")
        .split(['(', '{'])
        .next()
        .unwrap_or_default()
        .to_owned()
}
