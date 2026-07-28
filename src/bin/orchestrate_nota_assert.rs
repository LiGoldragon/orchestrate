use std::{env, process::ExitCode};

use meta_signal_orchestrate::schema::lib::Output as MetaOutput;
use nota::NotaSource;
use signal_orchestrate::schema::lib::{
    HandoffRejectionReason, MessengerDeliveryState, ModelUnavailableReason,
    OrchestratorMessageRejection, Output as OrdinaryOutput, ScopeReference, WorktreeStatus,
};

fn main() -> ExitCode {
    match ScenarioNotaAssertion::from_process_arguments().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-nota-assert: {error}");
            ExitCode::FAILURE
        }
    }
}

struct ScenarioNotaAssertion {
    tier: String,
    expected_route: String,
    reply: String,
}

impl ScenarioNotaAssertion {
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
        let output = NotaSource::new(&self.reply)
            .parse::<OrdinaryOutput>()
            .map_err(|error| error.to_string())?;
        let identifier = match output {
            OrdinaryOutput::AgentRegistered(registered) => registered.orchestrator_agent_identifier,
            OrdinaryOutput::AgentIdentityMinted(minted) => minted.into_payload(),
            other => {
                return Err(format!(
                    "expected an agent identifier output, got {:?}",
                    other.route()
                ));
            }
        };
        println!("{}", identifier.payload());
        Ok(())
    }

    fn verify(self) -> Result<(), String> {
        if self.tier == "ordinary" {
            let output = NotaSource::new(&self.reply)
                .parse::<OrdinaryOutput>()
                .map_err(|error| error.to_string())?;
            return match self.expected_route.as_str() {
                "ClaimRejection:NestedScope" => match output {
                    OrdinaryOutput::ClaimRejection(rejection)
                        if role_is(&rejection.role_name, "beta")
                            && exact_conflict(
                                &rejection.scope_conflicts,
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
                    OrdinaryOutput::HandoffAcceptance(acceptance)
                        if role_is(&acceptance.from, "alpha")
                            && role_is(&acceptance.to, "beta")
                            && exact_path(&acceptance.scope_references, "/scenario/shared") =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected alpha-to-beta handoff, got {other:?}")),
                },
                "HandoffRejection:SourceRoleDoesNotHold" => match output {
                    OrdinaryOutput::HandoffRejection(rejection)
                        if role_is(&rejection.from, "gamma")
                            && role_is(&rejection.to, "beta")
                            && matches!(
                                rejection.handoff_rejection_reason,
                                HandoffRejectionReason::SourceRoleDoesNotHold
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected missing-source handoff refusal, got {other:?}")),
                },
                "HandoffRejection:TargetRoleConflict" => match output {
                    OrdinaryOutput::HandoffRejection(rejection)
                        if role_is(&rejection.from, "mirror-source")
                            && role_is(&rejection.to, "mirror-target")
                            && matches!(
                                rejection.handoff_rejection_reason,
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
                    other => Err(format!("expected target-conflict handoff refusal, got {other:?}")),
                },
                "ActivityAcknowledgment:FirstSlot" => match output {
                    OrdinaryOutput::ActivityAcknowledgment(acknowledgement)
                        if *acknowledgement.payload() == 0 =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected first activity slot, got {other:?}")),
                },
                "ActivityList:ScenarioActivity" => match output {
                    OrdinaryOutput::ActivityList(list)
                        if list.payload().payload().iter().any(|activity| {
                            role_is(&activity.role_name, "alpha")
                                && scope_is_task(&activity.scope_reference, "scenario-task")
                                && activity.scope_reason.payload() == "record activity"
                        }) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected scenario activity record, got {other:?}")),
                },
                "WorkflowResolutionAccepted:FakeHarness" => match output {
                    OrdinaryOutput::WorkflowResolutionAccepted(resolution)
                        if resolution.model_resolved.named_model.payload()
                            == "stateful-scenario-model" =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected fake-harness resolution, got {other:?}")),
                },
                "WorkflowResolutionUnavailable:FakeHarness" => match output {
                    OrdinaryOutput::WorkflowResolutionUnavailable(unavailable)
                        if matches!(
                            unavailable.model_unavailable.model_unavailable_reason,
                            ModelUnavailableReason::ModelNotKnown
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected fake-harness unavailability, got {other:?}")),
                },
                "WorktreeRequestRejected:RepositoryNotFound" => match output {
                    OrdinaryOutput::WorktreeRequestRejected(rejection)
                        if matches!(
                            rejection.payload(),
                            signal_orchestrate::schema::lib::WorktreeRequestRejection::RepositoryNotFound
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected repository-not-found refusal, got {other:?}")),
                },
                "WorktreeConcluded:Archived" => match output {
                    OrdinaryOutput::WorktreeConcluded(conclusion)
                        if conclusion.worktree.worktree_status == WorktreeStatus::Archived =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected archived conclusion, got {other:?}")),
                },
                "WorktreeConcluded:Merged" => match output {
                    OrdinaryOutput::WorktreeConcluded(conclusion)
                        if conclusion.worktree.worktree_status == WorktreeStatus::Merged =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected merged conclusion, got {other:?}")),
                },
                "OrchestratorMessageRouted:FirstReceipt" => match output {
                    OrdinaryOutput::OrchestratorMessageRouted(routed)
                        if *routed.triage_slot_number.payload() == 0
                            && routed.orchestrator_agent_identifiers.payload().len() == 1
                            && matches!(
                                routed.messenger_delivery_state,
                                MessengerDeliveryState::Degraded(_)
                            ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected first routed triage receipt, got {other:?}")),
                },
                "OrchestratorMessageRejected:MissingCoordinator" => match output {
                    OrdinaryOutput::OrchestratorMessageRejected(rejection)
                        if matches!(
                            rejection.payload(),
                            OrchestratorMessageRejection::MissingCoordinator
                        ) =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected missing-coordinator refusal, got {other:?}")),
                },
                "LanesObserved:MirrorRestored" => match output {
                    OrdinaryOutput::LanesObserved(lanes) if mirrored_lanes_restored(&lanes) => Ok(()),
                    other => Err(format!("expected restored mirror lanes and claim, got {other:?}")),
                },
                "AgentLaunchRefused:UnknownAgent" => match output {
                    OrdinaryOutput::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.agent_launch_refusal_reason,
                            signal_orchestrate::schema::lib::AgentLaunchRefusalReason::UnknownAgent
                        ) => Ok(()),
                    other => Err(format!("expected UnknownAgent refusal, got {:?}", other.route())),
                },
                "AgentLaunchRefused:HarnessUnreachable" => match output {
                    OrdinaryOutput::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.agent_launch_refusal_reason,
                            signal_orchestrate::schema::lib::AgentLaunchRefusalReason::HarnessUnreachable
                        ) => Ok(()),
                    other => Err(format!("expected HarnessUnreachable refusal, got {:?}", other.route())),
                },
                "AgentRegistrationRejected:JudgeUnavailable" => match output {
                    OrdinaryOutput::AgentRegistrationRejected(refusal)
                        if matches!(
                            refusal.agent_registration_rejection_reason,
                            signal_orchestrate::schema::lib::AgentRegistrationRejectionReason::JudgeUnavailable
                        ) => Ok(()),
                    other => Err(format!("expected JudgeUnavailable refusal, got {:?}", other.route())),
                },
                _ => self.verify_ordinary_route(output),
            };
        }
        let meta_output = NotaSource::new(&self.reply)
            .parse::<MetaOutput>()
            .map_err(|error| error.to_string())?;
        match self.expected_route.as_str() {
            "WorktreeArchived:Archived" => match meta_output {
                MetaOutput::WorktreeArchived(archived)
                    if archived.payload().worktree_status
                        == WorktreeStatus::Archived =>
                {
                    Ok(())
                }
                other => Err(format!("expected archived worktree row, got {other:?}")),
            },
            "RoleCreationRejected:RoleAlreadyExists" => match meta_output {
                MetaOutput::RoleCreationRejected(refusal)
                    if matches!(
                        refusal.role_creation_rejection_reason,
                        meta_signal_orchestrate::schema::lib::RoleCreationRejectionReason::RoleAlreadyExists
                    ) => Ok(()),
                other => Err(format!("expected RoleAlreadyExists, got {:?}", other.route())),
            },
            "LaneAlreadyRegistered:FreshConflict" => match meta_output {
                MetaOutput::LaneAlreadyRegistered(reply)
                    if matches!(
                        reply.lane_already_registered_resolution,
                        meta_signal_orchestrate::schema::lib::LaneAlreadyRegisteredResolution::FreshConflict
                    ) => Ok(()),
                other => Err(format!("expected FreshConflict, got {:?}", other.route())),
            },
            "LaneAlreadyRegistered:RecoveryInherited" => match meta_output {
                MetaOutput::LaneAlreadyRegistered(reply)
                    if matches!(
                        reply.lane_already_registered_resolution,
                        meta_signal_orchestrate::schema::lib::LaneAlreadyRegisteredResolution::RecoveryInherited
                    ) => Ok(()),
                other => Err(format!("expected RecoveryInherited, got {:?}", other.route())),
            },
            _ => self.verify_meta_route(meta_output),
        }
    }

    fn verify_meta_route(self, output: MetaOutput) -> Result<(), String> {
        let actual_route = format!("{:?}", output.route());
        if actual_route == self.expected_route {
            Ok(())
        } else {
            Err(format!(
                "expected {} route {}, got {}",
                self.tier, self.expected_route, actual_route
            ))
        }
    }

    fn verify_ordinary_route(self, output: OrdinaryOutput) -> Result<(), String> {
        let actual_route = format!("{:?}", output.route());
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

fn role_is(role: &signal_orchestrate::schema::lib::RoleName, expected: &str) -> bool {
    role.payload().payload() == expected
}

fn exact_path(scopes: &signal_orchestrate::schema::lib::ScopeReferences, expected: &str) -> bool {
    matches!(
        scopes.payload().as_slice(),
        [ScopeReference::Path(path)] if path.payload() == expected
    )
}

fn scope_is_task(reference: &ScopeReference, expected: &str) -> bool {
    matches!(reference, ScopeReference::Task(token) if token.payload() == expected)
}

fn exact_conflict(
    conflicts: &signal_orchestrate::schema::lib::ScopeConflicts,
    scope: &str,
    holder: &str,
    reason: &str,
) -> bool {
    matches!(
        conflicts.payload().as_slice(),
        [conflict]
            if matches!(
                &conflict.scope_reference,
                ScopeReference::Path(path) if path.payload() == scope
            )
                && role_is(&conflict.role_name, holder)
                && conflict.scope_reason.payload() == reason
    )
}

fn mirrored_lanes_restored(lanes: &signal_orchestrate::schema::lib::LanesObserved) -> bool {
    let projections = lanes.payload().payload();
    ["mirror-source", "mirror-target", "mirror-conflict"]
        .iter()
        .all(|expected| {
            projections.iter().any(|projection| {
                projection.lane_registration.lane_status
                    == signal_orchestrate::schema::lib::LaneStatus::Active
                    && projection
                        .lane_registration
                        .lane_assignment
                        .lane_identifier
                        .payload()
                        == expected
            })
        })
        && projections.iter().any(|projection| {
            projection.lane_registration.lane_assignment.lane_identifier.payload()
                == "mirror-source"
                && projection.lane_resource_claims.payload().iter().any(|claim| {
                    matches!(
                        &claim.scope_reference,
                        ScopeReference::Path(path) if path.payload() == "/scenario/mirror-retained"
                    ) && claim.scope_reason.payload() == "known mirrored claim"
                })
        })
}
