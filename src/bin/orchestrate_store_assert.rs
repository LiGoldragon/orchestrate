use std::{env, process::ExitCode};

use orchestrate::{
    LaneStatus, OrchestrateTables, OrchestratorAgentStatus, ScopeReference, StoreLocation,
    StoredGuidanceMagnitude, StoredOrchestratorMessageKind, StoredTriageVerdict,
    StoredWorkflowModelResolutionOutcome, WorktreeStatus,
};

fn main() -> ExitCode {
    match RestartStoreAssertion::from_process_arguments().verify() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-store-assert: {error}");
            ExitCode::FAILURE
        }
    }
}

struct RestartStoreAssertion {
    store: StoreLocation,
    agent_identifier: String,
    archived_path: String,
    merged_path: String,
}

impl RestartStoreAssertion {
    fn from_process_arguments() -> Self {
        let mut arguments = env::args().skip(1);
        Self {
            store: StoreLocation::new(arguments.next().unwrap_or_default()),
            agent_identifier: arguments.next().unwrap_or_default(),
            archived_path: arguments.next().unwrap_or_default(),
            merged_path: arguments.next().unwrap_or_default(),
        }
    }

    fn verify(self) -> Result<(), String> {
        let tables = OrchestrateTables::open(&self.store).map_err(|error| error.to_string())?;
        self.verify_activity(&tables)?;
        self.verify_lane_and_claim(&tables)?;
        self.verify_worktrees(&tables)?;
        self.verify_agent_and_topic(&tables)?;
        self.verify_triage(&tables)?;
        self.verify_workflow_resolutions(&tables)
    }

    fn verify_activity(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let activities = tables
            .activity_records()
            .map_err(|error| error.to_string())?;
        let Some(activity) = activities.iter().find(|activity| {
            activity.slot == 0
                && activity.role.as_wire_token() == "alpha"
                && matches!(
                    &activity.scope,
                    ScopeReference::Task(token) if token.as_str() == "scenario-task"
                )
        }) else {
            return Err("missing exact activity slot 0 for alpha/scenario-task".to_owned());
        };
        if activity.reason.as_str() != "record activity" {
            return Err("activity slot 0 has the wrong reason".to_owned());
        }
        Ok(())
    }

    fn verify_lane_and_claim(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let lanes = tables.lane_records().map_err(|error| error.to_string())?;
        let Some(lane) = lanes.iter().find(|lane| {
            lane.assignment.session.as_wire_token() == "Alpha"
                && lane.assignment.lane.as_wire_token() == "alpha"
                && lane.status == LaneStatus::Active
        }) else {
            return Err("missing active Alpha/alpha lane after restart".to_owned());
        };
        if lane.assignment.details.as_str() != "alpha state" {
            return Err("alpha lane has the wrong declared scope".to_owned());
        }
        let claims = tables.claim_records().map_err(|error| error.to_string())?;
        if !claims.iter().any(|claim| {
            claim.lane.as_wire_token() == "alpha"
                && claim.reason.as_str() == "restart claim"
                && matches!(
                    &claim.scope,
                    ScopeReference::Path(path) if path.as_str() == "/scenario/restart-claim"
                )
        }) {
            return Err("missing alpha restart claim with its exact path and reason".to_owned());
        }
        Ok(())
    }

    fn verify_worktrees(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let worktrees = tables
            .worktree_records()
            .map_err(|error| error.to_string())?;
        self.verify_worktree(
            &worktrees,
            "archive",
            &self.archived_path,
            WorktreeStatus::Archived,
        )?;
        self.verify_worktree(
            &worktrees,
            "merged",
            &self.merged_path,
            WorktreeStatus::Merged,
        )
    }

    fn verify_worktree(
        &self,
        worktrees: &[orchestrate::StoredWorktree],
        branch: &str,
        path: &str,
        status: WorktreeStatus,
    ) -> Result<(), String> {
        if worktrees.iter().any(|worktree| {
            worktree.repository.as_str() == "orchestrate"
                && worktree.branch.as_str() == branch
                && worktree.path.as_str() == path
                && worktree.status == status
        }) {
            Ok(())
        } else {
            Err(format!(
                "missing {status:?} worktree orchestrate/{branch} at {path}"
            ))
        }
    }

    fn verify_agent_and_topic(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let agents = tables
            .orchestrator_agent_records()
            .map_err(|error| error.to_string())?;
        if !agents.iter().any(|agent| {
            agent.agent_identifier.as_str() == self.agent_identifier
                && agent.session.as_wire_token() == "Alpha"
                && agent.status == OrchestratorAgentStatus::Active
        }) {
            return Err(
                "missing active agent with its registered identifier and Alpha session".to_owned(),
            );
        }
        let topics = tables
            .orchestrator_topic_records()
            .map_err(|error| error.to_string())?;
        if !topics.iter().any(|topic| {
            topic.path.as_str() == "coordination"
                && topic.name.as_str() == "coordination"
                && topic.parent.is_none()
        }) {
            return Err("missing declared coordination topic".to_owned());
        }
        let memberships = tables
            .orchestrator_topic_membership_records()
            .map_err(|error| error.to_string())?;
        if !memberships.iter().any(|membership| {
            membership.agent_identifier.as_str() == self.agent_identifier
                && membership.topic.as_str() == "coordination"
        }) {
            return Err("missing coordination membership for registered agent".to_owned());
        }
        Ok(())
    }

    fn verify_triage(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let triage = tables
            .orchestrator_triage_records()
            .map_err(|error| error.to_string())?;
        let routed = triage.iter().any(|record| {
            record.slot == 0
                && record.sender.as_str() == self.agent_identifier
                && record.incoming_kind
                    == StoredOrchestratorMessageKind::Guidance(StoredGuidanceMagnitude::Standard)
                && matches!(
                    &record.verdict,
                    StoredTriageVerdict::Route { recipients, retyped: None }
                        if recipients.len() == 1 && recipients[0].as_str() == self.agent_identifier
                )
        });
        let escalated = triage.iter().any(|record| {
            record.slot == 1
                && record.sender.as_str() == self.agent_identifier
                && record.incoming_kind == StoredOrchestratorMessageKind::Report
                && matches!(record.verdict, StoredTriageVerdict::Escalate)
        });
        if routed && escalated {
            Ok(())
        } else {
            Err("missing exact routed and missing-coordinator triage receipts".to_owned())
        }
    }

    fn verify_workflow_resolutions(&self, tables: &OrchestrateTables) -> Result<(), String> {
        let resolutions = tables
            .workflow_model_resolution_records()
            .map_err(|error| error.to_string())?;
        let resolved = resolutions.iter().any(|resolution| {
            matches!(
                &resolution.outcome,
                StoredWorkflowModelResolutionOutcome::Resolved(model)
                    if model.model.as_str() == "stateful-scenario-model"
            )
        });
        let unavailable = resolutions.iter().any(|resolution| {
            matches!(
                &resolution.outcome,
                StoredWorkflowModelResolutionOutcome::Unavailable(model)
                    if model.reason == signal_harness::ModelUnavailableReason::ModelNotKnown
            )
        });
        if resolved && unavailable {
            Ok(())
        } else {
            Err("missing durable resolved and unavailable workflow rows".to_owned())
        }
    }
}
