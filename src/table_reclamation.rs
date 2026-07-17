//! Interim idle-age reaping for the orchestrate stores that otherwise grow
//! without bound: the orchestrator agent registry, its topic membership, the
//! topic registry, and the workflow model-resolution table.
//!
//! This is an interim mechanism. The psyche's standing direction is that these
//! lanes and tables should ultimately be "handled more smartly with a mind
//! combination"; until then, each store is bounded by the same deadline-driven,
//! push-not-pull discipline the lane reaper already uses (`src/lane.rs`,
//! `src/lane_reclamation.rs`). The invariant carries over unchanged: a record is
//! never reaped by anything but its own idle age, and real activity refreshes a
//! record's activity stamp so long-running work never ages out. See
//! `ARCHITECTURE.md` for the interim framing and its backing quote.
//!
//! The reaper holds one reconciliation instant and decides, per record, whether
//! it is a live record to keep or a dead record to reap. Reconciliation runs at
//! the same moments the lane reconciliation does: at daemon startup and on the
//! ordinary engine turn, with the shared reclamation worker sleeping to the next
//! published expiry rather than scanning on a timer.

use signal_orchestrate::{
    OrchestratorAgentIdentifier, OrchestratorAgentStatus, TimestampNanos, WorktreeStatus,
};

use crate::{OrchestrateTables, Result};

/// How long an `Active` orchestrator agent may sit idle — no registration,
/// reachability discovery, or triaged message — before the reaper retires it as
/// a leaked agent whose harness is gone. Generous by design, mirroring the
/// active-lane liveness window: a genuinely active agent refreshes its
/// last-activity stamp on every real use, so only an abandoned agent idles this
/// long. Tunable.
pub const ACTIVE_ORCHESTRATOR_AGENT_IDLE_LIMIT_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// How long a terminal (`Retired` or `Dead`) orchestrator agent is retained
/// after its terminal transition before the reaper hard-deletes it and its
/// topic seats. A short post-terminal window mirroring the terminal-lane
/// retention: finished or dead work is kept briefly for post-mortem, then gone.
/// The death transition re-stamps `last_activity`, so a dead agent's window is
/// measured from the death observation. Tunable.
pub const RETIRED_ORCHESTRATOR_AGENT_RETENTION_NANOS: u64 = 60 * 60 * 1_000_000_000;

/// How long a workflow model-resolution row is retained after it was stamped
/// before the reaper hard-deletes it. A resolution is a historical record of one
/// run's model outcome, not live state, so its own age since it was stamped is
/// its idle age. Generous enough to outlast any workflow run that still reads it.
/// Tunable.
pub const WORKFLOW_MODEL_RESOLUTION_RETENTION_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// How long an orchestrator topic that has become empty — no seated members and
/// no child topics — is retained before the reaper hard-deletes it. A populated
/// or parent topic is structural and never reaped; only a topic that has aged
/// out of use is. Tunable.
pub const EMPTY_ORCHESTRATOR_TOPIC_RETENTION_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// How long a terminal worktree tombstone — a `Recycled`, `Archived`, or `Merged`
/// row whose work is concluded — is retained after its last activity before the
/// reaper hard-deletes it. This bounds the worktree index, whose main growth is
/// one tombstone per concluded worktree. `Active` worktrees are live and never
/// reaped, and `Abandoned` worktrees are left for the `ConcludeWorktree` reclaim
/// path rather than deleted out from under it. Tunable.
pub const WORKTREE_TERMINAL_RETENTION_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// Reaps the unbounded orchestrator-seat and workflow-resolution stores against
/// a single reconciliation instant, by each record's own idle age.
pub struct BoundedTableReaper {
    now: TimestampNanos,
}

impl BoundedTableReaper {
    pub fn new(now: TimestampNanos) -> Self {
        Self { now }
    }

    /// Reap every dead record across the interim-bounded stores in one pass and
    /// report the tally. Order matters: agents are retired then deleted first, so
    /// a topic left empty by a deleted agent's departed seats is reapable in the
    /// same pass.
    pub fn reconcile(&self, tables: &OrchestrateTables) -> Result<BoundedTableReclamation> {
        let mut reclamation = BoundedTableReclamation::none();
        self.reap_agents(tables, &mut reclamation)?;
        self.reap_orphan_topic_memberships(tables, &mut reclamation)?;
        self.reap_empty_topics(tables, &mut reclamation)?;
        self.reap_workflow_model_resolutions(tables, &mut reclamation)?;
        self.reap_terminal_worktrees(tables, &mut reclamation)?;
        Ok(reclamation)
    }

    /// The next durable expiry across the interim-bounded stores, or `None` when
    /// nothing is currently reapable. The shared reclamation worker sleeps to the
    /// earliest of this and the lane expiry, then re-enters through the ordinary
    /// Signal path; it never scans on an interval.
    pub fn next_deadline(&self, tables: &OrchestrateTables) -> Result<Option<TimestampNanos>> {
        let mut earliest: Option<TimestampNanos> = None;
        let mut consider = |deadline: TimestampNanos| {
            earliest = Some(match earliest {
                Some(current) if current.value() <= deadline.value() => current,
                _ => deadline,
            });
        };
        for agent in tables.orchestrator_agent_records()? {
            consider(Self::agent_deadline(&agent.last_activity, agent.status));
        }
        for resolution in tables.workflow_model_resolution_records()? {
            consider(TimestampNanos::new(
                resolution
                    .stamped_at
                    .value()
                    .saturating_add(WORKFLOW_MODEL_RESOLUTION_RETENTION_NANOS),
            ));
        }
        // A populated or parent topic is not reapable, so it publishes no
        // deadline; only a currently-empty leaf topic arms one. When a topic's
        // last member leaves, the end-of-turn reschedule recomputes this.
        for topic in tables.orchestrator_topic_records()? {
            if Self::topic_is_empty_leaf(tables, &topic.path)? {
                consider(TimestampNanos::new(
                    topic
                        .created_at
                        .value()
                        .saturating_add(EMPTY_ORCHESTRATOR_TOPIC_RETENTION_NANOS),
                ));
            }
        }
        for worktree in tables.worktree_records()? {
            if Self::worktree_is_terminal_tombstone(worktree.status) {
                consider(TimestampNanos::new(
                    worktree
                        .last_activity
                        .value()
                        .saturating_add(WORKTREE_TERMINAL_RETENTION_NANOS),
                ));
            }
        }
        Ok(earliest)
    }

    fn reap_agents(
        &self,
        tables: &OrchestrateTables,
        reclamation: &mut BoundedTableReclamation,
    ) -> Result<()> {
        for agent in tables.orchestrator_agent_records()? {
            let idle_nanos = agent.idle_age_at(self.now).value();
            match agent.status {
                OrchestratorAgentStatus::Active
                    if idle_nanos >= ACTIVE_ORCHESTRATOR_AGENT_IDLE_LIMIT_NANOS =>
                {
                    tables.retire_orchestrator_agent(&agent.agent_identifier)?;
                    reclamation.retired_idle_agents += 1;
                }
                OrchestratorAgentStatus::Retired
                    if idle_nanos >= RETIRED_ORCHESTRATOR_AGENT_RETENTION_NANOS =>
                {
                    tables.remove_orchestrator_agent(&agent.agent_identifier)?;
                    reclamation.reaped_retired_agents += 1;
                }
                OrchestratorAgentStatus::Dead
                    if idle_nanos >= RETIRED_ORCHESTRATOR_AGENT_RETENTION_NANOS =>
                {
                    tables.remove_orchestrator_agent(&agent.agent_identifier)?;
                    reclamation.reaped_dead_agents += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Sweep any topic seat whose agent no longer exists. Removing an agent
    /// already reaps its seats, so this only catches a seat orphaned by an
    /// earlier partial state.
    fn reap_orphan_topic_memberships(
        &self,
        tables: &OrchestrateTables,
        reclamation: &mut BoundedTableReclamation,
    ) -> Result<()> {
        let live_agents = tables
            .orchestrator_agent_records()?
            .into_iter()
            .map(|agent| agent.agent_identifier)
            .collect::<std::collections::BTreeSet<OrchestratorAgentIdentifier>>();
        for membership in tables.orchestrator_topic_membership_records()? {
            if !live_agents.contains(&membership.agent_identifier) {
                tables.remove_topic_memberships_for_agent(&membership.agent_identifier)?;
                reclamation.reaped_orphan_memberships += 1;
            }
        }
        Ok(())
    }

    fn reap_empty_topics(
        &self,
        tables: &OrchestrateTables,
        reclamation: &mut BoundedTableReclamation,
    ) -> Result<()> {
        for topic in tables.orchestrator_topic_records()? {
            let idle_nanos = self.now.value().saturating_sub(topic.created_at.value());
            if idle_nanos >= EMPTY_ORCHESTRATOR_TOPIC_RETENTION_NANOS
                && Self::topic_is_empty_leaf(tables, &topic.path)?
            {
                tables.remove_orchestrator_topic(&topic.path)?;
                reclamation.reaped_empty_topics += 1;
            }
        }
        Ok(())
    }

    fn reap_workflow_model_resolutions(
        &self,
        tables: &OrchestrateTables,
        reclamation: &mut BoundedTableReclamation,
    ) -> Result<()> {
        for resolution in tables.workflow_model_resolution_records()? {
            let idle_nanos = self
                .now
                .value()
                .saturating_sub(resolution.stamped_at.value());
            if idle_nanos >= WORKFLOW_MODEL_RESOLUTION_RETENTION_NANOS {
                tables.remove_workflow_model_resolution(&resolution.handle)?;
                reclamation.reaped_workflow_resolutions += 1;
            }
        }
        Ok(())
    }

    fn reap_terminal_worktrees(
        &self,
        tables: &OrchestrateTables,
        reclamation: &mut BoundedTableReclamation,
    ) -> Result<()> {
        for worktree in tables.worktree_records()? {
            let idle_nanos = self
                .now
                .value()
                .saturating_sub(worktree.last_activity.value());
            if Self::worktree_is_terminal_tombstone(worktree.status)
                && idle_nanos >= WORKTREE_TERMINAL_RETENTION_NANOS
            {
                tables.remove_worktree(&worktree)?;
                reclamation.reaped_terminal_worktrees += 1;
            }
        }
        Ok(())
    }

    /// Whether a worktree row is a concluded tombstone the reaper may age out.
    /// `Active` is live work; `Abandoned` awaits `ConcludeWorktree` reclaim and is
    /// never deleted out from under that path.
    fn worktree_is_terminal_tombstone(status: WorktreeStatus) -> bool {
        matches!(
            status,
            WorktreeStatus::Recycled | WorktreeStatus::Archived | WorktreeStatus::Merged
        )
    }

    fn topic_is_empty_leaf(
        tables: &OrchestrateTables,
        path: &signal_orchestrate::OrchestratorTopicPath,
    ) -> Result<bool> {
        Ok(tables.topic_member_identifiers(path)?.is_empty()
            && !tables.orchestrator_topic_has_children(path)?)
    }

    fn agent_deadline(
        last_activity: &TimestampNanos,
        status: OrchestratorAgentStatus,
    ) -> TimestampNanos {
        let window = match status {
            OrchestratorAgentStatus::Active => ACTIVE_ORCHESTRATOR_AGENT_IDLE_LIMIT_NANOS,
            OrchestratorAgentStatus::Retired | OrchestratorAgentStatus::Dead => {
                RETIRED_ORCHESTRATOR_AGENT_RETENTION_NANOS
            }
        };
        TimestampNanos::new(last_activity.value().saturating_add(window))
    }
}

/// The tally of one interim-table reconciliation pass, so a caller (startup log,
/// test) can witness exactly what the reaper removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoundedTableReclamation {
    pub retired_idle_agents: u32,
    pub reaped_retired_agents: u32,
    pub reaped_dead_agents: u32,
    pub reaped_orphan_memberships: u32,
    pub reaped_empty_topics: u32,
    pub reaped_workflow_resolutions: u32,
    pub reaped_terminal_worktrees: u32,
}

impl BoundedTableReclamation {
    fn none() -> Self {
        Self::default()
    }

    pub fn total_removed(&self) -> u32 {
        self.reaped_retired_agents
            + self.reaped_dead_agents
            + self.reaped_orphan_memberships
            + self.reaped_empty_topics
            + self.reaped_workflow_resolutions
            + self.reaped_terminal_worktrees
    }
}
