use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_orchestrate::{
    Activity, ApplicationFailure, ApplicationSuccess, BranchName, DurationNanos, HarnessKind,
    LaneAssignment, LaneIdentifier, LaneName, LaneRegistration, LaneResourceClaim, LaneStatus,
    MintedIdentitySelection, MissionDescription, ModelResolved, ModelUnavailable,
    OrchestratorAgentIdentifier, OrchestratorAgentStatus, OrchestratorTopic, OrchestratorTopicPath,
    PartialApplied, PurposeText, PushedState, RepositoryIdentityState, RepositoryName,
    ResolvedWorkflowRunRequest, Role, RoleIdentifier, ScopeReason, ScopeReference,
    SessionIdentifier, TimestampNanos, TopicName, WirePath, WorkflowRunHandle, Worktree,
    WorktreeStatus,
};

use crate::orchestrator_agent_identifier::OrchestratorAgentIdentifierMint;
use crate::{Result, StoreLocation};

trait OrchestrateStoredValue: sema_engine::EngineStoredValue
where
    Self::Archived: rkyv::Deserialize<Self, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

impl<RecordValue> OrchestrateStoredValue for RecordValue
where
    RecordValue: sema_engine::EngineStoredValue,
    RecordValue::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

// This build admits exactly one store family generation. Every family identity
// must match its current descriptor exactly or store opening fails closed.
const ORCHESTRATE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(11);

const CLAIMS: TableName = TableName::new("claims");
const ROLES: TableName = TableName::new("roles");
const LANE_REGISTRY: TableName = TableName::new("lane_registry");
const REPOSITORIES: TableName = TableName::new("repositories");
const WORKTREES: TableName = TableName::new("worktrees");
const ACTIVITIES: TableName = TableName::new("activities");
const ACTIVITY_NEXT_SLOT: TableName = TableName::new("activity_next_slot");
const ACTIVITY_NEXT_SLOT_KEY: &str = "next";
const DIVERGENCES: TableName = TableName::new("divergences");
const DIVERGENCE_NEXT_SLOT: TableName = TableName::new("divergence_next_slot");
const DIVERGENCE_NEXT_SLOT_KEY: &str = "next";
const WORKFLOW_MODEL_RESOLUTIONS: TableName = TableName::new("workflow_model_resolutions");
const ORCHESTRATOR_AGENTS: TableName = TableName::new("orchestrator_agents");
const ORCHESTRATOR_TOPICS: TableName = TableName::new("orchestrator_topics");
const ORCHESTRATOR_TOPIC_MEMBERSHIP: TableName = TableName::new("orchestrator_topic_membership");
const ORCHESTRATOR_TRIAGE_AUDIT: TableName = TableName::new("orchestrator_triage_audit");
const ORCHESTRATOR_TRIAGE_NEXT_SLOT: TableName = TableName::new("orchestrator_triage_next_slot");
const ORCHESTRATOR_TRIAGE_NEXT_SLOT_KEY: &str = "next";

/// The live activity view is a bounded recent-history window, not an archive.
pub const CURRENT_ACTIVITY_LIMIT: usize = 256;
/// Divergences are diagnostic current reality; retain the latest bounded set.
pub const CURRENT_DIVERGENCE_LIMIT: usize = 128;
/// Triage audit supports current routing diagnosis, not unbounded transcript storage.
pub const CURRENT_ORCHESTRATOR_TRIAGE_LIMIT: usize = 256;

pub struct OrchestrateTables {
    engine: Engine,
    claims: TableReference<StoredClaim>,
    roles: TableReference<StoredRole>,
    lane_registry: TableReference<StoredLaneRegistration>,
    repositories: TableReference<StoredRepository>,
    worktrees: TableReference<StoredWorktree>,
    activities: TableReference<StoredActivity>,
    activity_next_slot: TableReference<u64>,
    divergences: TableReference<StoredDivergence>,
    divergence_next_slot: TableReference<u64>,
    workflow_model_resolutions: TableReference<StoredWorkflowRunResolution>,
    orchestrator_agents: TableReference<StoredOrchestratorAgent>,
    orchestrator_topics: TableReference<StoredOrchestratorTopic>,
    orchestrator_topic_membership: TableReference<StoredOrchestratorTopicMembership>,
    orchestrator_triage_audit: TableReference<StoredOrchestratorTriageRecord>,
    orchestrator_triage_next_slot: TableReference<u64>,
    atomic_commit_failure_for_test: AtomicBool,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredClaim {
    pub lane: LaneIdentifier,
    pub scope: ScopeReference,
    pub reason: ScopeReason,
    pub claimed_at: TimestampNanos,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredLaneRegistration {
    pub assignment: LaneAssignment,
    pub registered_at: TimestampNanos,
    pub updated_at: TimestampNanos,
    pub status: LaneStatus,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRole {
    pub role: RoleIdentifier,
    pub harness: HarnessKind,
    pub report_repository_path: WirePath,
    pub report_lane_path: WirePath,
}

/// One caller-supplied repository state record. `identity` is the identifying
/// key; `name` is the local alias and `path` is retained state.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRepository {
    pub identity: RepositoryIdentityState,
    pub name: RepositoryName,
    pub path: WirePath,
    pub active: bool,
    pub refreshed_at: TimestampNanos,
}

/// One worktree row, the durable form of a [`Worktree`] (Spirit eh5a). Keyed
/// `repository|branch` by `WorktreeKey`, beside [`StoredRepository`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredWorktree {
    pub repository: RepositoryName,
    pub branch: BranchName,
    pub path: WirePath,
    pub owning_lane: LaneName,
    pub status: WorktreeStatus,
    pub purpose: PurposeText,
    pub last_activity: TimestampNanos,
    pub pushed_state: PushedState,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredActivity {
    pub slot: u64,
    pub role: RoleIdentifier,
    pub scope: ScopeReference,
    pub reason: ScopeReason,
    pub stamped_at: TimestampNanos,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDivergence {
    pub slot: u64,
    pub succeeded: Vec<ApplicationSuccess>,
    pub failed: Vec<ApplicationFailure>,
    pub stamped_at: TimestampNanos,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredWorkflowRunResolution {
    pub handle: WorkflowRunHandle,
    pub request: ResolvedWorkflowRunRequest,
    pub outcome: StoredWorkflowModelResolutionOutcome,
    pub stamped_at: TimestampNanos,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StoredWorkflowModelResolutionOutcome {
    Resolved(ModelResolved),
    Unavailable(ModelUnavailable),
}

/// One registered agent, keyed by its minted [`OrchestratorAgentIdentifier`].
/// `reachability` is populated later by the discovery lane; it is `None` at
/// registration because reachability is discovered, never caller-declared.
///
/// `last_activity` records observed activity for inspection. It is refreshed
/// on real use (reachability discovery and a triage message), but it never
/// grants authority to retire an active agent or to delete a terminal record.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOrchestratorAgent {
    pub agent_identifier: OrchestratorAgentIdentifier,
    pub session: SessionIdentifier,
    pub mission: MissionDescription,
    pub harness: HarnessKind,
    pub reachability: Option<StoredAgentReachability>,
    pub registered_at: TimestampNanos,
    pub last_activity: TimestampNanos,
    pub status: OrchestratorAgentStatus,
}

impl StoredOrchestratorAgent {
    /// The elapsed time from an observation instant. It is used only to retain
    /// already-terminal records; it never authorizes an active-agent transition.
    pub fn idle_age_at(&self, observed_at: TimestampNanos) -> DurationNanos {
        TimestampInterval::new(self.last_activity, observed_at).duration()
    }
}

/// Where and how a registered agent is reached, discovered at registration by
/// the discovery lane. `harness_pid` plus `harness_start_time` together
/// disambiguate a recycled process identifier: a pid alone is not stable across
/// a harness restart, so the start time pins the exact process generation.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredAgentReachability {
    pub endpoint_kind: StoredAgentEndpointKind,
    pub target: String,
    pub harness_pid: u32,
    pub harness_start_time: u64,
}

/// How a reachability `target` is interpreted: a terminal cell located by its
/// session directory, or a harness process located by peer credentials.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredAgentEndpointKind {
    TerminalCell,
    HarnessProcess,
}

/// One topic in the orchestrator topic tree. Flattens the wire
/// [`OrchestratorTopic`] (`path`, `name`, `parent`) and adds the storage-owned
/// `created_at` stamp. The topic tree starts empty; topics are created
/// explicitly, never seeded at store open.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOrchestratorTopic {
    pub path: OrchestratorTopicPath,
    pub name: TopicName,
    pub parent: Option<OrchestratorTopicPath>,
    pub created_at: TimestampNanos,
}

/// One agent seated on one topic, keyed `agent_identifier|topic`.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOrchestratorTopicMembership {
    pub agent_identifier: OrchestratorAgentIdentifier,
    pub topic: OrchestratorTopicPath,
    pub joined_at: TimestampNanos,
}

/// One slotted triage-audit row: the store's append-only record of how a
/// message addressed to the orchestrator was triaged. Slots and timestamps are
/// store-minted, mirroring [`StoredActivity`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOrchestratorTriageRecord {
    pub slot: u64,
    pub sender: OrchestratorAgentIdentifier,
    pub incoming_kind: signal_orchestrator_message::OrchestratorMessageKind,
    pub verdict: StoredTriageVerdict,
    pub stamped_at: TimestampNanos,
}

/// The store's closed record of a triage verdict. Spawning is deliberately
/// inexpressible — there is no spawn or new-session variant. `Route` carries the
/// resolved recipients and the optional retyped kind; `Reject` carries the
/// typed reason.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StoredTriageVerdict {
    Route {
        recipients: Vec<OrchestratorAgentIdentifier>,
        retyped: Option<signal_orchestrator_message::OrchestratorMessageKind>,
    },
    Escalate,
    Reject {
        reason: StoredTriageRejectionReason,
    },
}

/// Why a triage rejected a message, storage-side projection of the
/// `signal-orchestrator-judge` triage rejection reasons.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredTriageRejectionReason {
    NoEligibleRecipient,
    SenderNotRegistered,
    MalformedPayload,
}

impl OrchestrateTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(Self::engine_open(store))?;
        let claims = engine.register_table(Self::family_descriptor(CLAIMS, "claim"))?;
        let roles = engine.register_table(Self::family_descriptor(ROLES, "role"))?;
        let lane_registry =
            engine.register_table(Self::family_descriptor(LANE_REGISTRY, "lane-registry"))?;
        let repositories =
            engine.register_table(Self::family_descriptor(REPOSITORIES, "repository"))?;
        let worktrees = engine.register_table(Self::family_descriptor(WORKTREES, "worktree"))?;
        let activities = engine.register_table(Self::family_descriptor(ACTIVITIES, "activity"))?;
        let activity_next_slot =
            engine.register_table(Self::family_descriptor(ACTIVITY_NEXT_SLOT, "activity-slot"))?;
        let divergences =
            engine.register_table(Self::family_descriptor(DIVERGENCES, "divergence"))?;
        let divergence_next_slot = engine.register_table(Self::family_descriptor(
            DIVERGENCE_NEXT_SLOT,
            "divergence-slot",
        ))?;
        let workflow_model_resolutions = engine.register_table(Self::family_descriptor(
            WORKFLOW_MODEL_RESOLUTIONS,
            "workflow-model-resolution",
        ))?;
        let orchestrator_agents = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_AGENTS,
            "orchestrator-agent",
        ))?;
        let orchestrator_topics = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TOPICS,
            "orchestrator-topic",
        ))?;
        let orchestrator_topic_membership = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TOPIC_MEMBERSHIP,
            "orchestrator-topic-membership",
        ))?;
        let orchestrator_triage_audit = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TRIAGE_AUDIT,
            "orchestrator-triage",
        ))?;
        let orchestrator_triage_next_slot = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TRIAGE_NEXT_SLOT,
            "orchestrator-triage-slot",
        ))?;
        Ok(Self {
            engine,
            claims,
            roles,
            lane_registry,
            repositories,
            worktrees,
            activities,
            activity_next_slot,
            divergences,
            divergence_next_slot,
            workflow_model_resolutions,
            orchestrator_agents,
            orchestrator_topics,
            orchestrator_topic_membership,
            orchestrator_triage_audit,
            orchestrator_triage_next_slot,
            atomic_commit_failure_for_test: AtomicBool::new(false),
        })
    }

    fn engine_open(store: &StoreLocation) -> EngineOpen {
        EngineOpen::new(store.as_path(), ORCHESTRATE_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("orchestrate"))
    }

    fn family_schema_hash(family: &str) -> SchemaHash {
        SchemaHash::for_label(format!(
            "orchestrate-{family}-v{}",
            ORCHESTRATE_SCHEMA_VERSION.value()
        ))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            Self::family_schema_hash(family),
        )
    }

    pub fn claim_records(&self) -> Result<Vec<StoredClaim>> {
        self.records(self.claims)
    }

    pub fn role_records(&self) -> Result<Vec<StoredRole>> {
        self.records(self.roles)
    }

    pub fn role_record(&self, role: &RoleIdentifier) -> Result<Option<StoredRole>> {
        self.record(self.roles, role.as_wire_token())
    }

    pub fn insert_role(&self, role: &StoredRole) -> Result<()> {
        self.upsert(self.roles, role.role.as_wire_token(), role)?;
        Ok(())
    }

    pub fn insert_role_if_missing(&self, role: &StoredRole) -> Result<()> {
        if self.role_record(&role.role)?.is_none() {
            self.insert_role(role)?;
        }
        Ok(())
    }

    pub fn remove_role(&self, role: &RoleIdentifier) -> Result<()> {
        self.remove_if_present(self.roles, role.as_wire_token())?;
        Ok(())
    }

    pub fn lane_records(&self) -> Result<Vec<StoredLaneRegistration>> {
        self.records(self.lane_registry)
    }

    pub fn lane_record(
        &self,
        session: &SessionIdentifier,
        lane: &LaneIdentifier,
    ) -> Result<Option<StoredLaneRegistration>> {
        self.record(
            self.lane_registry,
            LaneRegistrationKey::new(session, lane)
                .into_string()
                .as_str(),
        )
    }

    pub fn first_lane_record(
        &self,
        lane: &LaneIdentifier,
    ) -> Result<Option<StoredLaneRegistration>> {
        Ok(self
            .lane_records()?
            .into_iter()
            .find(|registration| registration.assignment.lane == *lane))
    }

    pub fn active_lane_record(
        &self,
        lane: &LaneIdentifier,
    ) -> Result<Option<StoredLaneRegistration>> {
        Ok(self.lane_records()?.into_iter().find(|registration| {
            registration.assignment.lane == *lane && registration.status == LaneStatus::Active
        }))
    }

    pub fn session_lane_records(
        &self,
        session: &SessionIdentifier,
    ) -> Result<Vec<StoredLaneRegistration>> {
        Ok(self
            .lane_records()?
            .into_iter()
            .filter(|registration| registration.assignment.session == *session)
            .collect())
    }

    pub fn insert_lane(&self, registration: &StoredLaneRegistration) -> Result<()> {
        let key = registration.key();
        self.upsert(self.lane_registry, key.as_str(), registration)?;
        Ok(())
    }

    /// Commit one ownership lifecycle transition. The lane registry and claims
    /// table are the canonical durable ownership view, so callers must change
    /// them together: a failed commit exposes neither half of the transition.
    pub fn transition_lane_ownership(
        &self,
        remove_lanes: &[StoredLaneRegistration],
        upsert_lanes: &[StoredLaneRegistration],
        remove_claim_keys: &[String],
        insert_claims: &[StoredClaim],
    ) -> Result<()> {
        let mut lane_changes = std::collections::BTreeMap::new();
        for lane in remove_lanes {
            lane_changes.insert(lane.key(), None);
        }
        for lane in upsert_lanes {
            lane_changes.insert(lane.key(), Some(lane.clone()));
        }

        let mut claim_changes = std::collections::BTreeMap::new();
        for key in remove_claim_keys {
            claim_changes.insert(key.clone(), None);
        }
        for claim in insert_claims {
            claim_changes.insert(claim.key(), Some(claim.clone()));
        }

        let mut commit = self.engine.begin_atomic_commit();
        let mut operations = 0;
        for (key, lane) in lane_changes {
            let existing = self.record(self.lane_registry, key.as_str())?;
            commit = match (lane, existing.is_some()) {
                (Some(lane), true) => commit.mutate(self.lane_registry, lane),
                (Some(lane), false) => commit.assert(self.lane_registry, lane),
                (None, true) => commit
                    .retract::<StoredLaneRegistration>(self.lane_registry, RecordKey::new(key)),
                (None, false) => continue,
            };
            operations += 1;
        }
        for (key, claim) in claim_changes {
            let existing = self.record(self.claims, key.as_str())?;
            commit = match (claim, existing.is_some()) {
                (Some(claim), true) => commit.mutate(self.claims, claim),
                (Some(claim), false) => commit.assert(self.claims, claim),
                (None, true) => commit.retract::<StoredClaim>(self.claims, RecordKey::new(key)),
                (None, false) => continue,
            };
            operations += 1;
        }
        if operations > 0 {
            self.commit_atomic(commit)?;
        }
        Ok(())
    }

    fn commit_atomic(&self, commit: sema_engine::AtomicCommit) -> Result<()> {
        if self
            .atomic_commit_failure_for_test
            .swap(false, Ordering::AcqRel)
        {
            return Err(std::io::Error::other("injected atomic commit failure").into());
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    /// Make the next fully constructed ownership commit fail at its storage
    /// boundary. This test seam proves callers expose no partial canonical state
    /// when the atomic storage commit is refused.
    #[doc(hidden)]
    pub fn fail_next_atomic_commit_for_test(&self) {
        self.atomic_commit_failure_for_test
            .store(true, Ordering::Release);
    }

    /// Refresh an active lane's observational `updated_at` stamp after real
    /// use. This metadata supports inspection only; it never authorizes a
    /// warning, recovery, retirement, claim release, or abandonment. A lane
    /// that is not currently active is left untouched.
    pub fn touch_lane(&self, lane: &LaneIdentifier) -> Result<()> {
        if let Some(mut registration) = self.active_lane_record(lane)? {
            registration.updated_at = self.current_timestamp()?;
            self.insert_lane(&registration)?;
        }
        Ok(())
    }

    pub fn replace_lanes(&self, lanes: &[StoredLaneRegistration]) -> Result<()> {
        let existing = self
            .lane_records()?
            .into_iter()
            .map(|registration| registration.key())
            .collect::<Vec<_>>();
        for key in existing {
            self.remove_if_present(self.lane_registry, key.as_str())?;
        }
        for registration in lanes {
            self.insert_lane(registration)?;
        }
        Ok(())
    }

    pub fn remove_lane(&self, session: &SessionIdentifier, lane: &LaneIdentifier) -> Result<()> {
        self.remove_if_present(
            self.lane_registry,
            LaneRegistrationKey::new(session, lane)
                .into_string()
                .as_str(),
        )?;
        Ok(())
    }

    pub fn remove_first_lane(&self, lane: &LaneIdentifier) -> Result<()> {
        if let Some(registration) = self.first_lane_record(lane)? {
            let key = registration.key();
            self.remove_if_present(self.lane_registry, key.as_str())?;
        }
        Ok(())
    }

    pub fn remove_lanes_for_session(
        &self,
        session: &SessionIdentifier,
    ) -> Result<Vec<StoredLaneRegistration>> {
        let removed_lanes = self.session_lane_records(session)?;
        for key in removed_lanes.iter().map(StoredLaneRegistration::key) {
            self.remove_if_present(self.lane_registry, key.as_str())?;
        }
        Ok(removed_lanes)
    }

    pub fn repository_records(&self) -> Result<Vec<StoredRepository>> {
        self.records(self.repositories)
    }

    pub fn replace_repositories(&self, repositories: &[StoredRepository]) -> Result<()> {
        let existing = self
            .repository_records()?
            .into_iter()
            .map(|repository| repository.name)
            .collect::<Vec<_>>();
        for name in existing {
            self.remove_if_present(self.repositories, name.as_str())?;
        }
        for repository in repositories {
            self.upsert(self.repositories, repository.name.as_str(), repository)?;
        }
        Ok(())
    }

    pub fn worktree_records(&self) -> Result<Vec<StoredWorktree>> {
        self.records(self.worktrees)
    }

    pub fn worktree_record(
        &self,
        repository: &RepositoryName,
        branch: &BranchName,
    ) -> Result<Option<StoredWorktree>> {
        self.record(
            self.worktrees,
            WorktreeKey::new(repository, branch).into_string().as_str(),
        )
    }

    pub fn insert_worktree(&self, worktree: &StoredWorktree) -> Result<()> {
        let key = WorktreeKey::new(&worktree.repository, &worktree.branch).into_string();
        self.upsert(self.worktrees, key.as_str(), worktree)?;
        Ok(())
    }

    /// Flip every `Active` worktree owned by `owning_lane` to
    /// [`WorktreeStatus::Abandoned`], returning how many were flipped. A pure
    /// status transition on durable state — no filesystem effect.
    pub fn mark_worktrees_abandoned_for_lane(&self, owning_lane: &str) -> Result<u32> {
        let mut flagged = 0;
        for mut record in self.worktree_records()? {
            if record.owning_lane.as_str() == owning_lane && record.status == WorktreeStatus::Active
            {
                record.status = WorktreeStatus::Abandoned;
                self.insert_worktree(&record)?;
                flagged += 1;
            }
        }
        Ok(flagged)
    }

    /// Delete one worktree row, keyed `repository|branch`.
    pub fn remove_worktree(&self, worktree: &StoredWorktree) -> Result<()> {
        self.remove_if_present(self.worktrees, worktree.key().as_str())
    }

    pub fn replace_worktrees(&self, worktrees: &[StoredWorktree]) -> Result<()> {
        let existing = self
            .worktree_records()?
            .iter()
            .map(StoredWorktree::key)
            .collect::<Vec<_>>();
        for key in existing {
            self.remove_if_present(self.worktrees, key.as_str())?;
        }
        for worktree in worktrees {
            self.insert_worktree(worktree)?;
        }
        Ok(())
    }

    pub fn replace_claims(
        &self,
        remove_keys: &[String],
        insert_claims: &[StoredClaim],
    ) -> Result<()> {
        // Claims are the canonical durable ownership view. Even a replacement
        // that spans several scopes is one commit, never a visible sequence of
        // removals followed by inserts.
        self.transition_lane_ownership(&[], &[], remove_keys, insert_claims)
    }

    pub fn replace_all_claims(&self, claims: &[StoredClaim]) -> Result<()> {
        let remove_keys = self
            .claim_records()?
            .iter()
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        self.replace_claims(&remove_keys, claims)
    }

    pub fn remove_claims_for_lane(&self, lane: &LaneIdentifier) -> Result<Vec<StoredClaim>> {
        let removed_claims = self
            .claim_records()?
            .into_iter()
            .filter(|claim| claim.lane == *lane)
            .collect::<Vec<_>>();
        let remove_keys = removed_claims
            .iter()
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        self.replace_claims(&remove_keys, &[])?;
        Ok(removed_claims)
    }

    pub fn remove_claims_for_role(&self, role: &RoleIdentifier) -> Result<Vec<StoredClaim>> {
        let mut role_lanes = std::collections::BTreeSet::new();
        for registration in self.lane_records()? {
            if registration.owner_role_identifier()? == *role {
                role_lanes.insert(registration.assignment.lane);
            }
        }
        let removed_claims = self
            .claim_records()?
            .into_iter()
            .filter(|claim| role_lanes.contains(&claim.lane))
            .collect::<Vec<_>>();
        let remove_keys = removed_claims
            .iter()
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        self.replace_claims(&remove_keys, &[])?;
        Ok(removed_claims)
    }

    pub fn remove_claims_without_lanes(&self) -> Result<Vec<StoredClaim>> {
        let active_lanes = self
            .lane_records()?
            .into_iter()
            .filter(|registration| registration.status == LaneStatus::Active)
            .map(|registration| registration.assignment.lane)
            .collect::<std::collections::BTreeSet<_>>();
        let removed_claims = self
            .claim_records()?
            .into_iter()
            .filter(|claim| !active_lanes.contains(&claim.lane))
            .collect::<Vec<_>>();
        let remove_keys = removed_claims
            .iter()
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        self.replace_claims(&remove_keys, &[])?;
        Ok(removed_claims)
    }

    pub fn append_activity(
        &self,
        role: RoleIdentifier,
        scope: ScopeReference,
        reason: ScopeReason,
    ) -> Result<StoredActivity> {
        let slot = self.next_activity_slot()?;
        let stamped_at = StoreClock::system().timestamp()?;
        let activity = StoredActivity::new(slot.value(), role, scope, reason, stamped_at);
        self.upsert(self.activities, &slot.key(), &activity)?;
        self.upsert(
            self.activity_next_slot,
            ACTIVITY_NEXT_SLOT_KEY,
            &slot.next_value(),
        )?;
        self.trim_activities()?;
        Ok(activity)
    }

    pub fn activity_records(&self) -> Result<Vec<StoredActivity>> {
        self.records(self.activities)
    }

    fn trim_activities(&self) -> Result<()> {
        let mut records = self.activity_records()?;
        records.sort_by_key(|record| record.slot);
        let expired = records.len().saturating_sub(CURRENT_ACTIVITY_LIMIT);
        for record in records.into_iter().take(expired) {
            self.remove_if_present(self.activities, &record.slot.to_string())?;
        }
        Ok(())
    }

    pub fn append_divergence(&self, partial: PartialApplied) -> Result<StoredDivergence> {
        let slot = self.next_divergence_slot()?;
        let stamped_at = self.current_timestamp()?;
        let divergence = StoredDivergence::new(slot.value(), partial, stamped_at);
        self.upsert(self.divergences, &slot.key(), &divergence)?;
        self.upsert(
            self.divergence_next_slot,
            DIVERGENCE_NEXT_SLOT_KEY,
            &slot.next_value(),
        )?;
        self.trim_divergences()?;
        Ok(divergence)
    }

    pub fn divergence_records(&self) -> Result<Vec<StoredDivergence>> {
        self.records(self.divergences)
    }

    fn trim_divergences(&self) -> Result<()> {
        let mut records = self.divergence_records()?;
        records.sort_by_key(|record| record.slot);
        let expired = records.len().saturating_sub(CURRENT_DIVERGENCE_LIMIT);
        for record in records.into_iter().take(expired) {
            self.remove_if_present(self.divergences, &record.slot.to_string())?;
        }
        Ok(())
    }

    pub fn insert_workflow_model_resolution(
        &self,
        resolution: &StoredWorkflowRunResolution,
    ) -> Result<()> {
        self.upsert(
            self.workflow_model_resolutions,
            resolution.handle.run.as_str(),
            resolution,
        )?;
        Ok(())
    }

    pub fn workflow_model_resolution_record(
        &self,
        handle: &WorkflowRunHandle,
    ) -> Result<Option<StoredWorkflowRunResolution>> {
        self.record(self.workflow_model_resolutions, handle.run.as_str())
    }

    pub fn workflow_model_resolution_records(&self) -> Result<Vec<StoredWorkflowRunResolution>> {
        self.records(self.workflow_model_resolutions)
    }

    pub fn orchestrator_agent_records(&self) -> Result<Vec<StoredOrchestratorAgent>> {
        self.records(self.orchestrator_agents)
    }

    pub fn orchestrator_agent_record(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<Option<StoredOrchestratorAgent>> {
        self.record(self.orchestrator_agents, agent_identifier.as_str())
    }

    /// Mint a fresh identity against the live registry key set. The
    /// orchestrator is the mint (psyche-ruled 2026-07-17); the registry's key
    /// set — including `Allocated` reservations — is the uniqueness domain.
    pub fn mint_orchestrator_agent_identifier(&self) -> Result<OrchestratorAgentIdentifier> {
        let used = self
            .orchestrator_agent_records()?
            .into_iter()
            .map(|agent| agent.agent_identifier.as_str().to_string());
        OrchestratorAgentIdentifierMint::from_identifiers(used).next_identifier()
    }

    /// Allocate an identity ahead of launch: mint against the live key set and
    /// seat the row `Allocated`, carrying the launch intent (session, mission,
    /// harness) so the reservation is honest from birth. Registration with the
    /// pre-minted identity later binds the row `Active`.
    pub fn allocate_orchestrator_agent(
        &self,
        session: SessionIdentifier,
        mission: MissionDescription,
        harness: HarnessKind,
    ) -> Result<StoredOrchestratorAgent> {
        let agent_identifier = self.mint_orchestrator_agent_identifier()?;
        let allocated_at = self.current_timestamp()?;
        let agent = StoredOrchestratorAgent {
            agent_identifier,
            session,
            mission,
            harness,
            reachability: None,
            registered_at: allocated_at,
            last_activity: allocated_at,
            status: OrchestratorAgentStatus::Allocated,
        };
        self.insert_orchestrator_agent(&agent)?;
        Ok(agent)
    }

    /// Register an agent on the orchestrator seat. `None` keeps the
    /// self-registration path: mint at registration, seat `Active` with no
    /// reachability yet (the discovery lane fills that in). `PreMinted` binds
    /// the named identity: registration binds, it does not mint — the existing
    /// row (any status, including a `Dead` generation being cold-respawned)
    /// is rebound `Active` with the registering payload as truth, fresh
    /// stamps, and reachability cleared for rediscovery. An unknown pre-minted
    /// identity is a typed error.
    pub fn register_orchestrator_agent(
        &self,
        session: SessionIdentifier,
        mission: MissionDescription,
        harness: HarnessKind,
        minted_identity: MintedIdentitySelection,
    ) -> Result<StoredOrchestratorAgent> {
        let agent_identifier = match minted_identity {
            MintedIdentitySelection::None => self.mint_orchestrator_agent_identifier()?,
            MintedIdentitySelection::PreMinted(agent_identifier) => {
                if self.orchestrator_agent_record(&agent_identifier)?.is_none() {
                    return Err(crate::Error::UnknownPreMintedAgentIdentity {
                        identifier: agent_identifier.as_str().to_string(),
                    });
                }
                agent_identifier
            }
        };
        let registered_at = self.current_timestamp()?;
        let agent = StoredOrchestratorAgent {
            agent_identifier,
            session,
            mission,
            harness,
            reachability: None,
            registered_at,
            last_activity: registered_at,
            status: OrchestratorAgentStatus::Active,
        };
        self.insert_orchestrator_agent(&agent)?;
        Ok(agent)
    }

    /// Refresh an agent's observational last-activity stamp after real use. It
    /// supports inspection and never authorizes retirement or abandonment. A
    /// no-op when the identifier is absent.
    pub fn touch_orchestrator_agent(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<()> {
        let Some(mut agent) = self.orchestrator_agent_record(agent_identifier)? else {
            return Ok(());
        };
        agent.last_activity = self.current_timestamp()?;
        self.insert_orchestrator_agent(&agent)?;
        Ok(())
    }

    /// Mark an `Active` agent `Retired`, re-stamping `last_activity` so the
    /// terminal retention window is measured from the retirement instant.
    /// Returns the retired row, or `None` when the identifier is absent.
    pub fn retire_orchestrator_agent(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<Option<StoredOrchestratorAgent>> {
        let Some(mut agent) = self.orchestrator_agent_record(agent_identifier)? else {
            return Ok(None);
        };
        agent.status = OrchestratorAgentStatus::Retired;
        agent.last_activity = self.current_timestamp()?;
        self.insert_orchestrator_agent(&agent)?;
        Ok(Some(agent))
    }

    /// Mark an `Active` agent `Dead` — its harness process generation is known
    /// exited — re-stamping `last_activity` so the terminal retention window is
    /// measured from the death observation. The transition fires once: an agent
    /// already terminal (`Retired` or `Dead`) is left untouched, so repeated
    /// reconciliation passes never extend a dead agent's retention. Returns the
    /// marked row, or `None` when the identifier is absent or not `Active`.
    pub fn mark_orchestrator_agent_dead(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<Option<StoredOrchestratorAgent>> {
        let Some(mut agent) = self.orchestrator_agent_record(agent_identifier)? else {
            return Ok(None);
        };
        if agent.status != OrchestratorAgentStatus::Active {
            return Ok(None);
        }
        agent.status = OrchestratorAgentStatus::Dead;
        agent.last_activity = self.current_timestamp()?;
        self.insert_orchestrator_agent(&agent)?;
        Ok(Some(agent))
    }

    /// Retire every `Active` agent registered to a session, returning how many
    /// were retired. Clearing a session is an explicit end-of-life for the agents
    /// that registered under it, so their retirement need not wait for the idle
    /// window to elapse.
    pub fn retire_session_orchestrator_agents(&self, session: &SessionIdentifier) -> Result<u32> {
        let mut retired = 0;
        for agent in self.orchestrator_agent_records()? {
            if agent.session == *session && agent.status == OrchestratorAgentStatus::Active {
                self.retire_orchestrator_agent(&agent.agent_identifier)?;
                retired += 1;
            }
        }
        Ok(retired)
    }

    /// Hard-delete an agent and every topic seat it held — a retired agent's
    /// memberships are reaped with it, never left orphaned.
    pub fn remove_orchestrator_agent(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<()> {
        self.remove_topic_memberships_for_agent(agent_identifier)?;
        self.remove_if_present(self.orchestrator_agents, agent_identifier.as_str())?;
        Ok(())
    }

    /// Delete every topic seat held by one agent, returning how many were
    /// removed.
    pub fn remove_topic_memberships_for_agent(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<u32> {
        let mut removed = 0;
        for membership in self.orchestrator_topic_membership_records()? {
            if membership.agent_identifier == *agent_identifier {
                self.remove_if_present(
                    self.orchestrator_topic_membership,
                    membership.key().as_str(),
                )?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Delete one topic by its path.
    pub fn remove_orchestrator_topic(&self, path: &OrchestratorTopicPath) -> Result<()> {
        self.remove_if_present(self.orchestrator_topics, path.as_str())
    }

    /// Whether any topic names `path` as its parent — a topic with children is
    /// structural and is never reaped out from under its subtree.
    pub fn orchestrator_topic_has_children(&self, path: &OrchestratorTopicPath) -> Result<bool> {
        Ok(self
            .orchestrator_topic_records()?
            .into_iter()
            .any(|topic| topic.parent.as_ref() == Some(path)))
    }

    /// Delete one workflow model-resolution row by its run handle.
    pub fn remove_workflow_model_resolution(&self, handle: &WorkflowRunHandle) -> Result<()> {
        self.remove_if_present(self.workflow_model_resolutions, handle.run.as_str())
    }

    /// Attach discovered reachability to an already-registered agent, returning
    /// the updated row. `None` when the identifier is absent (the agent was
    /// never registered). Reachability is discovered at registration; this is
    /// the write that records a match against a terminal-cell session.
    pub fn attach_agent_reachability(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
        reachability: StoredAgentReachability,
    ) -> Result<Option<StoredOrchestratorAgent>> {
        let Some(mut agent) = self.orchestrator_agent_record(agent_identifier)? else {
            return Ok(None);
        };
        agent.reachability = Some(reachability);
        // Reachability discovery records a fresh observation alongside the
        // reachability write; it does not alter lifecycle authority.
        agent.last_activity = self.current_timestamp()?;
        self.insert_orchestrator_agent(&agent)?;
        Ok(Some(agent))
    }

    /// Upsert an agent by its identifier. The discovery lane uses this to attach
    /// discovered reachability, and status transitions use it to retire.
    pub fn insert_orchestrator_agent(&self, agent: &StoredOrchestratorAgent) -> Result<()> {
        self.upsert(
            self.orchestrator_agents,
            agent.agent_identifier.as_str(),
            agent,
        )?;
        Ok(())
    }

    pub fn orchestrator_topic_records(&self) -> Result<Vec<StoredOrchestratorTopic>> {
        self.records(self.orchestrator_topics)
    }

    pub fn orchestrator_topic_record(
        &self,
        path: &OrchestratorTopicPath,
    ) -> Result<Option<StoredOrchestratorTopic>> {
        self.record(self.orchestrator_topics, path.as_str())
    }

    /// Create a topic only if its path is absent, returning the stored row
    /// either way. Explicit registration creates every topic in a selected
    /// path's lineage; a topic that already exists is joined, never
    /// overwritten, so its original name, parent, and `created_at` stand and no
    /// duplicate row is minted.
    pub fn ensure_orchestrator_topic(
        &self,
        path: OrchestratorTopicPath,
        name: TopicName,
        parent: Option<OrchestratorTopicPath>,
    ) -> Result<StoredOrchestratorTopic> {
        match self.orchestrator_topic_record(&path)? {
            Some(existing) => Ok(existing),
            None => self.insert_orchestrator_topic(path, name, parent),
        }
    }

    /// Create (or overwrite) a topic keyed by its path, stamping `created_at`.
    pub fn insert_orchestrator_topic(
        &self,
        path: OrchestratorTopicPath,
        name: TopicName,
        parent: Option<OrchestratorTopicPath>,
    ) -> Result<StoredOrchestratorTopic> {
        let created_at = self.current_timestamp()?;
        let topic = StoredOrchestratorTopic {
            path,
            name,
            parent,
            created_at,
        };
        self.upsert(self.orchestrator_topics, topic.path.as_str(), &topic)?;
        Ok(topic)
    }

    pub fn orchestrator_topic_membership_records(
        &self,
    ) -> Result<Vec<StoredOrchestratorTopicMembership>> {
        self.records(self.orchestrator_topic_membership)
    }

    /// Seat an agent on a topic, stamping `joined_at`. Keyed
    /// `agent_identifier|topic`, so re-seating updates the existing row.
    pub fn seat_agent_on_topic(
        &self,
        agent_identifier: OrchestratorAgentIdentifier,
        topic: OrchestratorTopicPath,
    ) -> Result<StoredOrchestratorTopicMembership> {
        let joined_at = self.current_timestamp()?;
        let membership = StoredOrchestratorTopicMembership {
            agent_identifier,
            topic,
            joined_at,
        };
        self.upsert(
            self.orchestrator_topic_membership,
            membership.key().as_str(),
            &membership,
        )?;
        Ok(membership)
    }

    pub fn topic_member_identifiers(
        &self,
        topic: &OrchestratorTopicPath,
    ) -> Result<Vec<OrchestratorAgentIdentifier>> {
        Ok(self
            .orchestrator_topic_membership_records()?
            .into_iter()
            .filter(|membership| membership.topic == *topic)
            .map(|membership| membership.agent_identifier)
            .collect())
    }

    pub fn agent_topic_paths(
        &self,
        agent_identifier: &OrchestratorAgentIdentifier,
    ) -> Result<Vec<OrchestratorTopicPath>> {
        Ok(self
            .orchestrator_topic_membership_records()?
            .into_iter()
            .filter(|membership| membership.agent_identifier == *agent_identifier)
            .map(|membership| membership.topic)
            .collect())
    }

    /// Append one triage-audit row, minting the slot and timestamp.
    pub fn append_orchestrator_triage_record(
        &self,
        sender: OrchestratorAgentIdentifier,
        incoming_kind: signal_orchestrator_message::OrchestratorMessageKind,
        verdict: StoredTriageVerdict,
    ) -> Result<StoredOrchestratorTriageRecord> {
        let slot = self.next_orchestrator_triage_slot()?;
        let stamped_at = self.current_timestamp()?;
        let record = StoredOrchestratorTriageRecord {
            slot: slot.value(),
            sender,
            incoming_kind,
            verdict,
            stamped_at,
        };
        self.upsert(self.orchestrator_triage_audit, &slot.key(), &record)?;
        self.upsert(
            self.orchestrator_triage_next_slot,
            ORCHESTRATOR_TRIAGE_NEXT_SLOT_KEY,
            &slot.next_value(),
        )?;
        // A triaged message records fresh sender activity for inspection; it
        // does not alter lifecycle authority.
        self.touch_orchestrator_agent(&record.sender)?;
        self.trim_orchestrator_triage_records()?;
        Ok(record)
    }

    pub fn orchestrator_triage_records(&self) -> Result<Vec<StoredOrchestratorTriageRecord>> {
        self.records(self.orchestrator_triage_audit)
    }

    fn trim_orchestrator_triage_records(&self) -> Result<()> {
        let mut records = self.orchestrator_triage_records()?;
        records.sort_by_key(|record| record.slot);
        let expired = records
            .len()
            .saturating_sub(CURRENT_ORCHESTRATOR_TRIAGE_LIMIT);
        for record in records.into_iter().take(expired) {
            self.remove_if_present(self.orchestrator_triage_audit, &record.slot.to_string())?;
        }
        Ok(())
    }

    fn next_orchestrator_triage_slot(&self) -> Result<ActivitySlot> {
        let stored = self.record(
            self.orchestrator_triage_next_slot,
            ORCHESTRATOR_TRIAGE_NEXT_SLOT_KEY,
        )?;
        match stored {
            Some(next_slot) => Ok(ActivitySlot::new(next_slot)),
            None => Ok(ActivitySlot::after_triage_records(
                &self.orchestrator_triage_records()?,
            )),
        }
    }

    pub fn current_timestamp(&self) -> Result<TimestampNanos> {
        StoreClock::system().timestamp()
    }

    pub fn current_commit_sequence(&self) -> Result<u64> {
        Ok(self.engine.current_commit_sequence()?.value())
    }

    fn next_activity_slot(&self) -> Result<ActivitySlot> {
        let stored = self.record(self.activity_next_slot, ACTIVITY_NEXT_SLOT_KEY)?;
        match stored {
            Some(next_slot) => Ok(ActivitySlot::new(next_slot)),
            None => Ok(ActivitySlot::after_activity_records(
                &self.activity_records()?,
            )),
        }
    }

    fn next_divergence_slot(&self) -> Result<ActivitySlot> {
        let stored = self.record(self.divergence_next_slot, DIVERGENCE_NEXT_SLOT_KEY)?;
        match stored {
            Some(next_slot) => Ok(ActivitySlot::new(next_slot)),
            None => Ok(ActivitySlot::after_divergence_records(
                &self.divergence_records()?,
            )),
        }
    }

    fn records<RecordValue>(&self, table: TableReference<RecordValue>) -> Result<Vec<RecordValue>>
    where
        RecordValue: OrchestrateStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::all(table))?
            .records()
            .to_vec())
    }

    fn record<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: &str,
    ) -> Result<Option<RecordValue>>
    where
        RecordValue: OrchestrateStoredValue,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::key(table, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    fn upsert<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: &str,
        record: &RecordValue,
    ) -> Result<()>
    where
        RecordValue: OrchestrateStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let record_key = RecordKey::new(key);
        if self.record(table, key)?.is_some() {
            self.engine
                .mutate_keyed(KeyedMutation::new(table, record_key, record.clone()))?;
        } else {
            self.engine
                .assert_keyed(KeyedAssertion::new(table, record_key, record.clone()))?;
        }
        Ok(())
    }

    fn remove_if_present<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: &str,
    ) -> Result<()>
    where
        RecordValue: OrchestrateStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        if self.record(table, key)?.is_some() {
            self.engine
                .retract(Retraction::new(table, RecordKey::new(key)))?;
        }
        Ok(())
    }
}

impl StoredRole {
    pub fn new(
        role: RoleIdentifier,
        harness: HarnessKind,
        report_repository_path: WirePath,
        report_lane_path: WirePath,
    ) -> Self {
        Self {
            role,
            harness,
            report_repository_path,
            report_lane_path,
        }
    }
}

impl StoredRepository {
    pub fn new(
        identity: RepositoryIdentityState,
        name: RepositoryName,
        path: WirePath,
        refreshed_at: TimestampNanos,
    ) -> Self {
        Self {
            identity,
            name,
            path,
            active: true,
            refreshed_at,
        }
    }

    /// The text that keys this repository across local hosting paths: the
    /// canonical real identity when identified, the local index alias as the
    /// honest fallback otherwise.
    pub fn identity_key(&self) -> String {
        self.identity.key_text(&self.name)
    }
}

impl From<StoredRepository> for signal_orchestrate::Repository {
    fn from(repository: StoredRepository) -> Self {
        Self {
            identity: repository.identity,
            name: repository.name,
            path: repository.path,
            active: repository.active,
            refreshed_at: repository.refreshed_at,
        }
    }
}

impl StoredWorktree {
    pub fn key(&self) -> String {
        WorktreeKey::new(&self.repository, &self.branch).into_string()
    }
}

impl From<Worktree> for StoredWorktree {
    fn from(worktree: Worktree) -> Self {
        Self {
            repository: worktree.repository,
            branch: worktree.branch,
            path: worktree.path,
            owning_lane: worktree.owning_lane,
            status: worktree.status,
            purpose: worktree.purpose,
            last_activity: worktree.last_activity,
            pushed_state: worktree.pushed_state,
        }
    }
}

impl From<StoredWorktree> for Worktree {
    fn from(worktree: StoredWorktree) -> Self {
        Self {
            repository: worktree.repository,
            branch: worktree.branch,
            path: worktree.path,
            owning_lane: worktree.owning_lane,
            status: worktree.status,
            purpose: worktree.purpose,
            last_activity: worktree.last_activity,
            pushed_state: worktree.pushed_state,
        }
    }
}

/// Composite key `repository|branch` for the worktrees table — the
/// `(repository, branch)` identity of a [`StoredWorktree`].
struct WorktreeKey {
    repository: String,
    branch: String,
}

impl WorktreeKey {
    fn new(repository: &RepositoryName, branch: &BranchName) -> Self {
        Self {
            repository: repository.as_str().to_string(),
            branch: branch.as_str().to_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.repository, self.branch)
    }
}

impl StoredActivity {
    fn new(
        slot: u64,
        role: RoleIdentifier,
        scope: ScopeReference,
        reason: ScopeReason,
        stamped_at: TimestampNanos,
    ) -> Self {
        Self {
            slot,
            role,
            scope,
            reason,
            stamped_at,
        }
    }

    pub fn into_activity(self) -> Activity {
        Activity {
            role: self.role,
            scope: self.scope,
            reason: self.reason,
            stamped_at: self.stamped_at,
        }
    }
}

impl StoredDivergence {
    fn new(slot: u64, partial: PartialApplied, stamped_at: TimestampNanos) -> Self {
        Self {
            slot,
            succeeded: partial.succeeded,
            failed: partial.failed,
            stamped_at,
        }
    }

    pub fn into_partial_applied(self) -> PartialApplied {
        PartialApplied {
            succeeded: self.succeeded,
            failed: self.failed,
        }
    }
}

impl StoredWorkflowRunResolution {
    pub fn resolved(
        handle: WorkflowRunHandle,
        request: ResolvedWorkflowRunRequest,
        resolution: ModelResolved,
        stamped_at: TimestampNanos,
    ) -> Self {
        Self {
            handle,
            request,
            outcome: StoredWorkflowModelResolutionOutcome::Resolved(resolution),
            stamped_at,
        }
    }

    pub fn unavailable(
        handle: WorkflowRunHandle,
        request: ResolvedWorkflowRunRequest,
        unavailable: ModelUnavailable,
        stamped_at: TimestampNanos,
    ) -> Self {
        Self {
            handle,
            request,
            outcome: StoredWorkflowModelResolutionOutcome::Unavailable(unavailable),
            stamped_at,
        }
    }
}

impl StoredOrchestratorTopic {
    /// Project back to the wire [`OrchestratorTopic`], dropping the
    /// storage-owned `created_at` stamp the wire form does not carry.
    pub fn into_orchestrator_topic(self) -> OrchestratorTopic {
        OrchestratorTopic {
            path: self.path,
            name: self.name,
            parent: self.parent,
        }
    }
}

impl StoredOrchestratorTopicMembership {
    fn key(&self) -> String {
        TopicMembershipKey::new(&self.agent_identifier, &self.topic).into_string()
    }
}

/// Composite key `agent_identifier|topic` for the topic-membership table —
/// the identity of a [`StoredOrchestratorTopicMembership`].
struct TopicMembershipKey {
    agent_identifier: String,
    topic: String,
}

impl TopicMembershipKey {
    fn new(agent_identifier: &OrchestratorAgentIdentifier, topic: &OrchestratorTopicPath) -> Self {
        Self {
            agent_identifier: agent_identifier.as_str().to_string(),
            topic: topic.as_str().to_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.agent_identifier, self.topic)
    }
}

impl EngineRecord for StoredClaim {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key())
    }
}

impl StoredClaim {
    pub fn new(
        lane: LaneIdentifier,
        scope: ScopeReference,
        reason: ScopeReason,
        claimed_at: TimestampNanos,
    ) -> Self {
        Self {
            lane,
            scope,
            reason,
            claimed_at,
        }
    }

    pub fn key(&self) -> String {
        ClaimKey::new(&self.lane, &self.scope).into_string()
    }

    pub fn resource_claim_at(&self, observed_at: TimestampNanos) -> LaneResourceClaim {
        LaneResourceClaim {
            scope: self.scope.clone(),
            reason: self.reason.clone(),
            claimed_at: self.claimed_at,
            age: self.age_at(observed_at),
        }
    }

    pub fn age_at(&self, observed_at: TimestampNanos) -> DurationNanos {
        TimestampInterval::new(self.claimed_at, observed_at).duration()
    }
}

impl EngineRecord for StoredLaneRegistration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key())
    }
}

impl StoredLaneRegistration {
    pub fn owner_role_identifier(&self) -> Result<RoleIdentifier> {
        RoleIdentifierForLaneOwner::new(&self.assignment.owner.role).role_identifier()
    }

    pub fn new(
        assignment: LaneAssignment,
        registered_at: TimestampNanos,
        updated_at: TimestampNanos,
        status: LaneStatus,
    ) -> Self {
        Self {
            assignment,
            registered_at,
            updated_at,
            status,
        }
    }

    pub fn active(assignment: LaneAssignment, registered_at: TimestampNanos) -> Self {
        Self::new(assignment, registered_at, registered_at, LaneStatus::Active)
    }

    pub fn key(&self) -> String {
        LaneRegistrationKey::new(&self.assignment.session, &self.assignment.lane).into_string()
    }

    pub fn registration(&self) -> LaneRegistration {
        LaneRegistration {
            assignment: self.assignment.clone(),
            registered_at: self.registered_at,
            status: self.status,
        }
    }

    pub fn age_at(&self, observed_at: TimestampNanos) -> DurationNanos {
        TimestampInterval::new(self.updated_at, observed_at).duration()
    }
}

struct RoleIdentifierForLaneOwner<'role> {
    role: &'role Role,
}

impl<'role> RoleIdentifierForLaneOwner<'role> {
    fn new(role: &'role Role) -> Self {
        Self { role }
    }

    fn role_identifier(&self) -> Result<RoleIdentifier> {
        let rendered = self
            .role
            .tokens()
            .iter()
            .map(|token| Self::pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        Ok(RoleIdentifier::from_wire_token(rendered)?)
    }

    fn pascal_to_kebab(value: &str) -> String {
        let mut rendered = String::new();
        for (index, character) in value.chars().enumerate() {
            if index > 0 && character.is_ascii_uppercase() {
                rendered.push('-');
            }
            rendered.push(character.to_ascii_lowercase());
        }
        rendered
    }
}

struct LaneRegistrationKey {
    session: String,
    lane: String,
}

impl LaneRegistrationKey {
    fn new(session: &SessionIdentifier, lane: &LaneIdentifier) -> Self {
        Self {
            session: session.as_wire_token().to_string(),
            lane: lane.as_wire_token().to_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.session, self.lane)
    }
}

struct TimestampInterval {
    start: TimestampNanos,
    end: TimestampNanos,
}

impl TimestampInterval {
    fn new(start: TimestampNanos, end: TimestampNanos) -> Self {
        Self { start, end }
    }

    fn duration(&self) -> DurationNanos {
        DurationNanos::new(self.end.value().saturating_sub(self.start.value()))
    }
}

struct ActivitySlot {
    value: u64,
}

impl ActivitySlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_activity_records(records: &[StoredActivity]) -> Self {
        let value = records
            .iter()
            .map(|activity| activity.slot)
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn after_divergence_records(records: &[StoredDivergence]) -> Self {
        let value = records
            .iter()
            .map(|divergence| divergence.slot)
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn after_triage_records(records: &[StoredOrchestratorTriageRecord]) -> Self {
        let value = records
            .iter()
            .map(|record| record.slot)
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn value(&self) -> u64 {
        self.value
    }

    fn next_value(&self) -> u64 {
        self.value + 1
    }

    fn key(&self) -> String {
        self.value.to_string()
    }
}

struct StoreClock {
    epoch: SystemTime,
}

impl StoreClock {
    fn system() -> Self {
        Self { epoch: UNIX_EPOCH }
    }

    fn timestamp(&self) -> Result<TimestampNanos> {
        let nanos = SystemTime::now()
            .duration_since(self.epoch)?
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        Ok(TimestampNanos::new(nanos))
    }
}

struct ClaimKey {
    lane: String,
    scope: String,
}

impl ClaimKey {
    fn new(lane: &LaneIdentifier, scope: &ScopeReference) -> Self {
        Self {
            lane: lane.as_wire_token().to_string(),
            scope: ScopeKey::new(scope).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.lane, self.scope)
    }
}

struct ScopeKey {
    value: String,
}

impl ScopeKey {
    fn new(scope: &ScopeReference) -> Self {
        let value = match scope {
            ScopeReference::Path(path) => format!("path:{}", path.as_str()),
            ScopeReference::Task(task) => format!("task:{}", task.as_str()),
        };
        Self { value }
    }

    fn into_string(self) -> String {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_orchestrate::{LaneAuthority, LaneDetails, LaneOwner, Role, RoleToken, TaskToken};

    struct TemporaryStore {
        path: std::path::PathBuf,
    }

    impl TemporaryStore {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "{name}-{}-{}.sema",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time after epoch")
                    .as_nanos()
            ));
            Self { path }
        }

        fn location(&self) -> StoreLocation {
            StoreLocation::new(self.path.to_string_lossy().into_owned())
        }
    }

    impl Drop for TemporaryStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn session_lane_rows_are_keyed_by_session_and_lane_with_age_support() {
        let temporary = TemporaryStore::new("orchestrate-session-lane-storage");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        let assignment = LaneAssignment {
            session: SessionIdentifier::from_camel_case_name("StorageRoundTrip").expect("session"),
            lane: LaneIdentifier::from_wire_token("storage-worker").expect("lane"),
            owner: LaneOwner {
                role: Role::try_new(vec![RoleToken::from_text("Designer").expect("role token")])
                    .expect("role"),
                authority: LaneAuthority::Structural,
            },
            details: LaneDetails::from_text("storage round-trip test lane").expect("details"),
        };
        let mut stored =
            StoredLaneRegistration::active(assignment.clone(), TimestampNanos::new(100));
        stored.updated_at = TimestampNanos::new(125);

        tables.insert_lane(&stored).expect("insert lane");

        let found = tables
            .lane_record(&assignment.session, &assignment.lane)
            .expect("read lane")
            .expect("stored lane");
        assert_eq!(found.assignment, assignment);
        assert_eq!(found.registration().registered_at, TimestampNanos::new(100));
        assert_eq!(found.age_at(TimestampNanos::new(175)).value(), 50);
        assert!(
            tables
                .session_lane_records(&found.assignment.session)
                .expect("session lanes")
                .iter()
                .any(|lane| lane.assignment.lane == found.assignment.lane)
        );
        let removed = tables
            .remove_lanes_for_session(&found.assignment.session)
            .expect("clear session lanes");
        assert_eq!(removed.len(), 1);
        assert!(
            tables
                .session_lane_records(&found.assignment.session)
                .expect("cleared session lanes")
                .is_empty()
        );
    }

    #[test]
    fn claim_rows_store_claimed_at_for_resource_age_evidence() {
        let temporary = TemporaryStore::new("orchestrate-claim-age-storage");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        let claim = StoredClaim::new(
            LaneIdentifier::from_wire_token("designer").expect("lane"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-storage").expect("path"),
            ),
            ScopeReason::from_text("observes storage round-trip").expect("reason"),
            TimestampNanos::new(200),
        );

        tables
            .replace_all_claims(std::slice::from_ref(&claim))
            .expect("insert claim");

        let stored = tables
            .claim_records()
            .expect("claims")
            .pop()
            .expect("claim");
        assert_eq!(stored.claimed_at, TimestampNanos::new(200));
        assert_eq!(stored.age_at(TimestampNanos::new(275)).value(), 75);
        let resource = stored.resource_claim_at(TimestampNanos::new(275));
        assert_eq!(resource.claimed_at, TimestampNanos::new(200));
        assert_eq!(resource.age.value(), 75);
        assert_eq!(resource.reason, claim.reason);
    }

    #[test]
    fn undeclared_store_family_identity_fails_closed() {
        let temporary = TemporaryStore::new("orchestrate-foreign-family");
        let location = temporary.location();
        let mut engine =
            Engine::open(OrchestrateTables::engine_open(&location)).expect("foreign store opens");
        engine
            .register_table::<StoredClaim>(TableDescriptor::new(
                CLAIMS,
                FamilyName::new("claim"),
                SchemaHash::for_label("foreign-claim-family"),
            ))
            .expect("foreign family registers");
        drop(engine);

        let outcome = OrchestrateTables::open(&location);
        assert!(matches!(
            outcome,
            Err(crate::Error::SemaEngine(
                sema_engine::Error::FamilyIdentityMismatch { .. }
            ))
        ));
    }

    fn test_session() -> SessionIdentifier {
        SessionIdentifier::from_camel_case_name("AgentCoordination").expect("session")
    }

    fn test_mission() -> MissionDescription {
        MissionDescription::from_text("map the orchestrator storage layer").expect("mission")
    }

    #[test]
    fn divergence_records_are_bounded_to_current_reality() {
        let temporary = TemporaryStore::new("orchestrate-bounded-divergences");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        for _ in 0..=CURRENT_DIVERGENCE_LIMIT {
            tables
                .append_divergence(PartialApplied {
                    succeeded: Vec::new(),
                    failed: Vec::new(),
                })
                .expect("append divergence");
        }
        let records = tables.divergence_records().expect("divergences");
        assert_eq!(records.len(), CURRENT_DIVERGENCE_LIMIT);
        assert_eq!(records.iter().map(|record| record.slot).min(), Some(1));
    }

    #[test]
    fn activity_records_are_bounded_to_current_reality() {
        let temporary = TemporaryStore::new("orchestrate-bounded-activity");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        for _ in 0..=CURRENT_ACTIVITY_LIMIT {
            tables
                .append_activity(
                    RoleIdentifier::from_wire_token("stateful-scenario").expect("role"),
                    ScopeReference::Task(
                        TaskToken::from_wire_token("scenario-bound").expect("task"),
                    ),
                    ScopeReason::from_text("bounded activity").expect("reason"),
                )
                .expect("append activity");
        }
        let records = tables.activity_records().expect("activities");
        assert_eq!(records.len(), CURRENT_ACTIVITY_LIMIT);
        assert_eq!(records.iter().map(|record| record.slot).min(), Some(1));
    }

    #[test]
    fn orchestrator_agents_round_trip_with_store_minted_unique_identifiers() {
        let temporary = TemporaryStore::new("orchestrate-agent-registry");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");

        let mut minted = std::collections::BTreeSet::new();
        for _ in 0..16 {
            let agent = tables
                .register_orchestrator_agent(
                    test_session(),
                    test_mission(),
                    HarnessKind::Claude,
                    MintedIdentitySelection::None,
                )
                .expect("register agent");
            assert_eq!(agent.status, OrchestratorAgentStatus::Active);
            assert!(agent.reachability.is_none());
            assert!(
                minted.insert(agent.agent_identifier.as_str().to_string()),
                "store minted a duplicate identifier {}",
                agent.agent_identifier.as_str()
            );
            let stored = tables
                .orchestrator_agent_record(&agent.agent_identifier)
                .expect("read agent")
                .expect("agent present");
            assert_eq!(stored, agent);
        }
        assert_eq!(
            tables.orchestrator_agent_records().expect("agents").len(),
            16
        );
    }

    #[test]
    fn agent_reachability_round_trips_when_the_discovery_lane_attaches_it() {
        let temporary = TemporaryStore::new("orchestrate-agent-reachability");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        let agent = tables
            .register_orchestrator_agent(
                test_session(),
                test_mission(),
                HarnessKind::Codex,
                MintedIdentitySelection::None,
            )
            .expect("register agent");

        let discovered = StoredOrchestratorAgent {
            reachability: Some(StoredAgentReachability {
                endpoint_kind: StoredAgentEndpointKind::TerminalCell,
                target: "terminal-cell-7".to_string(),
                harness_pid: 4242,
                harness_start_time: 99_887_766,
            }),
            ..agent.clone()
        };
        tables
            .insert_orchestrator_agent(&discovered)
            .expect("attach reachability");

        let stored = tables
            .orchestrator_agent_record(&agent.agent_identifier)
            .expect("read agent")
            .expect("agent present");
        assert_eq!(stored, discovered);
        let reachability = stored.reachability.expect("reachability present");
        assert_eq!(
            reachability.endpoint_kind,
            StoredAgentEndpointKind::TerminalCell
        );
        assert_eq!(reachability.harness_pid, 4242);
        assert_eq!(reachability.harness_start_time, 99_887_766);
    }

    #[test]
    fn orchestrator_topic_tree_starts_empty() {
        let temporary = TemporaryStore::new("orchestrate-empty-topic-tree");
        let tables = OrchestrateTables::open(&temporary.location()).expect("first open");
        assert!(
            tables
                .orchestrator_topic_records()
                .expect("topics")
                .is_empty(),
            "a fresh store seeds no topic; the tree starts empty"
        );
        drop(tables);
        let reopened = OrchestrateTables::open(&temporary.location()).expect("reopen");
        assert!(
            reopened
                .orchestrator_topic_records()
                .expect("topics after reopen")
                .is_empty(),
            "reopening the store must not seed a topic"
        );
    }

    #[test]
    fn orchestrator_topics_and_membership_round_trip() {
        let temporary = TemporaryStore::new("orchestrate-topics-membership");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");

        let root = tables
            .insert_orchestrator_topic(
                OrchestratorTopicPath::from_wire_token("engineering").expect("root path"),
                TopicName::from_text("Engineering").expect("root name"),
                None,
            )
            .expect("insert root topic");
        let topic = tables
            .insert_orchestrator_topic(
                OrchestratorTopicPath::from_wire_token("storage").expect("path"),
                TopicName::from_text("Storage layer").expect("name"),
                Some(root.path.clone()),
            )
            .expect("insert topic");
        let stored = tables
            .orchestrator_topic_record(&topic.path)
            .expect("read topic")
            .expect("topic present");
        assert_eq!(stored, topic);
        assert_eq!(stored.parent, Some(root.path.clone()));
        assert_eq!(
            stored.clone().into_orchestrator_topic().name.as_str(),
            "Storage layer"
        );

        let agent = tables
            .register_orchestrator_agent(
                test_session(),
                test_mission(),
                HarnessKind::Claude,
                MintedIdentitySelection::None,
            )
            .expect("register agent");
        tables
            .seat_agent_on_topic(agent.agent_identifier.clone(), topic.path.clone())
            .expect("seat agent");

        assert_eq!(
            tables
                .topic_member_identifiers(&topic.path)
                .expect("members"),
            vec![agent.agent_identifier.clone()]
        );
        assert_eq!(
            tables
                .agent_topic_paths(&agent.agent_identifier)
                .expect("agent topics"),
            vec![topic.path]
        );
    }

    #[test]
    fn triage_audit_appends_slotted_records_and_round_trips_verdicts() {
        let temporary = TemporaryStore::new("orchestrate-triage-audit");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");
        let sender = tables
            .register_orchestrator_agent(
                test_session(),
                test_mission(),
                HarnessKind::Claude,
                MintedIdentitySelection::None,
            )
            .expect("register sender");
        let recipient = tables
            .register_orchestrator_agent(
                test_session(),
                test_mission(),
                HarnessKind::Codex,
                MintedIdentitySelection::None,
            )
            .expect("register recipient");

        let routed = tables
            .append_orchestrator_triage_record(
                sender.agent_identifier.clone(),
                signal_orchestrator_message::OrchestratorMessageKind::Guidance(
                    signal_orchestrator_message::GuidanceMagnitude::Standard,
                ),
                StoredTriageVerdict::Route {
                    recipients: vec![recipient.agent_identifier.clone()],
                    retyped: Some(
                        signal_orchestrator_message::OrchestratorMessageKind::Interruption,
                    ),
                },
            )
            .expect("append routed verdict");
        let rejected = tables
            .append_orchestrator_triage_record(
                sender.agent_identifier.clone(),
                signal_orchestrator_message::OrchestratorMessageKind::Report,
                StoredTriageVerdict::Reject {
                    reason: StoredTriageRejectionReason::SenderNotRegistered,
                },
            )
            .expect("append rejected verdict");

        assert_eq!(routed.slot, 0);
        assert_eq!(rejected.slot, 1);
        let records = tables
            .orchestrator_triage_records()
            .expect("triage records");
        assert_eq!(records.len(), 2);
        assert!(records.contains(&routed));
        assert!(records.contains(&rejected));

        for _ in 0..CURRENT_ORCHESTRATOR_TRIAGE_LIMIT {
            tables
                .append_orchestrator_triage_record(
                    sender.agent_identifier.clone(),
                    signal_orchestrator_message::OrchestratorMessageKind::Report,
                    StoredTriageVerdict::Escalate,
                )
                .expect("append current triage record");
        }
        let bounded = tables
            .orchestrator_triage_records()
            .expect("bounded triage records");
        assert_eq!(bounded.len(), CURRENT_ORCHESTRATOR_TRIAGE_LIMIT);
        assert_eq!(bounded.iter().map(|record| record.slot).min(), Some(2));
    }

    #[test]
    fn mark_dead_transitions_an_active_agent_once() {
        let temporary = TemporaryStore::new("orchestrate-mark-dead");
        let tables = OrchestrateTables::open(&temporary.location()).expect("store opens");
        let agent = tables
            .register_orchestrator_agent(
                SessionIdentifier::from_camel_case_name("DeathTransition").expect("session"),
                MissionDescription::from_text("dies in this test").expect("mission"),
                HarnessKind::Codex,
                MintedIdentitySelection::None,
            )
            .expect("register agent");

        let marked = tables
            .mark_orchestrator_agent_dead(&agent.agent_identifier)
            .expect("mark dead")
            .expect("active agent transitions");
        assert_eq!(marked.status, OrchestratorAgentStatus::Dead);
        assert!(
            marked.last_activity.value() > agent.last_activity.value(),
            "death re-stamps last_activity so retention is measured from the observation"
        );

        // The transition fires once: a second mark is a no-op that never
        // extends the dead agent's retention window.
        assert!(
            tables
                .mark_orchestrator_agent_dead(&agent.agent_identifier)
                .expect("second mark")
                .is_none()
        );

        // A retired agent is terminal and is never re-marked dead.
        let retired = tables
            .register_orchestrator_agent(
                SessionIdentifier::from_camel_case_name("RetiredStaysRetired").expect("session"),
                MissionDescription::from_text("retires normally").expect("mission"),
                HarnessKind::Codex,
                MintedIdentitySelection::None,
            )
            .expect("register second agent");
        tables
            .retire_orchestrator_agent(&retired.agent_identifier)
            .expect("retire");
        assert!(
            tables
                .mark_orchestrator_agent_dead(&retired.agent_identifier)
                .expect("mark retired")
                .is_none()
        );
    }
}
