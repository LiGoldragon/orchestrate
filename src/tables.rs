use std::time::{SystemTime, UNIX_EPOCH};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::{
    Engine, EngineOpen, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan, RecordKey,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_harness::{ModelResolved, ModelUnavailable};
use signal_orchestrate::{
    Activity, ApplicationFailure, ApplicationSuccess, BranchName, DurationNanos, HarnessKind,
    LaneAssignment, LaneIdentifier, LaneName, LaneRegistration, LaneResourceClaim, LaneStatus,
    MissionDescription, OrchestratorAgentIdentifier, OrchestratorAgentStatus, OrchestratorTopic,
    OrchestratorTopicPath, PartialApplied, PurposeText, PushedState, RepositoryName,
    ResolvedWorkflowRunRequest, Role, RoleName, ScopeReason, ScopeReference, SessionIdentifier,
    TimestampNanos, TopicName, WirePath, WorkflowRunHandle, Worktree, WorktreeStatus,
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

// Bumped 7 -> 8 for the orchestrator-agent activity stamp: the agent registry
// gained a `last_activity` field so the interim table reaper can reap an agent
// by its own idle age, exactly as the lane reaper reaps a lane. Only the
// orchestrator-agent family layout changed, so only its family hash advances to
// v8; every other family stays pinned to the version at which its own layout was
// last set (stable claim/role/lane/worktree tables at v5, workflow model
// resolutions at v6, the topic tree / topic membership / triage audit at v7).
// Migration forward is additive for every unchanged family — an older store
// keeps its data — but the agent family's own layout changed, so its stored
// catalog identity no longer matches the declared one. A store carrying the
// agent registry under the old identity (whether it just bumped its file version
// or was already re-stamped) cannot re-register it additively; the migration
// reads that family's rows in their prior shape, retires the stale identity, and
// re-inserts the rows in the current shape, so the agent registry is carried
// forward rather than lost. See `OrchestrateStoreMigration`.
//
// Bumped 6 -> 7 for the orchestrator seat: the agent registry, topic tree,
// topic membership, and triage audit log. The orchestrator topic tree starts
// empty; there is no seeded topic.
const ORCHESTRATE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(8);
/// The version at which every orchestrator-seat family except the agent registry
/// was introduced; those families' layouts are unchanged at v8, so their family
/// hashes stay pinned here.
const ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY: SchemaVersion = SchemaVersion::new(7);
const ORCHESTRATE_SCHEMA_VERSION_BEFORE_ORCHESTRATOR_SEAT: SchemaVersion = SchemaVersion::new(6);
// Bumped 5 -> 6 for workflow model-resolution attempts. Existing v5 stores
// remain the lane-owned claim baseline; the new table records resolved or
// unavailable harness model outcomes by resolved workflow run handle. That handle
// includes the model-resolution request identity for the resolved workflow path.
const ORCHESTRATE_SCHEMA_VERSION_BEFORE_WORKFLOW_MODEL_RESOLUTIONS: SchemaVersion =
    SchemaVersion::new(5);

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

const SEMA_META: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("__sema_meta");
const SEMA_SCHEMA_VERSION_KEY: &str = "schema_version";
/// The sema-engine catalog table: one row per registered family, keyed by table
/// name, whose value is the rkyv-encoded family identity (name plus schema hash).
/// The engine reads this at open to reconstruct its family inventory, so a
/// migration that must retire a stale family identity edits this row directly.
/// The value type mirrors sema's `&[u8]` storage encoding; a migration only
/// removes a keyed row, so the value bytes are never decoded here.
const SEMA_ENGINE_CATALOG: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("__sema_engine_catalog");

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
    pub role: RoleName,
    pub harness: HarnessKind,
    pub report_repository_path: WirePath,
    pub report_lane_path: WirePath,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRepository {
    pub name: String,
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
    pub role: RoleName,
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
/// `last_activity` is the reaper's idle-age clock, mirroring a lane's
/// `updated_at`: it starts at `registered_at` and is refreshed on every real use
/// of the agent (reachability discovery, a triage message it sends), so a
/// genuinely active agent never ages out. The interim table reaper retires an
/// `Active` agent idle past the liveness window and deletes a `Retired` agent
/// past its terminal retention, and the retirement transition re-stamps
/// `last_activity` so the terminal window is measured from retirement.
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
    /// The elapsed idle age against an observation instant, read from the
    /// last-activity stamp — the reaper's sole liveness signal for an agent.
    pub fn idle_age_at(&self, observed_at: TimestampNanos) -> DurationNanos {
        TimestampInterval::new(self.last_activity, observed_at).duration()
    }
}

/// The prior on-disk shape of an orchestrator-agent row — the layout a store
/// written before the `last_activity` bump carries, registered under the
/// `orchestrate-orchestrator-agent-v7` family identity. It is byte-identical to
/// [`StoredOrchestratorAgent`] minus the trailing `last_activity` field, so the
/// store migration can register the table under its prior identity, read the old
/// rows in their own layout, and carry them forward. Read-only: nothing writes
/// this shape in normal operation; it exists solely to decode a legacy store.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOrchestratorAgentV7 {
    pub agent_identifier: OrchestratorAgentIdentifier,
    pub session: SessionIdentifier,
    pub mission: MissionDescription,
    pub harness: HarnessKind,
    pub reachability: Option<StoredAgentReachability>,
    pub registered_at: TimestampNanos,
    pub status: OrchestratorAgentStatus,
}

impl StoredOrchestratorAgentV7 {
    /// Carry a prior-shape row to the current shape. The `last_activity` reaper
    /// clock the bump introduced starts at `registered_at` for a freshly
    /// registered agent, so a migrated row adopts that same origin — the honest
    /// default, since a store written before the bump never recorded a distinct
    /// last-use instant.
    fn into_current(self) -> StoredOrchestratorAgent {
        StoredOrchestratorAgent {
            agent_identifier: self.agent_identifier,
            session: self.session,
            mission: self.mission,
            harness: self.harness,
            reachability: self.reachability,
            registered_at: self.registered_at,
            last_activity: self.registered_at,
            status: self.status,
        }
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
    pub incoming_kind: StoredOrchestratorMessageKind,
    pub verdict: StoredTriageVerdict,
    pub stamped_at: TimestampNanos,
}

/// The storage-side projection of a semantic message kind. The authoritative
/// vocabulary lives in the (separately built, not-yet-integrated)
/// `signal-orchestrator-message` crate; this projection lets the triage audit
/// persist the kind before that crate is a dependency. Keep it in step with the
/// `OrchestratorMessageKind` contract when that crate integrates.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredOrchestratorMessageKind {
    Guidance(StoredGuidanceMagnitude),
    Interruption,
    Report,
}

/// The magnitude carried by a `Guidance` message, storage-side projection.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredGuidanceMagnitude {
    Soft,
    Standard,
    Hard,
}

/// The store's closed record of a triage verdict. Spawning is deliberately
/// inexpressible — there is no spawn or new-session variant. `Route` carries the
/// resolved recipients and the optional retyped kind; `Reject` carries the
/// typed reason.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StoredTriageVerdict {
    Route {
        recipients: Vec<OrchestratorAgentIdentifier>,
        retyped: Option<StoredOrchestratorMessageKind>,
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
        let tables = match Self::open_current(store) {
            Ok(tables) => tables,
            Err(error) => OrchestrateStoreMigration::new(store).open_after_migration(error)?,
        };
        Ok(tables)
    }

    fn open_current(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(Self::engine_open(store))?;
        let claims = engine.register_table(Self::stable_family_descriptor(CLAIMS, "claim"))?;
        let roles = engine.register_table(Self::stable_family_descriptor(ROLES, "role"))?;
        let lane_registry = engine.register_table(Self::stable_family_descriptor(
            LANE_REGISTRY,
            "lane-registry",
        ))?;
        let repositories =
            engine.register_table(Self::stable_family_descriptor(REPOSITORIES, "repository"))?;
        let worktrees =
            engine.register_table(Self::stable_family_descriptor(WORKTREES, "worktree"))?;
        let activities =
            engine.register_table(Self::stable_family_descriptor(ACTIVITIES, "activity"))?;
        let activity_next_slot = engine.register_table(Self::stable_family_descriptor(
            ACTIVITY_NEXT_SLOT,
            "activity-slot",
        ))?;
        let divergences =
            engine.register_table(Self::stable_family_descriptor(DIVERGENCES, "divergence"))?;
        let divergence_next_slot = engine.register_table(Self::stable_family_descriptor(
            DIVERGENCE_NEXT_SLOT,
            "divergence-slot",
        ))?;
        let workflow_model_resolutions = engine.register_table(Self::family_descriptor(
            WORKFLOW_MODEL_RESOLUTIONS,
            "workflow-model-resolution",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_ORCHESTRATOR_SEAT,
        ))?;
        let orchestrator_agents = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_AGENTS,
            "orchestrator-agent",
            ORCHESTRATE_SCHEMA_VERSION,
        ))?;
        let orchestrator_topics = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TOPICS,
            "orchestrator-topic",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
        ))?;
        let orchestrator_topic_membership = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TOPIC_MEMBERSHIP,
            "orchestrator-topic-membership",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
        ))?;
        let orchestrator_triage_audit = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TRIAGE_AUDIT,
            "orchestrator-triage",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
        ))?;
        let orchestrator_triage_next_slot = engine.register_table(Self::family_descriptor(
            ORCHESTRATOR_TRIAGE_NEXT_SLOT,
            "orchestrator-triage-slot",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
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
        })
    }

    fn engine_open(store: &StoreLocation) -> EngineOpen {
        EngineOpen::new(store.as_path(), ORCHESTRATE_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("orchestrate"))
    }

    fn stable_family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        Self::family_descriptor(
            table,
            family,
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_WORKFLOW_MODEL_RESOLUTIONS,
        )
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
        version: SchemaVersion,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!("orchestrate-{family}-v{}", version.value())),
        )
    }

    pub fn claim_records(&self) -> Result<Vec<StoredClaim>> {
        self.records(self.claims)
    }

    pub fn role_records(&self) -> Result<Vec<StoredRole>> {
        self.records(self.roles)
    }

    pub fn role_record(&self, role: &RoleName) -> Result<Option<StoredRole>> {
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

    pub fn remove_role(&self, role: &RoleName) -> Result<()> {
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

    /// Refresh an active lane's last-activity stamp (`updated_at`) to now, the
    /// liveness signal the reaper reads: any real use of a lane — a claim,
    /// release, handoff, or recovery re-registration — pushes its idle clock
    /// back so genuine long-running work is never aged out. A lane that is not
    /// currently active is left untouched.
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
    /// status transition on durable state — no filesystem effect — so the lane
    /// reaper can flag orphans without the worktree layout.
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
        for key in remove_keys {
            self.remove_if_present(self.claims, key.as_str())?;
        }
        for claim in insert_claims {
            let key = claim.key();
            self.upsert(self.claims, key.as_str(), claim)?;
        }
        Ok(())
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

    pub fn remove_claims_for_role(&self, role: &RoleName) -> Result<Vec<StoredClaim>> {
        let mut role_lanes = std::collections::BTreeSet::new();
        for registration in self.lane_records()? {
            if registration.owner_role_name()? == *role {
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
        role: RoleName,
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

    /// Mint a fresh identity against the live registry key set. The store owns
    /// minting; callers never supply an identifier.
    pub fn mint_orchestrator_agent_identifier(&self) -> Result<OrchestratorAgentIdentifier> {
        let used = self
            .orchestrator_agent_records()?
            .into_iter()
            .map(|agent| agent.agent_identifier.as_str().to_string());
        OrchestratorAgentIdentifierMint::from_identifiers(used).next_identifier()
    }

    /// Mint an identity, stamp the registration time, and seat the agent as
    /// `Active` with no reachability yet (the discovery lane fills that in).
    pub fn register_orchestrator_agent(
        &self,
        session: SessionIdentifier,
        mission: MissionDescription,
        harness: HarnessKind,
    ) -> Result<StoredOrchestratorAgent> {
        let agent_identifier = self.mint_orchestrator_agent_identifier()?;
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

    /// Refresh an agent's last-activity stamp to now, the way a lane's
    /// `updated_at` advances on real use, so an actively used agent never ages
    /// out under the reaper. A no-op when the identifier is absent.
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
        // Reachability discovery is real use of the agent, so it refreshes the
        // reaper's idle-age clock alongside the reachability write.
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
        incoming_kind: StoredOrchestratorMessageKind,
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
        // A triaged message is real activity for its sender, so it refreshes the
        // sender agent's idle-age clock and keeps a message-active agent alive.
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
        role: RoleName,
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
    pub fn new(name: String, path: WirePath, refreshed_at: TimestampNanos) -> Self {
        Self {
            name,
            path,
            active: true,
            refreshed_at,
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

/// Composite redb key `repository|branch` for the worktrees table — the
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
        role: RoleName,
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

/// Composite redb key `agent_identifier|topic` for the topic-membership table —
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

impl StoredLaneRegistration {
    pub fn owner_role_name(&self) -> Result<RoleName> {
        RoleNameForLaneOwner::new(&self.assignment.owner.role).role_name()
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

struct RoleNameForLaneOwner<'role> {
    role: &'role Role,
}

impl<'role> RoleNameForLaneOwner<'role> {
    fn new(role: &'role Role) -> Self {
        Self { role }
    }

    fn role_name(&self) -> Result<RoleName> {
        let rendered = self
            .role
            .tokens()
            .iter()
            .map(|token| Self::pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        Ok(RoleName::from_wire_token(rendered)?)
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

/// One recoverable on-disk store defect the current build knows how to clear in
/// place before re-opening. An open error that maps to no variant is never
/// repaired — it surfaces unchanged so a genuinely incompatible store fails
/// closed rather than being silently mutated.
enum StoreRepair {
    /// The sema file is stamped at a known prior schema version whose every
    /// intervening family layout is additive up to the current version. Re-stamp
    /// the file to the current version: unchanged families keep their rows, and
    /// families introduced since open empty on the next registration.
    StampSchemaVersion(SchemaVersion),
    /// The orchestrator-agent registry is registered under a stale family schema
    /// hash — the agent layout changed (the `last_activity` stamp) but the catalog
    /// still names the prior identity, so re-registration under the current
    /// identity is rejected. Migrate the family forward: read the prior-shape rows
    /// under their old identity, retire the stale family, and re-insert the rows
    /// in the current shape. Every other family is untouched.
    MigrateAgentRegistry,
}

/// What an applied repair yields. Most repairs clear a defect and hand back to
/// the open loop to re-open; the agent migration opens the store itself as the
/// final step (it must re-insert the carried rows after the re-open), so it
/// returns the ready store directly rather than looping again.
enum RepairOutcome {
    Reopen,
    Opened(Box<OrchestrateTables>),
}

struct OrchestrateStoreMigration<'store> {
    store: &'store StoreLocation,
}

impl<'store> OrchestrateStoreMigration<'store> {
    fn new(store: &'store StoreLocation) -> Self {
        Self { store }
    }

    /// Clear recognised store defects and open. A single store can need more than
    /// one repair before it opens — a genuine prior-version file that also carries
    /// the stale agent registry is first re-stamped to the current version, then
    /// re-opened, which now surfaces the stale-identity rejection, which the agent
    /// drop clears. Each repair unblocks the next open; the same defect recurring
    /// after its own repair means the repair did not take, so the loop refuses to
    /// spin and surfaces the error.
    fn open_after_migration(&self, error: crate::Error) -> Result<OrchestrateTables> {
        let mut pending = error;
        let mut applied: Vec<std::mem::Discriminant<StoreRepair>> = Vec::new();
        loop {
            let Some(repair) = self.repair_for(&pending) else {
                return Err(pending);
            };
            let discriminant = std::mem::discriminant(&repair);
            if applied.contains(&discriminant) {
                return Err(pending);
            }
            applied.push(discriminant);
            match self.apply(repair)? {
                RepairOutcome::Opened(tables) => return Ok(*tables),
                RepairOutcome::Reopen => match OrchestrateTables::open_current(self.store) {
                    Ok(tables) => return Ok(tables),
                    Err(next) => pending = next,
                },
            }
        }
    }

    /// The repair, if any, that clears this open error. A prior-version schema
    /// stamp is additive-forward when the store sits at a known prior version and
    /// this build expects the current one; every intervening table is additive and
    /// opens empty, so a v5 store may migrate straight to the current version. A
    /// family-identity rejection is repairable only for the ephemeral
    /// orchestrator-agent registry, whose rows are recreated by running harnesses;
    /// any other family under a stale identity is a real incompatibility that
    /// fails closed.
    fn repair_for(&self, error: &crate::Error) -> Option<StoreRepair> {
        match error {
            crate::Error::SemaEngine(sema_engine::Error::Sema(
                sema_engine::StorageKernelError::SchemaVersionMismatch { expected, found },
            )) if *expected == ORCHESTRATE_SCHEMA_VERSION
                && Self::is_additive_prior_version(*found) =>
            {
                Some(StoreRepair::StampSchemaVersion(*found))
            }
            crate::Error::SemaEngine(sema_engine::Error::FamilyIdentityMismatch {
                table, ..
            }) if table.as_str() == ORCHESTRATOR_AGENTS.as_str() => {
                Some(StoreRepair::MigrateAgentRegistry)
            }
            _ => None,
        }
    }

    fn is_additive_prior_version(found: SchemaVersion) -> bool {
        found == ORCHESTRATE_SCHEMA_VERSION_BEFORE_WORKFLOW_MODEL_RESOLUTIONS
            || found == ORCHESTRATE_SCHEMA_VERSION_BEFORE_ORCHESTRATOR_SEAT
            || found == ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY
    }

    fn apply(&self, repair: StoreRepair) -> Result<RepairOutcome> {
        match repair {
            StoreRepair::StampSchemaVersion(found) => {
                self.stamp_current_schema_version(found)?;
                Ok(RepairOutcome::Reopen)
            }
            StoreRepair::MigrateAgentRegistry => Ok(RepairOutcome::Opened(Box::new(
                self.migrate_agent_registry()?,
            ))),
        }
    }

    fn stamp_current_schema_version(&self, found: SchemaVersion) -> Result<()> {
        let storage =
            sema::Sema::open_with_schema(self.store.as_path(), &sema::Schema { version: found })?;
        drop(storage);
        let database =
            redb::Database::create(self.store.as_path()).map_err(Self::store_migration_error)?;
        let transaction = database
            .begin_write()
            .map_err(Self::store_migration_error)?;
        {
            let mut table = transaction
                .open_table(SEMA_META)
                .map_err(Self::store_migration_error)?;
            table
                .insert(
                    SEMA_SCHEMA_VERSION_KEY,
                    ORCHESTRATE_SCHEMA_VERSION.value() as u64,
                )
                .map_err(Self::store_migration_error)?;
        }
        transaction.commit().map_err(Self::store_migration_error)?;
        Ok(())
    }

    /// Migrate the orchestrator-agent registry forward across the `last_activity`
    /// layout bump, carrying its rows rather than discarding them. Three steps:
    /// read the prior-shape rows under their old family identity; retire the stale
    /// family so a fresh registration under the current identity is accepted;
    /// re-open the store and re-insert the carried rows in the current shape
    /// through the ordinary write path (which logs them under the current family
    /// so the versioned history stays consistent). Every other family is untouched.
    fn migrate_agent_registry(&self) -> Result<OrchestrateTables> {
        let carried: Vec<StoredOrchestratorAgent> = self
            .read_prior_shape_agents()?
            .into_iter()
            .map(StoredOrchestratorAgentV7::into_current)
            .collect();
        self.retire_stale_agent_family()?;
        let tables = OrchestrateTables::open_current(self.store)?;
        for agent in &carried {
            tables.insert_orchestrator_agent(agent)?;
        }
        Ok(tables)
    }

    /// Read the orchestrator-agent rows in their prior on-disk shape by registering
    /// the table under its pre-bump (`v7`) family identity, which matches the
    /// identity the stale catalog still names. The rows decode as
    /// [`StoredOrchestratorAgentV7`] — their genuine layout — so the read is
    /// well-typed rather than a reinterpretation of current-shape bytes.
    fn read_prior_shape_agents(&self) -> Result<Vec<StoredOrchestratorAgentV7>> {
        let mut engine = Engine::open(OrchestrateTables::engine_open(self.store))?;
        let prior_agents = engine.register_table(OrchestrateTables::family_descriptor::<
            StoredOrchestratorAgentV7,
        >(
            ORCHESTRATOR_AGENTS,
            "orchestrator-agent",
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
        ))?;
        Ok(engine
            .match_records(QueryPlan::all(prior_agents))?
            .records()
            .to_vec())
    }

    /// Retire the stale orchestrator-agent family so the next open registers it
    /// fresh under the current family hash. Two edits under one transaction: the
    /// family's catalog registration is removed so re-registration sees an unbound
    /// family, and its data table is dropped so no prior-layout rows survive to be
    /// misread. Only the one table named by the identity mismatch is retired; every
    /// other family's registration and rows are untouched. Callers read the rows
    /// first (see [`Self::read_prior_shape_agents`]) so retirement loses nothing.
    ///
    /// The versioned commit log is a hash chain and is deliberately left intact:
    /// its historical agent-registry entries stay valid links, and orchestrate
    /// takes no checkpoints, so those entries are never materialized against the
    /// live catalog. See `NON_IDEAL_AGENTS.md` for the family-retirement follow-up.
    fn retire_stale_agent_family(&self) -> Result<()> {
        let database =
            redb::Database::create(self.store.as_path()).map_err(Self::store_migration_error)?;
        let transaction = database
            .begin_write()
            .map_err(Self::store_migration_error)?;
        {
            let mut catalog = transaction
                .open_table(SEMA_ENGINE_CATALOG)
                .map_err(Self::store_migration_error)?;
            catalog
                .remove(ORCHESTRATOR_AGENTS.as_str())
                .map_err(Self::store_migration_error)?;
        }
        transaction
            .delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                ORCHESTRATOR_AGENTS.as_str(),
            ))
            .map_err(Self::store_migration_error)?;
        transaction.commit().map_err(Self::store_migration_error)?;
        Ok(())
    }

    fn store_migration_error(source: impl std::fmt::Display) -> crate::Error {
        crate::Error::StoreMigration {
            message: source.to_string(),
        }
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
    use signal_orchestrate::{LaneAuthority, LaneDetails, LaneOwner, Role, RoleToken};

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
            session: SessionIdentifier::from_camel_case_name("StorageMigration").expect("session"),
            lane: LaneIdentifier::from_wire_token("storage-worker").expect("lane"),
            owner: LaneOwner {
                role: Role::try_new(vec![RoleToken::from_text("Designer").expect("role token")])
                    .expect("role"),
                authority: LaneAuthority::Structural,
            },
            details: LaneDetails::from_text("storage migration test lane").expect("details"),
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
    fn version_five_store_migrates_for_workflow_model_resolution_table() {
        let temporary = TemporaryStore::new("orchestrate-v5-to-v6-migration");
        sema::Sema::open_with_schema(
            temporary.path.as_path(),
            &sema::Schema {
                version: ORCHESTRATE_SCHEMA_VERSION_BEFORE_WORKFLOW_MODEL_RESOLUTIONS,
            },
        )
        .expect("v5 store opens");

        let tables = OrchestrateTables::open(&temporary.location()).expect("migrated tables open");

        assert!(tables.claim_records().expect("claims").is_empty());
        assert!(
            tables
                .workflow_model_resolution_records()
                .expect("new workflow table reads")
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
            ScopeReason::from_text("owns storage migration").expect("reason"),
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
    fn orchestrator_agents_round_trip_with_store_minted_unique_identifiers() {
        let temporary = TemporaryStore::new("orchestrate-agent-registry");
        let tables = OrchestrateTables::open(&temporary.location()).expect("tables open");

        let mut minted = std::collections::BTreeSet::new();
        for _ in 0..16 {
            let agent = tables
                .register_orchestrator_agent(test_session(), test_mission(), HarnessKind::Claude)
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
            .register_orchestrator_agent(test_session(), test_mission(), HarnessKind::Codex)
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
            .register_orchestrator_agent(test_session(), test_mission(), HarnessKind::Claude)
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
            .register_orchestrator_agent(test_session(), test_mission(), HarnessKind::Claude)
            .expect("register sender");
        let recipient = tables
            .register_orchestrator_agent(test_session(), test_mission(), HarnessKind::Codex)
            .expect("register recipient");

        let routed = tables
            .append_orchestrator_triage_record(
                sender.agent_identifier.clone(),
                StoredOrchestratorMessageKind::Guidance(StoredGuidanceMagnitude::Standard),
                StoredTriageVerdict::Route {
                    recipients: vec![recipient.agent_identifier.clone()],
                    retyped: Some(StoredOrchestratorMessageKind::Interruption),
                },
            )
            .expect("append routed verdict");
        let rejected = tables
            .append_orchestrator_triage_record(
                sender.agent_identifier.clone(),
                StoredOrchestratorMessageKind::Report,
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
                    StoredOrchestratorMessageKind::Report,
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
    fn version_six_store_migrates_and_gains_orchestrator_seat_tables() {
        let temporary = TemporaryStore::new("orchestrate-v6-to-v7-migration");
        sema::Sema::open_with_schema(
            temporary.path.as_path(),
            &sema::Schema {
                version: ORCHESTRATE_SCHEMA_VERSION_BEFORE_ORCHESTRATOR_SEAT,
            },
        )
        .expect("v6 store opens");

        let tables = OrchestrateTables::open(&temporary.location()).expect("migrated tables open");

        assert!(
            tables
                .orchestrator_agent_records()
                .expect("agents")
                .is_empty()
        );
        assert!(
            tables
                .orchestrator_topic_membership_records()
                .expect("membership")
                .is_empty()
        );
        assert!(
            tables
                .orchestrator_triage_records()
                .expect("triage")
                .is_empty()
        );
        assert!(
            tables
                .orchestrator_topic_records()
                .expect("topics")
                .is_empty(),
            "migration seeds no topic; the topic tree starts empty"
        );
        // Pre-existing table families remain readable and untouched.
        assert!(tables.claim_records().expect("claims").is_empty());
        assert!(
            tables
                .workflow_model_resolution_records()
                .expect("workflow resolutions")
                .is_empty()
        );
    }

    #[test]
    fn migration_preserves_existing_rows_forward_from_the_seat_baseline() {
        let temporary = TemporaryStore::new("orchestrate-migration-preserves-rows");
        let claim = StoredClaim::new(
            LaneIdentifier::from_wire_token("designer").expect("lane"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-migrate").expect("path"),
            ),
            ScopeReason::from_text("owns migration preservation test").expect("reason"),
            TimestampNanos::new(200),
        );
        {
            let tables =
                OrchestrateTables::open(&temporary.location()).expect("current store opens");
            tables
                .replace_all_claims(std::slice::from_ref(&claim))
                .expect("insert claim");
        }

        stamp_meta_schema_version(
            temporary.path.as_path(),
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_ORCHESTRATOR_SEAT,
        );

        let migrated =
            OrchestrateTables::open(&temporary.location()).expect("migrated store opens");
        let claims = migrated.claim_records().expect("claims after migration");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].claimed_at, TimestampNanos::new(200));
        assert_eq!(claims[0].lane, claim.lane);
        assert!(
            migrated
                .orchestrator_agent_records()
                .expect("agents")
                .is_empty()
        );
    }

    #[test]
    fn version_seven_store_migrates_and_preserves_families_pinned_before_agent_activity() {
        let temporary = TemporaryStore::new("orchestrate-v7-to-v8-migration");
        let topic_path = OrchestratorTopicPath::from_wire_token("engineering").expect("topic path");
        let claim = StoredClaim::new(
            LaneIdentifier::from_wire_token("designer").expect("lane"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-v7-migrate").expect("path"),
            ),
            ScopeReason::from_text("owns v7 migration test").expect("reason"),
            TimestampNanos::new(300),
        );
        {
            let tables =
                OrchestrateTables::open(&temporary.location()).expect("current store opens");
            tables
                .replace_all_claims(std::slice::from_ref(&claim))
                .expect("insert claim");
            tables
                .insert_orchestrator_topic(
                    topic_path.clone(),
                    TopicName::from_text("engineering").expect("topic name"),
                    None,
                )
                .expect("insert topic");
        }

        // Present the store as a genuine pre-agent-activity (v7) store; the topic
        // and claim families are unchanged at v8, so their family hashes match and
        // their rows survive the bump.
        stamp_meta_schema_version(
            temporary.path.as_path(),
            ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
        );

        let migrated =
            OrchestrateTables::open(&temporary.location()).expect("migrated store opens");
        let claims = migrated.claim_records().expect("claims after migration");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].claimed_at, TimestampNanos::new(300));
        let topics = migrated
            .orchestrator_topic_records()
            .expect("topics after migration");
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].path, topic_path);
    }

    #[test]
    fn store_with_prior_shape_agent_registry_migrates_rows_forward() {
        let temporary = TemporaryStore::new("orchestrate-agent-registry-migration");
        let claim = StoredClaim::new(
            LaneIdentifier::from_wire_token("designer").expect("lane"),
            ScopeReference::Path(
                WirePath::from_absolute_path("/tmp/orchestrate-prior-agent").expect("path"),
            ),
            ScopeReason::from_text("owns agent migration test").expect("reason"),
            TimestampNanos::new(400),
        );
        let agent_identifier =
            OrchestratorAgentIdentifierMint::from_identifiers(std::iter::empty::<String>())
                .next_identifier()
                .expect("mint agent identifier");
        let prior_agent = StoredOrchestratorAgentV7 {
            agent_identifier: agent_identifier.clone(),
            session: SessionIdentifier::from_camel_case_name("PriorShapeAgent").expect("session"),
            mission: MissionDescription::from_text("holds a seat before the bump")
                .expect("mission"),
            harness: HarnessKind::Codex,
            reachability: None,
            registered_at: TimestampNanos::new(700),
            status: OrchestratorAgentStatus::Active,
        };

        // Build the exact store a version before the `last_activity` bump wrote: the
        // agent rows are genuine prior-shape (v7) rows under the matching stale family
        // identity, not current-shape bytes relabelled. The sema file is already at the
        // current version, matching a store the current build has opened and stamped.
        build_prior_shape_store(&temporary.location(), &claim, &prior_agent);

        // The plain open path the daemon runs at startup crash-loops on the wedged
        // store: the agent family is registered under the current identity but stored
        // under the prior one, and no other family is at fault.
        let raw = OrchestrateTables::open_current(&temporary.location());
        let stale_rejection = matches!(
            &raw,
            Err(crate::Error::SemaEngine(sema_engine::Error::FamilyIdentityMismatch {
                table,
                ..
            })) if table.as_str() == ORCHESTRATOR_AGENTS.as_str()
        );
        assert!(
            stale_rejection,
            "expected a stale-identity rejection for orchestrator_agents from the plain open path"
        );

        // The repaired open migrates the agent family forward.
        let migrated =
            OrchestrateTables::open(&temporary.location()).expect("migrated store opens");

        // Every other family survives untouched: the pre-migration claim is still there.
        let claims = migrated.claim_records().expect("claims after migration");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].claimed_at, TimestampNanos::new(400));
        assert_eq!(claims[0].lane, claim.lane);

        // The prior-shape agent row is carried forward — not dropped — in the current
        // shape, with `last_activity` defaulted to `registered_at`.
        let agents = migrated
            .orchestrator_agent_records()
            .expect("agents after migration");
        assert_eq!(
            agents.len(),
            1,
            "the prior-shape agent row must be carried forward, not dropped"
        );
        assert_eq!(agents[0].agent_identifier, agent_identifier);
        assert_eq!(agents[0].session, prior_agent.session);
        assert_eq!(agents[0].registered_at, TimestampNanos::new(700));
        assert_eq!(agents[0].last_activity, TimestampNanos::new(700));

        // The migrated registry is immediately usable: a fresh registration under the
        // current hash succeeds alongside the carried row.
        migrated
            .register_orchestrator_agent(
                SessionIdentifier::from_camel_case_name("PostMigration").expect("session"),
                MissionDescription::from_text("seats after the migration").expect("mission"),
                HarnessKind::Codex,
            )
            .expect("register agent after migration");
        assert_eq!(
            migrated
                .orchestrator_agent_records()
                .expect("agents readable")
                .len(),
            2
        );
    }

    /// Build a store carrying the orchestrator-agent registry in its prior
    /// (pre-`last_activity`, v7) shape under the matching stale family identity,
    /// exactly as a store written before the bump does: the agent rows are genuine
    /// v7-layout rows written through the engine's own path, not current-shape bytes
    /// relabelled. A claim is seated too, so the migration can be shown to leave
    /// other families untouched.
    fn build_prior_shape_store(
        location: &StoreLocation,
        claim: &StoredClaim,
        agent: &StoredOrchestratorAgentV7,
    ) {
        let mut engine =
            Engine::open(OrchestrateTables::engine_open(location)).expect("open engine");
        let claims = engine
            .register_table(OrchestrateTables::stable_family_descriptor::<StoredClaim>(
                CLAIMS, "claim",
            ))
            .expect("register claims");
        let claim_key = claim.key();
        engine
            .assert_keyed(KeyedAssertion::new(
                claims,
                RecordKey::new(claim_key.as_str()),
                claim.clone(),
            ))
            .expect("seat claim");
        let prior_agents = engine
            .register_table(OrchestrateTables::family_descriptor::<
                StoredOrchestratorAgentV7,
            >(
                ORCHESTRATOR_AGENTS,
                "orchestrator-agent",
                ORCHESTRATE_SCHEMA_VERSION_BEFORE_AGENT_ACTIVITY,
            ))
            .expect("register prior-shape agents");
        engine
            .assert_keyed(KeyedAssertion::new(
                prior_agents,
                RecordKey::new(agent.agent_identifier.as_str()),
                agent.clone(),
            ))
            .expect("seat prior-shape agent");
    }

    fn stamp_meta_schema_version(path: &std::path::Path, version: SchemaVersion) {
        let database = redb::Database::create(path).expect("open store database");
        let transaction = database.begin_write().expect("begin write");
        {
            let mut table = transaction.open_table(SEMA_META).expect("open meta table");
            table
                .insert(SEMA_SCHEMA_VERSION_KEY, version.value() as u64)
                .expect("stamp schema version");
        }
        transaction.commit().expect("commit meta stamp");
    }
}
