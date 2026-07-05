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
use signal_orchestrate::{
    Activity, ApplicationFailure, ApplicationSuccess, BranchName, DurationNanos, HarnessKind,
    LaneAssignment, LaneIdentifier, LaneName, LaneRegistration, LaneResourceClaim, LaneStatus,
    PartialApplied, PurposeText, PushedState, RepositoryName, Role, RoleName, ScopeReason,
    ScopeReference, SessionIdentifier, TimestampNanos, WirePath, Worktree, WorktreeStatus,
};

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

// Bumped 4 -> 5 for lane-owned claims. Existing v4 stores kept ordinary
// claim owners in the old role-shaped field; the operator path is to stop the
// old daemon, keep the old sema store as a backup, start a fresh v5 store, and
// register first-class session lanes through the meta lane lifecycle before
// ordinary claims are accepted.
const ORCHESTRATE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(5);

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
/// `repository|branch` by [`WorktreeKey`], beside [`StoredRepository`].
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
        })
    }

    fn engine_open(store: &StoreLocation) -> EngineOpen {
        EngineOpen::new(store.as_path(), ORCHESTRATE_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("orchestrate"))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!(
                "orchestrate-{family}-v{}",
                ORCHESTRATE_SCHEMA_VERSION.value()
            )),
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
        Ok(activity)
    }

    pub fn activity_records(&self) -> Result<Vec<StoredActivity>> {
        self.records(self.activities)
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
        Ok(divergence)
    }

    pub fn divergence_records(&self) -> Result<Vec<StoredDivergence>> {
        self.records(self.divergences)
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

    pub fn resource_claim(&self) -> LaneResourceClaim {
        LaneResourceClaim {
            scope: self.scope.clone(),
            reason: self.reason.clone(),
            claimed_at: self.claimed_at,
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
        let resource = stored.resource_claim();
        assert_eq!(resource.claimed_at, TimestampNanos::new(200));
        assert_eq!(resource.reason, claim.reason);
    }
}
