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
    Activity, ApplicationFailure, ApplicationSuccess, HarnessKind, LaneIdentifier,
    LaneRegistration, PartialApplied, RoleName, ScopeReason, ScopeReference, TimestampNanos,
    WirePath,
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

const ORCHESTRATE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);

const CLAIMS: TableName = TableName::new("claims");
const ROLES: TableName = TableName::new("roles");
const LANE_REGISTRY: TableName = TableName::new("lane_registry");
const REPOSITORIES: TableName = TableName::new("repositories");
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
    lane_registry: TableReference<LaneRegistration>,
    repositories: TableReference<StoredRepository>,
    activities: TableReference<StoredActivity>,
    activity_next_slot: TableReference<u64>,
    divergences: TableReference<StoredDivergence>,
    divergence_next_slot: TableReference<u64>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredClaim {
    pub role: RoleName,
    pub scope: ScopeReference,
    pub reason: ScopeReason,
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

    pub fn lane_records(&self) -> Result<Vec<LaneRegistration>> {
        self.records(self.lane_registry)
    }

    pub fn lane_record(&self, lane: &LaneIdentifier) -> Result<Option<LaneRegistration>> {
        self.record(self.lane_registry, lane.as_wire_token())
    }

    pub fn insert_lane(&self, registration: &LaneRegistration) -> Result<()> {
        self.upsert(
            self.lane_registry,
            registration.lane.as_wire_token(),
            registration,
        )?;
        Ok(())
    }

    pub fn replace_lanes(&self, lanes: &[LaneRegistration]) -> Result<()> {
        let existing = self
            .lane_records()?
            .into_iter()
            .map(|registration| registration.lane)
            .collect::<Vec<_>>();
        for lane in existing {
            self.remove_if_present(self.lane_registry, lane.as_wire_token())?;
        }
        for registration in lanes {
            self.insert_lane(registration)?;
        }
        Ok(())
    }

    pub fn remove_lane(&self, lane: &LaneIdentifier) -> Result<()> {
        self.remove_if_present(self.lane_registry, lane.as_wire_token())?;
        Ok(())
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
        let key = RecordKey::new(key);
        if self.record(table, key.as_str())?.is_some() {
            self.engine
                .mutate_keyed(KeyedMutation::new(table, key, record.clone()))?;
        } else {
            self.engine
                .assert_keyed(KeyedAssertion::new(table, key, record.clone()))?;
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
    pub fn new(role: RoleName, scope: ScopeReference, reason: ScopeReason) -> Self {
        Self {
            role,
            scope,
            reason,
        }
    }

    pub fn key(&self) -> String {
        ClaimKey::new(&self.role, &self.scope).into_string()
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
    role: String,
    scope: String,
}

impl ClaimKey {
    fn new(role: &RoleName, scope: &ScopeReference) -> Self {
        Self {
            role: role.as_wire_token().to_string(),
            scope: ScopeKey::new(scope).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.role, self.scope)
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
