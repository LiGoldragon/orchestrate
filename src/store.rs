//! Orchestrate Nexus-owned durable Lock state.

use crate::ordinary::{Locks, Observes, OrdinaryOutcome, Releases};
use meta_signal_orchestrate::{Configure, Request as MetaRequest, Response as MetaResponse};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, Retraction,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_orchestrate::{
    Lock, LockOverlap, LockRejection, LockRequest, Observation, ObserveSelection, ReleaseRejection,
    Request as OrdinaryRequest, Response as OrdinaryResponse,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};
use thiserror::Error;

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration_v2");
const LOCKS_TABLE: TableName = TableName::new("locks_v2");
const ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator_v2");
const PREVIOUS_CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration");
const PREVIOUS_LOCKS_TABLE: TableName = TableName::new("locks");
const PREVIOUS_ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator");
const LEGACY_TABLE: TableName = TableName::new("active_path_locks");
const CONFIGURATION_KEY: &str = "configuration";
const ALLOCATOR_KEY: &str = "next";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the durable store has {count} configuration rows")]
    ConfigurationInvariant { count: usize },
    #[error("the durable store has {count} Lock ID allocator rows")]
    LockIdAllocatorInvariant { count: usize },
    #[error(
        "the old store still has {count} active PathLock rows; release them before deploying the Lock contract"
    )]
    LegacyActiveLocks { count: usize },
    #[error(
        "the previous Signal durable representation has {configuration_count} configuration and {lock_count} Lock rows; run the explicit one-time migration before activating the framed contract"
    )]
    PreviousSignalMigrationRequired {
        configuration_count: usize,
        lock_count: usize,
    },
    #[error(
        "the v2 migration target is not empty: {configuration_count} configuration, {lock_count} Lock, and {allocator_count} allocator rows"
    )]
    MigrationTargetNotEmpty {
        configuration_count: usize,
        lock_count: usize,
        allocator_count: usize,
    },
    #[error(
        "the v1 migration source has {configuration_count} configuration and {allocator_count} allocator rows; expected one of each"
    )]
    MigrationSourceInvariant {
        configuration_count: usize,
        allocator_count: usize,
    },
    #[error("the durable Lock ID allocator is exhausted")]
    LockIdExhausted,
    #[error("filesystem: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("lock path {path:?} is not absolute")]
    RelativePath { path: String },
    #[error("Lock has no paths")]
    EmptyPathSet,
    #[error("lock path {path:?} contains a parent component")]
    ParentPathComponent { path: String },
    #[error("Lock repeats normalized path {path:?}")]
    DuplicateNormalizedPath { path: String },
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredConfiguration {
    ordinary_socket: String,
    meta_socket: String,
}
impl EngineRecord for StoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredLock {
    lock_id: i64,
    lock_name: String,
    flow_id: String,
    paths: Vec<String>,
    reason: String,
}
impl EngineRecord for StoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.lock_id.to_string())
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredAllocator {
    next_lock_id: i64,
}

// These read-only v1 shapes are deliberately separate from the current public
// Signal types.  They make the durable break explicit instead of attempting a
// runtime compatibility conversion or silently ignoring old locks.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct PreviousConfigure(String, String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct PreviousStoredConfiguration {
    configuration: PreviousConfigure,
}
impl EngineRecord for PreviousStoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct PreviousLock(i64, String, String, Vec<String>, String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct PreviousStoredLock {
    lock: PreviousLock,
}
impl EngineRecord for PreviousStoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.lock.0.to_string())
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct PreviousStoredAllocator {
    next_lock_id: i64,
}
impl EngineRecord for PreviousStoredAllocator {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(ALLOCATOR_KEY)
    }
}

/// A Lock request whose path values have passed Nexus normalization.
///
/// This is a durable-transition input, not a second public contract type.
/// Keeping its path and overlap rules with the request prevents transport or
/// callers from acquiring a partially normalized Lock.
struct NormalizedLockRequest {
    request: LockRequest,
}

impl NormalizedLockRequest {
    fn from_request(mut request: LockRequest) -> Result<Self, StoreError> {
        if request.2.is_empty() {
            return Err(StoreError::EmptyPathSet);
        }
        let mut paths = BTreeSet::new();
        for path in &mut request.2 {
            let normalized = NormalizedLockPath::from_source(path.as_ref())?.0;
            *path = text(normalized.clone());
            if !paths.insert(normalized.clone()) {
                return Err(StoreError::DuplicateNormalizedPath { path: normalized });
            }
        }
        Ok(Self { request })
    }

    fn duplicates_name_of(&self, lock: &Lock) -> bool {
        self.request.0 == lock.1
    }

    fn overlapping_path_of(&self, lock: &Lock) -> Option<String> {
        self.request.2.iter().find_map(|requested| {
            lock.3.iter().find_map(|held| {
                NormalizedLockPath::from_normalized(requested.as_ref())
                    .overlaps(&NormalizedLockPath::from_normalized(held.as_ref()))
                    .then(|| requested.to_string())
            })
        })
    }

    fn into_lock(self, lock_id: i64) -> Lock {
        Lock(
            lock_id,
            self.request.0,
            self.request.1,
            self.request.2,
            self.request.3,
        )
    }
}

fn text(value: impl ToString) -> protos::Text {
    protos::Text::try_from(value.to_string()).expect("stored public text remains valid")
}

impl StoredConfiguration {
    fn from_public(value: &Configure) -> Self {
        Self {
            ordinary_socket: value.0.to_string(),
            meta_socket: value.1.to_string(),
        }
    }
    fn into_public(self) -> Configure {
        Configure(text(self.ordinary_socket), text(self.meta_socket))
    }
}

impl StoredLock {
    fn from_public(value: &Lock) -> Self {
        Self {
            lock_id: value.0,
            lock_name: value.1.to_string(),
            flow_id: value.2.to_string(),
            paths: value.3.iter().map(ToString::to_string).collect(),
            reason: value.4.to_string(),
        }
    }
    fn into_public(self) -> Lock {
        Lock(
            self.lock_id,
            text(self.lock_name),
            text(self.flow_id),
            self.paths.into_iter().map(text).collect(),
            text(self.reason),
        )
    }
}

/// A lexically normalized absolute Unix path used during Lock acquisition.
struct NormalizedLockPath(String);

impl NormalizedLockPath {
    fn from_source(path: &str) -> Result<Self, StoreError> {
        let parsed = Path::new(path);
        if !parsed.is_absolute() {
            return Err(StoreError::RelativePath {
                path: path.to_owned(),
            });
        }
        let mut normalized = String::from("/");
        for component in parsed.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(segment) => {
                    if normalized != "/" {
                        normalized.push('/');
                    }
                    normalized.push_str(&segment.to_string_lossy());
                }
                Component::ParentDir => {
                    return Err(StoreError::ParentPathComponent {
                        path: path.to_owned(),
                    });
                }
                Component::Prefix(_) => unreachable!("Unix paths have no prefix component"),
            }
        }
        Ok(Self(normalized))
    }

    fn from_normalized(path: &str) -> Self {
        Self(path.to_owned())
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.0 == other.0 || self.is_ancestor_of(other) || other.is_ancestor_of(self)
    }

    fn is_ancestor_of(&self, descendant: &Self) -> bool {
        self.0 == "/"
            || descendant
                .0
                .strip_prefix(&self.0)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}
impl EngineRecord for StoredAllocator {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(ALLOCATOR_KEY)
    }
}

// The legacy row is readable only to refuse a nonempty pre-0.25 store. It is
// never converted, so no old Lock acquires invented Flow attribution.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyName(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyPath(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyPaths(Vec<LegacyPath>);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyReason(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyLock {
    path_lock_name: LegacyName,
    path_lock_paths: LegacyPaths,
    path_lock_description: LegacyReason,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyStoredLock {
    name: String,
    lock: LegacyLock,
}
impl EngineRecord for LegacyStoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.name.clone())
    }
}

pub struct OrchestrateStore {
    engine: Engine,
    configuration: Configure,
    configurations: TableReference<StoredConfiguration>,
    locks: TableReference<StoredLock>,
    allocator: TableReference<StoredAllocator>,
}

/// Read-only evidence about a pre-0.25 ordinary-state table.
///
/// It never opens the configuration, Lock, or allocator tables and never
/// materializes old rows as new Locks.  The legacy table descriptor is exactly
/// the 0.24 family identity, so registration is an existing-family read.
pub struct LegacyStorePreflight {
    active_lock_count: usize,
}

/// Inspects a legacy store before activation of the breaking Lock contract.
pub trait PreflightsLegacyStore: Sized {
    fn inspect(store_path: &Path) -> Result<Self, StoreError>;
    fn active_lock_count(&self) -> usize;
}

impl PreflightsLegacyStore for LegacyStorePreflight {
    fn inspect(store_path: &Path) -> Result<Self, StoreError> {
        if !store_path.exists() {
            return Ok(Self {
                active_lock_count: 0,
            });
        }
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let legacy: TableReference<LegacyStoredLock> =
            engine.register_table(TableDescriptor::new(
                LEGACY_TABLE,
                FamilyName::new("orchestrate-path-lock"),
                SchemaHash::for_label("orchestrate-path-lock-v1"),
            ))?;
        Ok(Self {
            active_lock_count: engine
                .match_records(QueryPlan::all(legacy))?
                .records()
                .len(),
        })
    }

    fn active_lock_count(&self) -> usize {
        self.active_lock_count
    }
}

impl OrchestrateStore {
    /// Offline, one-time import of the retired v1 Signal records.
    ///
    /// The daemon must be stopped. This method is the only legacy reader; the
    /// runtime open path never invokes it.
    pub fn migrate_previous_signal(store_path: &Path) -> Result<(), StoreError> {
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let old_configurations: TableReference<PreviousStoredConfiguration> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))?;
        let old_locks: TableReference<PreviousStoredLock> =
            engine.register_table(TableDescriptor::new(
                PREVIOUS_LOCKS_TABLE,
                FamilyName::new("orchestrate-lock"),
                SchemaHash::for_label("orchestrate-lock-v1"),
            ))?;
        let old_allocator: TableReference<PreviousStoredAllocator> =
            engine.register_table(TableDescriptor::new(
                PREVIOUS_ALLOCATOR_TABLE,
                FamilyName::new("orchestrate-lock-id-allocator"),
                SchemaHash::for_label("orchestrate-lock-id-allocator-v1"),
            ))?;
        let configurations: TableReference<StoredConfiguration> =
            engine.register_table(TableDescriptor::new(
                CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v2"),
            ))?;
        let locks: TableReference<StoredLock> = engine.register_table(TableDescriptor::new(
            LOCKS_TABLE,
            FamilyName::new("orchestrate-lock"),
            SchemaHash::for_label("orchestrate-lock-v2"),
        ))?;
        let allocator: TableReference<StoredAllocator> =
            engine.register_table(TableDescriptor::new(
                ALLOCATOR_TABLE,
                FamilyName::new("orchestrate-lock-id-allocator"),
                SchemaHash::for_label("orchestrate-lock-id-allocator-v2"),
            ))?;

        let target_configuration_count = engine
            .match_records(QueryPlan::all(configurations))?
            .records()
            .len();
        let target_lock_count = engine.match_records(QueryPlan::all(locks))?.records().len();
        let target_allocator_count = engine
            .match_records(QueryPlan::all(allocator))?
            .records()
            .len();
        if target_configuration_count != 0 || target_lock_count != 0 || target_allocator_count != 0
        {
            return Err(StoreError::MigrationTargetNotEmpty {
                configuration_count: target_configuration_count,
                lock_count: target_lock_count,
                allocator_count: target_allocator_count,
            });
        }

        let old_configuration = engine
            .match_records(QueryPlan::all(old_configurations))?
            .records()
            .to_vec();
        let old_lock_rows = engine
            .match_records(QueryPlan::all(old_locks))?
            .records()
            .to_vec();
        let old_allocator_rows = engine
            .match_records(QueryPlan::all(old_allocator))?
            .records()
            .to_vec();
        if old_configuration.len() != 1 || old_allocator_rows.len() != 1 {
            return Err(StoreError::MigrationSourceInvariant {
                configuration_count: old_configuration.len(),
                allocator_count: old_allocator_rows.len(),
            });
        }

        let PreviousConfigure(ordinary_socket, meta_socket) =
            old_configuration[0].configuration.clone();
        let mut migration = engine.begin_atomic_commit().assert(
            configurations,
            StoredConfiguration {
                ordinary_socket,
                meta_socket,
            },
        );
        for row in &old_lock_rows {
            let PreviousLock(lock_id, lock_name, flow_id, paths, reason) = row.lock.clone();
            migration = migration.assert(
                locks,
                StoredLock {
                    lock_id,
                    lock_name,
                    flow_id,
                    paths,
                    reason,
                },
            );
        }
        migration = migration.assert(
            allocator,
            StoredAllocator {
                next_lock_id: old_allocator_rows[0].next_lock_id,
            },
        );
        for row in old_configuration {
            migration = migration.retract(old_configurations, row.record_key());
        }
        for row in old_lock_rows {
            migration = migration.retract(old_locks, row.record_key());
        }
        migration = migration.retract(old_allocator, RecordKey::new(ALLOCATOR_KEY));
        engine.commit_atomic(migration)?;
        Ok(())
    }
    pub fn open(store_path: &Path, defaults: Configure) -> Result<(Self, Configure), StoreError> {
        fs::create_dir_all(
            store_path
                .parent()
                .expect("configured store path has a parent"),
        )?;
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let previous_configurations: TableReference<PreviousStoredConfiguration> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))?;
        let previous_locks: TableReference<PreviousStoredLock> =
            engine.register_table(TableDescriptor::new(
                PREVIOUS_LOCKS_TABLE,
                FamilyName::new("orchestrate-lock"),
                SchemaHash::for_label("orchestrate-lock-v1"),
            ))?;
        let previous_configuration_count = engine
            .match_records(QueryPlan::all(previous_configurations))?
            .records()
            .len();
        let previous_lock_count = engine
            .match_records(QueryPlan::all(previous_locks))?
            .records()
            .len();
        if previous_configuration_count != 0 || previous_lock_count != 0 {
            return Err(StoreError::PreviousSignalMigrationRequired {
                configuration_count: previous_configuration_count,
                lock_count: previous_lock_count,
            });
        }
        let configurations = engine.register_table(TableDescriptor::new(
            CONFIGURATION_TABLE,
            FamilyName::new("orchestrate-configuration"),
            SchemaHash::for_label("orchestrate-configuration-v2"),
        ))?;
        let legacy: TableReference<LegacyStoredLock> =
            engine.register_table(TableDescriptor::new(
                LEGACY_TABLE,
                FamilyName::new("orchestrate-path-lock"),
                SchemaHash::for_label("orchestrate-path-lock-v1"),
            ))?;
        let legacy_count = engine
            .match_records(QueryPlan::all(legacy))?
            .records()
            .len();
        if legacy_count != 0 {
            return Err(StoreError::LegacyActiveLocks {
                count: legacy_count,
            });
        }
        let locks = engine.register_table(TableDescriptor::new(
            LOCKS_TABLE,
            FamilyName::new("orchestrate-lock"),
            SchemaHash::for_label("orchestrate-lock-v2"),
        ))?;
        let allocator = engine.register_table(TableDescriptor::new(
            ALLOCATOR_TABLE,
            FamilyName::new("orchestrate-lock-id-allocator"),
            SchemaHash::for_label("orchestrate-lock-id-allocator-v2"),
        ))?;
        let configuration = match engine
            .match_records(QueryPlan::all(configurations))?
            .records()
        {
            [] => {
                engine.assert(Assertion::new(
                    configurations,
                    StoredConfiguration::from_public(&defaults),
                ))?;
                engine
                    .match_records(QueryPlan::all(configurations))?
                    .records()[0]
                    .clone()
                    .into_public()
            }
            [stored] => stored.clone().into_public(),
            rows => return Err(StoreError::ConfigurationInvariant { count: rows.len() }),
        };
        match engine.match_records(QueryPlan::all(allocator))?.records() {
            [] => {
                engine.assert(Assertion::new(
                    allocator,
                    StoredAllocator { next_lock_id: 1 },
                ))?;
            }
            [_] => {}
            rows => return Err(StoreError::LockIdAllocatorInvariant { count: rows.len() }),
        }
        Ok((
            Self {
                engine,
                configuration: configuration.clone(),
                configurations,
                locks,
                allocator,
            },
            configuration,
        ))
    }
    pub fn ordinary(&mut self, request: OrdinaryRequest) -> Result<OrdinaryOutcome, StoreError> {
        match request {
            OrdinaryRequest::Lock(request) => self.lock(request),
            OrdinaryRequest::Release(id) => self.release(id),
            OrdinaryRequest::Observe(selection) => Ok(OrdinaryOutcome::Response(
                OrdinaryResponse::Observed(self.observe(selection)?),
            )),
        }
    }
    pub fn meta(&mut self, request: MetaRequest) -> Result<MetaResponse, StoreError> {
        match request {
            MetaRequest::Configure(configure) => {
                if configure != self.configuration {
                    self.engine.retract(Retraction::new(
                        self.configurations,
                        RecordKey::new(CONFIGURATION_KEY),
                    ))?;
                    self.engine.assert(Assertion::new(
                        self.configurations,
                        StoredConfiguration::from_public(&configure),
                    ))?;
                    self.configuration = configure.clone();
                }
                Ok(MetaResponse::Configured(configure))
            }
        }
    }
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError> {
        let mut locks: Vec<_> = self
            .engine
            .match_records(QueryPlan::all(self.locks))?
            .records()
            .iter()
            .map(|stored| stored.clone().into_public())
            .collect();
        locks.sort_by(|left, right| {
            left.1
                .as_ref()
                .cmp(right.1.as_ref())
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(locks)
    }
}
impl Locks for OrchestrateStore {
    fn lock(&mut self, request: LockRequest) -> Result<OrdinaryOutcome, StoreError> {
        let request = NormalizedLockRequest::from_request(request)?;
        for holder in self.current_locks()? {
            if request.duplicates_name_of(&holder) {
                return Ok(OrdinaryOutcome::Response(OrdinaryResponse::LockRejected(
                    LockRejection::DuplicateName(holder),
                )));
            }
            if let Some(path) = request.overlapping_path_of(&holder) {
                return Ok(OrdinaryOutcome::Response(OrdinaryResponse::LockRejected(
                    LockRejection::PathOverlap(LockOverlap(text(path), holder)),
                )));
            }
        }
        let allocator = match self
            .engine
            .match_records(QueryPlan::all(self.allocator))?
            .records()
        {
            [row] => row.clone(),
            rows => return Err(StoreError::LockIdAllocatorInvariant { count: rows.len() }),
        };
        let next_lock_id = allocator
            .next_lock_id
            .checked_add(1)
            .ok_or(StoreError::LockIdExhausted)?;
        let lock = request.into_lock(allocator.next_lock_id);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.locks, StoredLock::from_public(&lock))
                .mutate(self.allocator, StoredAllocator { next_lock_id }),
        )?;
        Ok(OrdinaryOutcome::Response(OrdinaryResponse::Locked(lock)))
    }
}
impl Releases for OrchestrateStore {
    fn release(&mut self, lock_id: i64) -> Result<OrdinaryOutcome, StoreError> {
        let key = RecordKey::new(lock_id.to_string());
        let stored = match self
            .engine
            .match_records(QueryPlan::key(self.locks, key.clone()))?
            .records()
        {
            [] => {
                return Ok(OrdinaryOutcome::Response(
                    OrdinaryResponse::ReleaseRejected(ReleaseRejection::UnknownLockId),
                ));
            }
            [row] => row.clone(),
            _ => unreachable!("Lock IDs are keys"),
        };
        self.engine.retract(Retraction::new(self.locks, key))?;
        Ok(OrdinaryOutcome::Response(OrdinaryResponse::Released(
            stored.into_public(),
        )))
    }
}
impl Observes for OrchestrateStore {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError> {
        match selection {
            ObserveSelection::Locks => Ok(Observation::Locks(self.current_locks()?)),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_does_not_create_a_missing_store() {
        let directory = tempfile::tempdir().expect("temporary preflight directory");
        let store_path = directory.path().join("missing.sema");
        let preflight = <LegacyStorePreflight as PreflightsLegacyStore>::inspect(&store_path)
            .expect("inspect missing store");
        assert_eq!(preflight.active_lock_count(), 0);
        assert!(!store_path.exists(), "read-only preflight creates no store");
    }

    #[test]
    fn preflight_counts_legacy_rows_without_materializing_them_as_locks() {
        let directory = tempfile::tempdir().expect("temporary legacy store");
        let store_path = directory.path().join("legacy.sema");
        let mut engine =
            Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION)).expect("open legacy store");
        let legacy = engine
            .register_table(TableDescriptor::new(
                LEGACY_TABLE,
                FamilyName::new("orchestrate-path-lock"),
                SchemaHash::for_label("orchestrate-path-lock-v1"),
            ))
            .expect("register legacy family");
        engine
            .assert(Assertion::new(
                legacy,
                LegacyStoredLock {
                    name: "active".to_owned(),
                    lock: LegacyLock {
                        path_lock_name: LegacyName("active".to_owned()),
                        path_lock_paths: LegacyPaths(vec![LegacyPath("/owned".to_owned())]),
                        path_lock_description: LegacyReason("legacy".to_owned()),
                    },
                },
            ))
            .expect("write legacy fixture");
        drop(engine);

        let preflight = <LegacyStorePreflight as PreflightsLegacyStore>::inspect(&store_path)
            .expect("inspect legacy store");
        assert_eq!(preflight.active_lock_count(), 1);

        let defaults = Configure(
            text(directory.path().join("ordinary.sock").display()),
            text(directory.path().join("meta.sock").display()),
        );
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults),
            Err(StoreError::LegacyActiveLocks { count: 1 })
        ));
    }

    #[test]
    fn previous_signal_rows_require_an_explicit_migration() {
        let directory = tempfile::tempdir().expect("temporary previous store");
        let store_path = directory.path().join("previous.sema");
        let mut engine = Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION))
            .expect("open previous store");
        let configurations: TableReference<PreviousStoredConfiguration> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))
            .expect("register previous configuration family");
        engine
            .assert(Assertion::new(
                configurations,
                PreviousStoredConfiguration {
                    configuration: PreviousConfigure(
                        "/tmp/ordinary.sock".to_owned(),
                        "/tmp/meta.sock".to_owned(),
                    ),
                },
            ))
            .expect("write previous row");
        let locks: TableReference<PreviousStoredLock> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_LOCKS_TABLE,
                FamilyName::new("orchestrate-lock"),
                SchemaHash::for_label("orchestrate-lock-v1"),
            ))
            .expect("register previous Lock family");
        engine
            .assert(Assertion::new(
                locks,
                PreviousStoredLock {
                    lock: PreviousLock(
                        7,
                        "retained".to_owned(),
                        "flow-542442".to_owned(),
                        vec!["/tmp/retained".to_owned()],
                        "active before upgrade".to_owned(),
                    ),
                },
            ))
            .expect("write active v1 Lock");
        let allocator: TableReference<PreviousStoredAllocator> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_ALLOCATOR_TABLE,
                FamilyName::new("orchestrate-lock-id-allocator"),
                SchemaHash::for_label("orchestrate-lock-id-allocator-v1"),
            ))
            .expect("register previous allocator family");
        engine
            .assert(Assertion::new(
                allocator,
                PreviousStoredAllocator { next_lock_id: 9 },
            ))
            .expect("write previous allocator");
        drop(engine);

        let defaults = Configure(
            text("/tmp/default-ordinary.sock"),
            text("/tmp/default-meta.sock"),
        );
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults),
            Err(StoreError::PreviousSignalMigrationRequired {
                configuration_count: 1,
                lock_count: 1
            })
        ));

        OrchestrateStore::migrate_previous_signal(&store_path).expect("offline migration");
        assert!(matches!(
            OrchestrateStore::migrate_previous_signal(&store_path),
            Err(StoreError::MigrationTargetNotEmpty {
                configuration_count: 1,
                lock_count: 1,
                allocator_count: 1,
            })
        ));

        let (mut reopened, configuration) = OrchestrateStore::open(
            &store_path,
            Configure(
                text("/tmp/default-ordinary.sock"),
                text("/tmp/default-meta.sock"),
            ),
        )
        .expect("restart daemon against migrated store");
        assert_eq!(configuration.0.as_ref(), "/tmp/ordinary.sock");
        assert_eq!(configuration.1.as_ref(), "/tmp/meta.sock");
        assert_eq!(
            reopened
                .observe(ObserveSelection::Locks)
                .expect("observe retained Lock after restart"),
            Observation::Locks(vec![Lock(
                7,
                text("retained"),
                text("flow-542442"),
                vec![text("/tmp/retained")],
                text("active before upgrade"),
            )])
        );
        let lock = reopened
            .lock(LockRequest(
                text("next"),
                text("flow"),
                vec![text("/tmp/next")],
                text("reason"),
            ))
            .expect("acquire after migration");
        assert!(matches!(
            lock,
            OrdinaryOutcome::Response(OrdinaryResponse::Locked(Lock(9, ..)))
        ));
    }
}
