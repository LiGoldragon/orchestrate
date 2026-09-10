//! Durable Lock state owned by the Orchestrate Nexus.

use crate::ordinary::{Locks, Observes, OrdinaryOutcome, Releases};
use meta_signal_orchestrate::{Query as MetaQuery, Response as MetaResponse};
use nexus::{Configurable, ConfigurationState, ConfigurationTransitionError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, Retraction,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason, Lock, LockOverlap,
    LockRejection, LockRequest, Observation, ObserveSelection, OrchestrateNexusConfiguration,
    Query as OrdinaryQuery, ReleaseRejection, Response as OrdinaryResponse,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};
use thiserror::Error;

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration_v2");
const LIFECYCLE_TABLE: TableName = TableName::new("orchestrate_configuration_lifecycle_v1");
const LOCKS_TABLE: TableName = TableName::new("locks_v2");
const ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator_v2");
const PREVIOUS_CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration");
const PREVIOUS_LOCKS_TABLE: TableName = TableName::new("locks");
const PREVIOUS_ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator");
const LEGACY_TABLE: TableName = TableName::new("active_path_locks");
const CONFIGURATION_KEY: &str = "configuration";
const LIFECYCLE_KEY: &str = "configuration-lifecycle";
const ALLOCATOR_KEY: &str = "next";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the durable store has {count} configuration rows")]
    ConfigurationInvariant { count: usize },
    #[error("the durable store has {count} configuration lifecycle rows")]
    ConfigurationLifecycleInvariant { count: usize },
    #[error("configuration lifecycle migration is required for the existing configuration")]
    ConfigurationLifecycleMigrationRequired,
    #[error("the durable store has {count} Lock ID allocator rows")]
    LockIdAllocatorInvariant { count: usize },
    #[error(
        "the old store still has {count} active PathLock rows; release them before deploying the Lock contract"
    )]
    LegacyActiveLocks { count: usize },
    #[error(
        "the previous Signal durable representation has {configuration_count} configuration and {lock_count} Lock rows; run the explicit one-time migration before activating the named contract"
    )]
    PreviousSignalMigrationRequired {
        configuration_count: usize,
        lock_count: usize,
    },
    #[error(
        "the migration target is not empty: {configuration_count} configuration, {lock_count} Lock, {allocator_count} allocator, and {lifecycle_count} lifecycle rows"
    )]
    MigrationTargetNotEmpty {
        configuration_count: usize,
        lock_count: usize,
        allocator_count: usize,
        lifecycle_count: usize,
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
pub struct StoredConfiguration {
    ordinary_socket: String,
    meta_socket: String,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredLifecycle {
    meta_configure_occurred: bool,
}
impl EngineRecord for StoredLifecycle {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(LIFECYCLE_KEY)
    }
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

trait NormalizesLockRequests: Sized {
    fn from_request(request: LockRequest) -> Result<Self, StoreError>;
    fn duplicates_name_of(&self, lock: &Lock) -> bool;
    fn overlapping_path_of(&self, lock: &Lock) -> Option<String>;
    fn into_lock(self, lock_id: i64) -> Lock;
}

impl NormalizesLockRequests for NormalizedLockRequest {
    fn from_request(mut request: LockRequest) -> Result<Self, StoreError> {
        if request.lock_path_vector.is_empty() {
            return Err(StoreError::EmptyPathSet);
        }
        let mut paths = BTreeSet::new();
        for path in &mut request.lock_path_vector {
            let normalized = NormalizedLockPath::from_source(path)?.0;
            *path = normalized.clone();
            if !paths.insert(normalized.clone()) {
                return Err(StoreError::DuplicateNormalizedPath { path: normalized });
            }
        }
        Ok(Self { request })
    }

    fn duplicates_name_of(&self, lock: &Lock) -> bool {
        self.request.lock_name == lock.lock_name
    }

    fn overlapping_path_of(&self, lock: &Lock) -> Option<String> {
        self.request.lock_path_vector.iter().find_map(|requested| {
            lock.lock_path_vector.iter().find_map(|held| {
                NormalizedLockPath::from_normalized(requested)
                    .overlaps(&NormalizedLockPath::from_normalized(held))
                    .then(|| requested.clone())
            })
        })
    }

    fn into_lock(self, lock_id: i64) -> Lock {
        Lock {
            lock_id,
            lock_name: self.request.lock_name,
            flow_id: self.request.flow_id,
            lock_path_vector: self.request.lock_path_vector,
            lock_reason: self.request.lock_reason,
        }
    }
}

trait StoresPublicConfiguration: Sized {
    fn from_public(value: &OrchestrateNexusConfiguration) -> Self;
    fn into_public(self) -> OrchestrateNexusConfiguration;
}

impl StoresPublicConfiguration for StoredConfiguration {
    fn from_public(value: &OrchestrateNexusConfiguration) -> Self {
        Self {
            ordinary_socket: value.ordinary_socket_path.clone(),
            meta_socket: value.meta_socket_path.clone(),
        }
    }
    fn into_public(self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.ordinary_socket,
            meta_socket_path: self.meta_socket,
        }
    }
}

trait StoresPublicLock: Sized {
    fn from_public(value: &Lock) -> Self;
    fn into_public(self) -> Lock;
}

impl StoresPublicLock for StoredLock {
    fn from_public(value: &Lock) -> Self {
        Self {
            lock_id: value.lock_id,
            lock_name: value.lock_name.clone(),
            flow_id: value.flow_id.clone(),
            paths: value.lock_path_vector.clone(),
            reason: value.lock_reason.clone(),
        }
    }
    fn into_public(self) -> Lock {
        Lock {
            lock_id: self.lock_id,
            lock_name: self.lock_name,
            flow_id: self.flow_id,
            lock_path_vector: self.paths,
            lock_reason: self.reason,
        }
    }
}

/// A lexically normalized absolute Unix path used during Lock acquisition.
struct NormalizedLockPath(String);

trait NormalizesLockPaths: Sized {
    fn from_source(path: &str) -> Result<Self, StoreError>;
    fn from_normalized(path: &str) -> Self;
    fn overlaps(&self, other: &Self) -> bool;
    fn is_ancestor_of(&self, descendant: &Self) -> bool;
}

impl NormalizesLockPaths for NormalizedLockPath {
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
    configuration: ConfigurationState<OrchestrateNexusConfiguration>,
    configurations: TableReference<StoredConfiguration>,
    lifecycle: TableReference<StoredLifecycle>,
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
pub trait LegacyStorePreflightInspectable: Sized {
    fn inspect(store_path: &Path) -> Result<Self, StoreError>;
    fn active_lock_count(&self) -> usize;
}

impl LegacyStorePreflightInspectable for LegacyStorePreflight {
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

pub trait PreviousSignalMigratable {
    fn migrate_previous_signal(store_path: &Path) -> Result<(), StoreError>;
}

impl PreviousSignalMigratable for OrchestrateStore {
    /// Offline, one-time import of the retired v1 Signal records.
    ///
    /// The daemon must be stopped. This method is the only legacy reader; the
    /// runtime open path never invokes it.
    fn migrate_previous_signal(store_path: &Path) -> Result<(), StoreError> {
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
        // Validate every source row before registering any v2 family. Table
        // registration is durable catalog state in sema-engine, so a malformed
        // v1 store must fail without empty v2 registrations.
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
        let lifecycle: TableReference<StoredLifecycle> =
            engine.register_table(TableDescriptor::new(
                LIFECYCLE_TABLE,
                FamilyName::new("orchestrate-configuration-lifecycle"),
                SchemaHash::for_label("orchestrate-configuration-lifecycle-v1"),
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
        let target_lifecycle_count = engine
            .match_records(QueryPlan::all(lifecycle))?
            .records()
            .len();
        if target_configuration_count != 0 || target_lock_count != 0 || target_allocator_count != 0 || target_lifecycle_count != 0
        {
            return Err(StoreError::MigrationTargetNotEmpty {
                configuration_count: target_configuration_count,
                lock_count: target_lock_count,
                allocator_count: target_allocator_count,
                lifecycle_count: target_lifecycle_count,
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
        migration = migration.assert(
            lifecycle,
            StoredLifecycle { meta_configure_occurred: false },
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
}

pub trait Openable: Sized {
    fn open(store_path: &Path, defaults: OrchestrateNexusConfiguration) -> Result<(Self, OrchestrateNexusConfiguration), StoreError>;
}

impl Openable for OrchestrateStore {
    fn open(store_path: &Path, defaults: OrchestrateNexusConfiguration) -> Result<(Self, OrchestrateNexusConfiguration), StoreError> {
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
        let lifecycle = engine.register_table(TableDescriptor::new(
            LIFECYCLE_TABLE,
            FamilyName::new("orchestrate-configuration-lifecycle"),
            SchemaHash::for_label("orchestrate-configuration-lifecycle-v1"),
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
                engine.commit_atomic(engine.begin_atomic_commit()
                    .assert(configurations, StoredConfiguration::from_public(&defaults))
                    .assert(lifecycle, StoredLifecycle { meta_configure_occurred: false }))?;
                ConfigurationState::from_default(defaults.clone())
            }
            [stored] => {
                let marker = match engine.match_records(QueryPlan::all(lifecycle))?.records() {
                    [marker] => marker.meta_configure_occurred,
                    [] => return Err(StoreError::ConfigurationLifecycleMigrationRequired),
                    rows => return Err(StoreError::ConfigurationLifecycleInvariant { count: rows.len() }),
                };
                ConfigurationState {
                    desired_configuration: stored.clone().into_public(),
                    meta_configure_occurred: marker,
                }
            }
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
                lifecycle,
                locks,
                allocator,
            },
            configuration.desired_configuration().clone(),
        ))
    }
}

pub trait OrdinaryHandleable {
    fn ordinary(&mut self, request: OrdinaryQuery) -> Result<OrdinaryOutcome, StoreError>;
}

impl OrdinaryHandleable for OrchestrateStore {
    fn ordinary(&mut self, request: OrdinaryQuery) -> Result<OrdinaryOutcome, StoreError> {
        match request {
            OrdinaryQuery::Configure(configuration) => {
                match self.configuration.ordinary_configure_if_unset(configuration.clone()) {
                    Ok(()) => {
                        self.persist_configuration()?;
                        Ok(OrdinaryOutcome::Response(OrdinaryResponse::ConfigurationAccepted(
                            self.configuration_receipt(),
                        )))
                    }
                    Err(ConfigurationTransitionError::OrdinaryConfigureClosed) => Ok(
                        OrdinaryOutcome::Response(OrdinaryResponse::ConfigurationRefused(
                            ConfigurationRejection {
                                configuration_rejection_reason: ConfigurationRejectionReason::MetaConfigureOccurred,
                            },
                        )),
                    ),
                }
            }
            OrdinaryQuery::Lock(request) => self.lock(request),
            OrdinaryQuery::Release(id) => self.release(id),
            OrdinaryQuery::Observe(selection) => Ok(OrdinaryOutcome::Response(
                OrdinaryResponse::Observed(self.observe(selection)?),
            )),
        }
    }
}

pub trait MetaHandleable {
    fn meta(&mut self, request: MetaQuery) -> Result<MetaResponse, StoreError>;
}

impl MetaHandleable for OrchestrateStore {
    fn meta(&mut self, request: MetaQuery) -> Result<MetaResponse, StoreError> {
        match request {
            MetaQuery::Configure(configure) => {
                self.configuration.meta_configure(configure);
                self.persist_configuration()?;
                Ok(MetaResponse::Configured(self.configuration_receipt()))
            }
            MetaQuery::ReverseMetaConfiguration => {
                self.configuration.meta_reverse();
                self.persist_configuration()?;
                Ok(MetaResponse::OrdinaryConfigurationReopened(self.configuration_receipt()))
            }
        }
    }
}

trait ConfigurationPersistable {
    fn persist_configuration(&mut self) -> Result<(), StoreError>;
    fn configuration_receipt(&self) -> ConfigurationReceipt;
}

impl ConfigurationPersistable for OrchestrateStore {
    fn persist_configuration(&mut self) -> Result<(), StoreError> {
        self.engine.commit_atomic(
            self.engine.begin_atomic_commit()
                .mutate(self.configurations, StoredConfiguration::from_public(self.configuration.desired_configuration()))
                .mutate(self.lifecycle, StoredLifecycle { meta_configure_occurred: self.configuration.meta_configure_occurred() }),
        )?;
        Ok(())
    }

    fn configuration_receipt(&self) -> ConfigurationReceipt {
        ConfigurationReceipt {
            orchestrate_nexus_configuration: self.configuration.desired_configuration().clone(),
            meta_configure_done: self.configuration.meta_configure_occurred(),
        }
    }
}

trait ReadsCurrentLocks {
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError>;
}

impl ReadsCurrentLocks for OrchestrateStore {
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError> {
        let mut locks: Vec<_> = self
            .engine
            .match_records(QueryPlan::all(self.locks))?
            .records()
            .iter()
            .map(|stored| stored.clone().into_public())
            .collect();
        locks.sort_by(|left, right| {
            left.lock_name
                .cmp(&right.lock_name)
                .then_with(|| left.lock_id.cmp(&right.lock_id))
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
                    LockRejection::PathOverlap(LockOverlap {
                        lock_path: path,
                        lock: holder,
                    }),
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

    // Audited mirrors of Orchestrate 0.29.2's persisted tuple records. The
    // migration reads these bytes through the independently declared
    // Previous* types above, so the witness fails if their rkyv layouts drift.
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct HistoricalConfigure(String, String);
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct HistoricalStoredConfiguration {
        configuration: HistoricalConfigure,
    }
    impl EngineRecord for HistoricalStoredConfiguration {
        fn record_key(&self) -> RecordKey {
            RecordKey::new(CONFIGURATION_KEY)
        }
    }
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct HistoricalLock(i64, String, String, Vec<String>, String);
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct HistoricalStoredLock {
        lock: HistoricalLock,
    }
    impl EngineRecord for HistoricalStoredLock {
        fn record_key(&self) -> RecordKey {
            RecordKey::new(self.lock.0.to_string())
        }
    }
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct HistoricalStoredAllocator {
        next_lock_id: i64,
    }
    impl EngineRecord for HistoricalStoredAllocator {
        fn record_key(&self) -> RecordKey {
            RecordKey::new(ALLOCATOR_KEY)
        }
    }

    #[test]
    fn preflight_does_not_create_a_missing_store() {
        let directory = tempfile::tempdir().expect("temporary preflight directory");
        let store_path = directory.path().join("missing.sema");
        let preflight = <LegacyStorePreflight as LegacyStorePreflightInspectable>::inspect(&store_path)
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

        let preflight = <LegacyStorePreflight as LegacyStorePreflightInspectable>::inspect(&store_path)
            .expect("inspect legacy store");
        assert_eq!(preflight.active_lock_count(), 1);

        let defaults = Configure {
            ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
            meta_socket_path: directory.path().join("meta.sock").display().to_string(),
        };
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults),
            Err(StoreError::LegacyActiveLocks { count: 1 })
        ));
    }

    #[test]
    fn migration_source_failure_preserves_v1_records() {
        let directory = tempfile::tempdir().expect("temporary malformed v1 store");
        let store_path = directory.path().join("malformed-v1.sema");
        let mut engine = Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION))
            .expect("open malformed v1 store");
        let configurations: TableReference<HistoricalStoredConfiguration> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))
            .expect("register previous configuration family");
        engine
            .assert(Assertion::new(
                configurations,
                HistoricalStoredConfiguration {
                    configuration: HistoricalConfigure(
                        "/tmp/ordinary.sock".to_owned(),
                        "/tmp/meta.sock".to_owned(),
                    ),
                },
            ))
            .expect("write malformed v1 configuration");
        drop(engine);

        assert!(matches!(
            OrchestrateStore::migrate_previous_signal(&store_path),
            Err(StoreError::MigrationSourceInvariant {
                configuration_count: 1,
                allocator_count: 0,
            })
        ));
        let mut reopened = Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION))
            .expect("reopen rejected v1 store");
        assert!(
            !reopened.catalog().is_registered(&CONFIGURATION_TABLE)
                && !reopened.catalog().is_registered(&LOCKS_TABLE)
                && !reopened.catalog().is_registered(&ALLOCATOR_TABLE),
            "failed source preflight must not leave empty v2 catalogue registrations"
        );
        let configurations: TableReference<PreviousStoredConfiguration> = reopened
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))
            .expect("read prior configuration family");
        assert_eq!(
            reopened
                .match_records(QueryPlan::all(configurations))
                .expect("read preserved v1 configuration")
                .records()
                .len(),
            1,
            "failed source preflight must preserve v1 records"
        );
    }

    #[test]
    fn store_refuses_a_second_open_owner() {
        let directory = tempfile::tempdir().expect("temporary exclusive store");
        let store_path = directory.path().join("exclusive.sema");
        let defaults = Configure {
            ordinary_socket_path: directory.path().join("ordinary.sock").display().to_string(),
            meta_socket_path: directory.path().join("meta.sock").display().to_string(),
        };
        let first = OrchestrateStore::open(&store_path, defaults.clone())
            .expect("open first durable store owner");
        assert!(
            OrchestrateStore::open(&store_path, defaults).is_err(),
            "redb's native writable file lock must reject a second owner"
        );
        drop(first);
    }

    #[test]
    fn previous_signal_rows_require_an_explicit_migration() {
        let directory = tempfile::tempdir().expect("temporary previous store");
        let store_path = directory.path().join("previous.sema");
        let mut engine = Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION))
            .expect("open previous store");
        let configurations: TableReference<HistoricalStoredConfiguration> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_CONFIGURATION_TABLE,
                FamilyName::new("orchestrate-configuration"),
                SchemaHash::for_label("orchestrate-configuration-v1"),
            ))
            .expect("register previous configuration family");
        engine
            .assert(Assertion::new(
                configurations,
                HistoricalStoredConfiguration {
                    configuration: HistoricalConfigure(
                        "/tmp/ordinary.sock".to_owned(),
                        "/tmp/meta.sock".to_owned(),
                    ),
                },
            ))
            .expect("write previous row");
        let locks: TableReference<HistoricalStoredLock> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_LOCKS_TABLE,
                FamilyName::new("orchestrate-lock"),
                SchemaHash::for_label("orchestrate-lock-v1"),
            ))
            .expect("register previous Lock family");
        engine
            .assert(Assertion::new(
                locks,
                HistoricalStoredLock {
                    lock: HistoricalLock(
                        7,
                        "retained".to_owned(),
                        "flow-542442".to_owned(),
                        vec!["/tmp/retained".to_owned()],
                        "active before upgrade".to_owned(),
                    ),
                },
            ))
            .expect("write active v1 Lock");
        engine
            .assert(Assertion::new(
                locks,
                HistoricalStoredLock {
                    lock: HistoricalLock(
                        8,
                        "second".to_owned(),
                        "flow-second".to_owned(),
                        vec!["/tmp/second-a".to_owned(), "/tmp/second-b".to_owned()],
                        "second active lock".to_owned(),
                    ),
                },
            ))
            .expect("write second active v1 Lock");
        let allocator: TableReference<HistoricalStoredAllocator> = engine
            .register_table(TableDescriptor::new(
                PREVIOUS_ALLOCATOR_TABLE,
                FamilyName::new("orchestrate-lock-id-allocator"),
                SchemaHash::for_label("orchestrate-lock-id-allocator-v1"),
            ))
            .expect("register previous allocator family");
        engine
            .assert(Assertion::new(
                allocator,
                HistoricalStoredAllocator { next_lock_id: 9 },
            ))
            .expect("write previous allocator");
        drop(engine);

        let defaults = Configure {
            ordinary_socket_path: "/tmp/default-ordinary.sock".to_owned(),
            meta_socket_path: "/tmp/default-meta.sock".to_owned(),
        };
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults),
            Err(StoreError::PreviousSignalMigrationRequired {
                configuration_count: 1,
                lock_count: 2
            })
        ));

        OrchestrateStore::migrate_previous_signal(&store_path).expect("offline migration");
        assert!(matches!(
            OrchestrateStore::migrate_previous_signal(&store_path),
            Err(StoreError::MigrationSourceInvariant {
                configuration_count: 0,
                allocator_count: 0,
            })
        ));

        let (mut reopened, configuration) = OrchestrateStore::open(
            &store_path,
            Configure {
                ordinary_socket_path: "/tmp/default-ordinary.sock".to_owned(),
                meta_socket_path: "/tmp/default-meta.sock".to_owned(),
            },
        )
        .expect("restart daemon against migrated store");
        assert_eq!(configuration.ordinary_socket_path, "/tmp/ordinary.sock");
        assert_eq!(configuration.meta_socket_path, "/tmp/meta.sock");
        assert_eq!(
            reopened
                .observe(ObserveSelection::Locks)
                .expect("observe retained Lock after restart"),
            Observation::Locks(vec![
                Lock {
                    lock_id: 7,
                    lock_name: "retained".to_owned(),
                    flow_id: "flow-542442".to_owned(),
                    lock_path_vector: vec!["/tmp/retained".to_owned()],
                    lock_reason: "active before upgrade".to_owned(),
                },
                Lock {
                    lock_id: 8,
                    lock_name: "second".to_owned(),
                    flow_id: "flow-second".to_owned(),
                    lock_path_vector: vec!["/tmp/second-a".to_owned(), "/tmp/second-b".to_owned(),],
                    lock_reason: "second active lock".to_owned(),
                },
            ])
        );
        let lock = reopened
            .lock(LockRequest {
                lock_name: "next".to_owned(),
                flow_id: "flow".to_owned(),
                lock_path_vector: vec!["/tmp/next".to_owned()],
                lock_reason: "reason".to_owned(),
            })
            .expect("acquire after migration");
        assert!(matches!(
            lock,
            OrdinaryOutcome::Response(OrdinaryResponse::Locked(Lock { lock_id: 9, .. }))
        ));
    }
}
