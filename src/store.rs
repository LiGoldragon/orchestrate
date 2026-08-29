//! Orchestrate Nexus-owned durable Lock state.

use crate::ordinary::{Locks, Observes, OrdinaryOutcome, Releases};
use meta_signal_orchestrate::{Configure, Configured, Reply as MetaReply, Request as MetaRequest};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, Retraction,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_orchestrate::{
    Lock, LockId, LockOverlap, LockRejection, LockRequest, Locks as LockSet, Observation,
    ObserveSelection, Refusal as OrdinaryRefusal, ReleaseRejection, Reply as OrdinaryReply,
    Request as OrdinaryRequest,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};
use thiserror::Error;

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration");
const LOCKS_TABLE: TableName = TableName::new("locks");
const ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator");
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
    configuration: Configure,
}
impl EngineRecord for StoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredLock {
    lock: Lock,
}
impl EngineRecord for StoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.lock.lock_id.0.to_string())
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredAllocator {
    next_lock_id: i64,
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
        if request.lock_paths.0.is_empty() {
            return Err(StoreError::EmptyPathSet);
        }
        let mut paths = BTreeSet::new();
        for path in &mut request.lock_paths.0 {
            let normalized = NormalizedLockPath::from_source(path.as_ref())?.0;
            *path = signal_orchestrate::LockPath::try_from(normalized.clone())
                .expect("normalized absolute paths are representable Datomic strings");
            if !paths.insert(normalized.clone()) {
                return Err(StoreError::DuplicateNormalizedPath { path: normalized });
            }
        }
        Ok(Self { request })
    }

    fn duplicates_name_of(&self, lock: &Lock) -> bool {
        self.request.lock_name == lock.lock_name
    }

    fn overlapping_path_of(&self, lock: &Lock) -> Option<signal_orchestrate::LockPath> {
        self.request.lock_paths.0.iter().find_map(|requested| {
            lock.lock_paths.0.iter().find_map(|held| {
                NormalizedLockPath::from_normalized(requested.as_ref())
                    .overlaps(&NormalizedLockPath::from_normalized(held.as_ref()))
                    .then(|| requested.clone())
            })
        })
    }

    fn into_lock(self, lock_id: LockId) -> Lock {
        Lock {
            lock_id,
            lock_name: self.request.lock_name,
            flow_id: self.request.flow_id,
            lock_paths: self.request.lock_paths,
            lock_reason: self.request.lock_reason,
        }
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
        let configurations = engine.register_table(TableDescriptor::new(
            CONFIGURATION_TABLE,
            FamilyName::new("orchestrate-configuration"),
            SchemaHash::for_label("orchestrate-configuration-v1"),
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
            SchemaHash::for_label("orchestrate-lock-v1"),
        ))?;
        let allocator = engine.register_table(TableDescriptor::new(
            ALLOCATOR_TABLE,
            FamilyName::new("orchestrate-lock-id-allocator"),
            SchemaHash::for_label("orchestrate-lock-id-allocator-v1"),
        ))?;
        let configuration = match engine
            .match_records(QueryPlan::all(configurations))?
            .records()
        {
            [] => {
                engine.assert(Assertion::new(
                    configurations,
                    StoredConfiguration {
                        configuration: defaults,
                    },
                ))?;
                engine
                    .match_records(QueryPlan::all(configurations))?
                    .records()[0]
                    .configuration
                    .clone()
            }
            [stored] => stored.configuration.clone(),
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
            OrdinaryRequest::Observe(selection) => Ok(OrdinaryOutcome::Reply(
                OrdinaryReply::Observed(self.observe(selection)?),
            )),
        }
    }
    pub fn meta(&mut self, request: MetaRequest) -> Result<MetaReply, StoreError> {
        match request {
            MetaRequest::Configure(configure) => {
                if configure != self.configuration {
                    self.engine.retract(Retraction::new(
                        self.configurations,
                        RecordKey::new(CONFIGURATION_KEY),
                    ))?;
                    self.engine.assert(Assertion::new(
                        self.configurations,
                        StoredConfiguration {
                            configuration: configure.clone(),
                        },
                    ))?;
                    self.configuration = configure.clone();
                }
                Ok(MetaReply::Configured(Configured { configure }))
            }
        }
    }
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError> {
        let mut locks: Vec<_> = self
            .engine
            .match_records(QueryPlan::all(self.locks))?
            .records()
            .iter()
            .map(|stored| stored.lock.clone())
            .collect();
        locks.sort_by(|left, right| {
            left.lock_name
                .as_ref()
                .cmp(right.lock_name.as_ref())
                .then_with(|| left.lock_id.0.cmp(&right.lock_id.0))
        });
        Ok(locks)
    }
}
impl Locks for OrchestrateStore {
    fn lock(&mut self, request: LockRequest) -> Result<OrdinaryOutcome, StoreError> {
        let request = NormalizedLockRequest::from_request(request)?;
        for holder in self.current_locks()? {
            if request.duplicates_name_of(&holder) {
                return Ok(OrdinaryOutcome::Refusal(OrdinaryRefusal::LockRejected(
                    LockRejection::DuplicateName(holder),
                )));
            }
            if let Some(lock_path) = request.overlapping_path_of(&holder) {
                return Ok(OrdinaryOutcome::Refusal(OrdinaryRefusal::LockRejected(
                    LockRejection::PathOverlap(LockOverlap {
                        lock_path,
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
        let lock = request.into_lock(LockId(allocator.next_lock_id));
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.locks, StoredLock { lock: lock.clone() })
                .mutate(self.allocator, StoredAllocator { next_lock_id }),
        )?;
        Ok(OrdinaryOutcome::Reply(OrdinaryReply::Locked(lock)))
    }
}
impl Releases for OrchestrateStore {
    fn release(&mut self, lock_id: LockId) -> Result<OrdinaryOutcome, StoreError> {
        let key = RecordKey::new(lock_id.0.to_string());
        let stored = match self
            .engine
            .match_records(QueryPlan::key(self.locks, key.clone()))?
            .records()
        {
            [] => {
                return Ok(OrdinaryOutcome::Refusal(OrdinaryRefusal::ReleaseRejected(
                    ReleaseRejection::UnknownLockId,
                )));
            }
            [row] => row.clone(),
            _ => unreachable!("Lock IDs are keys"),
        };
        self.engine.retract(Retraction::new(self.locks, key))?;
        Ok(OrdinaryOutcome::Reply(OrdinaryReply::Released(stored.lock)))
    }
}
impl Observes for OrchestrateStore {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError> {
        match selection {
            ObserveSelection::Locks => Ok(Observation::Locks(LockSet(self.current_locks()?))),
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

        let defaults = Configure {
            ordinary_socket_path: directory
                .path()
                .join("ordinary.sock")
                .display()
                .to_string()
                .try_into()
                .expect("temporary path is representable"),
            meta_socket_path: directory
                .path()
                .join("meta.sock")
                .display()
                .to_string()
                .try_into()
                .expect("temporary path is representable"),
        };
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults),
            Err(StoreError::LegacyActiveLocks { count: 1 })
        ));
    }
}
