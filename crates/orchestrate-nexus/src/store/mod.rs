//! Durable Lock state owned by the Orchestrate Nexus.
//!
//! One process owns one Sema store. It holds three families: the standard
//! Nexus metadata tree, the Locks, and the Lock id allocator. Every durable
//! transition passes through this module, and every one of them is typed.

pub mod cutover;
pub mod error;
pub mod legacy;
pub mod normalize;
pub mod record;
mod transition;

pub use error::StoreError;
pub use legacy::{LegacyStorePreflight, PreflightsLegacyStore};
pub use transition::Configures;

use std::{fs, path::Path};

use nexus::{Configurable, ConfigurationState};
use sema_engine::{Engine, EngineOpen, QueryPlan, TableReference};
use signal_orchestrate::OrchestrateNexusConfiguration;

use cutover::{CarriedConfiguration, Carrying};
use legacy::CountsActivePathLocks;
use record::{
    CONFIGURATION_TABLE, Familial, SCHEMA_VERSION, StoredAllocator, StoredConfiguration,
    StoredLock, StoredMetadata, Storing,
};

/// The single owner of the Nexus's durable state.
pub struct OrchestrateStore {
    engine: Engine,
    metadata: TableReference<StoredMetadata>,
    locks: TableReference<StoredLock>,
    allocator: TableReference<StoredAllocator>,
    state: ConfigurationState<StoredConfiguration>,
}

/// Opens the one durable store, seeding a fresh one from the executable's
/// defaults and resuming a populated one from what it holds.
pub trait OpensStore: Sized {
    fn open(
        store_path: &Path,
        defaults: OrchestrateNexusConfiguration,
    ) -> Result<(Self, OrchestrateNexusConfiguration), StoreError>;
}

impl OpensStore for OrchestrateStore {
    fn open(
        store_path: &Path,
        defaults: OrchestrateNexusConfiguration,
    ) -> Result<(Self, OrchestrateNexusConfiguration), StoreError> {
        fs::create_dir_all(
            store_path
                .parent()
                .expect("configured store path has a parent"),
        )?;
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let active_path_locks = engine.count_active_path_locks()?;
        if active_path_locks != 0 {
            return Err(StoreError::LegacyActiveLocks {
                count: active_path_locks,
            });
        }
        let metadata: TableReference<StoredMetadata> =
            engine.register_table(StoredMetadata::descriptor())?;
        let state = match engine.match_records(QueryPlan::all(metadata))?.records() {
            [] => engine.seed_metadata(metadata, &defaults)?,
            [stored] => stored.state.clone(),
            rows => return Err(StoreError::MetadataInvariant { count: rows.len() }),
        };
        let locks = engine.register_table(StoredLock::descriptor())?;
        let allocator: TableReference<StoredAllocator> =
            engine.register_table(StoredAllocator::descriptor())?;
        match engine.match_records(QueryPlan::all(allocator))?.records() {
            [] => {
                engine.assert(sema_engine::Assertion::new(
                    allocator,
                    StoredAllocator { next_lock_id: 1 },
                ))?;
            }
            [_] => {}
            rows => return Err(StoreError::LockIdAllocatorInvariant { count: rows.len() }),
        }
        let configuration = state.desired_configuration().clone().into_public();
        Ok((
            Self {
                engine,
                metadata,
                locks,
                allocator,
                state,
            },
            configuration,
        ))
    }
}

/// Writes the first standard metadata tree a store ever has.
trait SeedsMetadata {
    fn seed_metadata(
        &mut self,
        metadata: TableReference<StoredMetadata>,
        defaults: &OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationState<StoredConfiguration>, StoreError>;
}

impl SeedsMetadata for Engine {
    /// A store that a previous generation left with its own configuration
    /// family seeds from that row and clears it in the same commit; any other
    /// store seeds from the executable's defaults. See `cutover`.
    fn seed_metadata(
        &mut self,
        metadata: TableReference<StoredMetadata>,
        defaults: &OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationState<StoredConfiguration>, StoreError> {
        let carried = if self.catalog().is_registered(&CONFIGURATION_TABLE) {
            Some(CarriedConfiguration::read_from(self)?)
        } else {
            None
        };
        let seed = carried
            .as_ref()
            .and_then(Carrying::configuration)
            .cloned()
            .unwrap_or_else(|| StoredConfiguration::from_public(defaults));
        let state = ConfigurationState::from_default(seed);
        let mut commit = self.begin_atomic_commit().assert(
            metadata,
            StoredMetadata {
                state: state.clone(),
            },
        );
        if let Some(carried) = carried.as_ref().filter(|carried| carried.is_present()) {
            commit = commit.retract(
                carried.table(),
                sema_engine::RecordKey::new(record::CONFIGURATION_KEY),
            );
        }
        self.commit_atomic(commit)?;
        Ok(state)
    }
}

/// The metadata tree is the durable answer to what this Nexus is configured
/// to be and whether its privileged surface has ever configured it.
pub trait KeepsMetadata {
    fn table(&self) -> TableReference<StoredMetadata>;
    fn state(&self) -> &ConfigurationState<StoredConfiguration>;
    fn state_mut(&mut self) -> &mut ConfigurationState<StoredConfiguration>;
    fn persist_state(&mut self) -> Result<(), StoreError>;
}

impl KeepsMetadata for OrchestrateStore {
    fn table(&self) -> TableReference<StoredMetadata> {
        self.metadata
    }

    fn state(&self) -> &ConfigurationState<StoredConfiguration> {
        &self.state
    }

    fn state_mut(&mut self) -> &mut ConfigurationState<StoredConfiguration> {
        &mut self.state
    }

    fn persist_state(&mut self) -> Result<(), StoreError> {
        let record = StoredMetadata {
            state: self.state.clone(),
        };
        let table = self.metadata;
        self.engine
            .mutate(sema_engine::Mutation::new(table, record))?;
        Ok(())
    }
}
