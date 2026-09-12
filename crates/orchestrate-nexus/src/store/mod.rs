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
mod relocation;
mod situation;
mod transition;

pub use error::StoreError;
pub use legacy::{LegacyStorePreflight, PreflightsLegacyStore};
pub use relocation::Relocates;
pub use situation::Situates;
pub use transition::Configures;

use std::{fs, path::Path};

use nexus::{
    Bearing, Configurable, ConfigurationState, Identifying, Relocated, Situated, StoreIdentity,
};
use sema_engine::{Engine, EngineOpen, QueryPlan, TableReference};
use signal_orchestrate::OrchestrateNexusConfiguration;

use cutover::{CarriedConfiguration, Carrying};
use legacy::CountsActivePathLocks;
use record::{
    CONFIGURATION_TABLE, Familial, SCHEMA_VERSION, SUPERSEDED_SITUATION_TABLE, StoredAllocator,
    StoredConfiguration, StoredLock, StoredMetadata, StoredRelocation, StoredSituation, Storing,
};

/// The single owner of the Nexus's durable state.
pub struct OrchestrateStore {
    engine: Engine,
    /// The file this store actually is, not merely the name it was reached
    /// by: what the situation record is written from, and what the next open
    /// compares itself against.
    store: StoreIdentity,
    metadata: TableReference<StoredMetadata>,
    situation: TableReference<StoredSituation>,
    relocation: TableReference<StoredRelocation>,
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
        // A store guarded only by its path cannot tell a move from a copy,
        // and this generation's guard rests on the store's file identity. A
        // store still carrying the older family is refused rather than
        // silently admitted with no guard at all.
        if engine.catalog().is_registered(&SUPERSEDED_SITUATION_TABLE) {
            return Err(StoreError::SupersededSituation);
        }
        let situation: TableReference<StoredSituation> =
            engine.register_table(StoredSituation::descriptor())?;
        let relocation: TableReference<StoredRelocation> =
            engine.register_table(StoredRelocation::descriptor())?;
        // Before anything else this store holds is trusted: the socket paths
        // it is about to hand back belong to whichever Nexus last bound them,
        // and a second copy of a store is not that Nexus.
        //
        // The engine has created or opened the file by now, so this identity
        // is the file the Nexus is actually serving from.
        let store = StoreIdentity::of(store_path);
        let declared = match engine.match_records(QueryPlan::all(relocation))?.records() {
            [] => None,
            [row] => Some(row.relocation.clone()),
            rows => return Err(StoreError::RelocationInvariant { count: rows.len() }),
        };
        match engine.match_records(QueryPlan::all(situation))?.records() {
            [] => {}
            [recorded] => {
                let recorded = recorded.situation.clone();
                // Settled and Moved both pass: the first is a store at its own
                // address, and the second is one file that has changed address,
                // which no second claimant can be hiding behind. Only Carried
                // is two files bearing one record, and only Carried consults
                // the declaration.
                if recorded.bearing(&store) == Bearing::Carried {
                    let recorded_path = recorded.store().path().to_owned();
                    match declared {
                        Some(declaration) if declaration.admits(&recorded_path, store.path()) => {}
                        Some(declaration) => {
                            return Err(StoreError::UnrelatedRelocation {
                                declared_origin: declaration.origin,
                                declared_destination: declaration.destination,
                                recorded: recorded_path,
                                opened: store.path().to_owned(),
                            });
                        }
                        None => {
                            return Err(StoreError::CarriedStore {
                                recorded: recorded_path,
                                opened: store.path().to_owned(),
                            });
                        }
                    }
                }
            }
            rows => return Err(StoreError::SituationInvariant { count: rows.len() }),
        }
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
                store,
                metadata,
                situation,
                relocation,
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
    /// family seeds from that row, records the privileged Configure as done,
    /// and clears the row in the same commit; any other store seeds from the
    /// executable's defaults with ordinary Configure still open. See
    /// `cutover`.
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
        let state = match carried.as_ref().and_then(Carrying::configuration) {
            // A carried configuration was established by privileged means: in
            // the generation that wrote it the ordinary socket had no
            // Configure at all, so the value is either the executable's own
            // default or a meta Configure. Seeding it as unconfigured would
            // open the ordinary bootstrap window on a Nexus that has been in
            // service, and any ordinary peer could then repoint its sockets.
            // See `cutover`.
            Some(carried) => {
                let mut state = ConfigurationState::from_default(carried.clone());
                state.meta_configure(carried.clone());
                state
            }
            None => ConfigurationState::from_default(StoredConfiguration::from_public(defaults)),
        };
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
