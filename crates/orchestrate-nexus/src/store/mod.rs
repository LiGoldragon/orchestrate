//! Durable Lock state owned by the Orchestrate Nexus.
//!
//! One process owns one Memory store. It holds three families: the standard
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

use std::{
    fmt, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use memory_engine::{Engine, EngineOpen, QueryPlan, SemaConversion, TableReference};
use nexus::{
    Bearing, Configurable, ConfigurationState, Identifying, Relocated, Situated, StoreIdentity,
};
use signal_orchestrate::OrchestrateNexusConfiguration;

use crate::configuration::Validates;
use cutover::{CarriedConfiguration, Carrying};
use legacy::CountsActivePathLocks;
use record::{
    CONFIGURATION_TABLE, Configuration, DeclaresOrchestrateFamilies, Familial, SCHEMA_VERSION,
    SUPERSEDED_SITUATION_TABLE, StoredAllocator, StoredLock, StoredMetadata, StoredRelocation,
    StoredSituation,
};
use signal_orchestrate::LockId;

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
    state: ConfigurationState<OrchestrateNexusConfiguration>,
}

/// A filesystem store address represented exactly at the durable String
/// boundaries that record and report it.
pub(crate) trait RepresentsStorePath {
    fn exact_store_path(&self) -> Result<&str, StoreError>;
}

impl RepresentsStorePath for Path {
    fn exact_store_path(&self) -> Result<&str, StoreError> {
        self.to_str().ok_or_else(|| StoreError::NonUtf8StorePath {
            path: PathBuf::from(self),
        })
    }
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
        let exact_store_path = store_path.exact_store_path()?;
        // An absent path can only become a fresh store, so reject an unusable
        // default before the engine creates anything. A populated store does
        // not depend on today's defaults: its durable configuration wins.
        if !store_path.try_exists()? && defaults.validated().is_err() {
            return Err(StoreError::InvalidConfiguration);
        }
        let store_directory = store_path
            .parent()
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        fs::create_dir_all(store_directory)?;
        // Every family this Nexus keeps is declared to the open, so the
        // engine stamps the store and registers them in one write
        // transaction: an open that fails on a family, or a crash during it,
        // leaves the store unstamped rather than stamped forward. The
        // registrations below then only hand back references.
        let engine = Engine::open(
            EngineOpen::new(exact_store_path, SCHEMA_VERSION).declaring_orchestrate_families(),
        )?;
        // The engine may have upgraded the file from the sema format before
        // anything below judges it; a refusal from here on carries that
        // upgrade, so the opener can say so whether or not the open succeeds.
        let conversion = engine.sema_conversion().cloned();
        Self::resumed(engine, store_path, defaults)
            .map_err(|refusal| refusal.after_sema_conversion(conversion))
    }
}

/// The open past the engine: the families, the metadata tree, the store's
/// bearing and the allocator, judged over an engine already open.
trait Resumes: Sized {
    fn resumed(
        engine: Engine,
        store_path: &Path,
        defaults: OrchestrateNexusConfiguration,
    ) -> Result<(Self, OrchestrateNexusConfiguration), StoreError>;
}

impl Resumes for OrchestrateStore {
    fn resumed(
        mut engine: Engine,
        store_path: &Path,
        defaults: OrchestrateNexusConfiguration,
    ) -> Result<(Self, OrchestrateNexusConfiguration), StoreError> {
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
            [stored] => stored.configuration.0.clone(),
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
                engine.assert(memory_engine::Assertion::new(
                    allocator,
                    StoredAllocator { lock_id: LockId(1) },
                ))?;
            }
            [_] => {}
            rows => return Err(StoreError::LockIdAllocatorInvariant { count: rows.len() }),
        }
        let configuration = state.desired_configuration().clone();
        if configuration.validated().is_err() {
            return Err(StoreError::InvalidConfiguration);
        }
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

/// The line that tells an upgrade from the sema format, printed by every
/// program that opens the store: the Nexus, `orchestrate-relocate` and
/// `orchestrate-upgrade-preflight`. This is the one place it is made.
pub struct UpgradeLine<'path> {
    store_path: &'path Path,
}

impl fmt::Display for UpgradeLine<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "orchestrate-nexus upgraded {} from the sema format to the memory format",
            self.store_path.display()
        )
    }
}

/// A store, or the refusal of its open, says whether the open upgraded it
/// from the format sema-engine wrote, which the engine does on the first
/// open under the memory engine. The upgrade stands whether or not the open
/// then succeeded.
pub trait ReportsUpgrade {
    fn sema_conversion(&self) -> Option<&SemaConversion>;

    /// The line an opener prints when this open upgraded the store at
    /// `store_path`, and nothing when it found a memory store.
    fn upgrade_line<'path>(&self, store_path: &'path Path) -> Option<UpgradeLine<'path>> {
        self.sema_conversion().map(|_| UpgradeLine { store_path })
    }
}

impl<Opened: ReportsUpgrade + ?Sized> ReportsUpgrade for &Opened {
    fn sema_conversion(&self) -> Option<&SemaConversion> {
        (**self).sema_conversion()
    }
}

/// An open's outcome tells its upgrade whichever way it went.
impl<Opened: ReportsUpgrade, Refusal: ReportsUpgrade> ReportsUpgrade for Result<Opened, Refusal> {
    fn sema_conversion(&self) -> Option<&SemaConversion> {
        match self {
            Ok(opened) => opened.sema_conversion(),
            Err(refusal) => refusal.sema_conversion(),
        }
    }
}

impl ReportsUpgrade for OrchestrateStore {
    fn sema_conversion(&self) -> Option<&SemaConversion> {
        self.engine.sema_conversion()
    }
}

impl ReportsUpgrade for StoreError {
    fn sema_conversion(&self) -> Option<&SemaConversion> {
        match self {
            Self::Engine(refusal) => refusal.sema_conversion(),
            Self::RefusedAfterSemaConversion { conversion, .. } => Some(conversion),
            _ => None,
        }
    }
}

/// Writes the first standard metadata tree a store ever has.
trait SeedsMetadata {
    fn seed_metadata(
        &mut self,
        metadata: TableReference<StoredMetadata>,
        defaults: &OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationState<OrchestrateNexusConfiguration>, StoreError>;
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
    ) -> Result<ConfigurationState<OrchestrateNexusConfiguration>, StoreError> {
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
            None => ConfigurationState::from_default(defaults.clone()),
        };
        if state.desired_configuration().validated().is_err() {
            return Err(StoreError::InvalidConfiguration);
        }
        let mut commit = self.begin_atomic_commit().assert(
            metadata,
            StoredMetadata {
                configuration: Configuration(state.clone()),
            },
        );
        if let Some(carried) = carried.as_ref().filter(|carried| carried.is_present()) {
            commit = commit.retract(
                carried.table(),
                memory_engine::RecordKey::new(record::CONFIGURATION_KEY),
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
    fn state(&self) -> &ConfigurationState<OrchestrateNexusConfiguration>;
    fn state_mut(&mut self) -> &mut ConfigurationState<OrchestrateNexusConfiguration>;
    fn persist_state(&mut self) -> Result<(), StoreError>;
}

impl KeepsMetadata for OrchestrateStore {
    fn table(&self) -> TableReference<StoredMetadata> {
        self.metadata
    }

    fn state(&self) -> &ConfigurationState<OrchestrateNexusConfiguration> {
        &self.state
    }

    fn state_mut(&mut self) -> &mut ConfigurationState<OrchestrateNexusConfiguration> {
        &mut self.state
    }

    fn persist_state(&mut self) -> Result<(), StoreError> {
        let record = StoredMetadata {
            configuration: Configuration(self.state.clone()),
        };
        let table = self.metadata;
        self.engine
            .mutate(memory_engine::Mutation::new(table, record))?;
        Ok(())
    }
}
