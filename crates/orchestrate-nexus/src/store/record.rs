//! The durable records the Nexus keeps: their keys, their families, and the
//! open that declares them.
//!
//! The records themselves are declared in `memory.ethos` and generated into
//! `crate::memory`. Three families carry ordinary state — the standard Nexus
//! metadata tree, the Locks, and the Lock id allocator. The metadata tree is
//! the one the Nexus keeps about itself: the desired configuration together
//! with the durable fact of whether the privileged Configure was ever done.
//! Its lifecycle rule is the shared one, from the `nexus` library.

use memory_engine::{
    EngineOpen, EngineRecord, FamilyName, RecordKey, SchemaHash, SchemaVersion, TableDescriptor,
    TableName,
};

pub use crate::memory::{
    StoredAllocator, StoredConfiguration, StoredLock, StoredMetadata, StoredRelocation,
    StoredSituation,
};

pub const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
pub const METADATA_TABLE: TableName = TableName::new("orchestrate_nexus_metadata_v1");
pub const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration_v2");
/// The 0.34.0 situation family, which recorded the store by path alone.
///
/// Named here and never registered: a store still carrying it predates the
/// store identity the guard now rests on, and admitting it silently would
/// leave that store unguarded. `OrchestrateStore::open` refuses it instead.
pub const SUPERSEDED_SITUATION_TABLE: TableName = TableName::new("orchestrate_nexus_situation_v1");
pub const SITUATION_TABLE: TableName = TableName::new("orchestrate_nexus_situation_v2");
pub const RELOCATION_TABLE: TableName = TableName::new("orchestrate_nexus_relocation_v1");
pub const LOCKS_TABLE: TableName = TableName::new("locks_v2");
pub const ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator_v2");
pub const METADATA_KEY: &str = "metadata";
pub const CONFIGURATION_KEY: &str = "configuration";
pub const SITUATION_KEY: &str = "situation";
pub const RELOCATION_KEY: &str = "relocation";
pub const ALLOCATOR_KEY: &str = "next";

/// The standard metadata tree: everything standard about this Nexus.
impl EngineRecord for StoredMetadata {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(METADATA_KEY)
    }
}

/// Where the Nexus was last actually bound, and which store file it was.
///
/// Its own family rather than a second field on the metadata tree, because
/// the two are different kinds of fact: the tree is desired state, which the
/// meta socket writes and the next start reads as configuration, and this is
/// observed state, which the Nexus writes at bind and nothing ever reads as
/// configuration. Keeping them apart also means a store written before this
/// family existed simply has no row here, rather than a metadata tree that
/// has to be migrated to a new shape.
impl EngineRecord for StoredSituation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(SITUATION_KEY)
    }
}

/// The owner's standing declaration that this store was moved here.
///
/// Its own family for the same reason the situation is: it is neither desired
/// state nor observed state but an instruction, written by nobody the Nexus
/// can hear from — the store file itself, with no Nexus running — and read
/// exactly once, by the open it was written for. There is at most one,
/// because a store has at most one move it has not yet completed.
impl EngineRecord for StoredRelocation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(RELOCATION_KEY)
    }
}

/// A held Lock is keyed by its LockId, the one release target.
impl EngineRecord for StoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.held_lock.lock_id.0.to_string())
    }
}

/// The allocator holds the LockId the next Lock takes.
impl EngineRecord for StoredAllocator {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(ALLOCATOR_KEY)
    }
}

/// A durable family names itself once, so registration is never spelled twice.
pub trait Familial: Sized {
    fn descriptor() -> TableDescriptor<Self>;
}

impl Familial for StoredMetadata {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            METADATA_TABLE,
            FamilyName::new("orchestrate-nexus-metadata"),
            SchemaHash::for_label("orchestrate-nexus-metadata-v1"),
        )
    }
}

impl Familial for StoredSituation {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            SITUATION_TABLE,
            FamilyName::new("orchestrate-nexus-situation"),
            SchemaHash::for_label("orchestrate-nexus-situation-v2"),
        )
    }
}

impl Familial for StoredRelocation {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            RELOCATION_TABLE,
            FamilyName::new("orchestrate-nexus-relocation"),
            SchemaHash::for_label("orchestrate-nexus-relocation-v1"),
        )
    }
}

impl Familial for StoredLock {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            LOCKS_TABLE,
            FamilyName::new("orchestrate-lock"),
            SchemaHash::for_label("orchestrate-lock-v2"),
        )
    }
}

impl Familial for StoredAllocator {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            ALLOCATOR_TABLE,
            FamilyName::new("orchestrate-lock-id-allocator"),
            SchemaHash::for_label("orchestrate-lock-id-allocator-v2"),
        )
    }
}

/// An open that names every family the Nexus keeps, so the engine registers
/// them in the same write transaction as the store's layout stamp.
///
/// The superseded situation family and the pre-0.32 configuration family are
/// not declared: the first is only ever refused, and the second is only read
/// from a store that already carries it.
pub trait DeclaresOrchestrateFamilies {
    fn declaring_orchestrate_families(self) -> Self;
}

impl DeclaresOrchestrateFamilies for EngineOpen {
    fn declaring_orchestrate_families(self) -> Self {
        self.with_family(&StoredMetadata::descriptor())
            .with_family(&StoredSituation::descriptor())
            .with_family(&StoredRelocation::descriptor())
            .with_family(&StoredLock::descriptor())
            .with_family(&StoredAllocator::descriptor())
    }
}
