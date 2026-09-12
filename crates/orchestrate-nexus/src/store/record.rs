//! The durable records the Nexus keeps, and their public counterparts.
//!
//! Three families carry ordinary state — the standard Nexus metadata tree,
//! the Locks, and the Lock id allocator. The metadata tree is the one the
//! Nexus keeps about itself: the desired configuration together with the
//! durable fact of whether the privileged Configure was ever done. Its
//! lifecycle rule is the shared one, from the `nexus` library.

use nexus::{ConfigurationState, Situation};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName,
};
use signal_orchestrate::{Lock, OrchestrateNexusConfiguration};

pub const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
pub const METADATA_TABLE: TableName = TableName::new("orchestrate_nexus_metadata_v1");
pub const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration_v2");
pub const SITUATION_TABLE: TableName = TableName::new("orchestrate_nexus_situation_v1");
pub const LOCKS_TABLE: TableName = TableName::new("locks_v2");
pub const ALLOCATOR_TABLE: TableName = TableName::new("lock_id_allocator_v2");
pub const METADATA_KEY: &str = "metadata";
pub const CONFIGURATION_KEY: &str = "configuration";
pub const SITUATION_KEY: &str = "situation";
pub const ALLOCATOR_KEY: &str = "next";

/// The socket paths the Nexus binds, as they are stored.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredConfiguration {
    pub ordinary_socket: String,
    pub meta_socket: String,
}

/// The standard metadata tree: everything standard about this Nexus.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredMetadata {
    pub state: ConfigurationState<StoredConfiguration>,
}

impl EngineRecord for StoredMetadata {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(METADATA_KEY)
    }
}

/// Where the Nexus was last actually bound.
///
/// Its own family rather than a second field on the metadata tree, because
/// the two are different kinds of fact: the tree is desired state, which the
/// meta socket writes and the next start reads as configuration, and this is
/// observed state, which the Nexus writes at bind and nothing ever reads as
/// configuration. Keeping them apart also means a store written before this
/// family existed simply has no row here, rather than a metadata tree that
/// has to be migrated to a new shape.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredSituation {
    pub situation: Situation,
}

impl EngineRecord for StoredSituation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(SITUATION_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
pub struct StoredLock {
    pub lock_id: i64,
    pub lock_name: String,
    pub flow_id: String,
    pub paths: Vec<String>,
    pub reason: String,
}

impl EngineRecord for StoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.lock_id.to_string())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
pub struct StoredAllocator {
    pub next_lock_id: i64,
}

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
            SchemaHash::for_label("orchestrate-nexus-situation-v1"),
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

/// A durable record and the public value it carries are the same fact in two
/// places; the conversion belongs to the durable side.
pub trait Storing<Public>: Sized {
    fn from_public(value: &Public) -> Self;
    fn into_public(self) -> Public;
}

impl Storing<OrchestrateNexusConfiguration> for StoredConfiguration {
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

impl Storing<Lock> for StoredLock {
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
