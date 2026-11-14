//! The generated Memory keeps the bytes of the hand-written records it
//! replaces: each record archives exactly as the 0.38.0 record of the same
//! family did, so a store written before `memory.ethos` opens unchanged, with
//! no upgrade. The mirrors below are those 0.38.0 records, field for field.

use nexus::{Configurable, ConfigurationState};
use orchestrate_nexus::memory::{
    PathLock, PathLockDescription, PathLockName, PathLockPath, PathLockPaths, StoredAllocator,
    StoredConfiguration, StoredLock, StoredMetadata, StoredPathLock,
};
use rkyv::{Archive, Serialize, rancor::Error};
use signal_orchestrate::{
    ConfigurationPath, FlowId, HeldLock, LockId, LockName, LockPath, LockReason, MetaSocketPath,
    OrchestrateNexusConfiguration, OrdinarySocketPath,
};

#[derive(Archive, Serialize)]
struct Lock038 {
    lock_id: i64,
    lock_name: String,
    flow_id: String,
    paths: Vec<String>,
    reason: String,
}

#[derive(Archive, Serialize)]
struct Allocator038 {
    next_lock_id: i64,
}

#[derive(Archive, Serialize, Clone)]
struct Configuration038 {
    ordinary_socket: String,
    meta_socket: String,
}

#[derive(Archive, Serialize)]
struct Metadata038 {
    state: ConfigurationState<Configuration038>,
}

#[derive(Archive, Serialize)]
struct LegacyName(String);
#[derive(Archive, Serialize)]
struct LegacyPath(String);
#[derive(Archive, Serialize)]
struct LegacyPaths(Vec<LegacyPath>);
#[derive(Archive, Serialize)]
struct LegacyReason(String);
#[derive(Archive, Serialize)]
struct LegacyLock {
    path_lock_name: LegacyName,
    path_lock_paths: LegacyPaths,
    path_lock_description: LegacyReason,
}
#[derive(Archive, Serialize)]
struct LegacyStoredLock {
    name: String,
    lock: LegacyLock,
}

/// Archives a value as the store writes it.
fn archived<Value>(value: &Value) -> Vec<u8>
where
    Value: for<'a> Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            Error,
        >,
    >,
{
    rkyv::to_bytes::<Error>(value).expect("archive").to_vec()
}

fn configuration() -> OrchestrateNexusConfiguration {
    OrchestrateNexusConfiguration {
        ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(
            "/run/user/1000/orchestrate-nexus/orchestrate.sock".into(),
        )),
        meta_socket_path: MetaSocketPath(ConfigurationPath(
            "/run/user/1000/orchestrate-nexus/orchestrate-meta.sock".into(),
        )),
    }
}

fn configuration_038() -> Configuration038 {
    Configuration038 {
        ordinary_socket: "/run/user/1000/orchestrate-nexus/orchestrate.sock".into(),
        meta_socket: "/run/user/1000/orchestrate-nexus/orchestrate-meta.sock".into(),
    }
}

#[test]
fn a_held_lock_archives_as_the_0_38_lock_record() {
    let generated = StoredLock {
        held_lock: HeldLock {
            lock_id: LockId(169),
            lock_name: LockName("OrchestrateEthos27".into()),
            flow_id: FlowId("2e404d".into()),
            lock_path_vector: vec![LockPath("/a".into()), LockPath("/b/c".into())],
            lock_reason: LockReason("two words".into()),
        },
    };
    let previous = Lock038 {
        lock_id: 169,
        lock_name: "OrchestrateEthos27".into(),
        flow_id: "2e404d".into(),
        paths: vec!["/a".into(), "/b/c".into()],
        reason: "two words".into(),
    };
    assert_eq!(archived(&generated), archived(&previous));
}

#[test]
fn the_allocator_archives_as_the_0_38_allocator_record() {
    assert_eq!(
        archived(&StoredAllocator {
            lock_id: LockId(170)
        }),
        archived(&Allocator038 { next_lock_id: 170 })
    );
}

#[test]
fn the_metadata_tree_archives_as_the_0_38_metadata_record() {
    for closed in [false, true] {
        let mut generated = ConfigurationState::from_default(configuration());
        let mut previous = ConfigurationState::from_default(configuration_038());
        if closed {
            generated.meta_configure(configuration());
            previous.meta_configure(configuration_038());
        }
        assert_eq!(
            archived(&StoredMetadata {
                orchestrate_nexus_configuration_configuration_state: generated,
            }),
            archived(&Metadata038 { state: previous })
        );
    }
}

#[test]
fn the_carried_configuration_archives_as_the_0_30_configuration_record() {
    assert_eq!(
        archived(&StoredConfiguration {
            orchestrate_nexus_configuration: configuration(),
        }),
        archived(&configuration_038())
    );
}

#[test]
fn a_path_lock_archives_as_the_pre_0_25_record() {
    let generated = StoredPathLock {
        path_lock_name: PathLockName("active".into()),
        path_lock: PathLock {
            path_lock_name: PathLockName("active".into()),
            path_lock_paths: PathLockPaths(vec![PathLockPath("/owned".into())]),
            path_lock_description: PathLockDescription("legacy".into()),
        },
    };
    let previous = LegacyStoredLock {
        name: "active".into(),
        lock: LegacyLock {
            path_lock_name: LegacyName("active".into()),
            path_lock_paths: LegacyPaths(vec![LegacyPath("/owned".into())]),
            path_lock_description: LegacyReason("legacy".into()),
        },
    };
    assert_eq!(archived(&generated), archived(&previous));
}
