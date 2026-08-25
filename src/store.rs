//! Daemon-owned durable path-lock state.

use std::{
    collections::BTreeSet,
    path::{Component, Path},
};

use meta_signal_orchestrate::{
    ConfigurationRefusal, ConfigurationRejected, Configure, Configured, MetaOrchestrateReply,
    MetaOrchestrateRequest,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, Retraction,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_orchestrate::{
    OrchestrateReply, OrchestrateRequest, PathLock, PathLockOverlap, PathLockRegistered,
    PathLockRegistrationRefusal, PathLockRegistrationRejected, PathLockRelease,
    PathLockReleaseRefusal, PathLockReleaseRejected, PathLockReleased,
};
use thiserror::Error;

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const CONFIGURATION_TABLE: TableName = TableName::new("orchestrate_configuration");
const PATH_LOCKS_TABLE: TableName = TableName::new("active_path_locks");
const CONFIGURATION_KEY: &str = "configuration";

/// Failure to serve a request. Domain refusals remain generated reply values.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the durable store has {count} configuration rows")]
    ConfigurationInvariant { count: usize },
    #[error("persisted store path {persisted:?} differs from opened path {opened:?}")]
    StorePathMismatch { persisted: String, opened: String },
    #[error("path-lock path {path:?} is not absolute")]
    RelativePath { path: String },
    #[error("path lock has no paths")]
    EmptyPathSet,
    #[error("path-lock path {path:?} contains a parent component")]
    ParentPathComponent { path: String },
    #[error("path lock repeats normalized path {path:?}")]
    DuplicateNormalizedPath { path: String },
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredConfiguration {
    configuration: Configure,
}

impl EngineRecord for StoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredPathLock {
    name: String,
    lock: PathLock,
}

impl EngineRecord for StoredPathLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.name.clone())
    }
}

/// The transport serializes all calls to this sole durable state owner.
pub struct OrchestrateStore {
    engine: Engine,
    configuration: Configure,
    path_locks: TableReference<StoredPathLock>,
}

impl OrchestrateStore {
    /// A virgin store persists `startup`; a reopened one returns its durable
    /// configuration so the caller binds the sockets it originally configured.
    pub fn open(startup: Configure) -> Result<(Self, Configure), StoreError> {
        let opened_path = startup.store_path.0.clone();
        let mut engine = Engine::open(EngineOpen::new(&opened_path, SCHEMA_VERSION))?;
        let configurations = engine.register_table(TableDescriptor::new(
            CONFIGURATION_TABLE,
            FamilyName::new("orchestrate-configuration"),
            SchemaHash::for_label("orchestrate-configuration-v1"),
        ))?;
        let path_locks = engine.register_table(TableDescriptor::new(
            PATH_LOCKS_TABLE,
            FamilyName::new("orchestrate-path-lock"),
            SchemaHash::for_label("orchestrate-path-lock-v1"),
        ))?;
        let configuration = match engine
            .match_records(QueryPlan::all(configurations))?
            .records()
        {
            [] => {
                engine.assert(Assertion::new(
                    configurations,
                    StoredConfiguration {
                        configuration: startup,
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
        if configuration.store_path.0 != opened_path {
            return Err(StoreError::StorePathMismatch {
                persisted: configuration.store_path.0.clone(),
                opened: opened_path,
            });
        }
        Ok((
            Self {
                engine,
                configuration: configuration.clone(),
                path_locks,
            },
            configuration,
        ))
    }

    pub fn ordinary(
        &mut self,
        request: OrchestrateRequest,
    ) -> Result<OrchestrateReply, StoreError> {
        match request {
            OrchestrateRequest::Register(lock) => self.register(lock),
            OrchestrateRequest::Release(release) => self.release(release),
        }
    }

    pub fn meta(
        &mut self,
        request: MetaOrchestrateRequest,
    ) -> Result<MetaOrchestrateReply, StoreError> {
        match request {
            MetaOrchestrateRequest::Configure(configure)
                if configure.store_path != self.configuration.store_path =>
            {
                Ok(MetaOrchestrateReply::ConfigurationRejected(
                    ConfigurationRejected {
                        configure,
                        configuration_refusal: ConfigurationRefusal::StorePathImmutable,
                    },
                ))
            }
            MetaOrchestrateRequest::Configure(configure) if configure != self.configuration => Ok(
                MetaOrchestrateReply::ConfigurationRejected(ConfigurationRejected {
                    configure,
                    configuration_refusal: ConfigurationRefusal::InvalidConfiguration,
                }),
            ),
            MetaOrchestrateRequest::Configure(configure) => {
                Ok(MetaOrchestrateReply::Configured(Configured { configure }))
            }
        }
    }

    fn register(&mut self, lock: PathLock) -> Result<OrchestrateReply, StoreError> {
        let lock = normalize_lock(lock)?;
        for stored in self
            .engine
            .match_records(QueryPlan::all(self.path_locks))?
            .records()
        {
            let holder = &stored.lock;
            if holder.path_lock_name == lock.path_lock_name {
                return Ok(OrchestrateReply::PathLockRegistrationRejected(
                    PathLockRegistrationRejected {
                        path_lock: lock,
                        path_lock_registration_refusal:
                            PathLockRegistrationRefusal::DuplicateActiveName(holder.clone()),
                    },
                ));
            }
            for requested in &lock.path_lock_paths.0 {
                if holder
                    .path_lock_paths
                    .0
                    .iter()
                    .any(|held| paths_overlap(&requested.0, &held.0))
                {
                    return Ok(OrchestrateReply::PathLockRegistrationRejected(
                        PathLockRegistrationRejected {
                            path_lock: lock.clone(),
                            path_lock_registration_refusal:
                                PathLockRegistrationRefusal::PathOverlap(PathLockOverlap {
                                    path_lock_path: requested.clone(),
                                    path_lock: holder.clone(),
                                }),
                        },
                    ));
                }
            }
        }
        self.engine.assert(Assertion::new(
            self.path_locks,
            StoredPathLock {
                name: lock.path_lock_name.0.clone(),
                lock: lock.clone(),
            },
        ))?;
        Ok(OrchestrateReply::PathLockRegistered(PathLockRegistered {
            path_lock: lock,
        }))
    }

    fn release(&mut self, release: PathLockRelease) -> Result<OrchestrateReply, StoreError> {
        let key = RecordKey::new(release.path_lock_name.0.clone());
        if self
            .engine
            .match_records(QueryPlan::key(self.path_locks, key.clone()))?
            .records()
            .is_empty()
        {
            return Ok(OrchestrateReply::PathLockReleaseRejected(
                PathLockReleaseRejected {
                    path_lock_release: release,
                    path_lock_release_refusal: PathLockReleaseRefusal::UnknownActiveName,
                },
            ));
        }
        self.engine.retract(Retraction::new(self.path_locks, key))?;
        Ok(OrchestrateReply::PathLockReleased(PathLockReleased {
            path_lock_release: release,
        }))
    }
}

fn normalize_lock(mut lock: PathLock) -> Result<PathLock, StoreError> {
    if lock.path_lock_paths.0.is_empty() {
        return Err(StoreError::EmptyPathSet);
    }
    let mut paths = BTreeSet::new();
    for path in &mut lock.path_lock_paths.0 {
        path.0 = normalize_path(&path.0)?;
        if !paths.insert(path.0.clone()) {
            return Err(StoreError::DuplicateNormalizedPath {
                path: path.0.clone(),
            });
        }
    }
    Ok(lock)
}

fn normalize_path(path: &str) -> Result<String, StoreError> {
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
    Ok(normalized)
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == right || is_ancestor(left, right) || is_ancestor(right, left)
}

fn is_ancestor(ancestor: &str, descendant: &str) -> bool {
    ancestor == "/"
        || descendant
            .strip_prefix(ancestor)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta_signal_orchestrate::{MetaSocketPath, OrdinarySocketPath, StorePath};
    use signal_orchestrate::{PathLockDescription, PathLockName, PathLockPath, PathLockPaths};

    fn configure(dir: &tempfile::TempDir) -> Configure {
        Configure {
            store_path: StorePath(dir.path().join("orchestrate.sema").display().to_string()),
            ordinary_socket_path: OrdinarySocketPath(
                dir.path().join("ordinary.sock").display().to_string(),
            ),
            meta_socket_path: MetaSocketPath(dir.path().join("meta.sock").display().to_string()),
        }
    }

    fn lock(name: &str, paths: &[String]) -> PathLock {
        PathLock {
            path_lock_name: PathLockName(name.into()),
            path_lock_paths: PathLockPaths(paths.iter().cloned().map(PathLockPath).collect()),
            path_lock_description: PathLockDescription("test lock".into()),
        }
    }

    #[test]
    fn persists_normalized_locks_and_refuses_conflicts() {
        let dir = tempfile::tempdir().expect("isolated store directory");
        let configure = configure(&dir);
        let normalized = format!("{}/owned", dir.path().display());
        let unnormalized = format!(
            "//{}/./owned",
            dir.path().display().to_string().trim_start_matches('/')
        );
        let (mut store, persisted) =
            OrchestrateStore::open(configure.clone()).expect("open virgin store");
        assert_eq!(persisted, configure);
        assert!(matches!(
            store.ordinary(OrchestrateRequest::Register(lock("alpha", &[unnormalized]))),
            Ok(OrchestrateReply::PathLockRegistered(_))
        ));
        drop(store);
        let (mut reopened, persisted) =
            OrchestrateStore::open(configure.clone()).expect("reopen store");
        assert_eq!(persisted, configure);
        assert!(
            matches!(reopened.ordinary(OrchestrateRequest::Register(lock("alpha", &[format!("{}/elsewhere", dir.path().display())]))), Ok(OrchestrateReply::PathLockRegistrationRejected(PathLockRegistrationRejected { path_lock_registration_refusal: PathLockRegistrationRefusal::DuplicateActiveName(holder), .. })) if holder.path_lock_paths.0 == [PathLockPath(normalized.clone())])
        );
        assert!(
            matches!(reopened.ordinary(OrchestrateRequest::Register(lock("beta", &[format!("{normalized}/child")]))), Ok(OrchestrateReply::PathLockRegistrationRejected(PathLockRegistrationRejected { path_lock_registration_refusal: PathLockRegistrationRefusal::PathOverlap(PathLockOverlap { path_lock_path, path_lock: holder }), .. })) if path_lock_path == PathLockPath(format!("{normalized}/child")) && holder.path_lock_name.0 == "alpha")
        );
        assert!(
            matches!(reopened.ordinary(OrchestrateRequest::Register(lock("ancestor", &[dir.path().display().to_string()]))), Ok(OrchestrateReply::PathLockRegistrationRejected(PathLockRegistrationRejected { path_lock_registration_refusal: PathLockRegistrationRefusal::PathOverlap(PathLockOverlap { path_lock_path, path_lock: holder }), .. })) if path_lock_path == PathLockPath(dir.path().display().to_string()) && holder.path_lock_name.0 == "alpha")
        );
        assert!(matches!(
            reopened.ordinary(OrchestrateRequest::Release(PathLockRelease {
                path_lock_name: PathLockName("alpha".into())
            })),
            Ok(OrchestrateReply::PathLockReleased(_))
        ));
        assert!(matches!(
            reopened.ordinary(OrchestrateRequest::Release(PathLockRelease {
                path_lock_name: PathLockName("alpha".into())
            })),
            Ok(OrchestrateReply::PathLockReleaseRejected(
                PathLockReleaseRejected {
                    path_lock_release_refusal: PathLockReleaseRefusal::UnknownActiveName,
                    ..
                }
            ))
        ));
        assert!(matches!(
            reopened.ordinary(OrchestrateRequest::Register(lock("gamma", &[normalized]))),
            Ok(OrchestrateReply::PathLockRegistered(_))
        ));
    }

    #[test]
    fn configure_refuses_store_path_changes() {
        let dir = tempfile::tempdir().expect("isolated store directory");
        let configure = configure(&dir);
        let (mut store, _) = OrchestrateStore::open(configure.clone()).expect("open store");
        assert!(matches!(
            store.meta(MetaOrchestrateRequest::Configure(configure.clone())),
            Ok(MetaOrchestrateReply::Configured(_))
        ));
        let mut moved = configure;
        moved.store_path.0.push_str(".other");
        assert!(matches!(
            store.meta(MetaOrchestrateRequest::Configure(moved)),
            Ok(MetaOrchestrateReply::ConfigurationRejected(
                ConfigurationRejected {
                    configuration_refusal: ConfigurationRefusal::StorePathImmutable,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn rejects_an_empty_or_nonabsolute_path_set_before_writing() {
        let dir = tempfile::tempdir().expect("isolated store directory");
        let (mut store, _) = OrchestrateStore::open(configure(&dir)).expect("open store");
        assert!(matches!(
            store.ordinary(OrchestrateRequest::Register(lock("empty", &[]))),
            Err(StoreError::EmptyPathSet)
        ));
        assert!(matches!(
            store.ordinary(OrchestrateRequest::Register(lock(
                "relative",
                &["relative".to_owned()]
            ))),
            Err(StoreError::RelativePath { .. })
        ));
    }
}
