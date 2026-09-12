//! Lock path normalization and the overlap rule Lock acquisition applies.

use std::{
    collections::BTreeSet,
    path::{Component, Path},
};

use signal_orchestrate::{Lock, LockRequest};

use super::error::StoreError;

/// A Lock request whose path values have passed Nexus normalization.
///
/// This is a durable-transition input, not a second public contract type.
/// Keeping its path and overlap rules with the request prevents transport or
/// callers from acquiring a partially normalized Lock.
pub struct NormalizedLockRequest {
    request: LockRequest,
}

pub trait NormalizesLockRequests: Sized {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request(paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: "name".to_owned(),
            flow_id: "flow".to_owned(),
            lock_path_vector: paths.iter().map(|path| (*path).to_owned()).collect(),
            lock_reason: "reason".to_owned(),
        }
    }

    #[test]
    fn a_path_is_normalized_lexically_and_a_parent_component_is_refused() {
        let normalized = NormalizedLockRequest::from_request(request(&["/a/./b//c"]))
            .expect("normalize a lexical path");
        assert_eq!(
            normalized.into_lock(1).lock_path_vector,
            vec!["/a/b/c".to_owned()]
        );
        assert!(matches!(
            NormalizedLockRequest::from_request(request(&["/a/../b"])),
            Err(StoreError::ParentPathComponent { .. })
        ));
        assert!(matches!(
            NormalizedLockRequest::from_request(request(&["relative"])),
            Err(StoreError::RelativePath { .. })
        ));
        assert!(matches!(
            NormalizedLockRequest::from_request(request(&[])),
            Err(StoreError::EmptyPathSet)
        ));
        assert!(matches!(
            NormalizedLockRequest::from_request(request(&["/a/b", "/a/./b"])),
            Err(StoreError::DuplicateNormalizedPath { .. })
        ));
    }

    #[test]
    fn overlap_is_ancestry_in_either_direction_and_the_root_covers_everything() {
        let held = Lock {
            lock_id: 1,
            lock_name: "held".to_owned(),
            flow_id: "flow".to_owned(),
            lock_path_vector: vec!["/a/b".to_owned()],
            lock_reason: "reason".to_owned(),
        };
        for requested in ["/a/b", "/a/b/c", "/"] {
            assert_eq!(
                NormalizedLockRequest::from_request(request(&[requested]))
                    .expect("normalize")
                    .overlapping_path_of(&held),
                Some(requested.to_owned()),
                "{requested} overlaps /a/b"
            );
        }
        assert_eq!(
            NormalizedLockRequest::from_request(request(&["/a/bc"]))
                .expect("normalize")
                .overlapping_path_of(&held),
            None,
            "a shared textual prefix is not ancestry"
        );
    }
}
