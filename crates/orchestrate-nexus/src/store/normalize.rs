//! Lock path normalization and the overlap rule Lock acquisition applies.

use std::{
    collections::BTreeSet,
    path::{Component, Path},
};

use memory_engine::RecordPredicate;
use signal_orchestrate::{Lock, LockId, LockPath, LockRejection, LockRequest};

use super::record::StoredLock;

/// A Lock request whose path values have passed Nexus normalization.
///
/// This is a durable-transition input, not a second public contract type.
/// Keeping its path and overlap rules with the request prevents transport or
/// callers from acquiring a partially normalized Lock. A request whose own
/// paths are at fault is not normalized: it is rejected, in the contract's
/// `LockRejection`, naming the fault and the path at fault.
#[derive(Clone)]
pub struct NormalizedLockRequest {
    request: LockRequest,
}

pub trait NormalizesLockRequests: Sized {
    fn from_request(request: LockRequest) -> Result<Self, LockRejection>;
    fn duplicates_name_of(&self, lock: &Lock) -> bool;
    fn overlapping_path_of(&self, lock: &Lock) -> Option<String>;
    fn into_lock(self, lock_id: LockId) -> Lock;
}

impl NormalizesLockRequests for NormalizedLockRequest {
    fn from_request(mut request: LockRequest) -> Result<Self, LockRejection> {
        if request.lock_path_vector.is_empty() {
            return Err(LockRejection::NoPath);
        }
        let mut paths = BTreeSet::new();
        for path in &mut request.lock_path_vector {
            let normalized = NormalizedLockPath::from_source(&path.0)?.0;
            *path = LockPath(normalized.clone());
            if !paths.insert(normalized.clone()) {
                return Err(LockRejection::DuplicatePath(LockPath(normalized)));
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
                NormalizedLockPath::from_normalized(&requested.0)
                    .overlaps(&NormalizedLockPath::from_normalized(&held.0))
                    .then(|| requested.0.clone())
            })
        })
    }

    fn into_lock(self, lock_id: LockId) -> Lock {
        Lock {
            lock_id,
            lock_name: self.request.lock_name,
            flow_id: self.request.flow_id,
            lock_path_vector: self.request.lock_path_vector,
            lock_reason: self.request.lock_reason,
        }
    }
}

/// As a read plan's predicate, a request admits exactly the held Locks it
/// conflicts with: the one bearing its name, and every one holding a path that
/// overlaps a path it asks for. Lock acquisition reads only those.
impl RecordPredicate<StoredLock> for NormalizedLockRequest {
    fn admits(&self, held: &StoredLock) -> bool {
        self.duplicates_name_of(&held.lock) || self.overlapping_path_of(&held.lock).is_some()
    }
}

/// A lexically normalized absolute Unix path used during Lock acquisition.
struct NormalizedLockPath(String);

trait NormalizesLockPaths: Sized {
    fn from_source(path: &str) -> Result<Self, LockRejection>;
    fn from_normalized(path: &str) -> Self;
    fn overlaps(&self, other: &Self) -> bool;
    fn is_ancestor_of(&self, descendant: &Self) -> bool;
}

impl NormalizesLockPaths for NormalizedLockPath {
    fn from_source(path: &str) -> Result<Self, LockRejection> {
        if path.as_bytes().contains(&0) {
            return Err(LockRejection::NulCharacter(LockPath(path.to_owned())));
        }
        let parsed = Path::new(path);
        if !parsed.is_absolute() {
            return Err(LockRejection::RelativePath(LockPath(path.to_owned())));
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
                    return Err(LockRejection::ParentComponent(LockPath(path.to_owned())));
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
    use signal_orchestrate::{FlowId, LockId, LockName, LockPath, LockReason};

    fn request(paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: LockName("name".to_owned()),
            flow_id: FlowId("flow".to_owned()),
            lock_path_vector: paths
                .iter()
                .map(|path| LockPath((*path).to_owned()))
                .collect(),
            lock_reason: LockReason("reason".to_owned()),
        }
    }

    #[test]
    fn a_path_is_normalized_lexically_and_a_parent_component_is_refused() {
        let normalized = NormalizedLockRequest::from_request(request(&["/a/./b//c"]))
            .expect("normalize a lexical path");
        assert_eq!(
            normalized.into_lock(LockId(1)).lock_path_vector,
            vec![LockPath("/a/b/c".to_owned())]
        );
        for (paths, rejection) in [
            (
                &["/a/../b"][..],
                LockRejection::ParentComponent(LockPath("/a/../b".to_owned())),
            ),
            (
                &["relative"][..],
                LockRejection::RelativePath(LockPath("relative".to_owned())),
            ),
            (&[][..], LockRejection::NoPath),
            (
                &["/a/b", "/a/./b"][..],
                LockRejection::DuplicatePath(LockPath("/a/b".to_owned())),
            ),
            (
                &["/held\0phantom"][..],
                LockRejection::NulCharacter(LockPath("/held\0phantom".to_owned())),
            ),
        ] {
            assert_eq!(
                NormalizedLockRequest::from_request(request(paths)).err(),
                Some(rejection),
                "{paths:?}"
            );
        }
    }

    #[test]
    fn overlap_is_ancestry_in_either_direction_and_the_root_covers_everything() {
        let held = Lock {
            lock_id: LockId(1),
            lock_name: LockName("held".to_owned()),
            flow_id: FlowId("flow".to_owned()),
            lock_path_vector: vec![LockPath("/a/b".to_owned())],
            lock_reason: LockReason("reason".to_owned()),
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
