//! The typed refusals of the durable Lock store.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the durable store has {count} Nexus metadata rows")]
    MetadataInvariant { count: usize },
    #[error(
        "this store records that it was bound at {recorded:?} as a different file from the one opened at {opened:?}: a second copy of a store holds the socket paths of the Nexus still serving the first. Open it where it records itself; or, if this is the one store and it was moved here by means that did not carry the file itself, remove whatever remains at {recorded:?}, stop whatever is serving its sockets, and run orchestrate-relocate."
    )]
    CarriedStore { recorded: String, opened: String },
    #[error(
        "this store carries a declared move from {declared_origin:?} to {declared_destination:?}, and it records having been bound at {recorded:?} while being opened at {opened:?}: a declaration is honoured by the one move it names and by nothing else. Run orchestrate-relocate where this store now is."
    )]
    UnrelatedRelocation {
        declared_origin: String,
        declared_destination: String,
        recorded: String,
        opened: String,
    },
    #[error("the durable store has {count} relocation rows")]
    RelocationInvariant { count: usize },
    #[error(
        "this store carries the 0.34.0 situation family, which recorded where it was bound but not which file it was; a store guarded only by its path cannot tell a move from a copy. Serve it once under 0.34.0 where it lives, release its Locks, and start again from a fresh store."
    )]
    SupersededSituation,
    #[error("the durable store has {count} situation rows")]
    SituationInvariant { count: usize },
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
