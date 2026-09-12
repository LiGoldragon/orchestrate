//! The typed refusals of the durable Lock store.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the durable store has {count} Nexus metadata rows")]
    MetadataInvariant { count: usize },
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
