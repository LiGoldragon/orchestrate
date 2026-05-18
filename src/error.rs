use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("orchestrate service sequence lock was poisoned")]
    ServiceSequencePoisoned,
}
