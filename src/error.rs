use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("signal-persona-orchestrate: {0}")]
    SignalPersonaOrchestrate(#[from] signal_persona_orchestrate::Error),

    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("orchestrate service sequence lock was poisoned")]
    ServiceSequencePoisoned,

    #[error("path is not valid UTF-8")]
    PathIsNotUtf8,
}
