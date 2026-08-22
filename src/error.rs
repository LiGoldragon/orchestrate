use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("native Datom: {0:?}")]
    Datom(datom::DatomFault),

    #[error("sema storage kernel: {0}")]
    SemaStorageKernel(#[from] sema_engine::StorageKernelError),

    #[error("sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("the injected atomic storage failure prevented registration")]
    InjectedAtomicCommitFailure,
}

impl From<datom::DatomFault> for Error {
    fn from(error: datom::DatomFault) -> Self {
        Self::Datom(error)
    }
}
