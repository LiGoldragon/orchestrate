use orchestrate::schema::daemon::{DaemonBinder, DaemonError};
use orchestrate::{ConfigurationError, DaemonConfiguration, OrchestrateDaemon};
use thiserror::Error;
use triad_runtime::ExitReport;

fn main() -> std::process::ExitCode {
    ExitReport::new("orchestrate-daemon").from_result(run())
}

fn run() -> Result<(), StartupError> {
    let configuration = DaemonConfiguration::from_process_arguments()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    runtime.block_on(async {
        <OrchestrateDaemon as DaemonBinder>::bind(configuration)?
            .run()
            .await
            .map_err(DaemonError::from)
    })?;
    Ok(())
}

#[derive(Debug, Error)]
enum StartupError {
    #[error("invalid typed daemon startup arguments: {0}")]
    Configuration(#[from] ConfigurationError),

    #[error("daemon startup failed: {0}")]
    Daemon(#[from] DaemonError<OrchestrateDaemon>),
}
