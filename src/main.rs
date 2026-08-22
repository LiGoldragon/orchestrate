use orchestrate::{
    ConfigurationError, DaemonConfiguration, OrchestrateDaemon, OrchestrateDaemonError,
};
use thiserror::Error;
use triad_runtime::{AsyncMultiListenerDaemonError, ExitReport};

fn main() -> std::process::ExitCode {
    ExitReport::new("orchestrate-daemon").from_result(DaemonStartup::run())
}

struct DaemonStartup;

impl DaemonStartup {
    fn run() -> Result<(), StartupError> {
        let configuration = DaemonConfiguration::from_process_arguments()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(StartupError::Runtime)?;
        let daemon = OrchestrateDaemon::new(configuration)?;
        runtime.block_on(daemon.run_async())?;
        Ok(())
    }
}

#[derive(Debug, Error)]
enum StartupError {
    #[error("invalid typed daemon startup arguments: {0}")]
    Configuration(#[from] ConfigurationError),

    #[error("runtime initialization failed: {0}")]
    Runtime(std::io::Error),

    #[error("orchestrate engine startup failed: {0}")]
    Engine(#[from] OrchestrateDaemonError),

    #[error("daemon failed: {0}")]
    Daemon(#[from] AsyncMultiListenerDaemonError<OrchestrateDaemonError>),
}
