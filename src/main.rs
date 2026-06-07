use nota_next::NotaSource;
use orchestrate::{DaemonConfiguration, OrchestrateDaemon};
use triad_runtime::{ComponentArgument, ComponentCommand, ExitReport};

fn main() -> std::process::ExitCode {
    ExitReport::new("orchestrate-daemon").from_result(DaemonCommand::from_environment().run())
}

struct DaemonCommand {
    command: ComponentCommand,
}

impl DaemonCommand {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let configuration_text = self.configuration_text()?;
        let configuration = NotaSource::new(&configuration_text).parse::<DaemonConfiguration>()?;
        OrchestrateDaemon::open(configuration)?.run()?;
        Ok(())
    }

    fn configuration_text(&self) -> Result<String, Box<dyn std::error::Error>> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => Ok(std::fs::read_to_string(file.into_path())?),
            ComponentArgument::SignalFile(file) => Ok(std::fs::read_to_string(file.into_path())?),
        }
    }
}
