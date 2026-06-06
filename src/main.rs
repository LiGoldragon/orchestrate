use nota_codec::{Decoder, NotaDecode};
use orchestrate::{DaemonConfiguration, OrchestrateDaemon};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration_text = DaemonCommand::from_environment().configuration_text()?;
    let mut decoder = Decoder::new(&configuration_text);
    let configuration = DaemonConfiguration::decode(&mut decoder)?;
    OrchestrateDaemon::open(configuration)?.run()?;
    Ok(())
}

struct DaemonCommand {
    arguments: Vec<String>,
}

impl DaemonCommand {
    fn from_environment() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    fn configuration_text(mut self) -> Result<String, Box<dyn std::error::Error>> {
        if self.arguments.len() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "orchestrate-daemon accepts exactly one NOTA config argument",
            )
            .into());
        }
        let argument = self.arguments.remove(0);
        let path = std::path::Path::new(&argument);
        if path.is_file() {
            Ok(std::fs::read_to_string(path)?)
        } else {
            Ok(argument)
        }
    }
}
