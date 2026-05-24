use nota_codec::{Decoder, NotaDecode};
use orchestrate::{DaemonConfiguration, OrchestrateDaemon};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration_text = single_configuration_argument()?;
    let mut decoder = Decoder::new(&configuration_text);
    let configuration = DaemonConfiguration::decode(&mut decoder)?;
    OrchestrateDaemon::open(configuration)?.run()?;
    Ok(())
}

fn single_configuration_argument() -> Result<String, Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "orchestrate-daemon accepts exactly one NOTA config argument",
        )
        .into());
    }
    let argument = arguments.remove(0);
    let path = std::path::Path::new(&argument);
    if path.is_file() {
        Ok(std::fs::read_to_string(path)?)
    } else {
        Ok(argument)
    }
}
