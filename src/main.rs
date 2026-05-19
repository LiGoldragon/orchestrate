fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let Some(_config) = arguments.next() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "usage: persona-orchestrate-daemon <daemon-config.nota>",
        )
        .into());
    };
    if arguments.next().is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "persona-orchestrate-daemon accepts exactly one NOTA config argument",
        )
        .into());
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "persona-orchestrate daemon socket runtime is not implemented yet",
    )
    .into())
}
