//! Datomic edge client for the ordinary Orchestrate Nexus socket.

use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datomic::Textualizable;
use protos::Actualizable;
use signal_orchestrate::{Body, Frame, SIGNAL_VERSION, SignalFrameCodec};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            print!("{}", signal_orchestrate::ETHOS_SOURCE);
            Ok(())
        }
        [arg] if !arg.starts_with('-') => {
            let request: signal_orchestrate::Request =
                protos::Potential::<signal_orchestrate::Request, datomic::Datom>::from(arg.clone())
                    .actualize()
                    .map_err(|error| format!("{error:?}"))?;
            let socket_path = env::var("ORCHESTRATE_SOCKET")
                .map_err(|_| "ORCHESTRATE_SOCKET is required".to_owned())?;
            let mut stream = UnixStream::connect(socket_path)
                .map_err(|error| error.to_string())?;
            stream
                .write_all(
                    &Frame(SIGNAL_VERSION, Body::Request(request))
                        .encode_length_prefixed()
                        .map_err(|error| format!("{error:?}"))?,
                )
                .map_err(|error| error.to_string())?;
            let mut prefix = [0; 4];
            stream
                .read_exact(&mut prefix)
                .map_err(|error| error.to_string())?;
            let mut bytes = prefix.to_vec();
            bytes.resize(4 + u32::from_le_bytes(prefix) as usize, 0);
            stream
                .read_exact(&mut bytes[4..])
                .map_err(|error| error.to_string())?;
            match Frame::decode_length_prefixed(&bytes)
                .map_err(|error| format!("{error:?}"))?
                .1
            {
                Body::Reply(reply) => println!("{}", Textualizable::textualize(&reply)),
                Body::Refusal(refusal) => return Err(format!("{refusal:?}")),
                _ => return Err("Nexus returned a non-reply frame".to_owned()),
            }
            Ok(())
        }
        _ => Err("accepts exactly one Datomic object and no flags".to_owned()),
    }
}
