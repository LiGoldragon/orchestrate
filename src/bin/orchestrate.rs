//! Datomic edge client for the ordinary Orchestrate Nexus socket.

use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datomic::Textualizable;
use protos::{Actualizable, Printing, Protosizable};
use signal_orchestrate::{Body, Frame, SIGNAL_VERSION, SignalFrameCodec};

#[path = "../generated/client.rs"]
mod client;

const ETHOS_SOURCE: &str = include_str!("../../ethos/client.ethos");

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            print!("{}", signal_orchestrate::ETHOS_SOURCE);
            println!();
            use ethos_zero::{Actualizing, Potential};
            match Potential::from(ETHOS_SOURCE).actualize() {
                Ok(concept) => {
                    print!("{}", concept.protosize().print());
                }
                Err(_) => {
                    print!("{}", ETHOS_SOURCE);
                }
            }
            ExitCode::SUCCESS
        }
        [arg] if !arg.starts_with('-') => match exchange(arg) {
            Ok(()) => ExitCode::SUCCESS,
            Err(failure) => {
                eprintln!("{}", failure.textualize());
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!(
                "{}",
                client::ClientFailure::Unreadable(client::Situated(
                    None,
                    datomic::Fault::Corporal(
                        vec![],
                        datomic::Problem::Value("accepts one datom value and no flags".to_owned()),
                    ),
                ))
                .textualize()
            );
            ExitCode::FAILURE
        }
    }
}

fn unreachable(path: String, error: String) -> client::ClientFailure {
    client::ClientFailure::Unreachable(client::ClientFailureUnreachable(path, error))
}

fn exchange(arg: &str) -> Result<(), client::ClientFailure> {
    let request: signal_orchestrate::Request =
        protos::Potential::<signal_orchestrate::Request, datomic::Datom>::from(arg.to_owned())
            .actualize()
            .map_err(|s| client::ClientFailure::Unreadable(client::Situated(s.0, s.1)))?;
    let socket_path = env::var("ORCHESTRATE_SOCKET")
        .map_err(|_| unreachable(String::new(), "ORCHESTRATE_SOCKET is required".to_owned()))?;
    let mut stream = UnixStream::connect(&socket_path)
        .map_err(|e| unreachable(socket_path.clone(), e.to_string()))?;
    stream
        .write_all(
            &Frame(SIGNAL_VERSION, Body::Request(request))
                .encode_length_prefixed()
                .map_err(|e| unreachable(socket_path.clone(), format!("{e:?}")))?,
        )
        .map_err(|e| unreachable(socket_path.clone(), e.to_string()))?;
    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|e| unreachable(socket_path.clone(), e.to_string()))?;
    let mut bytes = prefix.to_vec();
    bytes.resize(4 + u32::from_le_bytes(prefix) as usize, 0);
    stream
        .read_exact(&mut bytes[4..])
        .map_err(|e| unreachable(socket_path.clone(), e.to_string()))?;
    let frame = Frame::decode_length_prefixed(&bytes)
        .map_err(|e| unreachable(socket_path, format!("{e:?}")))?;
    match frame.1 {
        Body::Reply(reply) => {
            println!("{}", reply.textualize());
            Ok(())
        }
        Body::Refusal(refusal) => Err(client::ClientFailure::Refused(refusal)),
        _ => Err(unreachable(
            String::new(),
            "Nexus returned a non-reply frame".to_owned(),
        )),
    }
}
