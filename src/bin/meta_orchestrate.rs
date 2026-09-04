//! Datomic edge client for the privileged Orchestrate Nexus socket.

use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datomic::Textualizable;
use meta_signal_orchestrate::{Body, Frame, SIGNAL_VERSION, SignalFrameCodec};
use protos::{Actualizable, Printing, Protosizable};

#[path = "../generated/meta_client.rs"]
mod meta_client;

const ETHOS_SOURCE: &str = include_str!("../../ethos/meta_client.ethos");

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            print!("{}", meta_signal_orchestrate::ETHOS_SOURCE);
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
                meta_client::ClientFailure::Unreadable(datomic::Situated(
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

fn unreachable(path: String, error: String) -> meta_client::ClientFailure {
    meta_client::ClientFailure::Unreachable(meta_client::ClientFailureUnreachable(path, error))
}

fn exchange(arg: &str) -> Result<(), meta_client::ClientFailure> {
    let request: meta_signal_orchestrate::Request =
        protos::Potential::<meta_signal_orchestrate::Request, datomic::Datom>::from(arg.to_owned())
            .actualize()
            .map_err(|s| meta_client::ClientFailure::Unreadable(datomic::Situated(s.0, s.1)))?;
    let socket_path = env::var("ORCHESTRATE_META_SOCKET").map_err(|_| {
        unreachable(
            String::new(),
            "ORCHESTRATE_META_SOCKET is required".to_owned(),
        )
    })?;
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
        Body::Refusal(refusal) => Err(meta_client::ClientFailure::Refused(refusal)),
        _ => Err(unreachable(
            String::new(),
            "Nexus returned a non-reply frame".to_owned(),
        )),
    }
}
