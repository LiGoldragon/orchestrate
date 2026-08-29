//! Datomic edge client for the privileged Orchestrate Nexus socket.

use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datomic::{Datomic, Text, TextEdge};
use meta_signal_orchestrate::{Frame, FrameBody, SignalFrameCodec};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("meta-orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let request = Text::<meta_signal_orchestrate::Request>::from(single_argument()?)
        .embody()
        .map_err(|error| format!("Datomic request: {error:?}"))?;
    let mut stream = UnixStream::connect(
        env::var("ORCHESTRATE_META_SOCKET")
            .map_err(|_| "ORCHESTRATE_META_SOCKET is required".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    stream
        .write_all(
            &frame(FrameBody::Request(request))
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
        .body
    {
        FrameBody::Reply(reply) => println!("{}", reply.textualize().as_ref()),
        FrameBody::Refusal(refusal) => println!("{}", refusal.textualize().as_ref()),
        _ => return Err("Nexus returned a non-reply frame".to_owned()),
    }
    Ok(())
}

fn frame(body: FrameBody) -> Frame {
    Frame {
        channel_contract_id: meta_signal_orchestrate::CHANNEL_CONTRACT_ID,
        channel_wire_revision: meta_signal_orchestrate::CHANNEL_WIRE_REVISION,
        protocol_version: meta_signal_orchestrate::PROTOCOL_VERSION,
        body,
    }
}

fn single_argument() -> Result<String, String> {
    single_argument_from(&env::args().skip(1).collect::<Vec<_>>())
}

fn single_argument_from(values: &[String]) -> Result<String, String> {
    match values {
        [value] if !value.starts_with('-') => Ok(value.clone()),
        _ => Err("accepts exactly one Datomic object and no flags".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_exactly_one_non_flag_datom() {
        assert!(single_argument_from(&["Configure.{a b}".to_owned()]).is_ok());
        assert!(single_argument_from(&[]).is_err());
        assert!(single_argument_from(&["--help".to_owned()]).is_err());
        assert!(single_argument_from(&["Configure.{a b}".to_owned(), "extra".to_owned()]).is_err());
    }
}
