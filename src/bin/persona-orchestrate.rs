use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use owner_signal_persona_orchestrate::{
    Frame as OwnerOrchestrateFrame, FrameBody as OwnerOrchestrateFrameBody, OwnerOrchestrateReply,
    OwnerOrchestrateRequest,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use signal_persona_orchestrate::{
    OrchestrateFrame, OrchestrateFrameBody, OrchestrateReply, OrchestrateRequest,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request_text = single_request_argument()?;
    if let Ok(request) = decode_nota::<OrchestrateRequest>(&request_text) {
        let reply = send_ordinary_request(request)?;
        println!("{}", encode_nota(&reply)?);
        return Ok(());
    }
    if let Ok(request) = decode_nota::<OwnerOrchestrateRequest>(&request_text) {
        let reply = send_owner_request(request)?;
        println!("{}", encode_nota(&reply)?);
        return Ok(());
    }

    Err(invalid_input(
        "argument must be one NOTA request from signal-persona-orchestrate or owner-signal-persona-orchestrate",
    ))
}

fn single_request_argument() -> Result<String, Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 1 {
        return Err(invalid_input(
            "persona-orchestrate accepts exactly one NOTA request argument",
        ));
    }
    let argument = arguments.remove(0);
    let path = Path::new(&argument);
    if path.is_file() {
        Ok(std::fs::read_to_string(path)?)
    } else {
        Ok(argument)
    }
}

fn decode_nota<Value>(text: &str) -> nota_codec::Result<Value>
where
    Value: NotaDecode,
{
    let mut decoder = Decoder::new(text);
    Value::decode(&mut decoder)
}

fn encode_nota<Value>(value: &Value) -> nota_codec::Result<String>
where
    Value: NotaEncode,
{
    let mut encoder = Encoder::new();
    value.encode(&mut encoder)?;
    Ok(encoder.into_string())
}

fn send_ordinary_request(
    request: OrchestrateRequest,
) -> Result<OrchestrateReply, Box<dyn std::error::Error>> {
    let frame = OrchestrateFrame::new(OrchestrateFrameBody::Request {
        exchange: exchange(),
        request: request.into_request(),
    });
    let bytes = exchange_bytes(ordinary_socket_path(), frame.encode_length_prefixed()?)?;
    let decoded = OrchestrateFrame::decode_length_prefixed(&bytes)?;
    match decoded.into_body() {
        OrchestrateFrameBody::Reply { reply, .. } => reply_payload(reply),
        _ => Err(invalid_input(
            "daemon did not reply with an ordinary reply frame",
        )),
    }
}

fn send_owner_request(
    request: OwnerOrchestrateRequest,
) -> Result<OwnerOrchestrateReply, Box<dyn std::error::Error>> {
    let frame = OwnerOrchestrateFrame::new(OwnerOrchestrateFrameBody::Request {
        exchange: exchange(),
        request: request.into_request(),
    });
    let bytes = exchange_bytes(owner_socket_path(), frame.encode_length_prefixed()?)?;
    let decoded = OwnerOrchestrateFrame::decode_length_prefixed(&bytes)?;
    match decoded.into_body() {
        OwnerOrchestrateFrameBody::Reply { reply, .. } => reply_payload(reply),
        _ => Err(invalid_input(
            "daemon did not reply with an owner reply frame",
        )),
    }
}

fn reply_payload<Payload>(reply: Reply<Payload>) -> Result<Payload, Box<dyn std::error::Error>>
where
    Payload: std::fmt::Debug,
{
    match reply {
        Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(payload) => Ok(payload),
            other => Err(invalid_input(format!(
                "daemon returned non-OK subreply: {other:?}"
            ))),
        },
        Reply::Rejected { reason } => {
            Err(invalid_input(format!("daemon rejected request: {reason}")))
        }
    }
}

fn exchange_bytes(
    socket_path: PathBuf,
    request_bytes: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut stream = UnixStream::connect(socket_path)?;
    stream.write_all(&request_bytes)?;
    stream.flush()?;

    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix)?;
    let payload_length = u32::from_be_bytes(prefix) as usize;
    let mut payload = vec![0_u8; payload_length];
    stream.read_exact(&mut payload)?;

    let mut response = Vec::with_capacity(4 + payload_length);
    response.extend_from_slice(&prefix);
    response.extend_from_slice(&payload);
    Ok(response)
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn ordinary_socket_path() -> PathBuf {
    std::env::var_os("PERSONA_ORCHESTRATE_ORDINARY_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| runtime_directory().join("persona-orchestrate/ordinary.sock"))
}

fn owner_socket_path() -> PathBuf {
    std::env::var_os("PERSONA_ORCHESTRATE_OWNER_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| runtime_directory().join("persona-orchestrate/owner.sock"))
}

fn runtime_directory() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}
