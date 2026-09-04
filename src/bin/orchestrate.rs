//! Datomic edge client for the ordinary Orchestrate Nexus socket.

use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datomic::{Datom, Datomic, Textualizable};
use protos::{Actualizable, Separator, Situated};
use signal_orchestrate::{Body, Frame, Refusal, SIGNAL_VERSION, SignalFrameCodec};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            print!("{}", signal_orchestrate::ETHOS_SOURCE);
            print!("\n");
            print!("{}", CLIENT_FAILURE_ETHOS);
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
                ClientFailure::Unreadable(Situated(
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

fn exchange(arg: &str) -> Result<(), ClientFailure> {
    let request: signal_orchestrate::Request =
        protos::Potential::<signal_orchestrate::Request, datomic::Datom>::from(arg.to_owned())
            .actualize()
            .map_err(ClientFailure::Unreadable)?;
    let socket_path =
        env::var("ORCHESTRATE_SOCKET").map_err(|_| ClientFailure::Unreachable(
            String::new(),
            "ORCHESTRATE_SOCKET is required".to_owned(),
        ))?;
    let mut stream = UnixStream::connect(&socket_path)
        .map_err(|e| ClientFailure::Unreachable(socket_path.clone(), e.to_string()))?;
    stream
        .write_all(
            &Frame(SIGNAL_VERSION, Body::Request(request))
                .encode_length_prefixed()
                .map_err(|e| ClientFailure::Unreachable(socket_path.clone(), format!("{e:?}")))?,
        )
        .map_err(|e| ClientFailure::Unreachable(socket_path.clone(), e.to_string()))?;
    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|e| ClientFailure::Unreachable(socket_path.clone(), e.to_string()))?;
    let mut bytes = prefix.to_vec();
    bytes.resize(4 + u32::from_le_bytes(prefix) as usize, 0);
    stream
        .read_exact(&mut bytes[4..])
        .map_err(|e| ClientFailure::Unreachable(socket_path.clone(), e.to_string()))?;
    let frame = Frame::decode_length_prefixed(&bytes)
        .map_err(|e| ClientFailure::Unreachable(socket_path, format!("{e:?}")))?;
    match frame.1 {
        Body::Reply(reply) => {
            println!("{}", reply.textualize());
            Ok(())
        }
        Body::Refusal(refusal) => Err(ClientFailure::Refused(refusal)),
        _ => Err(ClientFailure::Unreachable(
            String::new(),
            "Nexus returned a non-reply frame".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// ClientFailure: the typed outcome of every client fault
// ---------------------------------------------------------------------------

/// ; Orchestrate CLI client failure vocabulary.
/// Library.{ 0 27 0 }
/// [ protos:[ Integer Extent ]
///   datomic:[ Fault ] ]
/// [ Situated.{ Option<Extent> Fault }
///   ClientFailure.[ Unreadable.Situated  Unreachable.{ Text Text }  Refused.Refusal ] ]
/// []
/// []
const CLIENT_FAILURE_ETHOS: &str = "\
; Orchestrate CLI client failure vocabulary.
; Library.{ 0 27 0 }
; [ protos:[ Integer Extent ]
;   datomic:[ Fault ] ]
; [ Situated.{ Option<Extent> Fault }
;   ClientFailure.[ Unreadable.Situated  Unreachable.{ Text Text }  Refused.Refusal ] ]
; []
; []
";

enum ClientFailure {
    /// The argument could not be actualized as a request.
    Unreadable(Situated<datomic::Fault>),
    /// The socket is unreachable: (path, OS error text).
    Unreachable(String, String),
    /// The Nexus sent a wire-level refusal.
    Refused(Refusal),
}

impl protos::Corporal<Datom> for ClientFailure {
    type Fault = datomic::Fault;
    fn incorporate(_datom: Datom) -> Result<Self, datomic::Fault> {
        Err(datomic::Fault::Corporal(
            vec![],
            datomic::Problem::Value("ClientFailure is output-only".to_owned()),
        ))
    }
}

impl Datomic for ClientFailure {
    fn datomize(&self) -> Datom {
        match self {
            ClientFailure::Unreadable(situated) => Datom::Variant(
                "Unreadable".to_owned(),
                Separator::Period,
                Some(Box::new(datomize_situated(situated))),
            ),
            ClientFailure::Unreachable(path, error) => Datom::Variant(
                "Unreachable".to_owned(),
                Separator::Period,
                Some(Box::new(Datom::Struct(vec![
                    path.datomize(),
                    error.datomize(),
                ]))),
            ),
            ClientFailure::Refused(refusal) => Datom::Variant(
                "Refused".to_owned(),
                Separator::Period,
                Some(Box::new(refusal.datomize())),
            ),
        }
    }
}

fn datomize_situated(s: &Situated<datomic::Fault>) -> Datom {
    Datom::Struct(vec![datomize_option_extent(&s.0), s.1.datomize()])
}

fn datomize_option_extent(opt: &Option<protos::Extent>) -> Datom {
    match opt {
        None => Datom::Bare("None".to_owned()),
        Some(e) => Datom::Variant(
            "Some".to_owned(),
            Separator::Period,
            Some(Box::new(Datom::Struct(vec![
                e.0.datomize(),
                e.1.datomize(),
            ]))),
        ),
    }
}
