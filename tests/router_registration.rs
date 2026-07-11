//! THE ROUTER-REGISTRATION CLIENT WITNESS: orchestrate's registration seam emits
//! the exact `signal-router` `RegisterActor` frame the router daemon serves, and
//! interprets the router's typed reply. A fixture Unix socket stands in for the
//! router working socket: it decodes the frame orchestrate wrote, asserts the
//! actor it carries, and answers the typed reply the client maps back.

use std::io::Write;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use orchestrate::{
    RouterActorRegistration, RouterRegistrationDegradation, StoredAgentEndpointKind,
    StoredAgentReachability,
};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_orchestrate::OrchestratorAgentIdentifier;
use signal_router::{
    ActorIdentifier, ActorRegistered, ActorRegistrationDisposition, ActorRegistrationRefusalReason,
    ActorRegistrationRefused, EndpointKind, Frame, FrameBody, Input, Output,
};
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos()
}

fn socket_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orchestrate-router-registration-{name}-{}-{}.sock",
        std::process::id(),
        nanos()
    ))
}

fn agent_identifier() -> OrchestratorAgentIdentifier {
    OrchestratorAgentIdentifier::from_wire_token("ag4k").expect("valid minted-shape identifier")
}

fn terminal_cell_reachability(target: &str) -> StoredAgentReachability {
    StoredAgentReachability {
        endpoint_kind: StoredAgentEndpointKind::TerminalCell,
        target: target.to_string(),
        harness_pid: 4242,
        harness_start_time: 99,
    }
}

/// A fixture router working socket: accepts one connection, decodes the
/// `RegisterActor` request orchestrate wrote, and answers `reply`. The decoded
/// request `Input` is sent back to the test over a channel.
fn serve_one_registration(
    listener: UnixListener,
    reply: Output,
    captured: mpsc::Sender<Input>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("fixture accepts a connection");
        let codec = LengthPrefixedCodec::default();
        let body = codec.read_body(&mut stream).expect("fixture reads request");
        let frame = Frame::decode(&body.into_bytes()).expect("fixture decodes router frame");
        let FrameBody::Request { exchange, request } = frame.into_body() else {
            panic!("fixture expected a request frame");
        };
        let (input, _tail) = request.payloads.into_head_and_tail();
        captured
            .send(input)
            .expect("fixture reports captured input");
        let reply_frame = Frame::new(FrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        codec
            .write_body(
                &mut stream,
                &LengthPrefixedFrameBody::new(reply_frame.encode().expect("encode reply frame")),
            )
            .expect("fixture writes reply");
        stream.flush().expect("fixture flushes reply");
    })
}

#[test]
fn registration_emits_register_actor_frame_and_maps_accepted_reply() {
    let path = socket_path("accepted");
    let listener = UnixListener::bind(&path).expect("fixture socket binds");
    let (sender, receiver) = mpsc::channel();
    let server = serve_one_registration(
        listener,
        Output::actor_registered(ActorRegistered::new(
            ActorIdentifier::new("ag4k"),
            ActorRegistrationDisposition::Registered,
        )),
        sender,
    );

    let reachability = terminal_cell_reachability("/run/persona/X/session-a/data.sock");
    let disposition = RouterActorRegistration::new(path.clone())
        .register(&agent_identifier(), &reachability)
        .expect("router accepts the registration");
    assert_eq!(disposition, ActorRegistrationDisposition::Registered);

    let Input::RegisterActor(actor) = receiver.recv().expect("fixture captured the request") else {
        panic!("orchestrate emitted a non-RegisterActor router frame");
    };
    assert_eq!(
        actor.name.payload().payload().as_str(),
        "ag4k",
        "the router actor name is the minted orchestrate identity"
    );
    assert_eq!(
        *actor.process.payload(),
        4242,
        "the discovered harness pid rides as the actor process"
    );
    let endpoint = actor.endpoint().expect("registration carries an endpoint");
    assert_eq!(
        *endpoint.kind.payload(),
        EndpointKind::PtySocket,
        "a terminal-cell reachability maps to the router PtySocket endpoint kind"
    );
    assert_eq!(
        endpoint.target.payload().as_str(),
        "/run/persona/X/session-a/data.sock",
        "the discovered target rides as the endpoint target"
    );
    assert_eq!(endpoint.auxiliary(), None);

    server.join().expect("fixture thread joins");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn registration_maps_router_refusal_to_rejected_degradation() {
    let path = socket_path("refused");
    let listener = UnixListener::bind(&path).expect("fixture socket binds");
    let (sender, _receiver) = mpsc::channel();
    let server = serve_one_registration(
        listener,
        Output::actor_registration_refused(ActorRegistrationRefused::new(
            ActorIdentifier::new("ag4k"),
            ActorRegistrationRefusalReason::RemoteRouterEndpointNotLocal,
        )),
        sender,
    );

    let reachability = terminal_cell_reachability("/run/persona/X/session-a/data.sock");
    let degradation = RouterActorRegistration::new(path.clone())
        .register(&agent_identifier(), &reachability)
        .expect_err("a router refusal is a degradation");
    assert_eq!(
        degradation,
        RouterRegistrationDegradation::Rejected(
            ActorRegistrationRefusalReason::RemoteRouterEndpointNotLocal
        ),
    );

    server.join().expect("fixture thread joins");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn registration_maps_absent_router_socket_to_unreachable_degradation() {
    // No listener bound at this path: the connect fails, and an unreachable
    // router is a non-fatal degradation, never a panic or an error return that
    // would fail the agent's own registration.
    let path = socket_path("absent");
    let reachability = terminal_cell_reachability("/run/persona/X/session-a/data.sock");
    let degradation = RouterActorRegistration::new(path)
        .register(&agent_identifier(), &reachability)
        .expect_err("an absent router socket is a degradation");
    assert!(
        matches!(degradation, RouterRegistrationDegradation::Unreachable(_)),
        "an unreachable router socket maps to Unreachable, got {degradation:?}"
    );
}
