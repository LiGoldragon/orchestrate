use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use meta_signal_orchestrate::{
    Configure, Frame as MetaFrame, MetaOrchestrateRequest, MetaSocketPath, OrdinarySocketPath,
    StorePath,
};
use signal_frame::{
    ClientFrame, ExchangeIdentifier, ExchangeLane, LaneSequence, RequestPayload, SessionEpoch,
};

fn configure(
    store: &std::path::Path,
    ordinary: &std::path::Path,
    meta: &std::path::Path,
) -> Configure {
    Configure {
        store_path: StorePath(store.display().to_string()),
        ordinary_socket_path: OrdinarySocketPath(ordinary.display().to_string()),
        meta_socket_path: MetaSocketPath(meta.display().to_string()),
    }
}

fn startup_argument(configure: Configure) -> String {
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    );
    let frame = MetaFrame::request_frame(
        exchange,
        MetaOrchestrateRequest::Configure(configure).into_request(),
    )
    .expect("make startup Configure Signal frame");
    URL_SAFE_NO_PAD.encode(
        frame
            .encode_client_frame()
            .expect("encode startup Signal frame"),
    )
}

fn invoke(binary: &str, socket_variable: &str, socket: &std::path::Path, request: &str) -> String {
    let output = Command::new(binary)
        .env(socket_variable, socket)
        .arg(request)
        .output()
        .expect("run client");
    assert!(
        output.status.success(),
        "{binary} rejected {request:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply = String::from_utf8(output.stdout)
        .expect("client reply is utf-8 Datom")
        .trim()
        .to_owned();
    eprintln!("{binary} {request} -> {reply}");
    reply
}

#[test]
fn orchestrate_nexus_reserves_releases_and_configures_over_separate_signal_sockets() {
    let temporary = tempfile::tempdir().expect("temporary Nexus directory");
    let store = temporary.path().join("orchestrate.sema");
    let ordinary_socket = temporary.path().join("ordinary.sock");
    let meta_socket = temporary.path().join("meta.sock");
    let configuration = configure(&store, &ordinary_socket, &meta_socket);

    let mut nexus = Command::new(env!("CARGO_BIN_EXE_orchestrate-nexus"))
        .arg(startup_argument(configuration.clone()))
        .stdout(Stdio::piped())
        .spawn()
        .expect("start configured Orchestrate Nexus");
    let stdout = nexus.stdout.take().expect("Orchestrate Nexus stdout");
    let mut stdout = BufReader::new(stdout);
    let mut ready = String::new();
    stdout
        .read_line(&mut ready)
        .expect("wait for Orchestrate Nexus ready event");
    assert_eq!(ready, "orchestrate-nexus ready\n");

    let first_path = temporary.path().join("first");
    let overlap_path = first_path.join("nested");
    let first_request = format!(
        "PathLock.{{alpha [{}] (first reservation)}}",
        first_path.display()
    );
    assert_eq!(
        invoke(
            env!("CARGO_BIN_EXE_orchestrate"),
            "ORCHESTRATE_SOCKET",
            &ordinary_socket,
            &first_request,
        ),
        first_request.replacen("PathLock.", "PathLockRegistered.", 1)
    );

    let duplicate = format!(
        "PathLock.{{alpha [{}] (duplicate reservation)}}",
        temporary.path().join("elsewhere").display()
    );
    assert!(
        invoke(
            env!("CARGO_BIN_EXE_orchestrate"),
            "ORCHESTRATE_SOCKET",
            &ordinary_socket,
            &duplicate,
        )
        .contains("DuplicateActiveName"),
        "duplicate active name must receive its closed refusal"
    );

    let overlap = format!(
        "PathLock.{{beta [{}] (overlapping reservation)}}",
        overlap_path.display()
    );
    assert!(
        invoke(
            env!("CARGO_BIN_EXE_orchestrate"),
            "ORCHESTRATE_SOCKET",
            &ordinary_socket,
            &overlap,
        )
        .contains("PathOverlap"),
        "nested absolute path must receive its closed refusal"
    );

    assert_eq!(
        invoke(
            env!("CARGO_BIN_EXE_orchestrate"),
            "ORCHESTRATE_SOCKET",
            &ordinary_socket,
            "PathLockRelease.{alpha}",
        ),
        "PathLockReleased.{alpha}"
    );
    assert_eq!(
        invoke(
            env!("CARGO_BIN_EXE_orchestrate"),
            "ORCHESTRATE_SOCKET",
            &ordinary_socket,
            &first_request,
        ),
        first_request.replacen("PathLock.", "PathLockRegistered.", 1)
    );

    assert_eq!(
        invoke(
            env!("CARGO_BIN_EXE_meta-orchestrate"),
            "ORCHESTRATE_META_SOCKET",
            &meta_socket,
            &format!(
                "Configure.{{{} {} {}}}",
                store.display(),
                ordinary_socket.display(),
                meta_socket.display()
            ),
        ),
        format!(
            "Configured.{{{} {} {}}}",
            store.display(),
            ordinary_socket.display(),
            meta_socket.display()
        )
    );

    nexus.kill().expect("stop Orchestrate Nexus");
    nexus.wait().expect("reap Orchestrate Nexus");
}
