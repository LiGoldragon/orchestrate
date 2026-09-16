//! Process-level proof for the bounded single-string MCP component handler.

use std::{os::unix::net::UnixListener, path::PathBuf};

use serde_json::{Value, json};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{Observation, ObserveSelection, Query, Response};

#[allow(dead_code)]
#[path = "../src/mcp_component.rs"]
mod mcp_component;
#[allow(dead_code)]
#[path = "../src/mcp_server.rs"]
mod mcp_server;

use mcp_component::{Component, ComponentError, ComponentResult, handle_component_at_socket};
use mcp_server::{McpServer, ServingMcp};

struct OrchestrateHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesOrchestrateHarness: Sized {
    fn create() -> Self;
    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError>;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
}

impl CreatesOrchestrateHarness for OrchestrateHarness {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("temporary Orchestrate socket");
        let socket = directory.path().join("orchestrate.sock");
        Self {
            _directory: directory,
            socket,
        }
    }

    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError> {
        // The handler delegates to the real CLI, which reads this boundary.
        handle_component_at_socket(
            PathBuf::from(env!("CARGO_BIN_EXE_orchestrate")),
            self.socket.clone(),
            arguments,
        )
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept client");
            let capacity = FrameCapacity::default();
            let request =
                Signal::<Query>::from(Vec::from(stream.read_frame(capacity).expect("read Signal")))
                    .restore()
                    .expect("restore typed query");
            stream
                .write_frame(&response.signalize().expect("archive response"), capacity)
                .expect("write Signal reply");
            request
        })
    }
}

#[test]
fn one_string_orchestrate_component_uses_the_cli_signal_socket_and_typed_response() {
    let fixture = OrchestrateHarness::create();
    let server = fixture.answer_once(Response::Observed(Observation::Locks(Vec::new())));
    let result = fixture.handle(&["Orchestrate".to_owned()]);
    assert_eq!(
        result,
        Ok(ComponentResult::Orchestrate("Observed.Locks.[]".to_owned()))
    );
    assert_eq!(
        server.join().expect("fixture server"),
        Query::Observe(ObserveSelection::Locks)
    );
}

#[test]
fn malformed_and_unsupported_component_arguments_are_explicit() {
    let fixture = OrchestrateHarness::create();
    assert_eq!(fixture.handle(&[]), Err(ComponentError::Arguments));
    assert_eq!(
        fixture.handle(&["Orchestrate".to_owned(), "Message".to_owned()]),
        Err(ComponentError::Arguments)
    );
    assert_eq!(
        fixture.handle(&["Unknown".to_owned()]),
        Err(ComponentError::Unknown("Unknown".to_owned()))
    );
    for component in [
        Component::Message,
        Component::Persona,
        Component::Psyche,
        Component::Flow,
    ] {
        assert_eq!(
            fixture.handle(&[format!("{component:?}")]),
            Ok(ComponentResult::Unavailable(component))
        );
    }
}

#[test]
fn mcp_tools_list_and_call_validate_one_string_and_use_the_orchestrate_socket_fixture() {
    let fixture = OrchestrateHarness::create();
    let handler = McpServer::new(PathBuf::from(env!("CARGO_BIN_EXE_orchestrate")))
        .at_socket(fixture.socket.clone());
    let listed = handler
        .handle_request(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
        .expect("tools/list response");
    assert_eq!(
        listed["result"]["tools"][0]["name"],
        "orchestrate_component"
    );
    assert_eq!(
        listed["result"]["tools"][0]["inputSchema"]["required"],
        json!(["component"])
    );

    let server = fixture.answer_once(Response::Observed(Observation::Locks(Vec::new())));
    let called = handler
        .handle_request(json!({
            "jsonrpc": "2.0", "id": "call", "method": "tools/call",
            "params": { "name": "orchestrate_component", "arguments": { "component": "Orchestrate" } }
        }))
        .expect("tools/call response");
    assert_eq!(called["result"]["content"][0]["text"], "Observed.Locks.[]");
    assert_eq!(called["result"]["isError"], false);
    assert_eq!(
        server.join().expect("fixture server"),
        Query::Observe(ObserveSelection::Locks)
    );

    for arguments in [
        Value::Null,
        json!({}),
        json!({ "component": 7 }),
        json!({ "component": "Orchestrate", "extra": true }),
    ] {
        let response = handler
            .handle_request(json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": { "name": "orchestrate_component", "arguments": arguments }
            }))
            .expect("invalid argument response");
        assert_eq!(response["result"]["isError"], true);
    }
    let unavailable = handler
        .handle_request(json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "orchestrate_component", "arguments": { "component": "Message" } }
        }))
        .expect("unavailable response");
    assert_eq!(unavailable["result"]["isError"], true);
    assert_eq!(
        unavailable["result"]["content"][0]["text"],
        "McpComponent.Unavailable.Message"
    );
}

#[test]
fn mcp_stdio_fixture_returns_a_json_rpc_parse_error_for_malformed_input() {
    let handler = McpServer::new(PathBuf::from(env!("CARGO_BIN_EXE_orchestrate")));
    let mut output = Vec::new();
    handler.serve("{ not-json }\n".as_bytes(), &mut output);
    assert_eq!(
        serde_json::from_slice::<Value>(&output).expect("JSON-RPC error response"),
        json!({
            "jsonrpc": "2.0", "id": null,
            "error": { "code": -32700, "message": "Parse error" }
        })
    );
}
