//! Minimal MCP stdio tools server for the bounded component selector.

#[cfg(test)]
use crate::mcp_component::handle_component_at_socket;
use crate::mcp_component::{ComponentError, ComponentResult, handle_component};
use serde_json::{Value, json};
use std::{
    io::{BufRead, Write},
    path::PathBuf,
};

pub struct McpServer {
    orchestrate: PathBuf,
    #[cfg(test)]
    socket_path: Option<PathBuf>,
}

pub trait ServingMcp {
    fn serve<R: BufRead, W: Write>(&self, input: R, output: W);
    fn handle_request(&self, request: Value) -> Option<Value>;
}

impl McpServer {
    pub fn new(orchestrate: PathBuf) -> Self {
        Self {
            orchestrate,
            #[cfg(test)]
            socket_path: None,
        }
    }

    #[cfg(test)]
    pub fn at_socket(mut self, socket_path: PathBuf) -> Self {
        self.socket_path = Some(socket_path);
        self
    }
}

impl ServingMcp for McpServer {
    fn serve<R: BufRead, W: Write>(&self, input: R, mut output: W) {
        for line in input.lines() {
            let response = match line {
                Ok(line) => serde_json::from_str(&line).map_or_else(
                    |_| Some(protocol_error(Value::Null, -32700, "Parse error")),
                    |request| self.handle_request(request),
                ),
                Err(_) => break,
            };
            if let Some(response) = response {
                writeln!(output, "{response}").expect("write MCP response");
                output.flush().expect("flush MCP response");
            }
        }
    }

    fn handle_request(&self, request: Value) -> Option<Value> {
        let Some(object) = request.as_object() else {
            return Some(protocol_error(Value::Null, -32600, "Invalid Request"));
        };
        if object.get("jsonrpc") != Some(&Value::String("2.0".to_owned())) {
            return Some(protocol_error(Value::Null, -32600, "Invalid Request"));
        }
        let method = match object.get("method").and_then(Value::as_str) {
            Some(method) => method,
            None => return Some(protocol_error(id(object), -32600, "Invalid Request")),
        };
        let Some(request_id) = object.get("id").cloned() else {
            return None;
        };
        let result = match method {
            "initialize" => Ok(json!({
                "protocolVersion": "2025-06-18",
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "orchestrate-mcp-component", "version": env!("CARGO_PKG_VERSION") }
            })),
            "tools/list" => Ok(json!({ "tools": [tool_schema()] })),
            "tools/call" => self.call_tool(object.get("params")),
            _ => Err((-32601, "Method not found")),
        };
        Some(match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": request_id, "result": result }),
            Err((code, message)) => protocol_error(request_id, code, message),
        })
    }
}

fn id(object: &serde_json::Map<String, Value>) -> Value {
    object.get("id").cloned().unwrap_or(Value::Null)
}

fn protocol_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn tool_schema() -> Value {
    json!({
        "name": "orchestrate_component",
        "description": "Read the bounded Orchestrate component fixture. Other listed component labels are unavailable.",
        "inputSchema": {
            "type": "object",
            "properties": { "component": { "type": "string", "enum": ["Orchestrate", "Message", "Persona", "Psyche", "Flow"] } },
            "required": ["component"],
            "additionalProperties": false
        }
    })
}

impl McpServer {
    fn call_tool(&self, params: Option<&Value>) -> Result<Value, (i64, &'static str)> {
        let Some(params) = params.and_then(Value::as_object) else {
            return Err((-32602, "Invalid params"));
        };
        if params.get("name").and_then(Value::as_str) != Some("orchestrate_component") {
            return Err((-32601, "Method not found"));
        }
        let Some(arguments) = params.get("arguments").and_then(Value::as_object) else {
            return Ok(tool_error("component must be the only string argument"));
        };
        if arguments.len() != 1 {
            return Ok(tool_error("component must be the only string argument"));
        }
        let Some(component) = arguments.get("component").and_then(Value::as_str) else {
            return Ok(tool_error("component must be the only string argument"));
        };
        #[cfg(test)]
        let result = if let Some(socket_path) = &self.socket_path {
            handle_component_at_socket(
                self.orchestrate.clone(),
                socket_path.clone(),
                &[component.to_owned()],
            )
        } else {
            handle_component(self.orchestrate.clone(), &[component.to_owned()])
        };
        #[cfg(not(test))]
        let result = handle_component(self.orchestrate.clone(), &[component.to_owned()]);
        match result {
            Ok(ComponentResult::Orchestrate(result)) => Ok(tool_text(result, false)),
            Ok(ComponentResult::Unavailable(component)) => Ok(tool_error(&format!(
                "McpComponent.Unavailable.{component:?}"
            ))),
            Err(ComponentError::Unknown(component)) => {
                Ok(tool_error(&format!("McpComponent.Unknown.{component}")))
            }
            Err(ComponentError::Arguments) => {
                Ok(tool_error("component must be the only string argument"))
            }
            Err(ComponentError::ClientFailure(error)) => {
                Ok(tool_error(&format!("McpComponent.ClientFailure.{error}")))
            }
        }
    }
}

fn tool_text(text: String, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

fn tool_error(text: &str) -> Value {
    tool_text(text.to_owned(), true)
}
