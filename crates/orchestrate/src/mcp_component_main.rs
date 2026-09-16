//! MCP stdio server for the bounded component handler.

mod mcp_component;
mod mcp_server;

use mcp_server::{McpServer, ServingMcp};
use std::{env, io, path::PathBuf};

fn main() {
    let executable = env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join("orchestrate")))
        .unwrap_or_else(|| PathBuf::from("orchestrate"));
    McpServer::new(executable).serve(io::stdin().lock(), io::stdout().lock());
}
