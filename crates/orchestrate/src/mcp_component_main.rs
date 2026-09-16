//! CLI proof surface for the bounded MCP component handler.

mod mcp_component;

use mcp_component::{ComponentError, ComponentResult, handle_component};
use std::{env, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let executable = env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join("orchestrate")))
        .unwrap_or_else(|| PathBuf::from("orchestrate"));
    match handle_component(executable, &arguments) {
        Ok(ComponentResult::Orchestrate(response)) => {
            println!("{response}");
            ExitCode::SUCCESS
        }
        Ok(ComponentResult::Unavailable(component)) => {
            eprintln!("McpComponent.Unavailable.{component:?}");
            ExitCode::FAILURE
        }
        Err(ComponentError::Arguments) => {
            eprintln!("orchestrate-mcp-component: accepts exactly one string component");
            ExitCode::FAILURE
        }
        Err(ComponentError::Unknown(component)) => {
            eprintln!("McpComponent.Unknown.{component}");
            ExitCode::FAILURE
        }
        Err(ComponentError::ClientFailure(error)) => {
            eprintln!("McpComponent.ClientFailure.{error}");
            ExitCode::FAILURE
        }
    }
}
