//! Bounded MCP component selector.
//!
//! The only implemented component is `Orchestrate`. It delegates to the
//! ordinary `orchestrate` CLI, which in turn owns Datom actualization, the
//! typed Signal query, Unix-socket transport, and typed response rendering.
//! The remaining labels are deliberately reported as unavailable.

use std::{path::PathBuf, process::Command};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Component {
    Orchestrate,
    Message,
    Persona,
    Psyche,
    Flow,
}

trait SelectingComponent: Sized {
    fn select(source: &str) -> Option<Self>;
}

impl SelectingComponent for Component {
    fn select(source: &str) -> Option<Self> {
        match source {
            "Orchestrate" => Some(Self::Orchestrate),
            "Message" => Some(Self::Message),
            "Persona" => Some(Self::Persona),
            "Psyche" => Some(Self::Psyche),
            "Flow" => Some(Self::Flow),
            _ => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ComponentResult {
    Orchestrate(String),
    Unavailable(Component),
}

#[derive(Debug, Eq, PartialEq)]
pub enum ComponentError {
    Arguments,
    Unknown(String),
    ClientFailure(String),
}

pub struct McpComponentHandler {
    orchestrate: PathBuf,
    #[cfg(test)]
    socket_path: Option<PathBuf>,
}

trait HandlingMcpComponent {
    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError>;
}

impl McpComponentHandler {
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

impl HandlingMcpComponent for McpComponentHandler {
    /// Accept the MCP tool's single string argument. `Orchestrate` currently
    /// exposes only its bounded observation fixture; it does not route an
    /// arbitrary command or stand in for send-subflow.
    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError> {
        let [source] = arguments else {
            return Err(ComponentError::Arguments);
        };
        let component =
            Component::select(source).ok_or_else(|| ComponentError::Unknown(source.clone()))?;
        match component {
            Component::Orchestrate => {
                let mut command = Command::new(&self.orchestrate);
                command.arg("Observe.Locks");
                #[cfg(test)]
                if let Some(socket_path) = &self.socket_path {
                    command.env("ORCHESTRATE_SOCKET", socket_path);
                }
                let output = command
                    .output()
                    .map_err(|error| ComponentError::ClientFailure(error.to_string()))?;
                if !output.status.success() {
                    return Err(ComponentError::ClientFailure(
                        String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                    ));
                }
                Ok(ComponentResult::Orchestrate(
                    String::from_utf8_lossy(&output.stdout)
                        .trim_end()
                        .to_owned(),
                ))
            }
            unavailable => Ok(ComponentResult::Unavailable(unavailable)),
        }
    }
}

pub fn handle_component(
    orchestrate: PathBuf,
    arguments: &[String],
) -> Result<ComponentResult, ComponentError> {
    McpComponentHandler::new(orchestrate).handle(arguments)
}

#[cfg(test)]
pub fn handle_component_at_socket(
    orchestrate: PathBuf,
    socket_path: PathBuf,
    arguments: &[String],
) -> Result<ComponentResult, ComponentError> {
    McpComponentHandler::new(orchestrate)
        .at_socket(socket_path)
        .handle(arguments)
}
