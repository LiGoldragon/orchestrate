//! The configuration ontology shared by both Orchestrate sockets.
//!
//! The lifecycle rule itself is the standard one every Nexus keeps, and it
//! lives in the `nexus` library; what belongs here is Orchestrate's own
//! judgement of what a valid configuration is, and the shape of a privileged
//! configuration transition's outcome.

use meta_signal_orchestrate::Response as MetaResponse;
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejectionReason, OrchestrateNexusConfiguration,
};
use std::os::unix::net::SocketAddr;

/// What a privileged configuration transition did.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigurationOutcome {
    Configured(Configured),
    Invalid,
}

/// A privileged transition leaves ordinary Configure either closed or open.
#[derive(Debug, Clone, PartialEq)]
pub enum Configured {
    Closed(ConfigurationReceipt),
    Reopened(ConfigurationReceipt),
}

/// A configuration says whether it is one this Nexus can bind.
pub trait Validates {
    fn validated(&self) -> Result<(), ConfigurationRejectionReason>;
}

impl Validates for OrchestrateNexusConfiguration {
    /// Both sockets are bound by absolute path and must differ: a Nexus
    /// cannot serve its privileged and its ordinary contract on one socket.
    fn validated(&self) -> Result<(), ConfigurationRejectionReason> {
        let paths = [&self.ordinary_socket_path.0.0, &self.meta_socket_path.0.0];
        let bindable = paths.iter().all(|path| {
            let socket_path = std::path::Path::new(path);
            socket_path.is_absolute()
                && !path.ends_with('/')
                && SocketAddr::from_pathname(socket_path).is_ok()
        });
        if bindable && self.ordinary_socket_path.0.0 != self.meta_socket_path.0.0 {
            Ok(())
        } else {
            Err(ConfigurationRejectionReason::InvalidConfiguration)
        }
    }
}

/// A privileged outcome answers on the meta wire.
pub trait AnswersMeta {
    fn meta_response(self) -> MetaResponse;
}

impl AnswersMeta for ConfigurationOutcome {
    fn meta_response(self) -> MetaResponse {
        match self {
            Self::Configured(Configured::Closed(receipt)) => MetaResponse::Configured(receipt),
            Self::Configured(Configured::Reopened(receipt)) => {
                MetaResponse::OrdinaryConfigurationReopened(receipt)
            }
            Self::Invalid => MetaResponse::ConfigurationRejected(
                meta_signal_orchestrate::ConfigurationRejection {
                    configuration_rejection_reason:
                        meta_signal_orchestrate::ConfigurationRejectionReason::InvalidConfiguration,
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_orchestrate::{ConfigurationPath, MetaSocketPath, OrdinarySocketPath};

    fn configuration(ordinary: &str, meta: &str) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(ordinary.to_owned())),
            meta_socket_path: MetaSocketPath(ConfigurationPath(meta.to_owned())),
        }
    }

    #[test]
    fn only_two_distinct_absolute_socket_paths_are_bindable() {
        assert!(
            configuration("/run/a.sock", "/run/b.sock")
                .validated()
                .is_ok()
        );
        for (ordinary, meta) in [
            ("/run/a.sock", "/run/a.sock"),
            ("run/a.sock", "/run/b.sock"),
            ("", "/run/b.sock"),
            ("/run/a.sock", "/run/"),
        ] {
            assert_eq!(
                configuration(ordinary, meta).validated(),
                Err(ConfigurationRejectionReason::InvalidConfiguration),
                "{ordinary:?} {meta:?}"
            );
        }
    }

    #[test]
    fn a_socket_path_must_fit_the_unix_address_that_will_bind_it() {
        for invalid in [
            "/run/orchestrate\0.sock".to_owned(),
            format!("/run/{}.sock", "x".repeat(256)),
        ] {
            assert_eq!(
                configuration(&invalid, "/run/meta.sock").validated(),
                Err(ConfigurationRejectionReason::InvalidConfiguration),
                "the Unix transport cannot represent {invalid:?}"
            );
        }
    }
}
