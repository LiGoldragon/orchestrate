use std::{env, process::ExitCode};

use meta_signal_orchestrate::schema::lib::Output as MetaOutput;
use nota::NotaSource;
use signal_orchestrate::schema::lib::Output as OrdinaryOutput;

fn main() -> ExitCode {
    match ScenarioNotaAssertion::from_process_arguments().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-nota-assert: {error}");
            ExitCode::FAILURE
        }
    }
}

struct ScenarioNotaAssertion {
    tier: String,
    expected_route: String,
    reply: String,
}

impl ScenarioNotaAssertion {
    fn from_process_arguments() -> Self {
        let mut arguments = env::args().skip(1);
        Self {
            tier: arguments.next().unwrap_or_default(),
            expected_route: arguments.next().unwrap_or_default(),
            reply: arguments.next().unwrap_or_default(),
        }
    }

    fn run(self) -> Result<(), String> {
        if self.tier == "ordinary-identifier" {
            return self.print_ordinary_identifier();
        }
        self.verify()
    }

    fn print_ordinary_identifier(self) -> Result<(), String> {
        let output = NotaSource::new(&self.reply)
            .parse::<OrdinaryOutput>()
            .map_err(|error| error.to_string())?;
        let identifier = match output {
            OrdinaryOutput::AgentRegistered(registered) => registered.orchestrator_agent_identifier,
            OrdinaryOutput::AgentIdentityMinted(minted) => minted.into_payload(),
            other => {
                return Err(format!(
                    "expected an agent identifier output, got {:?}",
                    other.route()
                ));
            }
        };
        println!("{}", identifier.payload());
        Ok(())
    }

    fn verify(self) -> Result<(), String> {
        if self.tier == "ordinary" {
            let output = NotaSource::new(&self.reply)
                .parse::<OrdinaryOutput>()
                .map_err(|error| error.to_string())?;
            return match self.expected_route.as_str() {
                "AgentLaunchRefused:UnknownAgent" => match output {
                    OrdinaryOutput::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.agent_launch_refusal_reason,
                            signal_orchestrate::schema::lib::AgentLaunchRefusalReason::UnknownAgent
                        ) => Ok(()),
                    other => Err(format!("expected UnknownAgent refusal, got {:?}", other.route())),
                },
                "AgentLaunchRefused:HarnessUnreachable" => match output {
                    OrdinaryOutput::AgentLaunchRefused(refusal)
                        if matches!(
                            refusal.agent_launch_refusal_reason,
                            signal_orchestrate::schema::lib::AgentLaunchRefusalReason::HarnessUnreachable
                        ) => Ok(()),
                    other => Err(format!("expected HarnessUnreachable refusal, got {:?}", other.route())),
                },
                "AgentRegistrationRejected:JudgeUnavailable" => match output {
                    OrdinaryOutput::AgentRegistrationRejected(refusal)
                        if matches!(
                            refusal.agent_registration_rejection_reason,
                            signal_orchestrate::schema::lib::AgentRegistrationRejectionReason::JudgeUnavailable
                        ) => Ok(()),
                    other => Err(format!("expected JudgeUnavailable refusal, got {:?}", other.route())),
                },
                _ => self.verify_ordinary_route(output),
            };
        }
        let actual_route = match self.tier.as_str() {
            "meta" => format!(
                "{:?}",
                NotaSource::new(&self.reply)
                    .parse::<MetaOutput>()
                    .map_err(|error| error.to_string())?
                    .route()
            ),
            other => return Err(format!("unknown tier {other}")),
        };
        if actual_route == self.expected_route {
            Ok(())
        } else {
            Err(format!(
                "expected {} route {}, got {}",
                self.tier, self.expected_route, actual_route
            ))
        }
    }

    fn verify_ordinary_route(self, output: OrdinaryOutput) -> Result<(), String> {
        let actual_route = format!("{:?}", output.route());
        if actual_route == self.expected_route {
            Ok(())
        } else {
            Err(format!(
                "expected ordinary route {}, got {}",
                self.expected_route, actual_route
            ))
        }
    }
}
