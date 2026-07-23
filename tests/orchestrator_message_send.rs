//! Packet 3.4 witnesses: the orchestrator's Send/triage wire.
//!
//! A routed send commits a bounded triage audit row and hands the message to
//! the messenger (stubbed here at the socket level, speaking the published
//! `signal-message` contract); every rejection is typed and audited; the
//! messenger hop is best-effort and degrades in the reply without failing
//! the triage.

use std::io::Write;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::thread;

use orchestrate::{
    OrchestrateLayout, OrchestrateService, StoreLocation, StoredTriageRejectionReason,
    StoredTriageVerdict,
};
use signal_orchestrate::{
    HarnessKind, MessengerDeliveryState, MissionDescription, OrchestrateReply, OrchestrateRequest,
    OrchestratorAgentIdentifier, OrchestratorMessageRecipient, OrchestratorMessageRejection,
    OrchestratorMessageSubmission, OrchestratorTopicPath, SessionIdentifier, TopicSelection,
};
use signal_orchestrator_message::{
    GuidanceMagnitude, MessageContent, MessageSubject, OrchestratorMessage, OrchestratorMessageKind,
};
use tempfile::TempDir;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

struct Fixture {
    _temporary: TempDir,
    service: OrchestrateService,
}

impl Fixture {
    fn new(name: &str, messenger_socket: Option<PathBuf>) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\ndesigner\nsystem-operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace, git_index),
        )
        .expect("service opens")
        .with_messenger_registration_endpoint(messenger_socket);
        Self {
            _temporary: temporary,
            service,
        }
    }

    fn handle(&mut self, request: OrchestrateRequest) -> orchestrate::Result<OrchestrateReply> {
        block_on(self.service.handle(request))
    }

    fn register(&mut self, session_name: &str) -> OrchestratorAgentIdentifier {
        let reply = self
            .handle(OrchestrateRequest::RegisterAgent(
                signal_orchestrate::OrchestratorAgentRegistration {
                    session: SessionIdentifier::from_camel_case_name(session_name)
                        .expect("session"),
                    mission: MissionDescription::from_text("exchange messages").expect("mission"),
                    harness: HarnessKind::Codex,
                    topic_selection: TopicSelection::Explicit(vec![
                        OrchestratorTopicPath::from_wire_token("engineering").expect("topic"),
                    ]),
                    minted_identity: signal_orchestrate::MintedIdentitySelection::None,
                },
            ))
            .expect("registration reply");
        let OrchestrateReply::AgentRegistered(registered) = reply else {
            panic!("expected registration acceptance, got {reply:?}");
        };
        registered.agent_identifier
    }

    fn triage_verdicts(&mut self) -> Vec<StoredTriageVerdict> {
        self.service
            .orchestrator_triage_records()
            .expect("triage records")
            .into_iter()
            .map(|record| record.verdict)
            .collect()
    }
}

fn block_on<Future: std::future::Future>(future: Future) -> Future::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn message(subject: &str, content: &str) -> OrchestratorMessage {
    OrchestratorMessage::new(
        OrchestratorMessageKind::Guidance(GuidanceMagnitude::Standard),
        MessageSubject::new(subject).expect("subject"),
        MessageContent::new(content).expect("content"),
    )
}

fn send(
    fixture: &mut Fixture,
    sender: OrchestratorAgentIdentifier,
    recipient: OrchestratorMessageRecipient,
    payload: OrchestratorMessage,
) -> OrchestrateReply {
    fixture
        .handle(OrchestrateRequest::SendOrchestratorMessage(
            OrchestratorMessageSubmission {
                sender,
                recipient,
                message: payload,
            },
        ))
        .expect("send reply")
}

/// A stub messenger daemon: accepts working-socket connections, decodes the
/// published-contract frame, answers Assign/Bind/Submit affirmatively, and
/// reports every accepted Submit on a channel.
struct StubMessenger {
    socket_path: PathBuf,
    submissions: Receiver<(String, String)>,
}

impl StubMessenger {
    fn bind(path: PathBuf) -> Self {
        let listener = UnixListener::bind(&path).expect("stub messenger binds");
        let (report, submissions) = channel();
        thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let codec = LengthPrefixedCodec::default();
                let Ok(body) = codec.read_body(&mut stream) else {
                    continue;
                };
                let Ok((_route, input)) =
                    signal_message::Input::decode_signal_frame(&body.into_bytes())
                else {
                    continue;
                };
                let output = match input {
                    signal_message::Input::AssignAgentIdentity(assignment) => {
                        signal_message::Output::agent_identity_assigned(
                            signal_message::AssignedAgentIdentity {
                                agent_identifier: assignment.agent_identifier,
                                identity_provenance: signal_message::IdentityProvenance::Seated,
                            },
                        )
                    }
                    signal_message::Input::BindAgentEndpoint(binding) => {
                        signal_message::Output::agent_endpoint_bound(binding.agent_identifier)
                    }
                    signal_message::Input::Submit(submission) => {
                        let _ = report.send((
                            submission.message_recipient.as_str().to_string(),
                            submission.message_body.as_str().to_string(),
                        ));
                        signal_message::Output::submission_accepted(
                            signal_message::MessageSlot::new(1),
                        )
                    }
                    _ => continue,
                };
                let Ok(bytes) = output.encode_signal_frame() else {
                    continue;
                };
                let _ = codec.write_body(&mut stream, &LengthPrefixedFrameBody::new(bytes));
                let _ = stream.flush();
            }
        });
        Self {
            socket_path: path,
            submissions,
        }
    }

    fn submitted(&self) -> Option<(String, String)> {
        self.submissions
            .recv_timeout(std::time::Duration::from_secs(2))
            .ok()
    }
}

#[test]
fn routed_send_lands_in_the_messenger_and_persists_bounded_triage() {
    let socket_root = TempDir::new().expect("socket root");
    let stub = StubMessenger::bind(socket_root.path().join("message.sock"));
    let mut fixture = Fixture::new("send-routed", Some(stub.socket_path.clone()));
    let sender = fixture.register("SendRoutedSender");
    let recipient = fixture.register("SendRoutedRecipient");
    // Drain the registration-time seat pushes so only the Submit remains.
    while stub.submitted().is_some() {}

    let reply = send(
        &mut fixture,
        sender.clone(),
        OrchestratorMessageRecipient::Agent(recipient.clone()),
        message(
            "rebase first",
            "main moved; rebase your branch before landing",
        ),
    );

    let OrchestrateReply::OrchestratorMessageRouted(routed) = reply else {
        panic!("expected routed reply, got {reply:?}");
    };
    assert_eq!(routed.recipients, vec![recipient.clone()]);
    assert_eq!(
        routed.messenger_delivery_state,
        MessengerDeliveryState::Submitted
    );

    let (submitted_recipient, submitted_body) =
        stub.submitted().expect("messenger received the submit");
    assert_eq!(submitted_recipient, recipient.as_str());
    assert!(
        submitted_body.contains(sender.as_str())
            && submitted_body.contains("rebase first")
            && submitted_body.contains("main moved; rebase your branch before landing"),
        "delivery note carries sender, subject, and content: {submitted_body}"
    );

    let verdicts = fixture.triage_verdicts();
    assert!(
        verdicts.iter().any(|verdict| matches!(
            verdict,
            StoredTriageVerdict::Route { recipients, retyped: None }
                if recipients.len() == 1 && recipients[0].as_str() == recipient.as_str()
        )),
        "route verdict persisted: {verdicts:?}"
    );
}

#[test]
fn send_from_unregistered_sender_rejects_typed_and_audits() {
    let mut fixture = Fixture::new("send-unregistered", None);
    let ghost = OrchestratorAgentIdentifier::try_new("gh0s".to_owned()).expect("identifier");
    let registered = fixture.register("SendUnregisteredRecipient");

    let reply = send(
        &mut fixture,
        ghost,
        OrchestratorMessageRecipient::Agent(registered),
        message("hello", "from nowhere"),
    );

    let OrchestrateReply::OrchestratorMessageRejected(rejected) = reply else {
        panic!("expected rejection, got {reply:?}");
    };
    assert_eq!(
        rejected.rejection,
        OrchestratorMessageRejection::SenderNotRegistered
    );
    assert!(fixture.triage_verdicts().iter().any(|verdict| matches!(
        verdict,
        StoredTriageVerdict::Reject {
            reason: StoredTriageRejectionReason::SenderNotRegistered
        }
    )));
}

#[test]
fn send_to_unknown_recipient_rejects_no_eligible_recipient() {
    let mut fixture = Fixture::new("send-unknown-recipient", None);
    let sender = fixture.register("SendUnknownRecipientSender");
    let ghost = OrchestratorAgentIdentifier::try_new("gh0s".to_owned()).expect("identifier");

    let reply = send(
        &mut fixture,
        sender,
        OrchestratorMessageRecipient::Agent(ghost),
        message("hello", "to nobody"),
    );

    let OrchestrateReply::OrchestratorMessageRejected(rejected) = reply else {
        panic!("expected rejection, got {reply:?}");
    };
    assert_eq!(
        rejected.rejection,
        OrchestratorMessageRejection::NoEligibleRecipient
    );
    assert!(fixture.triage_verdicts().iter().any(|verdict| matches!(
        verdict,
        StoredTriageVerdict::Reject {
            reason: StoredTriageRejectionReason::NoEligibleRecipient
        }
    )));
}

#[test]
fn escalation_without_coordinator_rejects_missing_coordinator_and_audits_escalate() {
    let mut fixture = Fixture::new("send-escalation", None);
    let sender = fixture.register("SendEscalationSender");

    let reply = send(
        &mut fixture,
        sender,
        OrchestratorMessageRecipient::Orchestrator,
        message("attention", "please review the wedged store"),
    );

    let OrchestrateReply::OrchestratorMessageRejected(rejected) = reply else {
        panic!("expected rejection, got {reply:?}");
    };
    assert_eq!(
        rejected.rejection,
        OrchestratorMessageRejection::MissingCoordinator
    );
    assert!(
        fixture
            .triage_verdicts()
            .iter()
            .any(|verdict| matches!(verdict, StoredTriageVerdict::Escalate))
    );
}

#[test]
fn routed_send_without_messenger_socket_degrades_named_and_still_commits_triage() {
    let mut fixture = Fixture::new("send-degraded", None);
    let sender = fixture.register("SendDegradedSender");
    let recipient = fixture.register("SendDegradedRecipient");

    let reply = send(
        &mut fixture,
        sender,
        OrchestratorMessageRecipient::Agent(recipient.clone()),
        message("parked", "no messenger is configured"),
    );

    let OrchestrateReply::OrchestratorMessageRouted(routed) = reply else {
        panic!("expected routed reply, got {reply:?}");
    };
    assert!(matches!(
        routed.messenger_delivery_state,
        MessengerDeliveryState::Degraded(ref detail)
            if detail.as_str().contains("no messenger socket configured")
    ));
    assert!(
        fixture
            .triage_verdicts()
            .iter()
            .any(|verdict| matches!(verdict, StoredTriageVerdict::Route { .. }))
    );
}
