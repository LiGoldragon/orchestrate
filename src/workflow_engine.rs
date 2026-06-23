//! The orchestrate workflow-execution engine — the data-bearing noun the nexus
//! effect plane attaches to.
//!
//! This is the real engine behind `RunWorkflow`: it dispatches each workflow
//! step OUT to the `agent` component (its offline `FixtureProvider` in the thin
//! slice) as a cross-component effect, parses the agent's NOTA completion into a
//! step `EvaluationDecision`, combines the step outcomes under the workflow's
//! `CombinationRule`, and produces the local-plane `WorkflowReceipt` (unsigned —
//! the local execution chamber is trusted, per Spirit `ic4o`).
//!
//! The shape mirrors agent's own engine (record-970 forward-with-effect): a
//! `RunWorkflow` request becomes `NexusWork::SignalArrived`; `decide` emits
//! `CommandEffect(CallAgent)`; `run_effect` awaits the agent call off the engine
//! mailbox (never blocking a handler — the effect plane is the async seam); the
//! completion comes back as `EffectCompleted(AgentStepCompleted)` and `decide`
//! settles the run into a `WorkflowReceiptProduced` reply.
//!
//! The nexus effect plane carries the schema-emitted runtime nouns: the call
//! carries the agent `Prompt`, the completed effect carries the agent's raw
//! `Completion`. The engine parses that completion into orchestrate's own typed
//! `StepOutcome` (whose decision is the `signal-criome` `EvaluationDecision`, the
//! very noun the receipt is built from), so there is one type world from step
//! outcome to receipt.
//!
//! Durable SEMA run state is deferred for the thin slice: the single-step run
//! state lives in-engine. The effect-plane contract exists from day one so the
//! durable-run and multi-step DAG work is an extension, not a contract change.

use agent::provider::ProviderApiKey;
use agent::registry::{KeySource, KeySourceFuture, ProviderEntry, ProviderRegistry, SecretSource};
use agent::{AgentEngine, FixtureProvider};
use nota_next::{NotaDecodeError, NotaSource};
use signal_agent::{
    Call, ChatMessage, ChatTranscript, Completion, Input as AgentInput,
    ModelName as AgentModelName, Output as AgentOutput, OutputMode, Prompt, PromptOptions,
    ProviderName as AgentProviderName, ReasoningEffort, SystemText, ThinkingMode,
};
use signal_criome::{
    EvaluationDecision, OperationDigest, WorkflowProvenanceDigest, WorkflowReceipt,
};
use signal_orchestrate::{
    CombinationRule, HostName, ModelAttestation, ModelName, ProviderName, StepLog, StepOutcome,
    WorkflowDefinition, WorkflowReceiptProduced, WorkflowRunDigest, WorkflowRunHandle,
    WorkflowRunLog, WorkflowRunRequest, WorkflowStep, WorkflowStepName,
};

use crate::schema::nexus::{
    AgentStepCall, AgentStepResult, CommandEffect, EffectInput, EffectOutput,
};
use crate::{Error, Result};

/// A bootstrap key source: the fixture provider needs no real secret, so it
/// answers every handle with a fixed literal. The fixture agent makes no network
/// call, so the literal authenticates nothing.
struct FixtureKeySource;

impl KeySource for FixtureKeySource {
    fn resolve(&self, _source: SecretSource) -> KeySourceFuture<'_> {
        Box::pin(async { Ok(ProviderApiKey::new("fixture-key")) })
    }
}

/// The orchestrate workflow-execution engine. It owns the agent client (the
/// `AgentEngine` configured with the offline `FixtureProvider`) and the attested
/// facts that stamp each step's `ModelAttestation` (which provider/model/host it
/// ran on).
pub struct WorkflowEngine {
    agent: AgentEngine,
    provider: ProviderName,
    model: ModelName,
    host: HostName,
}

impl WorkflowEngine {
    const FIXTURE_PROVIDER: &'static str = "fixture-provider";
    const FIXTURE_ENDPOINT: &'static str = "https://fixture.invalid/v1";
    const FIXTURE_MODEL: &'static str = "fixture-model";
    const FIXTURE_HOST: &'static str = "local-orchestrate";
    const FIXTURE_KEY_HANDLE: &'static str = "ORCHESTRATE_FIXTURE_KEY";

    /// The thin-slice engine: an `AgentEngine` wired to the built-in
    /// `FixtureProvider` (offline, no network, no key) plus the attestation facts
    /// for the local host.
    pub fn fixture() -> Result<Self> {
        let mut registry = ProviderRegistry::new();
        registry.configure(ProviderEntry::new(
            Self::FIXTURE_PROVIDER,
            Self::FIXTURE_ENDPOINT,
            Self::FIXTURE_MODEL,
            SecretSource::environment(Self::FIXTURE_KEY_HANDLE),
        ));
        let agent = AgentEngine::new(
            registry,
            Box::new(FixtureProvider::new()),
            Box::new(FixtureKeySource),
        );
        Ok(Self {
            agent,
            provider: ProviderName::from_wire_token(Self::FIXTURE_PROVIDER)?,
            model: ModelName::from_wire_token(Self::FIXTURE_MODEL)?,
            host: HostName::from_wire_token(Self::FIXTURE_HOST)?,
        })
    }

    /// Run one workflow end to end and produce its `WorkflowReceiptProduced`
    /// reply. This is the engine's public driver: it walks the effect plane —
    /// `decide(SignalArrived)` → `run_effect(CallAgent)` → `decide(EffectCompleted)`
    /// — exactly the loop the daemon's mailbox runner walks, so the daemon wiring
    /// is a thin call site over this method.
    pub async fn run_workflow(
        &mut self,
        definition: &WorkflowDefinition,
        request: &WorkflowRunRequest,
    ) -> Result<WorkflowReceiptProduced> {
        let step = Self::single_step(definition)?;
        let handle = self.handle_for(request)?;
        let effect = self.decide_signal_arrived(step, request);
        let completed = self.run_effect(effect).await;
        let log = self.decide_effect_completed(step, completed)?;
        self.settle(definition, request, &handle, log)
    }

    /// The arrival transition: project a `RunWorkflow` step into the agent-call
    /// effect, building the step's NOTA-output guardian `Prompt`.
    fn decide_signal_arrived(
        &self,
        step: &WorkflowStep,
        request: &WorkflowRunRequest,
    ) -> CommandEffect {
        let call = AgentStepCall {
            step: crate::schema::nexus::WorkflowStepName::new(step.name.as_str().to_owned()),
            prompt: Self::guardian_prompt(step, request),
        };
        CommandEffect::new(EffectInput::call_agent(call))
    }

    /// Run the one effect the engine declares: call the agent component for one
    /// step. The agent's own `CallProvider` effect makes the (here fixture)
    /// completion; orchestrate awaits the agent Signal call off its mailbox and
    /// carries the raw completion back on the effect plane.
    async fn run_effect(&mut self, effect: CommandEffect) -> EffectOutput {
        let EffectInput::CallAgent(call) = effect.into_payload();
        let AgentStepCall { step, prompt } = call;
        let output = self.agent.handle(AgentInput::Call(Call::new(prompt))).await;
        EffectOutput::agent_step_completed(AgentStepResult {
            step,
            completion: Self::completion_of(output),
        })
    }

    /// The completion transition: parse the agent's reply into the step's typed
    /// `StepLog` (outcome + the model attestation of which model judged it).
    fn decide_effect_completed(
        &self,
        step: &WorkflowStep,
        completed: EffectOutput,
    ) -> Result<StepLog> {
        let EffectOutput::AgentStepCompleted(result) = completed;
        let outcome = self.step_outcome(&step.name, &result.completion)?;
        let attestation = self.attestation_for(&outcome);
        Ok(StepLog {
            step: step.name.clone(),
            attestation,
            outcome,
        })
    }

    /// The agent reply carried on the effect plane: the completion, or a fixture
    /// rejection rendered into a completion-shaped NOTA the parser will fail on
    /// (so a rejected call surfaces as a `Failed` step, not a silent success).
    fn completion_of(output: AgentOutput) -> Completion {
        match output {
            AgentOutput::Completed(completion) => completion,
            AgentOutput::CallRejected(rejection) => Self::rejection_completion(format!(
                "{:?}: {}",
                rejection.reason,
                rejection.detail.payload()
            )),
            AgentOutput::RequestUnimplemented(unimplemented) => Self::rejection_completion(
                format!("agent operation unimplemented: {:?}", unimplemented.operation),
            ),
            // A one-shot Call never opens a stream or emits a stream event; any
            // such reply is a protocol violation, surfaced as a failed step.
            AgentOutput::StreamOpened(_)
            | AgentOutput::StreamCancelled(_)
            | AgentOutput::Event(_) => Self::rejection_completion(
                "agent returned a streaming reply to a one-shot call".to_owned(),
            ),
        }
    }

    fn rejection_completion(detail: String) -> Completion {
        use signal_agent::{CompletionText, StopReasonText, TokenUsage};
        Completion {
            completion_text: CompletionText::new(format!("[|agent rejected: {detail}|]")),
            stop_reason: StopReasonText::new("rejected".to_owned()),
            token_usage: TokenUsage::new(None, None),
        }
    }

    /// Parse the agent's completion into a typed `StepOutcome`. A completion that
    /// decodes (via signal-criome NOTA decode) into the step's
    /// `EvaluationDecision` is `Produced`; anything else is a `Failed` step.
    fn step_outcome(
        &self,
        step: &WorkflowStepName,
        completion: &Completion,
    ) -> Result<StepOutcome> {
        match StepDecision::try_from(completion) {
            Ok(decision) => Ok(StepOutcome::Produced(decision.into_inner())),
            Err(error) => {
                let _ = step;
                let reason = signal_orchestrate::ScopeReason::from_text(format!(
                    "step completion was not a NOTA EvaluationDecision: {error}"
                ))
                .unwrap_or_else(|_| {
                    signal_orchestrate::ScopeReason::from_text("step completion undecodable")
                        .expect("static reason")
                });
                Ok(StepOutcome::Failed(reason))
            }
        }
    }

    fn attestation_for(&self, outcome: &StepOutcome) -> ModelAttestation {
        ModelAttestation {
            provider: self.provider.clone(),
            model: self.model.clone(),
            host: self.host.clone(),
            call: Self::call_digest(outcome),
        }
    }

    /// Settle the run: combine the step outcomes under the `CombinationRule` into
    /// the workflow `EvaluationDecision`, then build the local-plane
    /// `WorkflowReceipt` (unsigned) addressing its run log by provenance digest.
    fn settle(
        &self,
        definition: &WorkflowDefinition,
        request: &WorkflowRunRequest,
        handle: &WorkflowRunHandle,
        step_log: StepLog,
    ) -> Result<WorkflowReceiptProduced> {
        let log = self.run_log(handle, step_log);
        let outcomes: Vec<StepOutcome> = log
            .step_logs
            .iter()
            .map(|step_log| step_log.outcome.clone())
            .collect();
        let outcome = WorkflowCombination::new(&definition.combination).combine(&outcomes)?;
        let provenance = WorkflowProvenanceDigest::from_bytes(handle.run.as_str().as_bytes());
        let receipt = WorkflowReceipt {
            workflow: request.workflow.clone(),
            operation: OperationDigest::new(request.operation.digest.clone()),
            outcome,
            provenance,
        };
        // The run log (which model judged the step, where) is held in orchestrate;
        // the receipt addresses it by provenance digest. The durable SEMA run
        // record is deferred for the thin slice.
        Ok(WorkflowReceiptProduced {
            handle: handle.clone(),
            receipt,
        })
    }

    /// The single-step run log: which model judged the step, where, and what it
    /// produced — the "orchestrate has LLM logs" provenance.
    fn run_log(&self, handle: &WorkflowRunHandle, step_log: StepLog) -> WorkflowRunLog {
        WorkflowRunLog {
            run: handle.run.clone(),
            step_logs: vec![step_log],
        }
    }

    fn handle_for(&self, request: &WorkflowRunRequest) -> Result<WorkflowRunHandle> {
        let run = format!(
            "workflow-run-{}-{}-{}",
            request.workflow.object_digest().as_str(),
            request.operation.digest.as_str(),
            request.contract.object_digest().as_str()
        );
        Ok(WorkflowRunHandle {
            run: WorkflowRunDigest::from_wire_token(run)?,
        })
    }

    fn single_step(definition: &WorkflowDefinition) -> Result<&WorkflowStep> {
        match definition.steps.as_slice() {
            [step] => Ok(step),
            steps => Err(Error::SingleStepWorkflowRequired {
                step_count: steps.len(),
            }),
        }
    }

    /// Build the guardian prompt for a step: a NOTA-output judge prompt at high
    /// reasoning effort with thinking enabled — the deliberate-judge shape
    /// signal-agent's contract documents for the Spirit guardian.
    fn guardian_prompt(step: &WorkflowStep, request: &WorkflowRunRequest) -> Prompt {
        let instruction = format!(
            "You are workflow step {step}. Judge the operation {operation} and reply with \
             exactly one NOTA EvaluationDecision (for example: Authorized).",
            step = step.name.as_str(),
            operation = request.operation.digest.as_str(),
        );
        Prompt::new(
            Some(SystemText::new(
                "You are a deliberate guardian. You judge intent and reply with one NOTA \
                 EvaluationDecision."
                    .to_owned(),
            )),
            ChatTranscript::new(vec![ChatMessage::user(instruction)]),
            PromptOptions::new(
                Some(AgentModelName::new(Self::FIXTURE_MODEL.to_owned())),
                step.provider
                    .as_ref()
                    .map(|name| AgentProviderName::new(name.as_str().to_owned())),
                None,
                None,
                OutputMode::Nota,
                Some(ReasoningEffort::High),
                Some(ThinkingMode::Enabled),
            ),
        )
    }

    /// The content-addressed call digest stamped into a step's attestation: the
    /// digest of the decision the step produced. (A richer prompt+response digest
    /// is a later refinement.)
    fn call_digest(outcome: &StepOutcome) -> OperationDigest {
        let label = match outcome {
            StepOutcome::Produced(decision) => format!("decision:{decision:?}"),
            StepOutcome::Failed(reason) => format!("failed:{}", reason.as_str()),
        };
        OperationDigest::from_bytes(label.as_bytes())
    }
}

/// Apply the workflow's `CombinationRule` over the step outcomes to yield the
/// workflow's own `EvaluationDecision`.
struct WorkflowCombination<'rule> {
    rule: &'rule CombinationRule,
}

impl<'rule> WorkflowCombination<'rule> {
    fn new(rule: &'rule CombinationRule) -> Self {
        Self { rule }
    }

    fn combine(&self, outcomes: &[StepOutcome]) -> Result<EvaluationDecision> {
        let decisions = self.decisions(outcomes)?;
        match self.rule {
            CombinationRule::Unanimous => Self::unanimous(&decisions),
            CombinationRule::AnyApprove => Self::any_approve(&decisions),
            CombinationRule::Threshold(threshold) => Self::threshold(&decisions, threshold.value()),
        }
    }

    fn decisions(&self, outcomes: &[StepOutcome]) -> Result<Vec<EvaluationDecision>> {
        outcomes
            .iter()
            .map(|outcome| match outcome {
                StepOutcome::Produced(decision) => Ok(decision.clone()),
                StepOutcome::Failed(reason) => Err(Error::AgentStepRejected {
                    step: "<combination>".to_owned(),
                    detail: reason.as_str().to_owned(),
                }),
            })
            .collect()
    }

    fn unanimous(decisions: &[EvaluationDecision]) -> Result<EvaluationDecision> {
        if decisions.is_empty() {
            return Err(Error::CombinationIndecisive);
        }
        if decisions.iter().all(DecisionAuthorization::is_authorized) {
            Ok(EvaluationDecision::Authorized)
        } else {
            Self::first_non_authorized(decisions)
        }
    }

    fn any_approve(decisions: &[EvaluationDecision]) -> Result<EvaluationDecision> {
        if decisions.iter().any(DecisionAuthorization::is_authorized) {
            Ok(EvaluationDecision::Authorized)
        } else {
            decisions
                .first()
                .cloned()
                .ok_or(Error::CombinationIndecisive)
        }
    }

    fn threshold(decisions: &[EvaluationDecision], required: u64) -> Result<EvaluationDecision> {
        let approvals = decisions
            .iter()
            .filter(|decision| decision.is_authorized())
            .count() as u64;
        if approvals >= required {
            Ok(EvaluationDecision::Authorized)
        } else {
            Self::first_non_authorized(decisions)
        }
    }

    fn first_non_authorized(decisions: &[EvaluationDecision]) -> Result<EvaluationDecision> {
        decisions
            .iter()
            .find(|decision| !decision.is_authorized())
            .cloned()
            .ok_or(Error::CombinationIndecisive)
    }
}

/// Whether a step decision authorizes, in the combination algebra.
trait DecisionAuthorization {
    fn is_authorized(&self) -> bool;
}

impl DecisionAuthorization for EvaluationDecision {
    fn is_authorized(&self) -> bool {
        matches!(self, EvaluationDecision::Authorized)
    }
}

/// The agent fixture's NOTA verdict vocabulary — the offline stand-in's reply
/// when asked for a verdict in NOTA output mode.
const FIXTURE_VERDICT_ACCEPTED: &str = "(Verdict accepted)";

/// A step's adjudication, carried as the `signal-criome` `EvaluationDecision` —
/// the noun the receipt is built from. Parsing an agent `Completion` lands here.
struct StepDecision(EvaluationDecision);

impl StepDecision {
    fn into_inner(self) -> EvaluationDecision {
        self.0
    }
}

/// Parse an agent `Completion` into a step's criome `EvaluationDecision`. The
/// completion text is one NOTA expression: a real model returns an
/// `EvaluationDecision` directly (decoded via signal-criome's NOTA codec); the
/// offline `FixtureProvider` returns its `(Verdict accepted)` stand-in, which the
/// bootstrap bridges to `Authorized`.
impl TryFrom<&Completion> for StepDecision {
    type Error = NotaDecodeError;

    fn try_from(completion: &Completion) -> std::result::Result<Self, Self::Error> {
        let text = completion.completion_text.payload().trim().to_owned();
        match NotaSource::new(&text).parse::<EvaluationDecision>() {
            Ok(decision) => Ok(Self(decision)),
            Err(_) if text == FIXTURE_VERDICT_ACCEPTED => {
                Ok(Self(EvaluationDecision::Authorized))
            }
            Err(error) => Err(error),
        }
    }
}
