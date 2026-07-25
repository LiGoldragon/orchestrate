//! Typed ordinary-CLI presentation.
//!
//! The daemon contract keeps exact times as nanoseconds. This module owns the
//! CLI-only projection that turns those wire values into the closed
//! `HumanReadableTime` values provided by `relative-age-display`. Raw
//! nanosecond values are a debugging interface: they survive unchanged under
//! `(Explicit (Canonical ...))`, and never appear in ordinary human output.
//!
//! Two kinds of wire value become one presented age:
//!
//! - A `DurationNanos` field is already an elapsed span and converts directly.
//! - A `TimestampNanos` field is an instant, and its age is its distance from
//!   the [`ObservationClock`] captured once per reply. The daemon and CLI share
//!   a host clock, so this is the same reference the daemon stamped against.
//!
//! A reply variant with no temporal field has nothing to render and stays
//! canonical.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nota_human::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode};
use relative_age_display::{HumanReadableTime, RelativeAge};
use signal_orchestrate::schema::lib::{
    Activity, ActivityList, ClaimEntry, DurationNanos, HarnessKind, Input, LaneProjection,
    LaneStatus, LanesObserved, MainIntegration, Output, PushedState, RoleSnapshot, RoleStatus,
    ScopeReference, TeardownRefusal, TimestampNanos, Worktree, WorktreeConcluded, WorktreeStatus,
    WorktreeTeardownRefused, WorktreesObserved,
};

/// The requested ordinary-CLI response presentation.
///
/// This is an invocation concern only. The daemon receives the same `Input`
/// frame regardless of choice and continues to return its canonical `Output`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, nota::NotaDecode, nota::NotaEncode)]
pub enum OrchestratorPresentation {
    Human,
    Canonical,
}

/// The explicit one-argument CLI invocation form.
///
/// Its legacy NOTA form is `(Explicit (Human (Observe Lanes)))` or `(Explicit
/// (Canonical (Observe Lanes)))`. Existing ordinary contract input is the
/// shorthand form and lowers to `Human` through [`ResolvedOrchestratorInvocation`].
#[derive(Debug, Clone, PartialEq, Eq, nota::NotaDecode, nota::NotaEncode)]
pub enum ExplicitOrchestratorInvocation {
    Explicit(OrchestratorPresentation, Input),
}

impl ExplicitOrchestratorInvocation {
    /// Make an explicit human presentation request.
    pub fn human(input: Input) -> Self {
        Self::Explicit(OrchestratorPresentation::Human, input)
    }

    /// Make an explicit canonical presentation request.
    pub fn canonical(input: Input) -> Self {
        Self::Explicit(OrchestratorPresentation::Canonical, input)
    }

    /// Lower the explicit syntax into the one request/presentation pipeline.
    pub fn into_resolved(self) -> ResolvedOrchestratorInvocation {
        match self {
            Self::Explicit(presentation, input) => ResolvedOrchestratorInvocation {
                presentation,
                input,
            },
        }
    }
}

/// One normalized CLI request after shorthand or explicit parsing.
pub struct ResolvedOrchestratorInvocation {
    presentation: OrchestratorPresentation,
    input: Input,
}

impl ResolvedOrchestratorInvocation {
    /// Lower an ordinary contract input shorthand to human presentation.
    pub fn human_shorthand(input: Input) -> Self {
        Self {
            presentation: OrchestratorPresentation::Human,
            input,
        }
    }

    /// The unchanged daemon request carried by this invocation.
    pub fn input(&self) -> &Input {
        &self.input
    }

    /// The selected output presentation.
    pub const fn presentation(&self) -> OrchestratorPresentation {
        self.presentation
    }
}

impl OrchestratorPresentation {
    /// Select a CLI-side rendering for one unchanged daemon reply.
    pub fn present<'output>(
        self,
        output: &'output Output,
    ) -> OrchestratorPresentationOutput<'output> {
        match self {
            Self::Canonical => OrchestratorPresentationOutput::Canonical(output),
            Self::Human => HumanOutput::from_output(output, ObservationClock::capture())
                .map(OrchestratorPresentationOutput::Human)
                .unwrap_or(OrchestratorPresentationOutput::Canonical(output)),
        }
    }
}

/// The instant one reply is rendered against.
///
/// Wire timestamps are instants, not spans, so presenting one as an age needs a
/// reference point. Capturing it once per reply keeps every age in a single
/// presentation consistent with every other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationClock {
    since_epoch: Duration,
}

impl ObservationClock {
    /// Capture the presentation instant from the host clock.
    pub fn capture() -> Self {
        Self {
            since_epoch: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
        }
    }

    /// A clock fixed at an epoch offset — the deterministic form tests render
    /// against.
    pub const fn at_nanoseconds(nanoseconds: u64) -> Self {
        Self {
            since_epoch: Duration::from_nanos(nanoseconds),
        }
    }

    /// The age of one wire instant, saturating to zero when the stamp is ahead
    /// of this clock.
    fn age_of(self, timestamp: &TimestampNanos) -> HumanReadableTime {
        self.idle_since_nanoseconds(*timestamp.payload())
    }

    /// The span between a raw epoch-nanosecond stamp and this clock, saturating
    /// to zero when the stamp is ahead of it.
    pub fn idle_since_nanoseconds(self, nanoseconds: u64) -> HumanReadableTime {
        RelativeAge::elapsed_between(Duration::from_nanos(nanoseconds), self.since_epoch)
            .into_human_readable_time()
    }
}

/// A contract time value that can present itself as a reader-facing age.
///
/// The daemon already measures some spans for itself, so those need no clock:
/// the span is the age.
trait PresentedAge {
    /// This value as the closed human age a reader sees.
    fn presented_age(&self) -> HumanReadableTime;
}

impl PresentedAge for DurationNanos {
    fn presented_age(&self) -> HumanReadableTime {
        RelativeAge::from_nanoseconds(*self.payload()).into_human_readable_time()
    }
}

/// The rendered result of the single presentation pipeline.
///
/// Human output uses the current typed NOTA codec. Canonical output retains the
/// daemon contract's existing codec byte-for-byte for programmatic callers.
pub enum OrchestratorPresentationOutput<'output> {
    Human(HumanOutput),
    Canonical(&'output Output),
}

impl OrchestratorPresentationOutput<'_> {
    /// Encode the selected presentation for stdout.
    pub fn to_stdout_nota(&self) -> String {
        match self {
            Self::Human(output) => output.to_nota(),
            Self::Canonical(output) => <Output as nota::NotaEncode>::to_nota(output),
        }
    }
}

/// A typed human projection of every reply that carries a time.
#[derive(Debug, Clone, PartialEq)]
pub enum HumanOutput {
    LanesObserved(HumanLaneAgeReport),
    RoleSnapshot(HumanRoleReport),
    WorktreesObserved(HumanWorktreeReport),
    ActivityList(HumanActivityReport),
    WorktreeScaffolded(HumanWorktree),
    WorktreeConcluded(HumanWorktreeConclusion),
    WorktreeTeardownRefused(HumanWorktreeTeardownRefusal),
}

impl HumanOutput {
    /// Project the reply variants that carry a temporal field. The rest have no
    /// time to render and stay canonical.
    pub fn from_output(output: &Output, clock: ObservationClock) -> Option<Self> {
        match output {
            Output::LanesObserved(lanes) => Some(Self::LanesObserved(
                HumanLaneAgeReport::from_observation(lanes),
            )),
            Output::RoleSnapshot(snapshot) => Some(Self::RoleSnapshot(
                HumanRoleReport::from_snapshot(snapshot, clock),
            )),
            Output::WorktreesObserved(worktrees) => Some(Self::WorktreesObserved(
                HumanWorktreeReport::from_observation(worktrees, clock),
            )),
            Output::ActivityList(activities) => Some(Self::ActivityList(
                HumanActivityReport::from_list(activities, clock),
            )),
            Output::WorktreeScaffolded(scaffolded) => Some(Self::WorktreeScaffolded(
                HumanWorktree::from_wire(scaffolded.payload(), clock),
            )),
            Output::WorktreeConcluded(concluded) => Some(Self::WorktreeConcluded(
                HumanWorktreeConclusion::from_wire(concluded, clock),
            )),
            Output::WorktreeTeardownRefused(refused) => Some(Self::WorktreeTeardownRefused(
                HumanWorktreeTeardownRefusal::from_wire(refused, clock),
            )),
            _ => None,
        }
    }

    /// Decode the closed human projection from its current NOTA form.
    fn from_variant_payload(block: &Block) -> Result<Self, NotaDecodeError> {
        let (head, payload) = block
            .as_application()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanOutput",
            })?;
        let variant = head
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanOutput",
            })?;
        match variant {
            "LanesObserved" => Ok(Self::LanesObserved(HumanLaneAgeReport::from_nota_block(
                payload,
            )?)),
            "RoleSnapshot" => Ok(Self::RoleSnapshot(HumanRoleReport::from_nota_block(payload)?)),
            "WorktreesObserved" => Ok(Self::WorktreesObserved(
                HumanWorktreeReport::from_nota_block(payload)?,
            )),
            "ActivityList" => Ok(Self::ActivityList(HumanActivityReport::from_nota_block(
                payload,
            )?)),
            "WorktreeScaffolded" => Ok(Self::WorktreeScaffolded(HumanWorktree::from_nota_block(
                payload,
            )?)),
            "WorktreeConcluded" => Ok(Self::WorktreeConcluded(
                HumanWorktreeConclusion::from_nota_block(payload)?,
            )),
            "WorktreeTeardownRefused" => Ok(Self::WorktreeTeardownRefused(
                HumanWorktreeTeardownRefusal::from_nota_block(payload)?,
            )),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanOutput",
                variant: other.to_owned(),
            }),
        }
    }
}

impl NotaEncode for HumanOutput {
    fn to_nota(&self) -> String {
        match self {
            Self::LanesObserved(report) => format!("LanesObserved.{}", report.to_nota()),
            Self::RoleSnapshot(report) => format!("RoleSnapshot.{}", report.to_nota()),
            Self::WorktreesObserved(report) => format!("WorktreesObserved.{}", report.to_nota()),
            Self::ActivityList(report) => format!("ActivityList.{}", report.to_nota()),
            Self::WorktreeScaffolded(worktree) => {
                format!("WorktreeScaffolded.{}", worktree.to_nota())
            }
            Self::WorktreeConcluded(conclusion) => {
                format!("WorktreeConcluded.{}", conclusion.to_nota())
            }
            Self::WorktreeTeardownRefused(refusal) => {
                format!("WorktreeTeardownRefused.{}", refusal.to_nota())
            }
        }
    }
}

impl NotaDecode for HumanOutput {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Self::from_variant_payload(block)
    }
}

// ─── Lanes ────────────────────────────────────────────────

/// A typed human lane-observation collection.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanLaneAgeReport {
    lanes: Vec<HumanLaneAge>,
}

impl HumanLaneAgeReport {
    /// Convert each contract lane projection into presented ages.
    pub fn from_observation(lanes: &LanesObserved) -> Self {
        Self {
            lanes: lanes
                .payload()
                .payload()
                .iter()
                .map(HumanLaneAge::from_projection)
                .collect(),
        }
    }

    /// The ordered lane projections in this report.
    pub fn lanes(&self) -> &[HumanLaneAge] {
        &self.lanes
    }
}

impl NotaEncode for HumanLaneAgeReport {
    fn to_nota(&self) -> String {
        Delimiter::SquareBracket.wrap(self.lanes.iter().map(NotaEncode::to_nota))
    }
}

impl NotaDecode for HumanLaneAgeReport {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Ok(Self {
            lanes: NotaBlock::new(block)
                .expect_delimited(Delimiter::SquareBracket, "HumanLaneAgeReport")?
                .iter()
                .map(HumanLaneAge::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One lane with its typed elapsed age.
///
/// The wire projection also carries the daemon's observation instant. That
/// instant is the reference the age is already measured from, so presenting it
/// as well would only restate the age as `Seconds.0`.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanLaneAge {
    session: String,
    lane: String,
    status: HumanLaneStatus,
    elapsed: HumanReadableTime,
    resource_claims: HumanResourceClaimAges,
}

impl HumanLaneAge {
    /// Project a wire lane observation into typed human elapsed values.
    pub fn from_projection(projection: &LaneProjection) -> Self {
        let assignment = &projection.lane_registration.lane_assignment;
        Self {
            session: assignment.session_identifier.payload().clone(),
            lane: assignment.lane_identifier.payload().clone(),
            status: HumanLaneStatus::from(&projection.lane_registration.lane_status),
            elapsed: projection.duration_nanos.presented_age(),
            resource_claims: HumanResourceClaimAges::from_projection(projection),
        }
    }

    /// The lane identifier retained in this human observation.
    pub fn lane(&self) -> &str {
        &self.lane
    }

    /// The typed, unit-bearing elapsed lane age.
    pub fn elapsed(&self) -> HumanReadableTime {
        self.elapsed
    }
}

impl NotaEncode for HumanLaneAge {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.session.to_nota(),
            self.lane.to_nota(),
            self.status.to_nota(),
            self.elapsed.to_nota(),
            self.resource_claims.to_nota(),
        ])
    }
}

impl NotaDecode for HumanLaneAge {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanLaneAge", 5)?;
        Ok(Self {
            session: String::from_nota_block(&fields[0])?,
            lane: String::from_nota_block(&fields[1])?,
            status: HumanLaneStatus::from_nota_block(&fields[2])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[3])?,
            resource_claims: HumanResourceClaimAges::from_nota_block(&fields[4])?,
        })
    }
}

/// A typed human projection of resource-claim ages held by one lane.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanResourceClaimAges {
    claims: Vec<HumanResourceClaimAge>,
}

impl HumanResourceClaimAges {
    /// Convert every resource claim age in the owning lane projection.
    pub fn from_projection(projection: &LaneProjection) -> Self {
        Self {
            claims: projection
                .lane_resource_claims
                .payload()
                .iter()
                .map(HumanResourceClaimAge::from_projection)
                .collect(),
        }
    }
}

impl NotaEncode for HumanResourceClaimAges {
    fn to_nota(&self) -> String {
        Delimiter::SquareBracket.wrap(self.claims.iter().map(NotaEncode::to_nota))
    }
}

impl NotaDecode for HumanResourceClaimAges {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Ok(Self {
            claims: NotaBlock::new(block)
                .expect_delimited(Delimiter::SquareBracket, "HumanResourceClaimAges")?
                .iter()
                .map(HumanResourceClaimAge::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One resource claim with its scope and typed elapsed age.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanResourceClaimAge {
    scope: HumanScopeReference,
    reason: String,
    elapsed: HumanReadableTime,
}

impl HumanResourceClaimAge {
    /// Convert one contract resource-claim age without flattening it to text.
    pub fn from_projection(
        projection: &signal_orchestrate::schema::lib::LaneResourceClaim,
    ) -> Self {
        Self {
            scope: HumanScopeReference::from(&projection.scope_reference),
            reason: projection.scope_reason.payload().clone(),
            elapsed: projection.duration_nanos.presented_age(),
        }
    }
}

impl NotaEncode for HumanResourceClaimAge {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.scope.to_nota(),
            self.reason.to_nota(),
            self.elapsed.to_nota(),
        ])
    }
}

impl NotaDecode for HumanResourceClaimAge {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields =
            NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanResourceClaimAge", 3)?;
        Ok(Self {
            scope: HumanScopeReference::from_nota_block(&fields[0])?,
            reason: String::from_nota_block(&fields[1])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[2])?,
        })
    }
}

// ─── Roles ────────────────────────────────────────────────

/// The role snapshot with every claim and activity age presented.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanRoleReport {
    roles: Vec<HumanRoleStatus>,
    recent_activity: Vec<HumanActivity>,
}

impl HumanRoleReport {
    /// Project the wire role snapshot into presented ages.
    pub fn from_snapshot(snapshot: &RoleSnapshot, clock: ObservationClock) -> Self {
        Self {
            roles: snapshot
                .role_statuses
                .payload()
                .iter()
                .map(HumanRoleStatus::from_wire)
                .collect(),
            recent_activity: snapshot
                .activities
                .payload()
                .iter()
                .map(|activity| HumanActivity::from_wire(activity, clock))
                .collect(),
        }
    }

    /// The ordered role statuses in this report.
    pub fn roles(&self) -> &[HumanRoleStatus] {
        &self.roles
    }
}

impl NotaEncode for HumanRoleReport {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            Delimiter::SquareBracket.wrap(self.roles.iter().map(NotaEncode::to_nota)),
            Delimiter::SquareBracket.wrap(self.recent_activity.iter().map(NotaEncode::to_nota)),
        ])
    }
}

impl NotaDecode for HumanRoleReport {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanRoleReport", 2)?;
        Ok(Self {
            roles: NotaBlock::new(&fields[0])
                .expect_delimited(Delimiter::SquareBracket, "HumanRoleReport")?
                .iter()
                .map(HumanRoleStatus::from_nota_block)
                .collect::<Result<_, _>>()?,
            recent_activity: NotaBlock::new(&fields[1])
                .expect_delimited(Delimiter::SquareBracket, "HumanRoleReport")?
                .iter()
                .map(HumanActivity::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One role, its harness, and the presented age of each claim it holds.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanRoleStatus {
    role: String,
    harness: HumanHarnessKind,
    claims: Vec<HumanClaimEntry>,
}

impl HumanRoleStatus {
    /// Project one wire role status into presented claim ages.
    pub fn from_wire(status: &RoleStatus) -> Self {
        Self {
            role: status.role_name.payload().payload().clone(),
            harness: HumanHarnessKind::from(&status.harness_kind),
            claims: status
                .claim_entries
                .payload()
                .iter()
                .map(HumanClaimEntry::from_wire)
                .collect(),
        }
    }

    /// The role identifier retained in this status.
    pub fn role(&self) -> &str {
        &self.role
    }
}

impl NotaEncode for HumanRoleStatus {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.role.to_nota(),
            self.harness.to_nota(),
            Delimiter::SquareBracket.wrap(self.claims.iter().map(NotaEncode::to_nota)),
        ])
    }
}

impl NotaDecode for HumanRoleStatus {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanRoleStatus", 3)?;
        Ok(Self {
            role: String::from_nota_block(&fields[0])?,
            harness: HumanHarnessKind::from_nota_block(&fields[1])?,
            claims: NotaBlock::new(&fields[2])
                .expect_delimited(Delimiter::SquareBracket, "HumanRoleStatus")?
                .iter()
                .map(HumanClaimEntry::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One held claim with its scope and typed elapsed age.
///
/// The wire entry also carries the instant the claim was taken. That instant is
/// what the age already measures, so only the age is presented.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanClaimEntry {
    scope: HumanScopeReference,
    reason: String,
    elapsed: HumanReadableTime,
}

impl HumanClaimEntry {
    /// Project one wire claim entry into its presented age.
    pub fn from_wire(entry: &ClaimEntry) -> Self {
        Self {
            scope: HumanScopeReference::from(&entry.scope_reference),
            reason: entry.scope_reason.payload().clone(),
            elapsed: entry.duration_nanos.presented_age(),
        }
    }
}

impl NotaEncode for HumanClaimEntry {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.scope.to_nota(),
            self.reason.to_nota(),
            self.elapsed.to_nota(),
        ])
    }
}

impl NotaDecode for HumanClaimEntry {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanClaimEntry", 3)?;
        Ok(Self {
            scope: HumanScopeReference::from_nota_block(&fields[0])?,
            reason: String::from_nota_block(&fields[1])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[2])?,
        })
    }
}

// ─── Activity ─────────────────────────────────────────────

/// A typed human activity listing.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanActivityReport {
    records: Vec<HumanActivity>,
}

impl HumanActivityReport {
    /// Project the wire activity list into presented ages.
    pub fn from_list(activities: &ActivityList, clock: ObservationClock) -> Self {
        Self {
            records: activities
                .payload()
                .payload()
                .iter()
                .map(|activity| HumanActivity::from_wire(activity, clock))
                .collect(),
        }
    }

    /// The ordered activity records in this report.
    pub fn records(&self) -> &[HumanActivity] {
        &self.records
    }
}

impl NotaEncode for HumanActivityReport {
    fn to_nota(&self) -> String {
        Delimiter::SquareBracket.wrap(self.records.iter().map(NotaEncode::to_nota))
    }
}

impl NotaDecode for HumanActivityReport {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Ok(Self {
            records: NotaBlock::new(block)
                .expect_delimited(Delimiter::SquareBracket, "HumanActivityReport")?
                .iter()
                .map(HumanActivity::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One activity record whose stamp is presented as its age.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanActivity {
    role: String,
    scope: HumanScopeReference,
    reason: String,
    elapsed: HumanReadableTime,
}

impl HumanActivity {
    /// Project one wire activity, converting its stamp into an age.
    pub fn from_wire(activity: &Activity, clock: ObservationClock) -> Self {
        Self {
            role: activity.role_name.payload().payload().clone(),
            scope: HumanScopeReference::from(&activity.scope_reference),
            reason: activity.scope_reason.payload().clone(),
            elapsed: clock.age_of(&activity.timestamp_nanos),
        }
    }
}

impl NotaEncode for HumanActivity {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.role.to_nota(),
            self.scope.to_nota(),
            self.reason.to_nota(),
            self.elapsed.to_nota(),
        ])
    }
}

impl NotaDecode for HumanActivity {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanActivity", 4)?;
        Ok(Self {
            role: String::from_nota_block(&fields[0])?,
            scope: HumanScopeReference::from_nota_block(&fields[1])?,
            reason: String::from_nota_block(&fields[2])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[3])?,
        })
    }
}

// ─── Worktrees ────────────────────────────────────────────

/// A typed human worktree-observation collection.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanWorktreeReport {
    worktrees: Vec<HumanWorktree>,
}

impl HumanWorktreeReport {
    /// Project the wire worktree observation into presented ages.
    pub fn from_observation(worktrees: &WorktreesObserved, clock: ObservationClock) -> Self {
        Self {
            worktrees: worktrees
                .payload()
                .payload()
                .iter()
                .map(|worktree| HumanWorktree::from_wire(worktree, clock))
                .collect(),
        }
    }

    /// The ordered worktrees in this report.
    pub fn worktrees(&self) -> &[HumanWorktree] {
        &self.worktrees
    }
}

impl NotaEncode for HumanWorktreeReport {
    fn to_nota(&self) -> String {
        Delimiter::SquareBracket.wrap(self.worktrees.iter().map(NotaEncode::to_nota))
    }
}

impl NotaDecode for HumanWorktreeReport {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Ok(Self {
            worktrees: NotaBlock::new(block)
                .expect_delimited(Delimiter::SquareBracket, "HumanWorktreeReport")?
                .iter()
                .map(HumanWorktree::from_nota_block)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One registered worktree whose last-activity stamp is presented as its age.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanWorktree {
    repository: String,
    branch: String,
    path: String,
    lane: String,
    status: HumanWorktreeStatus,
    purpose: String,
    idle: HumanReadableTime,
    pushed_state: HumanPushedState,
}

impl HumanWorktree {
    /// Project one wire worktree, converting its last-activity stamp into the
    /// idle age a reader actually wants.
    pub fn from_wire(worktree: &Worktree, clock: ObservationClock) -> Self {
        Self {
            repository: worktree.repository_name.payload().clone(),
            branch: worktree.branch_name.payload().clone(),
            path: worktree.wire_path.payload().clone(),
            lane: worktree.lane_name.payload().clone(),
            status: HumanWorktreeStatus::from(&worktree.worktree_status),
            purpose: worktree.purpose_text.payload().clone(),
            idle: clock.age_of(&worktree.timestamp_nanos),
            pushed_state: HumanPushedState::from(&worktree.pushed_state),
        }
    }

    /// The branch identifying this worktree.
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// How long this worktree has been idle.
    pub fn idle(&self) -> HumanReadableTime {
        self.idle
    }
}

impl NotaEncode for HumanWorktree {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([
            self.repository.to_nota(),
            self.branch.to_nota(),
            self.path.to_nota(),
            self.lane.to_nota(),
            self.status.to_nota(),
            self.purpose.to_nota(),
            self.idle.to_nota(),
            self.pushed_state.to_nota(),
        ])
    }
}

impl NotaDecode for HumanWorktree {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanWorktree", 8)?;
        Ok(Self {
            repository: String::from_nota_block(&fields[0])?,
            branch: String::from_nota_block(&fields[1])?,
            path: String::from_nota_block(&fields[2])?,
            lane: String::from_nota_block(&fields[3])?,
            status: HumanWorktreeStatus::from_nota_block(&fields[4])?,
            purpose: String::from_nota_block(&fields[5])?,
            idle: HumanReadableTime::from_nota_block(&fields[6])?,
            pushed_state: HumanPushedState::from_nota_block(&fields[7])?,
        })
    }
}

/// A concluded worktree with its age presented and how main took the work.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanWorktreeConclusion {
    worktree: HumanWorktree,
    main_integration: HumanMainIntegration,
}

impl HumanWorktreeConclusion {
    /// Project one wire worktree conclusion.
    pub fn from_wire(concluded: &WorktreeConcluded, clock: ObservationClock) -> Self {
        Self {
            worktree: HumanWorktree::from_wire(&concluded.worktree, clock),
            main_integration: HumanMainIntegration::from(&concluded.main_integration),
        }
    }
}

impl NotaEncode for HumanWorktreeConclusion {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([self.worktree.to_nota(), self.main_integration.to_nota()])
    }
}

impl NotaDecode for HumanWorktreeConclusion {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields =
            NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanWorktreeConclusion", 2)?;
        Ok(Self {
            worktree: HumanWorktree::from_nota_block(&fields[0])?,
            main_integration: HumanMainIntegration::from_nota_block(&fields[1])?,
        })
    }
}

/// A refused worktree teardown with the worktree's age presented.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanWorktreeTeardownRefusal {
    worktree: HumanWorktree,
    reason: HumanTeardownRefusal,
}

impl HumanWorktreeTeardownRefusal {
    /// Project one wire teardown refusal.
    pub fn from_wire(refused: &WorktreeTeardownRefused, clock: ObservationClock) -> Self {
        Self {
            worktree: HumanWorktree::from_wire(&refused.worktree, clock),
            reason: HumanTeardownRefusal::from(&refused.teardown_refusal),
        }
    }
}

impl NotaEncode for HumanWorktreeTeardownRefusal {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([self.worktree.to_nota(), self.reason.to_nota()])
    }
}

impl NotaDecode for HumanWorktreeTeardownRefusal {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(
            Delimiter::Brace,
            "HumanWorktreeTeardownRefusal",
            2,
        )?;
        Ok(Self {
            worktree: HumanWorktree::from_nota_block(&fields[0])?,
            reason: HumanTeardownRefusal::from_nota_block(&fields[1])?,
        })
    }
}

// ─── Shared closed vocabularies ───────────────────────────

/// The claim scope carried by human claim and activity records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HumanScopeReference {
    Path(String),
    Task(String),
}

impl From<&ScopeReference> for HumanScopeReference {
    fn from(scope: &ScopeReference) -> Self {
        match scope {
            ScopeReference::Path(path) => Self::Path(path.payload().clone()),
            ScopeReference::Task(task) => Self::Task(task.payload().clone()),
        }
    }
}

impl NotaEncode for HumanScopeReference {
    fn to_nota(&self) -> String {
        match self {
            Self::Path(path) => {
                Delimiter::Parenthesis.wrap(["Path".to_owned(), path.to_nota()])
            }
            Self::Task(task) => {
                Delimiter::Parenthesis.wrap(["Task".to_owned(), task.to_nota()])
            }
        }
    }
}

impl NotaDecode for HumanScopeReference {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields =
            NotaBlock::new(block).expect_children(Delimiter::Parenthesis, "HumanScopeReference", 2)?;
        let variant = fields[0]
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanScopeReference",
            })?;
        match variant {
            "Path" => Ok(Self::Path(String::from_nota_block(&fields[1])?)),
            "Task" => Ok(Self::Task(String::from_nota_block(&fields[1])?)),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanScopeReference",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed lane-status vocabulary carried by human lane observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanLaneStatus {
    Active,
    Released,
    HandoverEnded,
    Suspect,
}

impl From<&LaneStatus> for HumanLaneStatus {
    fn from(status: &LaneStatus) -> Self {
        match status {
            LaneStatus::Active => Self::Active,
            LaneStatus::Released => Self::Released,
            LaneStatus::HandoverEnded => Self::HandoverEnded,
            LaneStatus::Suspect => Self::Suspect,
        }
    }
}

impl NotaEncode for HumanLaneStatus {
    fn to_nota(&self) -> String {
        match self {
            Self::Active => "Active".to_owned(),
            Self::Released => "Released".to_owned(),
            Self::HandoverEnded => "HandoverEnded".to_owned(),
            Self::Suspect => "Suspect".to_owned(),
        }
    }
}

impl NotaDecode for HumanLaneStatus {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let status = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanLaneStatus",
            })?;
        match status {
            "Active" => Ok(Self::Active),
            "Released" => Ok(Self::Released),
            "HandoverEnded" => Ok(Self::HandoverEnded),
            "Suspect" => Ok(Self::Suspect),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanLaneStatus",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed worktree-status vocabulary carried by human worktree records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanWorktreeStatus {
    Active,
    Merged,
    Archived,
    Recycled,
    Abandoned,
}

impl From<&WorktreeStatus> for HumanWorktreeStatus {
    fn from(status: &WorktreeStatus) -> Self {
        match status {
            WorktreeStatus::Active => Self::Active,
            WorktreeStatus::Merged => Self::Merged,
            WorktreeStatus::Archived => Self::Archived,
            WorktreeStatus::Recycled => Self::Recycled,
            WorktreeStatus::Abandoned => Self::Abandoned,
        }
    }
}

impl From<&signal_orchestrate::WorktreeStatus> for HumanWorktreeStatus {
    fn from(status: &signal_orchestrate::WorktreeStatus) -> Self {
        match status {
            signal_orchestrate::WorktreeStatus::Active => Self::Active,
            signal_orchestrate::WorktreeStatus::Merged => Self::Merged,
            signal_orchestrate::WorktreeStatus::Archived => Self::Archived,
            signal_orchestrate::WorktreeStatus::Recycled => Self::Recycled,
            signal_orchestrate::WorktreeStatus::Abandoned => Self::Abandoned,
        }
    }
}

impl NotaEncode for HumanWorktreeStatus {
    fn to_nota(&self) -> String {
        match self {
            Self::Active => "Active".to_owned(),
            Self::Merged => "Merged".to_owned(),
            Self::Archived => "Archived".to_owned(),
            Self::Recycled => "Recycled".to_owned(),
            Self::Abandoned => "Abandoned".to_owned(),
        }
    }
}

impl NotaDecode for HumanWorktreeStatus {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let status = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanWorktreeStatus",
            })?;
        match status {
            "Active" => Ok(Self::Active),
            "Merged" => Ok(Self::Merged),
            "Archived" => Ok(Self::Archived),
            "Recycled" => Ok(Self::Recycled),
            "Abandoned" => Ok(Self::Abandoned),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanWorktreeStatus",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed pushed-state vocabulary carried by human worktree records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanPushedState {
    Unpushed,
    Pushed,
    AncestorOfMain,
}

impl From<&PushedState> for HumanPushedState {
    fn from(state: &PushedState) -> Self {
        match state {
            PushedState::Unpushed => Self::Unpushed,
            PushedState::Pushed => Self::Pushed,
            PushedState::AncestorOfMain => Self::AncestorOfMain,
        }
    }
}

impl From<&signal_orchestrate::PushedState> for HumanPushedState {
    fn from(state: &signal_orchestrate::PushedState) -> Self {
        match state {
            signal_orchestrate::PushedState::Unpushed => Self::Unpushed,
            signal_orchestrate::PushedState::Pushed => Self::Pushed,
            signal_orchestrate::PushedState::AncestorOfMain => Self::AncestorOfMain,
        }
    }
}

impl NotaEncode for HumanPushedState {
    fn to_nota(&self) -> String {
        match self {
            Self::Unpushed => "Unpushed".to_owned(),
            Self::Pushed => "Pushed".to_owned(),
            Self::AncestorOfMain => "AncestorOfMain".to_owned(),
        }
    }
}

impl NotaDecode for HumanPushedState {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let state = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanPushedState",
            })?;
        match state {
            "Unpushed" => Ok(Self::Unpushed),
            "Pushed" => Ok(Self::Pushed),
            "AncestorOfMain" => Ok(Self::AncestorOfMain),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanPushedState",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed harness vocabulary carried by human role statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanHarnessKind {
    Codex,
    Claude,
}

impl From<&HarnessKind> for HumanHarnessKind {
    fn from(kind: &HarnessKind) -> Self {
        match kind {
            HarnessKind::Codex => Self::Codex,
            HarnessKind::Claude => Self::Claude,
        }
    }
}

impl NotaEncode for HumanHarnessKind {
    fn to_nota(&self) -> String {
        match self {
            Self::Codex => "Codex".to_owned(),
            Self::Claude => "Claude".to_owned(),
        }
    }
}

impl NotaDecode for HumanHarnessKind {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let kind = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanHarnessKind",
            })?;
        match kind {
            "Codex" => Ok(Self::Codex),
            "Claude" => Ok(Self::Claude),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanHarnessKind",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed teardown-refusal vocabulary carried by human refusals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanTeardownRefusal {
    UnmergedWorkPresent,
    AutoRebaseConflicted,
    MainPushRejected,
}

impl From<&TeardownRefusal> for HumanTeardownRefusal {
    fn from(refusal: &TeardownRefusal) -> Self {
        match refusal {
            TeardownRefusal::UnmergedWorkPresent => Self::UnmergedWorkPresent,
            TeardownRefusal::AutoRebaseConflicted => Self::AutoRebaseConflicted,
            TeardownRefusal::MainPushRejected => Self::MainPushRejected,
        }
    }
}

impl NotaEncode for HumanTeardownRefusal {
    fn to_nota(&self) -> String {
        match self {
            Self::UnmergedWorkPresent => "UnmergedWorkPresent".to_owned(),
            Self::AutoRebaseConflicted => "AutoRebaseConflicted".to_owned(),
            Self::MainPushRejected => "MainPushRejected".to_owned(),
        }
    }
}

impl NotaDecode for HumanTeardownRefusal {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let refusal = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanTeardownRefusal",
            })?;
        match refusal {
            "UnmergedWorkPresent" => Ok(Self::UnmergedWorkPresent),
            "AutoRebaseConflicted" => Ok(Self::AutoRebaseConflicted),
            "MainPushRejected" => Ok(Self::MainPushRejected),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanTeardownRefusal",
                variant: other.to_owned(),
            }),
        }
    }
}

/// The closed vocabulary for how main took a concluded worktree's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanMainIntegration {
    AlreadyAncestor,
    FastForwarded,
    Rebased,
    Discarded,
}

impl From<&MainIntegration> for HumanMainIntegration {
    fn from(integration: &MainIntegration) -> Self {
        match integration {
            MainIntegration::AlreadyAncestor => Self::AlreadyAncestor,
            MainIntegration::FastForwarded => Self::FastForwarded,
            MainIntegration::Rebased => Self::Rebased,
            MainIntegration::Discarded => Self::Discarded,
        }
    }
}

impl NotaEncode for HumanMainIntegration {
    fn to_nota(&self) -> String {
        match self {
            Self::AlreadyAncestor => "AlreadyAncestor".to_owned(),
            Self::FastForwarded => "FastForwarded".to_owned(),
            Self::Rebased => "Rebased".to_owned(),
            Self::Discarded => "Discarded".to_owned(),
        }
    }
}

impl NotaDecode for HumanMainIntegration {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let integration = block
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "HumanMainIntegration",
            })?;
        match integration {
            "AlreadyAncestor" => Ok(Self::AlreadyAncestor),
            "FastForwarded" => Ok(Self::FastForwarded),
            "Rebased" => Ok(Self::Rebased),
            "Discarded" => Ok(Self::Discarded),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "HumanMainIntegration",
                variant: other.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relative_age_display::HumanReadableMagnitude;

    fn minutes(value: f64) -> HumanReadableTime {
        HumanReadableTime::Minutes(HumanReadableMagnitude::from_unit_value(value))
    }

    fn days(value: f64) -> HumanReadableTime {
        HumanReadableTime::Days(HumanReadableMagnitude::from_unit_value(value))
    }

    fn round_trips(output: &HumanOutput) {
        let encoded = output.to_nota();
        assert_eq!(
            &nota_human::NotaSource::new(&encoded)
                .parse::<HumanOutput>()
                .expect("human output decodes"),
            output
        );
    }

    #[test]
    fn human_lane_age_output_round_trips_typed_units() {
        let output = HumanOutput::LanesObserved(HumanLaneAgeReport {
            lanes: vec![HumanLaneAge {
                session: "TypedTime".to_owned(),
                lane: "typed-time".to_owned(),
                status: HumanLaneStatus::Active,
                elapsed: minutes(10.0),
                resource_claims: HumanResourceClaimAges {
                    claims: vec![HumanResourceClaimAge {
                        scope: HumanScopeReference::Path("/tmp/typed".to_owned()),
                        reason: "typed time".to_owned(),
                        elapsed: days(3.2),
                    }],
                },
            }],
        });

        assert_eq!(
            output.to_nota(),
            "LanesObserved.[{TypedTime typed-time Active Minutes.10 \
             [{(Path /tmp/typed) (typed time) Days.(3.2)}]}]"
        );
        round_trips(&output);
    }

    #[test]
    fn human_role_snapshot_renders_claim_and_activity_ages() {
        let output = HumanOutput::RoleSnapshot(HumanRoleReport {
            roles: vec![HumanRoleStatus {
                role: "general-code-implementer".to_owned(),
                harness: HumanHarnessKind::Codex,
                claims: vec![HumanClaimEntry {
                    scope: HumanScopeReference::Path("/tmp/claim".to_owned()),
                    reason: "held work".to_owned(),
                    elapsed: minutes(10.0),
                }],
            }],
            recent_activity: vec![HumanActivity {
                role: "operator".to_owned(),
                scope: HumanScopeReference::Task("Deploy".to_owned()),
                reason: "stamped work".to_owned(),
                elapsed: days(3.2),
            }],
        });

        assert_eq!(
            output.to_nota(),
            "RoleSnapshot.{[{general-code-implementer Codex \
             [{(Path /tmp/claim) (held work) Minutes.10}]}] \
             [{operator (Task Deploy) (stamped work) Days.(3.2)}]}"
        );
        round_trips(&output);
    }

    #[test]
    fn human_worktree_report_renders_idle_age_instead_of_a_stamp() {
        let output = HumanOutput::WorktreesObserved(HumanWorktreeReport {
            worktrees: vec![HumanWorktree {
                repository: "orchestrate".to_owned(),
                branch: "RenderedTime".to_owned(),
                path: "/home/li/wt/orchestrate/RenderedTime".to_owned(),
                lane: "rendered-time".to_owned(),
                status: HumanWorktreeStatus::Active,
                purpose: "render elapsed time".to_owned(),
                idle: days(3.2),
                pushed_state: HumanPushedState::AncestorOfMain,
            }],
        });

        assert_eq!(
            output.to_nota(),
            "WorktreesObserved.[{orchestrate RenderedTime \
             /home/li/wt/orchestrate/RenderedTime rendered-time Active \
             (render elapsed time) Days.(3.2) AncestorOfMain}]"
        );
        round_trips(&output);
    }

    #[test]
    fn a_wire_stamp_becomes_its_age_against_the_observation_clock() {
        let clock = ObservationClock::at_nanoseconds(1_800_000_000_000_000_000);
        // Three hours clears the ninety-minute rung, so the ladder presents
        // hours rather than restating the span as one hundred eighty minutes.
        let three_hours_earlier =
            TimestampNanos::new(1_800_000_000_000_000_000 - 3 * 3_600_000_000_000);

        assert_eq!(
            clock.age_of(&three_hours_earlier),
            HumanReadableTime::Hours(HumanReadableMagnitude::from_unit_value(3.0))
        );
    }

    #[test]
    fn a_stamp_ahead_of_the_clock_reads_as_no_elapsed_age() {
        let clock = ObservationClock::at_nanoseconds(1_800_000_000_000_000_000);
        let ahead = TimestampNanos::new(1_800_000_000_000_000_000 + 5_000_000_000);

        assert_eq!(
            clock.age_of(&ahead),
            HumanReadableTime::Seconds(HumanReadableMagnitude::from_unit_value(0.0))
        );
    }

    #[test]
    fn explicit_invocation_round_trips_through_legacy_cli_notation() {
        let input = Input::Observe(signal_orchestrate::schema::lib::Observation::Lanes);
        let canonical = ExplicitOrchestratorInvocation::canonical(input.clone());
        let human = ExplicitOrchestratorInvocation::human(input);
        let canonical_nota =
            <ExplicitOrchestratorInvocation as nota::NotaEncode>::to_nota(&canonical);
        let human_nota = <ExplicitOrchestratorInvocation as nota::NotaEncode>::to_nota(&human);

        assert_eq!(canonical_nota, "(Explicit (Canonical (Observe Lanes)))");
        assert_eq!(human_nota, "(Explicit (Human (Observe Lanes)))");
        assert_eq!(
            nota::NotaSource::new(&canonical_nota)
                .parse::<ExplicitOrchestratorInvocation>()
                .expect("canonical explicit invocation decodes"),
            canonical
        );
        assert_eq!(
            nota::NotaSource::new(&human_nota)
                .parse::<ExplicitOrchestratorInvocation>()
                .expect("human explicit invocation decodes"),
            human
        );
    }
}
