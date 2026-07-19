//! Typed ordinary-CLI presentation.
//!
//! The daemon contract keeps exact elapsed durations as nanoseconds. This
//! module owns the CLI-only projection that turns observed lane ages into the
//! closed `HumanReadableTime` values provided by `relative-age-display`.
//! Canonical presentation remains the unchanged daemon `Output`; human
//! presentation never changes timestamps into elapsed values.

use nota_human::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode};
use relative_age_display::{HumanReadableTime, RelativeAge};
use signal_orchestrate::schema::lib::{
    Input, LaneProjection, LaneStatus, LanesObserved, Output, TimestampNanos,
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
            Self::Human => HumanOutput::from_output(output)
                .map(OrchestratorPresentationOutput::Human)
                .unwrap_or(OrchestratorPresentationOutput::Canonical(output)),
        }
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

/// A typed human projection of the ordinary replies that carry elapsed ages.
#[derive(Debug, Clone, PartialEq)]
pub enum HumanOutput {
    LanesObserved(HumanLaneAgeReport),
}

impl HumanOutput {
    /// Project only reply variants that carry elapsed durations. Other replies
    /// remain canonical because their values have no elapsed-time field to
    /// transform.
    pub fn from_output(output: &Output) -> Option<Self> {
        match output {
            Output::LanesObserved(lanes) => Some(Self::LanesObserved(
                HumanLaneAgeReport::from_observation(lanes),
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
        }
    }
}

impl NotaDecode for HumanOutput {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Self::from_variant_payload(block)
    }
}

/// A typed human lane-observation collection.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanLaneAgeReport {
    lanes: Vec<HumanLaneAge>,
}

impl HumanLaneAgeReport {
    /// Convert each contract lane projection without changing its observation
    /// timestamp or nanosecond source data inside the daemon reply.
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

/// One lane with its typed elapsed age and unchanged observation timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanLaneAge {
    session: String,
    lane: String,
    status: HumanLaneStatus,
    observed_at: HumanTimestampNanos,
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
            observed_at: HumanTimestampNanos::from(&projection.timestamp_nanos),
            elapsed: RelativeAge::from_nanoseconds(*projection.duration_nanos.payload())
                .into_human_readable_time(),
            resource_claims: HumanResourceClaimAges::from_projection(projection),
        }
    }

    /// The lane identifier retained in this human observation.
    pub fn lane(&self) -> &str {
        &self.lane
    }

    /// The exact timestamp at which the daemon observed this lane.
    pub const fn observed_at(&self) -> HumanTimestampNanos {
        self.observed_at
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
            self.observed_at.to_nota(),
            self.elapsed.to_nota(),
            self.resource_claims.to_nota(),
        ])
    }
}

impl NotaDecode for HumanLaneAge {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanLaneAge", 6)?;
        Ok(Self {
            session: String::from_nota_block(&fields[0])?,
            lane: String::from_nota_block(&fields[1])?,
            status: HumanLaneStatus::from_nota_block(&fields[2])?,
            observed_at: HumanTimestampNanos::from_nota_block(&fields[3])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[4])?,
            resource_claims: HumanResourceClaimAges::from_nota_block(&fields[5])?,
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

/// One resource claim with separate timestamp and typed elapsed age.
#[derive(Debug, Clone, PartialEq)]
pub struct HumanResourceClaimAge {
    observed_at: HumanTimestampNanos,
    elapsed: HumanReadableTime,
}

impl HumanResourceClaimAge {
    /// Convert one contract resource-claim age without flattening it to text.
    pub fn from_projection(
        projection: &signal_orchestrate::schema::lib::LaneResourceClaim,
    ) -> Self {
        Self {
            observed_at: HumanTimestampNanos::from(&projection.timestamp_nanos),
            elapsed: RelativeAge::from_nanoseconds(*projection.duration_nanos.payload())
                .into_human_readable_time(),
        }
    }
}

impl NotaEncode for HumanResourceClaimAge {
    fn to_nota(&self) -> String {
        Delimiter::Brace.wrap([self.observed_at.to_nota(), self.elapsed.to_nota()])
    }
}

impl NotaDecode for HumanResourceClaimAge {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        let fields =
            NotaBlock::new(block).expect_children(Delimiter::Brace, "HumanResourceClaimAge", 2)?;
        Ok(Self {
            observed_at: HumanTimestampNanos::from_nota_block(&fields[0])?,
            elapsed: HumanReadableTime::from_nota_block(&fields[1])?,
        })
    }
}

/// A timestamp copied exactly from the daemon reply and deliberately distinct
/// from a [`HumanReadableTime`] elapsed duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanTimestampNanos(u64);

impl From<&TimestampNanos> for HumanTimestampNanos {
    fn from(timestamp: &TimestampNanos) -> Self {
        Self(*timestamp.payload())
    }
}

impl NotaEncode for HumanTimestampNanos {
    fn to_nota(&self) -> String {
        self.0.to_nota()
    }
}

impl NotaDecode for HumanTimestampNanos {
    fn from_nota_block(block: &Block) -> Result<Self, NotaDecodeError> {
        Ok(Self(u64::from_nota_block(block)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use relative_age_display::HumanReadableMagnitude;

    #[test]
    fn human_lane_age_output_round_trips_typed_units_and_timestamps() {
        let output = HumanOutput::LanesObserved(HumanLaneAgeReport {
            lanes: vec![HumanLaneAge {
                session: "TypedTime".to_owned(),
                lane: "typed-time".to_owned(),
                status: HumanLaneStatus::Active,
                observed_at: HumanTimestampNanos(700),
                elapsed: HumanReadableTime::Minutes(HumanReadableMagnitude::from_unit_value(10.0)),
                resource_claims: HumanResourceClaimAges {
                    claims: vec![HumanResourceClaimAge {
                        observed_at: HumanTimestampNanos(600),
                        elapsed: HumanReadableTime::Days(HumanReadableMagnitude::from_unit_value(
                            3.2,
                        )),
                    }],
                },
            }],
        });

        let encoded = output.to_nota();
        assert_eq!(
            encoded,
            "LanesObserved.[{TypedTime typed-time Active 700 Minutes.10 [{600 Days.(3.2)}]}]"
        );
        assert_eq!(
            nota_human::NotaSource::new(&encoded)
                .parse::<HumanOutput>()
                .expect("human lane output decodes"),
            output
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
