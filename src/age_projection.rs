//! Human-facing rendering of observed record ages.
//!
//! Observed lanes carry their age as a raw `DurationNanos` on the wire; a human
//! reading `Observe Lanes` output wants that age as a readable relative age, not
//! a nanosecond count. This module renders each observed lane's own age and the
//! ages of the resource claims it holds through the shared
//! [`relative_age_display`] library, so every age a human sees is formatted the
//! one uniform way — seconds up to three minutes, then minutes, hours, days,
//! weeks, months, years, each with two decimals.

use relative_age_display::RelativeAge;
use signal_orchestrate::schema::lib::{LaneProjection, LanesObserved};

/// One lane's human age line: its identity, status token, its own relative age,
/// and the relative ages of the resource claims it holds.
pub struct LaneAgeLine {
    session: String,
    lane: String,
    status: String,
    age: RelativeAge,
    claim_ages: Vec<RelativeAge>,
}

impl LaneAgeLine {
    pub fn new(
        session: String,
        lane: String,
        status: String,
        age: RelativeAge,
        claim_ages: Vec<RelativeAge>,
    ) -> Self {
        Self {
            session,
            lane,
            status,
            age,
            claim_ages,
        }
    }

    /// Build one line from an observed lane projection, reading its age and each
    /// claim's age from their wire `DurationNanos` counts.
    pub fn from_projection(projection: &LaneProjection) -> Self {
        let assignment = &projection.lane_registration.lane_assignment;
        let claim_ages = projection
            .lane_resource_claims
            .payload()
            .iter()
            .map(|claim| RelativeAge::from_nanoseconds(*claim.duration_nanos.payload()))
            .collect();
        Self::new(
            assignment.session_identifier.payload().clone(),
            assignment.lane_identifier.payload().clone(),
            format!("{:?}", projection.lane_registration.lane_status),
            RelativeAge::from_nanoseconds(*projection.duration_nanos.payload()),
            claim_ages,
        )
    }

    fn render_into(&self, report: &mut String) {
        report.push_str(&format!(
            "  {}/{} [{}] age {}\n",
            self.session, self.lane, self.status, self.age
        ));
        for claim_age in &self.claim_ages {
            report.push_str(&format!("    resource claim age {claim_age}\n"));
        }
    }
}

/// The human age summary for a whole lane observation.
pub struct LaneAgeReport {
    lines: Vec<LaneAgeLine>,
}

impl LaneAgeReport {
    pub fn from_observation(lanes: &LanesObserved) -> Self {
        Self {
            lines: lanes
                .payload()
                .payload()
                .iter()
                .map(LaneAgeLine::from_projection)
                .collect(),
        }
    }

    /// The rendered multi-line summary, one block per lane, ages as relative
    /// ages. A lane-less observation renders an explicit empty line so the reader
    /// sees the reaper left no live lanes rather than nothing at all.
    pub fn render(&self) -> String {
        if self.lines.is_empty() {
            return "observed lane ages: none\n".to_string();
        }
        let mut report = String::from("observed lane ages:\n");
        for line in &self.lines {
            line.render_into(&mut report);
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn lane_line_renders_identity_status_and_relative_ages() {
        let line = LaneAgeLine::new(
            "StorageMigration".to_string(),
            "storage-worker".to_string(),
            "Active".to_string(),
            RelativeAge::from_duration(Duration::from_secs_f64(3.42 * 86_400.0)),
            vec![RelativeAge::from_duration(Duration::from_secs_f64(42.17))],
        );
        let mut rendered = String::new();
        line.render_into(&mut rendered);
        assert_eq!(
            rendered,
            "  StorageMigration/storage-worker [Active] age 3.42 days\n    resource claim age 42.17 seconds\n"
        );
    }

    #[test]
    fn report_with_no_lanes_renders_an_explicit_empty_line() {
        let report = LaneAgeReport { lines: Vec::new() };
        assert_eq!(report.render(), "observed lane ages: none\n");
    }

    #[test]
    fn report_renders_one_block_per_lane_with_a_header() {
        let report = LaneAgeReport {
            lines: vec![
                LaneAgeLine::new(
                    "SessionOne".to_string(),
                    "first-lane".to_string(),
                    "Active".to_string(),
                    RelativeAge::from_duration(Duration::from_secs(90 * 60)),
                    Vec::new(),
                ),
                LaneAgeLine::new(
                    "SessionTwo".to_string(),
                    "second-lane".to_string(),
                    "Released".to_string(),
                    RelativeAge::from_duration(Duration::from_secs_f64(2.0 * 604_800.0)),
                    Vec::new(),
                ),
            ],
        };
        assert_eq!(
            report.render(),
            "observed lane ages:\n  SessionOne/first-lane [Active] age 1.50 hours\n  SessionTwo/second-lane [Released] age 2.00 weeks\n"
        );
    }
}
