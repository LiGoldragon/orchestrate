use std::collections::BTreeMap;

use meta_signal_orchestrate::{
    LaneAlreadyRegistered, LaneAlreadyRegisteredResolution, LaneAuthorityChange, LaneAuthoritySet,
    LaneRegistered, LaneRegistrationMode, LaneRegistrationRequest, LaneRetired, LaneUnregistered,
    LaneUnregistrationRequest, MetaOrchestrateReply, SessionClearRequest, SessionCleared,
};
use signal_orchestrate::{
    LaneAuthority, LaneIdentifier, LaneProjection, LaneStatus, LanesObserved, OrchestrateReply,
    Role, RoleName, SessionProjection, SessionsObserved, TimestampNanos,
};

use crate::{Error, OrchestrateTables, Result, StoredClaim, StoredLaneRegistration};

/// How long a terminal lane record (`Released` / `HandoverEnded`) is retained
/// after its last update before the reaper hard-deletes it. Terminal lanes are
/// finished work; a short window keeps them briefly for post-mortem, then they
/// are gone so the live view reflects only real lanes. Tunable.
pub const TERMINAL_LANE_RETENTION_NANOS: u64 = 60 * 60 * 1_000_000_000;

/// How long an `Active` lane may sit idle — no claim, release, handoff, or
/// recovery re-registration — before the reaper treats it as a leaked lane
/// whose owning agent is gone and hard-deletes it. Generous by design: genuine
/// long-running work refreshes its last-activity stamp on every real use, so
/// only an abandoned lane idles this long. Tunable.
pub const ACTIVE_LANE_IDLE_LIMIT_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

pub struct LaneRegistry<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> LaneRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    pub fn register(&self, request: LaneRegistrationRequest) -> Result<MetaOrchestrateReply> {
        if request.assignment.owner.role.tokens.is_empty() {
            return Err(Error::EmptyLaneRole);
        }

        // Only a live (`Active`) lane holding this identity is a real
        // registration that can block a `Fresh` request or be inherited by a
        // `Recovery` one. A terminal record (`Released` / `HandoverEnded`) is
        // finished work, not a real lane, so it is invisible here and never
        // blocks — this is why "Fresh follows the closed lane record" is
        // literally true. Lane identity is global (claims, liveness, and the
        // reaper all key on the lane alone), so the live-lane check is global
        // too: no two agents hold the same live lane name.
        if let Some(active) = self.tables.active_lane_record(&request.assignment.lane)? {
            let resolution = match request.mode {
                LaneRegistrationMode::Fresh => LaneAlreadyRegisteredResolution::FreshConflict,
                LaneRegistrationMode::Recovery => {
                    // Recovery is real use: resuming a lane refreshes its
                    // last-activity stamp so an inherited lane does not age out
                    // from under the agent that just reclaimed it.
                    self.tables.touch_lane(&request.assignment.lane)?;
                    LaneAlreadyRegisteredResolution::RecoveryInherited
                }
            };
            let observed_at = self.tables.current_timestamp()?;
            return Ok(MetaOrchestrateReply::LaneAlreadyRegistered(
                LaneAlreadyRegistered {
                    requested: request,
                    active: self.projection_for(
                        active,
                        &self.tables.claim_records()?,
                        observed_at,
                    )?,
                    resolution,
                },
            ));
        }

        // No live lane holds this identity. Any record that remains is a dead
        // terminal one: supersede it in one operation — drop the dead record and
        // any stale claims it left behind — then register the lane anew below.
        // Both `Fresh` and `Recovery` converge on this path, so a `Recovery`
        // that finds only a closed or absent record genuinely re-registers the
        // lane and answers with a truthful `LaneRegistered`, never a silent
        // no-op hidden behind a success variant. Only this exact lane identity
        // is touched, so unrelated terminal records keep their full retention
        // window for inspection.
        self.supersede_dead_record(&request.assignment.lane)?;

        let registered_at = self.tables.current_timestamp()?;
        let registration = StoredLaneRegistration::active(request.assignment, registered_at);
        self.tables.insert_lane(&registration)?;
        Ok(MetaOrchestrateReply::LaneRegistered(LaneRegistered {
            registration: registration.registration(),
        }))
    }

    /// Drop the dead terminal record for `lane`, if one remains, along with any
    /// stale claims it left behind. The caller guarantees no `Active` record
    /// holds `lane`, so whatever record is found is terminal and superseded, not
    /// a live lane. Lane identity is global, so a terminal record registered
    /// under a prior session is superseded too, never left to squat the name.
    fn supersede_dead_record(&self, lane: &LaneIdentifier) -> Result<()> {
        if self.tables.first_lane_record(lane)?.is_some() {
            self.tables.remove_claims_for_lane(lane)?;
            self.tables.remove_first_lane(lane)?;
        }
        Ok(())
    }

    pub fn unregister(&self, request: LaneUnregistrationRequest) -> Result<MetaOrchestrateReply> {
        let Some(mut registration) = self.tables.lane_record(&request.session, &request.lane)?
        else {
            return Err(Error::LaneNotRegistered {
                lane: request.lane.as_wire_token().to_string(),
            });
        };
        let ended_at = self.tables.current_timestamp()?;
        registration.status = signal_orchestrate::LaneStatus::Released;
        registration.updated_at = ended_at;
        self.tables.remove_claims_for_lane(&request.lane)?;
        self.tables.insert_lane(&registration)?;
        Ok(MetaOrchestrateReply::LaneUnregistered(LaneUnregistered {
            session: request.session,
            lane: request.lane,
            ended_at,
            details: request.details,
        }))
    }

    pub fn clear_session(&self, request: SessionClearRequest) -> Result<MetaOrchestrateReply> {
        let ended_at = self.tables.current_timestamp()?;
        let cleared_lanes = self.tables.remove_lanes_for_session(&request.session)?;
        for lane in &cleared_lanes {
            self.tables.remove_claims_for_lane(&lane.assignment.lane)?;
        }
        // Clearing a session ends the agents that registered under it. Retire
        // them here as an explicit lifecycle event; the interim table reaper then
        // deletes each retired agent (and its topic seats) after the terminal
        // retention window, exactly as it does for a lane.
        self.tables
            .retire_session_orchestrator_agents(&request.session)?;
        Ok(MetaOrchestrateReply::SessionCleared(SessionCleared {
            session: request.session,
            cleared_lanes: cleared_lanes.len() as u32,
            ended_at,
            details: request.details,
        }))
    }

    pub fn retire(&self, lane: LaneIdentifier) -> Result<MetaOrchestrateReply> {
        if self.tables.first_lane_record(&lane)?.is_none() {
            return Err(Error::LaneNotRegistered {
                lane: lane.as_wire_token().to_string(),
            });
        }
        self.tables.remove_claims_for_lane(&lane)?;
        self.tables.remove_first_lane(&lane)?;
        Ok(MetaOrchestrateReply::LaneRetired(LaneRetired { lane }))
    }

    pub fn set_authority(&self, change: LaneAuthorityChange) -> Result<MetaOrchestrateReply> {
        let Some(mut registration) = self.tables.first_lane_record(&change.lane)? else {
            return Err(Error::LaneNotRegistered {
                lane: change.lane.as_wire_token().to_string(),
            });
        };
        registration.assignment.owner.authority = change.authority;
        registration.updated_at = self.tables.current_timestamp()?;
        self.tables.insert_lane(&registration)?;
        Ok(MetaOrchestrateReply::LaneAuthoritySet(LaneAuthoritySet {
            lane: change.lane,
            authority: change.authority,
        }))
    }

    /// Reap dead lane records, then report the current live registry. Every
    /// read of the registry first reconciles it, so the observed count always
    /// reflects only real lanes without a background timer: a terminal record
    /// past its retention window and an `Active` lane idle past the generous
    /// liveness window are both hard-deleted here.
    pub fn reconcile(&self) -> Result<LaneReconciliation> {
        let reaper = LaneReaper::new(self.tables.current_timestamp()?);
        let mut reconciliation = LaneReconciliation::none();
        for lane in self.tables.lane_records()? {
            let Some(reason) = reaper.reap_reason(&lane) else {
                continue;
            };
            // An idle `Active` lane is a leaked owner: flag its worktrees
            // `Abandoned` (durable status only, never a filesystem effect) so a
            // later `ConcludeWorktree` — the shared teardown primitive — can
            // reclaim them. The projection catches up on the next worktree op.
            if matches!(reason, LaneReapReason::ActiveIdle) {
                reconciliation.flagged_abandoned_worktrees += self
                    .tables
                    .mark_worktrees_abandoned_for_lane(lane.assignment.lane.as_str())?;
            }
            self.tables.remove_claims_for_lane(&lane.assignment.lane)?;
            self.tables
                .remove_lane(&lane.assignment.session, &lane.assignment.lane)?;
            reconciliation.record(reason);
        }
        Ok(reconciliation)
    }

    /// Return the next durable lane expiry. Lifecycle mutations publish this
    /// deadline to the daemon-owned reclaimer, which sleeps until it rather
    /// than scanning the registry on an interval.
    pub fn next_reclamation_deadline(&self) -> Result<Option<TimestampNanos>> {
        Ok(self
            .tables
            .lane_records()?
            .into_iter()
            .map(|lane| LaneReaper::deadline_for(&lane))
            .min_by_key(|deadline| deadline.value()))
    }

    pub fn observe(&self) -> Result<OrchestrateReply> {
        self.reconcile()?;
        let observed_at = self.tables.current_timestamp()?;
        let claims = self.tables.claim_records()?;
        let mut lanes = self
            .tables
            .lane_records()?
            .into_iter()
            .map(|registration| self.projection_for(registration, &claims, observed_at))
            .collect::<Result<Vec<_>>>()?;
        lanes.sort_by(|left, right| {
            left.registration
                .assignment
                .session
                .cmp(&right.registration.assignment.session)
                .then_with(|| {
                    left.registration
                        .assignment
                        .lane
                        .cmp(&right.registration.assignment.lane)
                })
        });
        Ok(OrchestrateReply::LanesObserved(LanesObserved { lanes }))
    }

    pub fn observe_session(
        &self,
        session: signal_orchestrate::SessionIdentifier,
    ) -> Result<OrchestrateReply> {
        self.reconcile()?;
        let observed_at = self.tables.current_timestamp()?;
        let claims = self.tables.claim_records()?;
        let mut lanes = self
            .tables
            .session_lane_records(&session)?
            .into_iter()
            .map(|registration| self.projection_for(registration, &claims, observed_at))
            .collect::<Result<Vec<_>>>()?;
        lanes.sort_by(|left, right| {
            left.registration
                .assignment
                .lane
                .cmp(&right.registration.assignment.lane)
        });
        Ok(OrchestrateReply::LanesObserved(LanesObserved { lanes }))
    }

    pub fn observe_sessions(&self) -> Result<OrchestrateReply> {
        self.reconcile()?;
        let mut sessions = BTreeMap::new();
        for registration in self.tables.lane_records()? {
            let active_lanes = sessions.entry(registration.assignment.session).or_insert(0);
            if registration.status == signal_orchestrate::LaneStatus::Active {
                *active_lanes += 1;
            }
        }
        Ok(OrchestrateReply::SessionsObserved(SessionsObserved {
            sessions: sessions
                .into_iter()
                .map(|(session, active_lanes)| SessionProjection {
                    session,
                    active_lanes,
                })
                .collect(),
        }))
    }

    pub fn derive_identifier(
        role: &Role,
        authority: LaneAuthority,
        prior_count: usize,
    ) -> Result<LaneIdentifier> {
        if role.tokens.is_empty() {
            return Err(Error::EmptyLaneRole);
        }
        let role_part = role
            .tokens()
            .iter()
            .map(|token| Self::pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        let with_authority = match authority {
            LaneAuthority::Structural => role_part,
            LaneAuthority::Support => format!("{role_part}-assistant"),
        };
        let lane = if prior_count == 0 {
            with_authority
        } else {
            format!("{}-{with_authority}", Self::ordinal_word(prior_count + 1)?)
        };
        Ok(LaneIdentifier::from_wire_token(lane)?)
    }

    fn projection_for(
        &self,
        registration: StoredLaneRegistration,
        claims: &[StoredClaim],
        observed_at: signal_orchestrate::TimestampNanos,
    ) -> Result<LaneProjection> {
        let resource_claims = claims
            .iter()
            .filter(|claim| claim.lane == registration.assignment.lane)
            .map(|claim| claim.resource_claim_at(observed_at))
            .collect();
        Ok(LaneProjection {
            age: registration.age_at(observed_at),
            registration: registration.registration(),
            resource_claims,
            observed_at,
        })
    }

    pub(crate) fn role_name_for(role: &Role) -> Result<RoleName> {
        let rendered = role
            .tokens()
            .iter()
            .map(|token| Self::pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        Ok(RoleName::from_wire_token(rendered)?)
    }

    fn pascal_to_kebab(value: &str) -> String {
        let mut rendered = String::new();
        for (index, character) in value.chars().enumerate() {
            if index > 0 && character.is_ascii_uppercase() {
                rendered.push('-');
            }
            rendered.push(character.to_ascii_lowercase());
        }
        rendered
    }

    fn ordinal_word(ordinal: usize) -> Result<&'static str> {
        match ordinal {
            2 => Ok("second"),
            3 => Ok("third"),
            4 => Ok("fourth"),
            5 => Ok("fifth"),
            6 => Ok("sixth"),
            7 => Ok("seventh"),
            8 => Ok("eighth"),
            9 => Ok("ninth"),
            10 => Ok("tenth"),
            _ => Err(Error::UnsupportedLaneOrdinal { ordinal }),
        }
    }
}

/// Decides, against a single reconciliation instant, whether one stored lane is
/// a real live lane to keep or a dead record to reap. Liveness is read from the
/// lane's own `updated_at` last-activity stamp: the two terminal states and the
/// `Active` state each dissolve into the same idle-past-a-window rule, only with
/// a different window. A lane is never reaped by anything but its own idle age.
struct LaneReaper {
    now: TimestampNanos,
}

impl LaneReaper {
    fn new(now: TimestampNanos) -> Self {
        Self { now }
    }

    /// Why this lane should be reaped, or `None` to keep it. `Active` lanes use
    /// the generous liveness window; terminal lanes use the short retention
    /// window. A lane whose stamp is in the future (clock skew) reads as zero
    /// idle and is always kept.
    fn reap_reason(&self, lane: &StoredLaneRegistration) -> Option<LaneReapReason> {
        let idle_nanos = self.now.value().saturating_sub(lane.updated_at.value());
        match lane.status {
            LaneStatus::Active => {
                (idle_nanos >= ACTIVE_LANE_IDLE_LIMIT_NANOS).then_some(LaneReapReason::ActiveIdle)
            }
            LaneStatus::Released | LaneStatus::HandoverEnded => (idle_nanos
                >= TERMINAL_LANE_RETENTION_NANOS)
                .then_some(LaneReapReason::TerminalExpired),
        }
    }

    fn deadline_for(lane: &StoredLaneRegistration) -> TimestampNanos {
        let retention = match lane.status {
            LaneStatus::Active => ACTIVE_LANE_IDLE_LIMIT_NANOS,
            LaneStatus::Released | LaneStatus::HandoverEnded => TERMINAL_LANE_RETENTION_NANOS,
        };
        TimestampNanos::new(lane.updated_at.value().saturating_add(retention))
    }
}

/// Why the reaper hard-deleted a lane: an `Active` lane idle past the liveness
/// window (a leaked lane whose agent is gone), or a terminal record past its
/// retention window (finished work whose brief post-mortem window elapsed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneReapReason {
    ActiveIdle,
    TerminalExpired,
}

/// The tally of one reconciliation pass: how many lanes were reaped for each
/// reason, so a caller (startup log, test) can witness the cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LaneReconciliation {
    pub reaped_idle_active: u32,
    pub reaped_terminal: u32,
    pub flagged_abandoned_worktrees: u32,
}

impl LaneReconciliation {
    fn none() -> Self {
        Self::default()
    }

    fn record(&mut self, reason: LaneReapReason) {
        match reason {
            LaneReapReason::ActiveIdle => self.reaped_idle_active += 1,
            LaneReapReason::TerminalExpired => self.reaped_terminal += 1,
        }
    }

    pub fn total_reaped(&self) -> u32 {
        self.reaped_idle_active + self.reaped_terminal
    }
}
