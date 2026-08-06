use std::collections::BTreeMap;

use meta_signal_orchestrate::{
    LaneAlreadyRegistered, LaneAlreadyRegisteredResolution, LaneAuthorityChange, LaneAuthoritySet,
    LaneRegistered, LaneRegistrationMode, LaneRegistrationRequest, LaneRetired, LaneUnregistered,
    LaneUnregistrationRequest, MetaOrchestrateReply, SessionClearRequest, SessionCleared,
};
use signal_orchestrate::{
    LaneAuthority, LaneIdentifier, LaneProjection, LanesObserved, OrchestrateReply, Role,
    RoleIdentifier, SessionProjection, SessionsObserved,
};

use crate::{Error, OrchestrateTables, Result, StoredClaim, StoredLaneRegistration};

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
        // literally true. Lane identity is global, so no two agents hold the
        // same live lane name.
        if let Some(active) = self.tables.active_lane_record(&request.assignment.lane)? {
            let resolution = match request.mode {
                LaneRegistrationMode::Fresh => LaneAlreadyRegisteredResolution::FreshConflict,
                LaneRegistrationMode::Recovery => {
                    // Recovery is real use: resuming a lane refreshes its
                    // observational activity stamp without changing lifecycle
                    // authority over the inherited active lane.
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
        // terminal one. Superseding it and dropping its stale claims is one
        // durable ownership transition, so a partial failure cannot expose a
        // new owner beside the old owner's claims.
        let registered_at = self.tables.current_timestamp()?;
        let registration = StoredLaneRegistration::active(request.assignment, registered_at);
        self.supersede_dead_record(&registration)?;
        Ok(MetaOrchestrateReply::LaneRegistered(LaneRegistered {
            registration: registration.registration(),
        }))
    }

    /// Drop the dead terminal record for `lane`, if one remains, along with any
    /// stale claims it left behind. The caller guarantees no `Active` record
    /// holds `lane`, so whatever record is found is terminal and superseded, not
    /// a live lane. Lane identity is global, so a terminal record registered
    /// under a prior session is superseded too, never left to squat the name.
    fn supersede_dead_record(&self, registration: &StoredLaneRegistration) -> Result<()> {
        let lane = &registration.assignment.lane;
        let prior = self.tables.first_lane_record(lane)?;
        let remove_claim_keys = self
            .tables
            .claim_records()?
            .into_iter()
            .filter(|claim| claim.lane == *lane)
            .map(|claim| claim.key())
            .collect::<Vec<_>>();
        self.tables.transition_lane_ownership(
            prior.iter().cloned().collect::<Vec<_>>().as_slice(),
            std::slice::from_ref(registration),
            &remove_claim_keys,
            &[],
        )
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
        self.tables.transition_lane_ownership(
            &[],
            std::slice::from_ref(&registration),
            &self.claim_keys_for_lane(&request.lane)?,
            &[],
        )?;
        Ok(MetaOrchestrateReply::LaneUnregistered(LaneUnregistered {
            session: request.session,
            lane: request.lane,
            ended_at,
            details: request.details,
        }))
    }

    pub fn clear_session(&self, request: SessionClearRequest) -> Result<MetaOrchestrateReply> {
        let ended_at = self.tables.current_timestamp()?;
        let cleared_lanes = self.tables.session_lane_records(&request.session)?;
        let mut remove_claim_keys = Vec::new();
        for lane in &cleared_lanes {
            remove_claim_keys.extend(self.claim_keys_for_lane(&lane.assignment.lane)?);
        }
        self.tables
            .transition_lane_ownership(&cleared_lanes, &[], &remove_claim_keys, &[])?;
        // Clearing a session ends the agents that registered under it as an
        // explicit lifecycle event; their durable records remain observable.
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
        let registration = self
            .tables
            .first_lane_record(&lane)?
            .expect("lane presence checked above");
        self.tables.transition_lane_ownership(
            std::slice::from_ref(&registration),
            &[],
            &self.claim_keys_for_lane(&lane)?,
            &[],
        )?;
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

    pub fn observe(&self) -> Result<OrchestrateReply> {
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

    fn claim_keys_for_lane(&self, lane: &LaneIdentifier) -> Result<Vec<String>> {
        Ok(self
            .tables
            .claim_records()?
            .into_iter()
            .filter(|claim| claim.lane == *lane)
            .map(|claim| claim.key())
            .collect())
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

    pub(crate) fn role_identifier_for(role: &Role) -> Result<RoleIdentifier> {
        let rendered = role
            .tokens()
            .iter()
            .map(|token| Self::pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        Ok(RoleIdentifier::from_wire_token(rendered)?)
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
