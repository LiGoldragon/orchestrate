use std::collections::BTreeMap;

use meta_signal_orchestrate::{
    LaneAlreadyRegistered, LaneAlreadyRegisteredResolution, LaneAuthorityChange, LaneAuthoritySet,
    LaneRegistered, LaneRegistrationMode, LaneRegistrationRequest, LaneRetired, LaneUnregistered,
    LaneUnregistrationRequest, MetaOrchestrateReply, SessionClearRequest, SessionCleared,
};
use signal_orchestrate::{
    LaneAuthority, LaneIdentifier, LaneProjection, LanesObserved, OrchestrateReply, Role, RoleName,
    SessionProjection, SessionsObserved,
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

        if let Some(active) = self
            .tables
            .lane_record(&request.assignment.session, &request.assignment.lane)?
        {
            let resolution = match request.mode {
                LaneRegistrationMode::Fresh => LaneAlreadyRegisteredResolution::FreshConflict,
                LaneRegistrationMode::Recovery => {
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

        let registered_at = self.tables.current_timestamp()?;
        let registration = StoredLaneRegistration::active(request.assignment, registered_at);
        self.tables.insert_lane(&registration)?;
        Ok(MetaOrchestrateReply::LaneRegistered(LaneRegistered {
            registration: registration.registration(),
        }))
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
            if registration.status == signal_orchestrate::LaneStatus::Active {
                *sessions.entry(registration.assignment.session).or_insert(0) += 1;
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
            .map(StoredClaim::resource_claim)
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
