use owner_signal_persona_orchestrate::{
    LaneAuthorityChange, LaneAuthoritySet, LaneRegistered, LaneRegistrationRequest, LaneRetired,
    OwnerOrchestrateReply,
};
use signal_persona_orchestrate::{
    LaneAuthority, LaneIdentifier, LaneRegistration, LanesObserved, OrchestrateReply, Role,
};

use crate::{Error, OrchestrateTables, Result};

pub struct LaneRegistry<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> LaneRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    pub fn register(&self, request: LaneRegistrationRequest) -> Result<OwnerOrchestrateReply> {
        if request.role.tokens.is_empty() {
            return Err(Error::EmptyLaneRole);
        }

        let records = self.tables.lane_records()?;
        let mut prior_count = records
            .iter()
            .filter(|record| record.role == request.role && record.authority == request.authority)
            .count();
        let lane = loop {
            let candidate = Self::derive_identifier(&request.role, request.authority, prior_count)?;
            if self.tables.lane_record(&candidate)?.is_none() {
                break candidate;
            }
            prior_count += 1;
        };

        let registration = LaneRegistration {
            lane,
            role: request.role,
            authority: request.authority,
        };
        self.tables.insert_lane(&registration)?;
        Ok(OwnerOrchestrateReply::LaneRegistered(LaneRegistered {
            registration,
        }))
    }

    pub fn retire(&self, lane: LaneIdentifier) -> Result<OwnerOrchestrateReply> {
        if self.tables.lane_record(&lane)?.is_none() {
            return Err(Error::LaneNotRegistered {
                lane: lane.as_wire_token().to_string(),
            });
        }
        self.tables.remove_lane(&lane)?;
        Ok(OwnerOrchestrateReply::LaneRetired(LaneRetired { lane }))
    }

    pub fn set_authority(&self, change: LaneAuthorityChange) -> Result<OwnerOrchestrateReply> {
        let Some(mut registration) = self.tables.lane_record(&change.lane)? else {
            return Err(Error::LaneNotRegistered {
                lane: change.lane.as_wire_token().to_string(),
            });
        };
        registration.authority = change.authority;
        self.tables.insert_lane(&registration)?;
        Ok(OwnerOrchestrateReply::LaneAuthoritySet(LaneAuthoritySet {
            lane: change.lane,
            authority: change.authority,
        }))
    }

    pub fn observe(&self) -> Result<OrchestrateReply> {
        let mut lanes = self.tables.lane_records()?;
        lanes.sort_by(|left, right| left.lane.cmp(&right.lane));
        Ok(OrchestrateReply::LanesObserved(LanesObserved { lanes }))
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
            .map(|token| pascal_to_kebab(token.as_str()))
            .collect::<Vec<_>>()
            .join("-");
        let with_authority = match authority {
            LaneAuthority::Structural => role_part,
            LaneAuthority::Support => format!("{role_part}-assistant"),
        };
        let lane = if prior_count == 0 {
            with_authority
        } else {
            format!("{}-{with_authority}", ordinal_word(prior_count + 1)?)
        };
        Ok(LaneIdentifier::from_wire_token(lane)?)
    }
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
