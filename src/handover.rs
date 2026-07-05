use signal_version_handover::{Date, HandoverMarker, MirrorPayload, Time};
use std::time::{SystemTime, UNIX_EPOCH};
use version_projection::{ComponentName, ContractVersion, RecordKind};

use crate::{Error, OrchestrateTables, Result, StoredClaim, StoredLaneRegistration};

const COMPONENT_NAME: &str = "orchestrate";
const MIRROR_SNAPSHOT_KIND: &str = "MirrorSnapshot";
const CURRENT_CONTRACT_VERSION: ContractVersion = ContractVersion::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorVersions {
    source: ContractVersion,
    target: ContractVersion,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MirrorSnapshot {
    pub claims: Vec<StoredClaim>,
    pub lanes: Vec<StoredLaneRegistration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum HandoverState {
    #[default]
    Active,
    Mirrored {
        restored_marker: HandoverMarker,
    },
    Ready {
        accepted_marker: HandoverMarker,
    },
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandoverClockReading {
    pub date: Date,
    pub time: Time,
}

impl MirrorVersions {
    pub const fn new(source: ContractVersion, target: ContractVersion) -> Self {
        Self { source, target }
    }

    pub const fn source(self) -> ContractVersion {
        self.source
    }

    pub const fn target(self) -> ContractVersion {
        self.target
    }
}

impl HandoverClockReading {
    pub fn now() -> Result<Self> {
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let total_seconds = elapsed.as_secs();
        let days = (total_seconds / 86_400) as i64;
        let seconds_in_day = total_seconds % 86_400;
        let (year, month, day) = Self::civil_date_from_unix_days(days);
        Ok(Self {
            date: Date::new(year as u16, month as u8, day as u8),
            time: Time::new(
                (seconds_in_day / 3_600) as u8,
                ((seconds_in_day % 3_600) / 60) as u8,
                (seconds_in_day % 60) as u8,
            ),
        })
    }

    fn civil_date_from_unix_days(days: i64) -> (i32, u32, u32) {
        let shifted = days + 719_468;
        let era = if shifted >= 0 {
            shifted
        } else {
            shifted - 146_096
        } / 146_097;
        let day_of_era = shifted - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_prime = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
        let month = month_prime + if month_prime < 10 { 3 } else { -9 };
        let year = year + if month <= 2 { 1 } else { 0 };
        (year as i32, month as u32, day as u32)
    }
}

impl MirrorSnapshot {
    pub fn capture(tables: &OrchestrateTables) -> Result<Self> {
        Ok(Self {
            claims: tables.claim_records()?,
            lanes: tables.lane_records()?,
        })
    }

    pub fn restore_into(&self, tables: &OrchestrateTables) -> Result<()> {
        tables.replace_all_claims(&self.claims)?;
        tables.replace_lanes(&self.lanes)
    }

    pub fn into_mirror_payload(self, versions: MirrorVersions) -> Result<MirrorPayload> {
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self)
            .map_err(|error| Error::MirrorArchiveEncode {
                message: error.to_string(),
            })?
            .to_vec();
        Ok(MirrorPayload {
            component: Self::component_name(),
            source_version: versions.source(),
            target_version: versions.target(),
            kind: Self::record_kind(),
            payload,
        })
    }

    pub fn from_mirror_payload(payload: &MirrorPayload) -> Result<Self> {
        Self::validate_component(&payload.component)?;
        Self::validate_kind(&payload.kind)?;
        Self::validate_target_version(payload.target_version)?;
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&payload.payload).map_err(|error| {
            Error::MirrorArchiveDecode {
                message: error.to_string(),
            }
        })
    }

    pub fn component_name() -> ComponentName {
        ComponentName::new(COMPONENT_NAME)
    }

    pub const fn current_contract_version() -> ContractVersion {
        CURRENT_CONTRACT_VERSION
    }

    pub fn record_kind() -> RecordKind {
        RecordKind::new(MIRROR_SNAPSHOT_KIND)
    }

    fn validate_component(component: &ComponentName) -> Result<()> {
        if component.as_str() == COMPONENT_NAME {
            Ok(())
        } else {
            Err(Error::MirrorComponentMismatch {
                expected: COMPONENT_NAME,
                actual: component.as_str().to_string(),
            })
        }
    }

    fn validate_kind(kind: &RecordKind) -> Result<()> {
        if kind.as_str() == MIRROR_SNAPSHOT_KIND {
            Ok(())
        } else {
            Err(Error::MirrorKindMismatch {
                expected: MIRROR_SNAPSHOT_KIND,
                actual: kind.as_str().to_string(),
            })
        }
    }

    fn validate_target_version(target: ContractVersion) -> Result<()> {
        let expected = Self::current_contract_version();
        if target == expected {
            Ok(())
        } else {
            Err(Error::MirrorTargetVersionMismatch {
                expected,
                actual: target,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HandoverClockReading;

    #[test]
    fn civil_date_from_unix_days_marks_epoch() {
        assert_eq!(
            HandoverClockReading::civil_date_from_unix_days(0),
            (1970, 1, 1)
        );
    }
}
