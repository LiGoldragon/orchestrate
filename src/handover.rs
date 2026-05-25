use signal_orchestrate::LaneRegistration;
use signal_version_handover::MirrorPayload;
use version_projection::{ComponentName, ContractVersion, RecordKind};

use crate::{Error, OrchestrateTables, Result, StoredClaim};

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
    pub lanes: Vec<LaneRegistration>,
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
