use std::time::{SystemTime, UNIX_EPOCH};

use sema::{SchemaVersion, Table};
use sema_engine::{Engine, EngineOpen};
use signal_persona_orchestrate::{Activity, RoleName, ScopeReason, ScopeReference, TimestampNanos};

use crate::{Result, StoreLocation};

const ORCHESTRATE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);

const CLAIMS: Table<&'static str, StoredClaim> = Table::new("claims");
const ACTIVITIES: Table<u64, StoredActivity> = Table::new("activities");
const ACTIVITY_NEXT_SLOT: Table<&'static str, u64> = Table::new("activity_next_slot");
const ACTIVITY_NEXT_SLOT_KEY: &str = "next";

pub struct OrchestrateTables {
    engine: Engine,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredClaim {
    pub role: RoleName,
    pub scope: ScopeReference,
    pub reason: ScopeReason,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredActivity {
    pub slot: u64,
    pub role: RoleName,
    pub scope: ScopeReference,
    pub reason: ScopeReason,
    pub stamped_at: TimestampNanos,
}

impl OrchestrateTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let engine = Engine::open(EngineOpen::new(store.as_path(), ORCHESTRATE_SCHEMA_VERSION))?;
        engine.storage_kernel().write(|transaction| {
            CLAIMS.ensure(transaction)?;
            ACTIVITIES.ensure(transaction)?;
            ACTIVITY_NEXT_SLOT.ensure(transaction)?;
            Ok(())
        })?;
        Ok(Self { engine })
    }

    pub fn claim_records(&self) -> Result<Vec<StoredClaim>> {
        Ok(self.engine.storage_kernel().read(|transaction| {
            Ok(CLAIMS
                .iter(transaction)?
                .into_iter()
                .map(|(_key, claim)| claim)
                .collect())
        })?)
    }

    pub fn replace_claims(
        &self,
        remove_keys: &[String],
        insert_claims: &[StoredClaim],
    ) -> Result<()> {
        self.engine.storage_kernel().write(|transaction| {
            for key in remove_keys {
                CLAIMS.remove(transaction, key.as_str())?;
            }
            for claim in insert_claims {
                let key = claim.key();
                CLAIMS.insert(transaction, key.as_str(), claim)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub fn append_activity(
        &self,
        role: RoleName,
        scope: ScopeReference,
        reason: ScopeReason,
    ) -> Result<StoredActivity> {
        let slot = self.next_activity_slot()?;
        let stamped_at = StoreClock::system().timestamp()?;
        let activity = StoredActivity::new(slot.value(), role, scope, reason, stamped_at);
        self.engine.storage_kernel().write(|transaction| {
            ACTIVITIES.insert(transaction, slot.value(), &activity)?;
            ACTIVITY_NEXT_SLOT.insert(transaction, ACTIVITY_NEXT_SLOT_KEY, &slot.next_value())?;
            Ok(())
        })?;
        Ok(activity)
    }

    pub fn activity_records(&self) -> Result<Vec<StoredActivity>> {
        Ok(self.engine.storage_kernel().read(|transaction| {
            Ok(ACTIVITIES
                .iter(transaction)?
                .into_iter()
                .map(|(_slot, activity)| activity)
                .collect())
        })?)
    }

    fn next_activity_slot(&self) -> Result<ActivitySlot> {
        let stored = self
            .engine
            .storage_kernel()
            .read(|transaction| ACTIVITY_NEXT_SLOT.get(transaction, ACTIVITY_NEXT_SLOT_KEY))?;
        match stored {
            Some(next_slot) => Ok(ActivitySlot::new(next_slot)),
            None => Ok(ActivitySlot::after_records(&self.activity_records()?)),
        }
    }
}

impl StoredActivity {
    fn new(
        slot: u64,
        role: RoleName,
        scope: ScopeReference,
        reason: ScopeReason,
        stamped_at: TimestampNanos,
    ) -> Self {
        Self {
            slot,
            role,
            scope,
            reason,
            stamped_at,
        }
    }

    pub fn into_activity(self) -> Activity {
        Activity {
            role: self.role,
            scope: self.scope,
            reason: self.reason,
            stamped_at: self.stamped_at,
        }
    }
}

impl StoredClaim {
    pub fn new(role: RoleName, scope: ScopeReference, reason: ScopeReason) -> Self {
        Self {
            role,
            scope,
            reason,
        }
    }

    pub fn key(&self) -> String {
        ClaimKey::new(self.role, &self.scope).into_string()
    }
}

struct ActivitySlot {
    value: u64,
}

impl ActivitySlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredActivity]) -> Self {
        let value = records
            .iter()
            .map(|activity| activity.slot)
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn value(&self) -> u64 {
        self.value
    }

    fn next_value(&self) -> u64 {
        self.value + 1
    }
}

struct StoreClock {
    epoch: SystemTime,
}

impl StoreClock {
    fn system() -> Self {
        Self { epoch: UNIX_EPOCH }
    }

    fn timestamp(&self) -> Result<TimestampNanos> {
        let nanos = SystemTime::now()
            .duration_since(self.epoch)?
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        Ok(TimestampNanos::new(nanos))
    }
}

struct ClaimKey {
    role: RoleName,
    scope: String,
}

impl ClaimKey {
    fn new(role: RoleName, scope: &ScopeReference) -> Self {
        Self {
            role,
            scope: ScopeKey::new(scope).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}|{}", self.role.as_wire_token(), self.scope)
    }
}

struct ScopeKey {
    value: String,
}

impl ScopeKey {
    fn new(scope: &ScopeReference) -> Self {
        let value = match scope {
            ScopeReference::Path(path) => format!("path:{}", path.as_str()),
            ScopeReference::Task(task) => format!("task:{}", task.as_str()),
        };
        Self { value }
    }

    fn into_string(self) -> String {
        self.value
    }
}
