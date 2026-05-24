use signal_orchestrate::{
    Activity, ClaimAcceptance, ClaimEntry, ClaimRejection, HandoffAcceptance, HandoffRejection,
    HandoffRejectionReason, OrchestrateReply, ReleaseAcknowledgment, RoleClaim, RoleHandoff,
    RoleName, RoleRelease, RoleSnapshot, RoleStatus, ScopeConflict, ScopeReference,
};

use crate::{OrchestrateTables, Result, StoredActivity, StoredClaim};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimState {
    role: RoleName,
    scopes: Vec<ScopeReference>,
}

impl ClaimState {
    pub fn new(role: RoleName) -> Self {
        Self {
            role,
            scopes: Vec::new(),
        }
    }

    pub fn claim(&mut self, scope: ScopeReference) {
        if !self.scopes.iter().any(|current| current == &scope) {
            self.scopes.push(scope);
        }
    }

    pub fn owns(&self, scope: &ScopeReference) -> bool {
        self.scopes.iter().any(|current| current == scope)
    }

    pub fn role(&self) -> RoleName {
        self.role.clone()
    }
}

pub struct ClaimLedger<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> ClaimLedger<'tables> {
    const ROLE_OBSERVATION_ACTIVITY_LIMIT: usize = 20;

    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    pub fn apply_claim(&self, claim: RoleClaim) -> Result<OrchestrateReply> {
        let entries = self.tables.claim_records()?;
        let conflicts = Self::conflicts_for(&entries, &claim);
        if !conflicts.is_empty() {
            return Ok(OrchestrateReply::ClaimRejection(ClaimRejection {
                role: claim.role,
                conflicts,
            }));
        }

        let mut next_entries = entries.clone();
        for scope in &claim.scopes {
            if Self::role_already_owns(&next_entries, &claim.role, scope) {
                continue;
            }
            next_entries
                .retain(|entry| entry.role != claim.role || !scope_contains(scope, &entry.scope));
            next_entries.push(StoredClaim::new(
                claim.role.clone(),
                scope.clone(),
                claim.reason.clone(),
            ));
        }

        let remove_keys = entries
            .iter()
            .filter(|entry| entry.role == claim.role)
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        let insert_claims = next_entries
            .iter()
            .filter(|entry| entry.role == claim.role)
            .cloned()
            .collect::<Vec<_>>();
        self.tables.replace_claims(&remove_keys, &insert_claims)?;

        Ok(OrchestrateReply::ClaimAcceptance(ClaimAcceptance {
            role: claim.role,
            scopes: claim.scopes,
        }))
    }

    pub fn apply_release(&self, release: RoleRelease) -> Result<OrchestrateReply> {
        let entries = self.tables.claim_records()?;
        let released_scopes = entries
            .iter()
            .filter(|entry| entry.role == release.role)
            .map(|entry| entry.scope.clone())
            .collect::<Vec<_>>();
        let remove_keys = entries
            .iter()
            .filter(|entry| entry.role == release.role)
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        self.tables.replace_claims(&remove_keys, &[])?;

        Ok(OrchestrateReply::ReleaseAcknowledgment(
            ReleaseAcknowledgment {
                role: release.role,
                released_scopes,
            },
        ))
    }

    pub fn apply_handoff(&self, handoff: RoleHandoff) -> Result<OrchestrateReply> {
        let entries = self.tables.claim_records()?;
        if !Self::source_holds_all(&entries, &handoff) {
            return Ok(OrchestrateReply::HandoffRejection(HandoffRejection {
                from: handoff.from,
                to: handoff.to,
                reason: HandoffRejectionReason::SourceRoleDoesNotHold,
            }));
        }

        let conflicts = Self::target_conflicts_for(&entries, &handoff);
        if !conflicts.is_empty() {
            return Ok(OrchestrateReply::HandoffRejection(HandoffRejection {
                from: handoff.from,
                to: handoff.to,
                reason: HandoffRejectionReason::TargetRoleConflict(conflicts),
            }));
        }

        let remove_keys = entries
            .iter()
            .filter(|entry| Self::removed_by_handoff(entry, &handoff))
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        let insert_claims = handoff
            .scopes
            .iter()
            .filter(|scope| !Self::role_already_owns(&entries, &handoff.to, scope))
            .map(|scope| {
                StoredClaim::new(handoff.to.clone(), scope.clone(), handoff.reason.clone())
            })
            .collect::<Vec<_>>();

        self.tables.replace_claims(&remove_keys, &insert_claims)?;

        Ok(OrchestrateReply::HandoffAcceptance(HandoffAcceptance {
            from: handoff.from,
            to: handoff.to,
            scopes: handoff.scopes,
        }))
    }

    pub fn observe(&self) -> Result<OrchestrateReply> {
        let entries = self.tables.claim_records()?;
        let recent_activity = Self::recent_activity(
            self.tables.activity_records()?,
            Self::ROLE_OBSERVATION_ACTIVITY_LIMIT,
        );
        let mut role_records = self.tables.role_records()?;
        role_records.sort_by(|left, right| left.role.cmp(&right.role));
        let roles = role_records
            .into_iter()
            .map(|role| RoleStatus {
                claims: Self::claims_for(&entries, &role.role),
                role: role.role,
                harness: role.harness,
            })
            .collect();

        Ok(OrchestrateReply::RoleSnapshot(RoleSnapshot {
            roles,
            recent_activity,
        }))
    }

    fn conflicts_for(entries: &[StoredClaim], claim: &RoleClaim) -> Vec<ScopeConflict> {
        claim
            .scopes
            .iter()
            .flat_map(|scope| {
                entries
                    .iter()
                    .filter(move |entry| {
                        entry.role != claim.role && scopes_overlap(scope, &entry.scope)
                    })
                    .map(move |entry| ScopeConflict {
                        scope: scope.clone(),
                        held_by: entry.role.clone(),
                        held_reason: entry.reason.clone(),
                    })
            })
            .collect()
    }

    fn role_already_owns(entries: &[StoredClaim], role: &RoleName, scope: &ScopeReference) -> bool {
        entries
            .iter()
            .any(|entry| entry.role == *role && scope_contains(&entry.scope, scope))
    }

    fn source_holds_all(entries: &[StoredClaim], handoff: &RoleHandoff) -> bool {
        handoff
            .scopes
            .iter()
            .all(|scope| Self::role_holds_exact(entries, &handoff.from, scope))
    }

    fn role_holds_exact(entries: &[StoredClaim], role: &RoleName, scope: &ScopeReference) -> bool {
        entries
            .iter()
            .any(|entry| entry.role == *role && entry.scope == *scope)
    }

    fn target_conflicts_for(entries: &[StoredClaim], handoff: &RoleHandoff) -> Vec<ScopeConflict> {
        handoff
            .scopes
            .iter()
            .flat_map(|scope| {
                entries
                    .iter()
                    .filter(move |entry| {
                        entry.role != handoff.from
                            && entry.role != handoff.to
                            && scopes_overlap(scope, &entry.scope)
                    })
                    .map(move |entry| ScopeConflict {
                        scope: scope.clone(),
                        held_by: entry.role.clone(),
                        held_reason: entry.reason.clone(),
                    })
            })
            .collect()
    }

    fn removed_by_handoff(entry: &StoredClaim, handoff: &RoleHandoff) -> bool {
        handoff.scopes.iter().any(|scope| {
            (entry.role == handoff.from && entry.scope == *scope)
                || (entry.role == handoff.to && scope_contains(scope, &entry.scope))
        })
    }

    fn claims_for(entries: &[StoredClaim], role: &RoleName) -> Vec<ClaimEntry> {
        entries
            .iter()
            .filter(|entry| entry.role == *role)
            .map(|entry| ClaimEntry {
                scope: entry.scope.clone(),
                reason: entry.reason.clone(),
            })
            .collect()
    }

    fn recent_activity(mut records: Vec<StoredActivity>, limit: usize) -> Vec<Activity> {
        records.sort_by_key(|activity| activity.slot);
        records.reverse();
        records
            .into_iter()
            .take(limit)
            .map(StoredActivity::into_activity)
            .collect()
    }
}

fn scopes_overlap(left: &ScopeReference, right: &ScopeReference) -> bool {
    scope_contains(left, right) || scope_contains(right, left)
}

fn scope_contains(left: &ScopeReference, right: &ScopeReference) -> bool {
    match (left, right) {
        (ScopeReference::Path(left), ScopeReference::Path(right)) => {
            path_contains(left.as_str(), right.as_str())
        }
        (ScopeReference::Task(left), ScopeReference::Task(right)) => left == right,
        _ => false,
    }
}

fn path_contains(left: &str, right: &str) -> bool {
    left == "/"
        || left == right
        || right
            .strip_prefix(left)
            .is_some_and(|tail| tail.starts_with('/'))
}
