use signal_orchestrate::{
    Activity, ClaimAcceptance, ClaimEntry, ClaimRejection, HandoffAcceptance, HandoffRejection,
    HandoffRejectionReason, LaneIdentifier, OrchestrateReply, ReleaseAcknowledgment, RoleClaim,
    RoleHandoff, RoleName, RoleRelease, RoleSnapshot, RoleStatus, ScopeConflict, ScopeReference,
};

use crate::{
    Error, LaneRegistry, OrchestrateTables, Result, StoredActivity, StoredClaim,
    StoredLaneRegistration,
};

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
        let claimant = ClaimLane::from_role_name(&claim.role)?.registered(self.tables)?;
        let entries = self.tables.claim_records()?;
        let conflicts = Self::conflicts_for(&entries, &claim, claimant.lane())?;
        if !conflicts.is_empty() {
            return Ok(OrchestrateReply::ClaimRejection(ClaimRejection {
                role: claim.role,
                conflicts,
            }));
        }

        let claimed_at = self.tables.current_timestamp()?;
        let mut next_entries = entries.clone();
        for scope in &claim.scopes {
            if Self::lane_already_owns(&next_entries, claimant.lane(), scope) {
                continue;
            }
            next_entries.retain(|entry| {
                entry.lane != *claimant.lane()
                    || !ScopeRelation::new(scope, &entry.scope).left_contains_right()
            });
            next_entries.push(StoredClaim::new(
                claimant.lane().clone(),
                scope.clone(),
                claim.reason.clone(),
                claimed_at,
            ));
        }

        let remove_keys = entries
            .iter()
            .filter(|entry| entry.lane == *claimant.lane())
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        let insert_claims = next_entries
            .iter()
            .filter(|entry| entry.lane == *claimant.lane())
            .cloned()
            .collect::<Vec<_>>();
        self.tables.replace_claims(&remove_keys, &insert_claims)?;

        Ok(OrchestrateReply::ClaimAcceptance(ClaimAcceptance {
            role: claim.role,
            scopes: claim.scopes,
        }))
    }

    pub fn apply_release(&self, release: RoleRelease) -> Result<OrchestrateReply> {
        let released_lane = ClaimLane::from_role_name(&release.role)?.registered(self.tables)?;
        let entries = self.tables.claim_records()?;
        let released_scopes = entries
            .iter()
            .filter(|entry| entry.lane == *released_lane.lane())
            .map(|entry| entry.scope.clone())
            .collect::<Vec<_>>();
        let remove_keys = entries
            .iter()
            .filter(|entry| entry.lane == *released_lane.lane())
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
        let from_lane = ClaimLane::from_role_name(&handoff.from)?.registered(self.tables)?;
        let to_lane = ClaimLane::from_role_name(&handoff.to)?.registered(self.tables)?;
        let entries = self.tables.claim_records()?;
        if !Self::source_holds_all(&entries, from_lane.lane(), &handoff) {
            return Ok(OrchestrateReply::HandoffRejection(HandoffRejection {
                from: handoff.from,
                to: handoff.to,
                reason: HandoffRejectionReason::SourceRoleDoesNotHold,
            }));
        }

        let conflicts =
            Self::target_conflicts_for(&entries, &handoff, from_lane.lane(), to_lane.lane())?;
        if !conflicts.is_empty() {
            return Ok(OrchestrateReply::HandoffRejection(HandoffRejection {
                from: handoff.from,
                to: handoff.to,
                reason: HandoffRejectionReason::TargetRoleConflict(conflicts),
            }));
        }

        let remove_keys = entries
            .iter()
            .filter(|entry| {
                Self::removed_by_handoff(entry, &handoff, from_lane.lane(), to_lane.lane())
            })
            .map(StoredClaim::key)
            .collect::<Vec<_>>();
        let claimed_at = self.tables.current_timestamp()?;
        let insert_claims = handoff
            .scopes
            .iter()
            .filter(|scope| !Self::lane_already_owns(&entries, to_lane.lane(), scope))
            .map(|scope| {
                StoredClaim::new(
                    to_lane.lane().clone(),
                    scope.clone(),
                    handoff.reason.clone(),
                    claimed_at,
                )
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
        let lane_records = self.tables.lane_records()?;
        let recent_activity = Self::recent_activity(
            self.tables.activity_records()?,
            Self::ROLE_OBSERVATION_ACTIVITY_LIMIT,
        );
        let mut role_records = self.tables.role_records()?;
        role_records.sort_by(|left, right| left.role.cmp(&right.role));
        let roles = role_records
            .into_iter()
            .map(|role| RoleStatus {
                claims: Self::claims_for_role(&entries, &lane_records, &role.role),
                role: role.role,
                harness: role.harness,
            })
            .collect();

        Ok(OrchestrateReply::RoleSnapshot(RoleSnapshot {
            roles,
            recent_activity,
        }))
    }

    fn conflicts_for(
        entries: &[StoredClaim],
        claim: &RoleClaim,
        claimant: &LaneIdentifier,
    ) -> Result<Vec<ScopeConflict>> {
        let mut conflicts = Vec::new();
        for scope in &claim.scopes {
            for entry in entries.iter().filter(|entry| {
                entry.lane != *claimant && ScopeRelation::new(scope, &entry.scope).overlaps()
            }) {
                conflicts.push(ScopeConflict {
                    scope: scope.clone(),
                    held_by: ClaimLane::new(entry.lane.clone()).as_role_name()?,
                    held_reason: entry.reason.clone(),
                });
            }
        }
        Ok(conflicts)
    }

    fn lane_already_owns(
        entries: &[StoredClaim],
        lane: &LaneIdentifier,
        scope: &ScopeReference,
    ) -> bool {
        entries.iter().any(|entry| {
            entry.lane == *lane && ScopeRelation::new(&entry.scope, scope).left_contains_right()
        })
    }

    fn source_holds_all(
        entries: &[StoredClaim],
        lane: &LaneIdentifier,
        handoff: &RoleHandoff,
    ) -> bool {
        handoff
            .scopes
            .iter()
            .all(|scope| Self::lane_holds_exact(entries, lane, scope))
    }

    fn lane_holds_exact(
        entries: &[StoredClaim],
        lane: &LaneIdentifier,
        scope: &ScopeReference,
    ) -> bool {
        entries
            .iter()
            .any(|entry| entry.lane == *lane && entry.scope == *scope)
    }

    fn target_conflicts_for(
        entries: &[StoredClaim],
        handoff: &RoleHandoff,
        from_lane: &LaneIdentifier,
        to_lane: &LaneIdentifier,
    ) -> Result<Vec<ScopeConflict>> {
        let mut conflicts = Vec::new();
        for scope in &handoff.scopes {
            for entry in entries.iter().filter(|entry| {
                entry.lane != *from_lane
                    && entry.lane != *to_lane
                    && ScopeRelation::new(scope, &entry.scope).overlaps()
            }) {
                conflicts.push(ScopeConflict {
                    scope: scope.clone(),
                    held_by: ClaimLane::new(entry.lane.clone()).as_role_name()?,
                    held_reason: entry.reason.clone(),
                });
            }
        }
        Ok(conflicts)
    }

    fn removed_by_handoff(
        entry: &StoredClaim,
        handoff: &RoleHandoff,
        from_lane: &LaneIdentifier,
        to_lane: &LaneIdentifier,
    ) -> bool {
        handoff.scopes.iter().any(|scope| {
            (entry.lane == *from_lane && entry.scope == *scope)
                || (entry.lane == *to_lane
                    && ScopeRelation::new(scope, &entry.scope).left_contains_right())
        })
    }

    fn claims_for_role(
        entries: &[StoredClaim],
        lanes: &[StoredLaneRegistration],
        role: &RoleName,
    ) -> Vec<ClaimEntry> {
        let role_lanes = lanes
            .iter()
            .filter(|lane| lane.status == signal_orchestrate::LaneStatus::Active)
            .filter_map(|lane| {
                LaneRegistry::role_name_for(&lane.assignment.owner.role)
                    .ok()
                    .filter(|owner| owner == role)
                    .map(|_| lane.assignment.lane.clone())
            })
            .collect::<Vec<_>>();
        entries
            .iter()
            .filter(|entry| role_lanes.iter().any(|lane| lane == &entry.lane))
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClaimLane {
    lane: LaneIdentifier,
}

impl ClaimLane {
    fn new(lane: LaneIdentifier) -> Self {
        Self { lane }
    }

    fn from_role_name(role: &RoleName) -> Result<Self> {
        Ok(Self::new(LaneIdentifier::from_wire_token(
            role.as_wire_token().to_string(),
        )?))
    }

    fn registered(self, tables: &OrchestrateTables) -> Result<RegisteredClaimLane> {
        if tables.active_lane_record(&self.lane)?.is_none() {
            return Err(Error::LaneNotRegistered {
                lane: self.lane.as_wire_token().to_string(),
            });
        }
        Ok(RegisteredClaimLane { lane: self.lane })
    }

    fn as_role_name(&self) -> Result<RoleName> {
        Ok(RoleName::from_wire_token(
            self.lane.as_wire_token().to_string(),
        )?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegisteredClaimLane {
    lane: LaneIdentifier,
}

impl RegisteredClaimLane {
    fn lane(&self) -> &LaneIdentifier {
        &self.lane
    }
}

struct ScopeRelation<'scope> {
    left: &'scope ScopeReference,
    right: &'scope ScopeReference,
}

impl<'scope> ScopeRelation<'scope> {
    fn new(left: &'scope ScopeReference, right: &'scope ScopeReference) -> Self {
        Self { left, right }
    }

    fn overlaps(&self) -> bool {
        self.left_contains_right() || self.right_contains_left()
    }

    fn left_contains_right(&self) -> bool {
        match (self.left, self.right) {
            (ScopeReference::Path(left), ScopeReference::Path(right)) => {
                PathRelation::new(left.as_str(), right.as_str()).left_contains_right()
            }
            (ScopeReference::Task(left), ScopeReference::Task(right)) => left == right,
            _ => false,
        }
    }

    fn right_contains_left(&self) -> bool {
        Self::new(self.right, self.left).left_contains_right()
    }
}

struct PathRelation<'path> {
    left: &'path str,
    right: &'path str,
}

impl<'path> PathRelation<'path> {
    fn new(left: &'path str, right: &'path str) -> Self {
        Self { left, right }
    }

    fn left_contains_right(&self) -> bool {
        self.left == "/"
            || self.left == self.right
            || self
                .right
                .strip_prefix(self.left)
                .is_some_and(|tail| tail.starts_with('/'))
    }
}
