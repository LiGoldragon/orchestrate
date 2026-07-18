use signal_orchestrate::{
    Activity, ClaimAcceptance, ClaimEntry, ClaimRejection, DurationNanos, HandoffAcceptance,
    HandoffRejection, HandoffRejectionReason, LaneIdentifier, LaneName, OrchestrateReply,
    ReleaseAcknowledgment, RepositoryMainContended, RoleClaim, RoleHandoff,
    RoleName, RoleRelease, RoleSnapshot, RoleStatus, ScopeConflict, ScopeReference, WirePath,
    Worktree,
    WorktreeStatus,
};

use crate::repository::RepositoryDirectory;
use crate::{
    Error, LaneRegistry, OrchestrateLayout, OrchestrateTables, Result, StoredActivity, StoredClaim,
    StoredLaneRegistration, StoredRepository, WorktreeRegistry,
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
    layout: &'tables OrchestrateLayout,
}

impl<'tables> ClaimLedger<'tables> {
    const ROLE_OBSERVATION_ACTIVITY_LIMIT: usize = 20;

    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn apply_claim(&self, claim: RoleClaim) -> Result<OrchestrateReply> {
        let claim = Self::canonicalized_claim(claim);
        let claimant = ClaimLane::from_role_name(&claim.role)?.registered(self.tables)?;
        self.tables.touch_lane(claimant.lane())?;
        let entries = self.tables.claim_records()?;
        let conflicts = Self::conflicts_for(&entries, &claim, claimant.lane())?;
        if !conflicts.is_empty() {
            let repositories = self.tables.repository_records()?;
            let directory = RepositoryDirectory::new(&repositories, self.layout);
            if let Some(contention) =
                RepositoryContention::detect(&entries, &claim, claimant.lane(), &directory)
            {
                return contention.answer(self.tables, self.layout, &claim);
            }
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
        self.tables.touch_lane(released_lane.lane())?;
        let entries = self.tables.claim_records()?;
        let released_scopes = entries
            .iter()
            .filter(|entry| entry.lane == *released_lane.lane())
            .map(|entry| entry.scope.clone())
            .collect::<Vec<_>>();
        let started_branches = self.started_branches(&released_scopes, released_lane.lane())?;
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
                started_branches,
            },
        ))
    }

    /// Claim scopes are compared and stored in canonical filesystem form: a
    /// path that reaches a checkout through a symlink — the workspace
    /// `repos/<name>` link — names the same files as the checkout path, so
    /// both must be one scope (one repository, however many local paths).
    /// Best-effort: a path that does not (yet) exist keeps its claimed form.
    fn canonicalized_claim(claim: RoleClaim) -> RoleClaim {
        RoleClaim {
            role: claim.role,
            scopes: claim
                .scopes
                .into_iter()
                .map(Self::canonicalized_scope)
                .collect(),
            reason: claim.reason,
        }
    }

    fn canonicalized_scope(scope: ScopeReference) -> ScopeReference {
        match scope {
            ScopeReference::Path(path) => {
                let canonical = std::fs::canonicalize(path.as_str())
                    .ok()
                    .and_then(|resolved| resolved.to_str().map(str::to_owned))
                    .and_then(|resolved| WirePath::from_absolute_path(resolved).ok());
                ScopeReference::Path(canonical.unwrap_or(path))
            }
            other => other,
        }
    }

    /// The release-time contention notice: for every registered repository
    /// whose main checkout the released scopes covered, the un-integrated
    /// feature worktrees other lanes started while this lane held it —
    /// "branch X was started off this repo while you held main". A live view
    /// of the worktree registry, never a separate ledger: integrated and
    /// rejected branches have already dropped to `Recycled` and vanish here.
    fn started_branches(
        &self,
        released_scopes: &[ScopeReference],
        releasing_lane: &LaneIdentifier,
    ) -> Result<Vec<Worktree>> {
        let repositories = self.tables.repository_records()?;
        let directory = RepositoryDirectory::new(&repositories, self.layout);
        let held_repositories = repositories
            .iter()
            .filter(|repository| {
                released_scopes
                    .iter()
                    .any(|scope| directory.scope_covers_repository(scope, repository))
            })
            .map(|repository| repository.name.as_str().to_owned())
            .collect::<Vec<_>>();
        if held_repositories.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .tables
            .worktree_records()?
            .into_iter()
            .filter(|record| {
                held_repositories
                    .iter()
                    .any(|name| name == record.repository.as_str())
                    && record.status != WorktreeStatus::Recycled
                    && record.owning_lane.as_str() != releasing_lane.as_wire_token()
            })
            .map(Worktree::from)
            .collect())
    }

    pub fn apply_handoff(&self, handoff: RoleHandoff) -> Result<OrchestrateReply> {
        let from_lane = ClaimLane::from_role_name(&handoff.from)?.registered(self.tables)?;
        let to_lane = ClaimLane::from_role_name(&handoff.to)?.registered(self.tables)?;
        self.tables.touch_lane(from_lane.lane())?;
        self.tables.touch_lane(to_lane.lane())?;
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
        let observed_at = self.tables.current_timestamp()?;
        let recent_activity = Self::recent_activity(
            self.tables.activity_records()?,
            Self::ROLE_OBSERVATION_ACTIVITY_LIMIT,
        );
        let mut role_records = self.tables.role_records()?;
        role_records.sort_by(|left, right| left.role.cmp(&right.role));
        let roles = role_records
            .into_iter()
            .map(|role| RoleStatus {
                claims: Self::claims_for_role(&entries, &lane_records, &role.role, observed_at),
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
        observed_at: signal_orchestrate::TimestampNanos,
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
                claimed_at: entry.claimed_at,
                age: entry.age_at(observed_at),
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

/// The repo-main contention contact point: a claim blocked because another
/// live lane holds a registered repository's whole main checkout. Detection
/// is deliberately tight — the holder must cover the repository root (the
/// working-on-main default), not merely a narrow path inside it; narrow-path
/// conflicts keep the plain [`ClaimRejection`].
struct RepositoryContention {
    repository: StoredRepository,
    holding: StoredClaim,
}

impl RepositoryContention {
    /// First conflict pair where the requested scope falls inside a
    /// registered repository whose root the holding lane's scope covers.
    /// Resolution goes through the identity-keyed directory, so any local
    /// path of the one repository — the git-index checkout or the workspace
    /// link — names the same repository.
    fn detect(
        entries: &[StoredClaim],
        claim: &RoleClaim,
        claimant: &LaneIdentifier,
        directory: &RepositoryDirectory<'_>,
    ) -> Option<Self> {
        for scope in &claim.scopes {
            let Some(repository) = directory.repository_covering_scope(scope) else {
                continue;
            };
            let holding = entries.iter().find(|entry| {
                entry.lane != *claimant
                    && directory.scope_covers_repository(&entry.scope, repository)
            });
            if let Some(holding) = holding {
                return Some(Self {
                    repository: repository.clone(),
                    holding: holding.clone(),
                });
            }
        }
        None
    }

    /// The automatic answer: who holds main and for how long, plus the
    /// claimant's own feature worktree — scaffolded on the spot with the
    /// branch named after the claimant's lane, or the one already standing.
    fn answer(
        &self,
        tables: &OrchestrateTables,
        layout: &OrchestrateLayout,
        claim: &RoleClaim,
    ) -> Result<OrchestrateReply> {
        let repository = self.repository.name.clone();
        let lane = LaneName::from_text(claim.role.as_wire_token().to_owned())?;
        let redirect = WorktreeRegistry::new(tables, layout).feature_worktree_for(
            repository.clone(),
            lane,
            &claim.reason,
        )?;
        let observed_at = tables.current_timestamp()?;
        Ok(OrchestrateReply::RepositoryMainContended(
            RepositoryMainContended {
                repository,
                holder: ClaimLane::new(self.holding.lane.clone()).as_role_name()?,
                held_reason: self.holding.reason.clone(),
                held_age: DurationNanos::new(
                    observed_at
                        .value()
                        .saturating_sub(self.holding.claimed_at.value()),
                ),
                redirect,
            },
        ))
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

pub(crate) struct PathRelation<'path> {
    left: &'path str,
    right: &'path str,
}

impl<'path> PathRelation<'path> {
    pub(crate) fn new(left: &'path str, right: &'path str) -> Self {
        Self { left, right }
    }

    pub(crate) fn left_contains_right(&self) -> bool {
        self.left == "/"
            || self.left == self.right
            || self
                .right
                .strip_prefix(self.left)
                .is_some_and(|tail| tail.starts_with('/'))
    }
}
