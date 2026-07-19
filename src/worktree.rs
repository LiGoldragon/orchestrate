//! The worktree registry (Spirit eh5a).
//!
//! Parallel to [`crate::RepositoryRegistry`]: it owns the `worktrees` redb
//! table through [`OrchestrateTables`], scans
//! `<worktree_index_root>/<repository>/<branch>` (the
//! `~/wt/github.com/LiGoldragon/<repo>/<name>` layout designer lanes use),
//! derives each worktree's [`PushedState`] and `last_activity` from `jj`, and
//! serves the `RegisterWorktree` / `RefreshWorktreeIndex` meta orders plus the
//! `Observe(Worktrees)` working read. The GC manifest is the
//! [`crate::WorktreeProjection`] sibling that renders `orchestrate/worktrees.nota`.

use std::path::{Path, PathBuf};
use std::process::Command;

use meta_signal_orchestrate::{
    ArchiveWorktreeOrder, MetaOrchestrateReply, RegisterWorktree, WorktreeArchived,
    WorktreeIndexRefreshed, WorktreeRegistered,
};
use signal_orchestrate::{
    BranchName, FeatureWorktree, LaneName, MainIntegration, OrchestrateReply, PurposeText,
    PushedState, RepositoryName, ScopeReason, TeardownRefusal, TimestampNanos, WirePath, Worktree,
    WorktreeConcluded, WorktreeConclusion, WorktreeConclusionRequest, WorktreeRequest,
    WorktreeRequestRejected, WorktreeRequestRejection, WorktreeScaffolded, WorktreeStatus,
    WorktreeTeardownRefused, WorktreesObserved,
};

use crate::repository::RepositoryDirectory;
use crate::{
    Error, OrchestrateLayout, OrchestrateTables, Result, StoredWorktree, layout::wire_path,
};

pub struct WorktreeRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> WorktreeRegistry<'tables> {
    /// The newest commit reachable from `@` that is not an empty
    /// description-less placeholder — the revision a `Rejected` teardown
    /// salvages to the remote. Skipping the placeholder matters because `jj`
    /// parks the working copy on one and refuses to push it.
    const SALVAGE_REVSET: &'static str =
        r#"latest(heads(::@ & ~(empty() & description(exact:""))))"#;

    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    /// Register (upsert) a single worktree. The agent supplies repository,
    /// branch, path, owning lane, status, and purpose; the daemon re-derives
    /// `last_activity` and `pushed_state` from the worktree path so those
    /// stay infrastructure-minted, never agent-supplied.
    pub fn register(&self, order: RegisterWorktree) -> Result<MetaOrchestrateReply> {
        let supplied = order.worktree;
        let probe = WorktreePathProbe::new(supplied.path.as_str());
        let derived = probe.derive()?;
        let worktree = Worktree {
            repository: supplied.repository,
            branch: supplied.branch,
            path: supplied.path,
            owning_lane: supplied.owning_lane,
            status: supplied.status,
            purpose: supplied.purpose,
            last_activity: derived.last_activity,
            pushed_state: derived.pushed_state,
        };
        let stored = StoredWorktree::from(worktree.clone());
        self.tables.insert_worktree(&stored)?;
        Ok(MetaOrchestrateReply::WorktreeRegistered(
            WorktreeRegistered { worktree },
        ))
    }

    /// Re-scan the whole worktree index and replace the table. Mirrors
    /// [`crate::RepositoryRegistry::refresh`]. For an already-registered
    /// `(repository, branch)`, filesystem discovery refreshes only its derived
    /// `jj` facts and preserves its durable owner, status, and purpose. A new
    /// filesystem discovery starts at the scanner's `Active`/`unknown` floor.
    pub fn refresh(&self) -> Result<MetaOrchestrateReply> {
        let root = self.layout.worktree_index_root();
        std::fs::create_dir_all(root)?;
        // Filesystem discovery can re-derive only infrastructure facts. Keep
        // durable ownership, purpose, and lifecycle state for an identity we
        // already know instead of replacing those semantic facts with the
        // scanner's `unknown` fallback on every refresh.
        let registered = self.tables.worktree_records()?;
        let mut worktrees = Vec::new();
        for repository_entry in std::fs::read_dir(root)? {
            let repository_entry = repository_entry?;
            if !repository_entry.file_type()?.is_dir() {
                continue;
            }
            let repository_token = repository_entry.file_name().to_string_lossy().into_owned();
            if repository_token.starts_with('.') {
                continue;
            }
            let Ok(repository) = RepositoryName::from_text(repository_token) else {
                continue;
            };
            for branch_entry in std::fs::read_dir(repository_entry.path())? {
                let branch_entry = branch_entry?;
                if !branch_entry.file_type()?.is_dir() {
                    continue;
                }
                let branch_token = branch_entry.file_name().to_string_lossy().into_owned();
                if branch_token.starts_with('.') {
                    continue;
                }
                let Ok(branch) = BranchName::from_text(branch_token) else {
                    continue;
                };
                let path = branch_entry.path();
                if !path.join(".jj").exists() && !path.join(".git").exists() {
                    continue;
                }
                let registered_worktree = registered
                    .iter()
                    .find(|record| record.repository == repository && record.branch == branch);
                worktrees.push(self.scan_worktree(
                    repository.clone(),
                    branch,
                    &path,
                    registered_worktree,
                )?);
            }
        }

        worktrees.sort_by(|left, right| {
            (left.repository.as_str(), left.branch.as_str())
                .cmp(&(right.repository.as_str(), right.branch.as_str()))
        });
        self.tables.replace_worktrees(&worktrees)?;
        Ok(MetaOrchestrateReply::WorktreeIndexRefreshed(
            WorktreeIndexRefreshed::new(worktrees.len().min(u32::MAX as usize) as u32),
        ))
    }

    /// Transition a single registered worktree to [`WorktreeStatus::Archived`].
    ///
    /// Scans all table rows for the first entry whose path matches
    /// `order.path`, updates its status to `Archived`, re-inserts it, and
    /// returns the updated [`Worktree`] as the ack. Returns
    /// [`Error::WorktreeNotFound`] when no registered worktree carries that path.
    pub fn archive(&self, order: ArchiveWorktreeOrder) -> Result<MetaOrchestrateReply> {
        let records = self.tables.worktree_records()?;
        let mut stored = records
            .into_iter()
            .find(|r| r.path.as_str() == order.path.as_str())
            .ok_or_else(|| Error::WorktreeNotFound {
                path: order.path.as_str().to_owned(),
            })?;
        stored.status = WorktreeStatus::Archived;
        self.tables.insert_worktree(&stored)?;
        Ok(MetaOrchestrateReply::WorktreeArchived(WorktreeArchived {
            worktree: Worktree::from(stored),
        }))
    }

    /// Scaffold a fresh worktree at the canonical root
    /// (`<worktree_index_root>/<repository>/<branch>`) and register it.
    ///
    /// The daemon creates the `jj` workspace off `main`, sets the feature
    /// bookmark, derives `pushed_state`/`last_activity`, and inserts the row.
    /// Refuses (as a reply, mutating nothing) when the repository has no source
    /// checkout or when a worktree already occupies the `(repository, branch)`
    /// identity. A `jj` failure surfaces as [`Error::WorktreeScaffold`] with no
    /// row committed.
    pub fn request(&self, order: WorktreeRequest) -> Result<OrchestrateReply> {
        // Resolution goes through the identity-keyed repository index first;
        // the filesystem stays the discovery floor for an unrefreshed index.
        let repositories = self.tables.repository_records()?;
        let directory = RepositoryDirectory::new(&repositories, self.layout);
        let repository_checkout = match directory.resolve_name(&order.repository) {
            Some(row) => {
                let checkout = std::path::PathBuf::from(row.path.as_str());
                if !Self::is_checkout(&checkout) {
                    // The index knows this repository; its local hosting is
                    // gone. When the real identity is known the refusal names
                    // it — the typed seam for a future clone-on-demand.
                    let reason = match &row.identity {
                        signal_orchestrate::RepositoryIdentityState::Identified(identity) => {
                            WorktreeRequestRejection::RepositoryAbsentLocally(identity.clone())
                        }
                        signal_orchestrate::RepositoryIdentityState::IdentityUnknown(_) => {
                            WorktreeRequestRejection::RepositoryNotFound
                        }
                    };
                    return Ok(OrchestrateReply::WorktreeRequestRejected(
                        WorktreeRequestRejected { reason },
                    ));
                }
                checkout
            }
            None => {
                let checkout = self.layout.git_index_root().join(order.repository.as_str());
                if !Self::is_checkout(&checkout) {
                    return Ok(OrchestrateReply::WorktreeRequestRejected(
                        WorktreeRequestRejected {
                            reason: WorktreeRequestRejection::RepositoryNotFound,
                        },
                    ));
                }
                checkout
            }
        };
        let destination = self
            .layout
            .worktree_index_root()
            .join(order.repository.as_str())
            .join(order.branch.as_str());
        let registered = self.tables.worktree_records()?;
        let already_registered = registered.iter().any(|record| {
            record.repository.as_str() == order.repository.as_str()
                && record.branch.as_str() == order.branch.as_str()
                && record.status != WorktreeStatus::Recycled
        });
        if already_registered || Self::directory_is_occupied(&destination) {
            return Ok(OrchestrateReply::WorktreeRequestRejected(
                WorktreeRequestRejected {
                    reason: WorktreeRequestRejection::WorktreeAlreadyExists,
                },
            ));
        }
        self.scaffold_workspace(&repository_checkout, &destination, order.branch.as_str())?;
        let derived = WorktreePathProbe::from_path(&destination).derive()?;
        let worktree = Worktree {
            repository: order.repository,
            branch: order.branch,
            path: wire_path(&destination)?,
            owning_lane: order.owning_lane,
            status: WorktreeStatus::Active,
            purpose: order.purpose,
            last_activity: derived.last_activity,
            pushed_state: derived.pushed_state,
        };
        self.tables
            .insert_worktree(&StoredWorktree::from(worktree.clone()))?;
        Ok(OrchestrateReply::WorktreeScaffolded(WorktreeScaffolded {
            worktree,
        }))
    }

    /// Mark the worktree owned by `owning_lane` terminal and tear its workspace
    /// down. A `Merged` disposition lands the work on `main` first: work
    /// already an ancestor of `main` passes straight through; otherwise the
    /// daemon fetches, rebases the work onto the latest `main`, advances the
    /// `main` bookmark, and pushes — the MVP has no review gate. A rebase
    /// with real conflicts or a push that stays rejected after retry is fully
    /// unwound (`jj op restore`) and refused typed, parking the worktree.
    /// `Rejected` preserves the newest real commit on a remote
    /// `discard/<branch>` bookmark — real uncommitted working-copy changes
    /// are described first so `jj` will push them, and the empty
    /// description-less placeholder `jj` parks the working copy on is skipped
    /// — then discards everything local. On success the row transitions to
    /// [`WorktreeStatus::Recycled`]. This is the shared teardown primitive
    /// any abandonment trigger reuses.
    pub fn conclude(&self, order: WorktreeConclusionRequest) -> Result<OrchestrateReply> {
        let mut candidates = self
            .tables
            .worktree_records()?
            .into_iter()
            .filter(|record| {
                record.owning_lane.as_str() == order.owning_lane.as_str()
                    && record.status != WorktreeStatus::Recycled
            })
            .collect::<Vec<_>>();
        let mut stored = match candidates.len() {
            0 => {
                return Err(Error::WorktreeLaneNotFound {
                    lane: order.owning_lane.as_str().to_owned(),
                });
            }
            1 => candidates.pop().expect("one candidate"),
            _ => {
                let mut worktrees = candidates
                    .iter()
                    .map(|record| {
                        format!("{}/{}", record.repository.as_str(), record.branch.as_str())
                    })
                    .collect::<Vec<_>>();
                worktrees.sort();
                return Err(Error::WorktreeLaneAmbiguous {
                    lane: order.owning_lane.as_str().to_owned(),
                    worktrees: worktrees.join(", "),
                });
            }
        };
        let destination = PathBuf::from(stored.path.as_str());
        let pushed_state = WorktreePathProbe::from_path(&destination).pushed_state()?;
        let integration = match (&order.disposition, pushed_state) {
            (WorktreeConclusion::Merged, PushedState::AncestorOfMain) => {
                MainIntegration::AlreadyAncestor
            }
            (WorktreeConclusion::Merged, _) => match AutoLand::new(&destination).land()? {
                LandOutcome::Landed(integration) => integration,
                LandOutcome::Refused(reason) => {
                    stored.pushed_state =
                        WorktreePathProbe::from_path(&destination).pushed_state()?;
                    return Ok(OrchestrateReply::WorktreeTeardownRefused(
                        WorktreeTeardownRefused {
                            worktree: Worktree::from(stored),
                            reason,
                        },
                    ));
                }
            },
            (WorktreeConclusion::Rejected, _) => MainIntegration::Discarded,
        };
        let pushed_state = WorktreePathProbe::from_path(&destination).pushed_state()?;
        let repository_checkout = self
            .layout
            .git_index_root()
            .join(stored.repository.as_str());
        let workspace = Self::workspace_name(&destination);
        let branch = stored.branch.as_str().to_owned();
        let teardown_error = |path: &std::path::Path| {
            let path = path.display().to_string();
            move |message: String| Error::WorktreeTeardown { path, message }
        };
        if matches!(order.disposition, WorktreeConclusion::Rejected) {
            let discard = format!("discard/{branch}");
            // `jj git push` refuses description-less commits, and salvage must
            // not drop real working-copy changes, so name them before pushing.
            if WorktreePathProbe::from_path(&destination).holds_undescribed_changes()? {
                Self::jj_effect(
                    &destination,
                    &[
                        "describe",
                        "-r",
                        "@",
                        "-m",
                        "salvaged rejected working copy",
                    ],
                )
                .map_err(teardown_error(&destination))?;
            }
            // `--allow-backwards`: a retried teardown finds the previous
            // attempt's leftover bookmark parked on the placeholder commit.
            Self::jj_effect(
                &destination,
                &[
                    "bookmark",
                    "set",
                    &discard,
                    "-r",
                    Self::SALVAGE_REVSET,
                    "--allow-backwards",
                ],
            )
            .map_err(teardown_error(&destination))?;
            if let Err(message) =
                Self::jj_effect(&destination, &["git", "push", "--bookmark", &discard])
            {
                // A failed push must not leave the salvage bookmark behind for
                // the retry to trip over.
                let _ = Self::jj_effect(&destination, &["bookmark", "delete", &discard]);
                return Err(teardown_error(&destination)(message));
            }
        }
        Self::jj_effect(&repository_checkout, &["workspace", "forget", &workspace])
            .map_err(teardown_error(&destination))?;
        std::fs::remove_dir_all(&destination).map_err(|error| Error::WorktreeTeardown {
            path: destination.display().to_string(),
            message: format!("could not remove worktree directory: {error}"),
        })?;
        // Local bookmarks are best-effort: the remote `discard/<branch>` ref is
        // the salvage, and a missing local bookmark must not fail teardown.
        let _ = Self::jj_effect(&repository_checkout, &["bookmark", "delete", &branch]);
        if matches!(order.disposition, WorktreeConclusion::Rejected) {
            let _ = Self::jj_effect(
                &repository_checkout,
                &["bookmark", "delete", &format!("discard/{branch}")],
            );
        }
        stored.status = WorktreeStatus::Recycled;
        stored.pushed_state = pushed_state;
        self.tables.insert_worktree(&stored)?;
        Ok(OrchestrateReply::WorktreeConcluded(WorktreeConcluded {
            worktree: Worktree::from(stored),
            integration,
        }))
    }

    /// The claimant's redirect for a contended repository main: find the
    /// standing feature worktree for `(repository, lane)` or scaffold a fresh
    /// one whose branch is the lane name — the psyche-ruled default that a
    /// feature-named lane becomes the feature branch.
    pub fn feature_worktree_for(
        &self,
        repository: RepositoryName,
        lane: LaneName,
        reason: &ScopeReason,
    ) -> Result<FeatureWorktree> {
        let standing = self.tables.worktree_records()?.into_iter().find(|record| {
            record.repository.as_str() == repository.as_str()
                && record.branch.as_str() == lane.as_str()
                && record.status != WorktreeStatus::Recycled
        });
        if let Some(record) = standing {
            return Ok(FeatureWorktree::Existing(Worktree::from(record)));
        }
        let branch = BranchName::from_text(lane.as_str().to_owned())?;
        let purpose = PurposeText::from_text(reason.as_str().to_owned())?;
        match self.request(WorktreeRequest {
            repository: repository.clone(),
            branch,
            owning_lane: lane,
            purpose,
        })? {
            OrchestrateReply::WorktreeScaffolded(scaffolded) => {
                Ok(FeatureWorktree::Scaffolded(scaffolded.worktree))
            }
            OrchestrateReply::WorktreeRequestRejected(rejected) => {
                Err(Error::FeatureWorktreeUnavailable {
                    repository: repository.as_str().to_owned(),
                    reason: format!("{:?}", rejected.reason),
                })
            }
            other => Err(Error::FeatureWorktreeUnavailable {
                repository: repository.as_str().to_owned(),
                reason: format!("unexpected scaffold reply {other:?}"),
            }),
        }
    }

    /// Create the `jj` workspace off `main` and the feature bookmark. The
    /// workspace name is the canonical path's final component so
    /// [`Self::conclude`]'s `workspace forget` can name it deterministically.
    fn scaffold_workspace(
        &self,
        repository_checkout: &Path,
        destination: &Path,
        branch: &str,
    ) -> Result<()> {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| Error::WorktreeScaffold {
                path: destination.display().to_string(),
                message: format!("could not create worktree parent directory: {error}"),
            })?;
        }
        let workspace = Self::workspace_name(destination);
        let destination_text = destination.to_string_lossy().into_owned();
        let scaffold_error = || {
            let path = destination.display().to_string();
            move |message: String| Error::WorktreeScaffold { path, message }
        };
        Self::jj_effect(
            repository_checkout,
            &[
                "workspace",
                "add",
                "--revision",
                "main",
                "--name",
                &workspace,
                &destination_text,
            ],
        )
        .map_err(scaffold_error())?;
        Self::jj_effect(destination, &["bookmark", "create", branch, "-r", "@"])
            .map_err(scaffold_error())?;
        Ok(())
    }

    /// Flag every `Active` worktree owned by `owning_lane` as
    /// [`WorktreeStatus::Abandoned`], returning how many were flagged. The
    /// filesystem is never touched — this only marks orphans (owner reaped
    /// before a terminal mark) for later reclamation through [`Self::conclude`].
    pub fn flag_abandoned(&self, owning_lane: &LaneName) -> Result<u32> {
        self.tables
            .mark_worktrees_abandoned_for_lane(owning_lane.as_str())
    }

    /// A directory is a real checkout when it holds a `.jj` or `.git` entry.
    fn is_checkout(path: &Path) -> bool {
        path.join(".jj").exists() || path.join(".git").exists()
    }

    /// A destination is occupied when it exists and is not an empty directory.
    fn directory_is_occupied(path: &Path) -> bool {
        match std::fs::read_dir(path) {
            Ok(mut entries) => entries.next().is_some(),
            Err(_) => false,
        }
    }

    /// The `jj` workspace name for a canonical worktree path: its final path
    /// component (the branch directory).
    fn workspace_name(destination: &Path) -> String {
        destination
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    /// Run one `jj` write effect in `directory`, returning the trimmed stderr as
    /// the error string on non-zero exit so callers can wrap it in the right
    /// typed error for their phase.
    fn jj_effect(directory: &Path, arguments: &[&str]) -> std::result::Result<(), String> {
        let output = Command::new("jj")
            .arg("--no-pager")
            .arg("--color")
            .arg("never")
            .arg("-R")
            .arg(directory)
            .args(arguments)
            .output()
            .map_err(|error| format!("could not run jj: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
        }
    }

    /// Read the worktree table, ordered by `(repository, branch)`.
    pub fn observe(&self) -> Result<OrchestrateReply> {
        let mut records = self.tables.worktree_records()?;
        records.sort_by(|left, right| {
            (left.repository.as_str(), left.branch.as_str())
                .cmp(&(right.repository.as_str(), right.branch.as_str()))
        });
        let worktrees = records.into_iter().map(Worktree::from).collect();
        Ok(OrchestrateReply::WorktreesObserved(WorktreesObserved {
            worktrees,
        }))
    }

    fn scan_worktree(
        &self,
        repository: RepositoryName,
        branch: BranchName,
        path: &Path,
        registered: Option<&StoredWorktree>,
    ) -> Result<StoredWorktree> {
        let probe = WorktreePathProbe::from_path(path);
        let derived = probe.derive()?;
        let path = wire_path(path)?;
        if let Some(registered) =
            registered.filter(|record| record.status != WorktreeStatus::Recycled)
        {
            return Ok(StoredWorktree {
                repository,
                branch,
                path,
                owning_lane: registered.owning_lane.clone(),
                status: registered.status,
                purpose: registered.purpose.clone(),
                last_activity: derived.last_activity,
                pushed_state: derived.pushed_state,
            });
        }
        let lane = self.derive_owning_lane(&branch);
        let purpose = PurposeText::from_text(format!("scanned worktree {}", branch.as_str()))
            .unwrap_or_else(|_| {
                PurposeText::from_text("scanned worktree").expect("static purpose is valid")
            });
        Ok(StoredWorktree {
            repository,
            branch,
            path,
            owning_lane: lane,
            status: WorktreeStatus::Active,
            purpose,
            last_activity: derived.last_activity,
            pushed_state: derived.pushed_state,
        })
    }

    /// A scan cannot know which lane owns a worktree; fall back to a neutral
    /// `unknown` lane. Registration carries the real owning lane.
    fn derive_owning_lane(&self, _branch: &BranchName) -> LaneName {
        LaneName::from_text("unknown").expect("static lane name is valid")
    }
}

/// Derives the infrastructure-minted facts about one worktree path — its
/// [`PushedState`] (relationship of the working-copy branch to its remote and
/// to `main`) and `last_activity` (the working-copy commit's timestamp) — by
/// running `jj`, mirroring `orchestrate-cli`'s `verify_jj` machinery.
struct WorktreePathProbe {
    path: PathBuf,
}

struct DerivedWorktreeFacts {
    pushed_state: PushedState,
    last_activity: TimestampNanos,
}

impl WorktreePathProbe {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn from_path(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    fn derive(&self) -> Result<DerivedWorktreeFacts> {
        Ok(DerivedWorktreeFacts {
            pushed_state: self.pushed_state()?,
            last_activity: self.last_activity().unwrap_or(TimestampNanos::new(0)),
        })
    }

    /// `AncestorOfMain` when the working-copy parent is already an ancestor of
    /// `main` (merge complete); otherwise `Pushed` if the local bookmark has a
    /// real (non-`git`) remote, else `Unpushed`.
    fn pushed_state(&self) -> Result<PushedState> {
        if self.parent_is_ancestor_of_main()? {
            return Ok(PushedState::AncestorOfMain);
        }
        if self.working_branch_has_remote()? {
            Ok(PushedState::Pushed)
        } else {
            Ok(PushedState::Unpushed)
        }
    }

    fn parent_is_ancestor_of_main(&self) -> Result<bool> {
        // A worktree whose `main` bookmark is absent (a fresh repo, or one
        // tracking only a remote main) is simply not an ancestor of a local
        // main; tolerate the missing-revset error rather than failing the scan.
        match self.run_jj(&[
            "log",
            "-r",
            "@-::main",
            "--no-graph",
            "-T",
            "commit_id.short()",
        ]) {
            Ok(output) => Ok(!output.trim().is_empty()),
            Err(Error::WorktreeScan { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn working_branch_has_remote(&self) -> Result<bool> {
        // Bookmarks pointing at the working-copy parent, with their remotes;
        // a non-empty real-remote row means the branch is pushed.
        let output = self.run_jj(&[
            "bookmark",
            "list",
            "-r",
            "@-",
            "--all-remotes",
            "-T",
            "remote ++ \"\\n\"",
        ])?;
        Ok(output
            .lines()
            .map(str::trim)
            .any(|remote| !remote.is_empty() && remote != "git"))
    }

    fn last_activity(&self) -> Result<TimestampNanos> {
        let output = self.run_jj(&[
            "log",
            "-r",
            "@-",
            "--no-graph",
            "-T",
            "committer.timestamp().format(\"%s\")",
        ])?;
        let seconds = output
            .trim()
            .parse::<u64>()
            .map_err(|error| Error::WorktreeScan {
                path: self.path.display().to_string(),
                message: format!("could not parse worktree commit timestamp: {error}"),
            })?;
        Ok(TimestampNanos::new(seconds.saturating_mul(1_000_000_000)))
    }

    /// Whether the working copy holds real changes with no description — work
    /// `jj git push` would refuse and salvage must therefore describe first.
    fn holds_undescribed_changes(&self) -> Result<bool> {
        let output = self.run_jj_snapshotting(&[
            "log",
            "--no-graph",
            "-r",
            r#"@ & ~empty() & description(exact:"")"#,
            "-T",
            "commit_id.short()",
        ])?;
        Ok(!output.trim().is_empty())
    }

    fn run_jj(&self, arguments: &[&str]) -> Result<String> {
        self.run_jj_with(&["--ignore-working-copy"], arguments)
    }

    /// Reads that ask about uncommitted working-copy state must snapshot the
    /// working copy instead of ignoring it.
    fn run_jj_snapshotting(&self, arguments: &[&str]) -> Result<String> {
        self.run_jj_with(&[], arguments)
    }

    fn run_jj_with(&self, mode_arguments: &[&str], arguments: &[&str]) -> Result<String> {
        let output = Command::new("jj")
            .args(mode_arguments)
            .arg("--no-pager")
            .arg("--color")
            .arg("never")
            .arg("-R")
            .arg(&self.path)
            .args(arguments)
            .output()
            .map_err(|error| Error::WorktreeScan {
                path: self.path.display().to_string(),
                message: format!("could not run jj: {error}"),
            })?;
        if !output.status.success() {
            return Err(Error::WorktreeScan {
                path: self.path.display().to_string(),
                message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

impl From<WirePath> for WorktreePathProbe {
    fn from(path: WirePath) -> Self {
        Self::new(path.as_str())
    }
}

/// How an [`AutoLand`] attempt ended: the work landed on `main`, or the land
/// was fully unwound and the conclusion must be refused typed.
enum LandOutcome {
    Landed(MainIntegration),
    Refused(TeardownRefusal),
}

/// The MVP auto-integration: land a worktree's real work on `main` with no
/// review gate — fetch, rebase onto the latest `main`, advance the bookmark,
/// push. Every jj semantic here was verified against deployed jj 0.40:
/// a tracked `main` auto-advances on fetch; `jj rebase` reports conflicts as
/// conflicted commits (never a failed exit); `jj op restore` fully unwinds a
/// bad attempt including bookmark moves.
struct AutoLand {
    destination: PathBuf,
}

impl AutoLand {
    /// One retry after a rejected push: the remote moved between fetch and
    /// push; a second fetch-rebase-push closes the race or the land refuses.
    const PUSH_ATTEMPTS: u32 = 2;

    fn new(destination: &Path) -> Self {
        Self {
            destination: destination.to_path_buf(),
        }
    }

    fn land(&self) -> Result<LandOutcome> {
        let probe = WorktreePathProbe::from_path(&self.destination);
        if probe.holds_undescribed_changes()? {
            self.jj(&["describe", "-r", "@", "-m", "auto-land working copy"])?;
        }
        let has_remote = self.has_remote()?;
        for attempt in 1..=Self::PUSH_ATTEMPTS {
            if has_remote {
                self.jj(&["git", "fetch"])?;
            }
            let unwind_point = self.operation_head()?;
            let before = self.salvage_commit()?;
            self.jj(&[
                "rebase",
                "-b",
                WorktreeRegistry::SALVAGE_REVSET,
                "-d",
                "main",
            ])?;
            if self.holds_conflicts()? {
                self.jj(&["op", "restore", &unwind_point])?;
                return Ok(LandOutcome::Refused(TeardownRefusal::AutoRebaseConflicted));
            }
            let after = self.salvage_commit()?;
            let integration = if before == after {
                MainIntegration::FastForwarded
            } else {
                MainIntegration::Rebased
            };
            self.jj(&[
                "bookmark",
                "set",
                "main",
                "-r",
                WorktreeRegistry::SALVAGE_REVSET,
            ])?;
            if !has_remote {
                return Ok(LandOutcome::Landed(integration));
            }
            match self.jj(&["git", "push", "--bookmark", "main"]) {
                Ok(()) => return Ok(LandOutcome::Landed(integration)),
                Err(_) if attempt < Self::PUSH_ATTEMPTS => {
                    self.jj(&["op", "restore", &unwind_point])?;
                }
                Err(_) => {
                    self.jj(&["op", "restore", &unwind_point])?;
                    return Ok(LandOutcome::Refused(TeardownRefusal::MainPushRejected));
                }
            }
        }
        Ok(LandOutcome::Refused(TeardownRefusal::MainPushRejected))
    }

    /// Whether the worktree's repo has any real (non-`git`) remote — a
    /// remote-less repo lands locally and skips the push leg.
    fn has_remote(&self) -> Result<bool> {
        let output = self.read(&["git", "remote", "list"])?;
        Ok(output
            .lines()
            .map(str::trim)
            .any(|line| !line.is_empty() && !line.starts_with("git ")))
    }

    /// The newest operation-log head — the unwind point `jj op restore`
    /// returns to when a land attempt must be abandoned.
    fn operation_head(&self) -> Result<String> {
        let output = self.read(&["op", "log", "--no-graph", "-n", "1", "-T", "id.short()"])?;
        Ok(output.trim().to_owned())
    }

    fn salvage_commit(&self) -> Result<String> {
        let output = self.read(&[
            "log",
            "--no-graph",
            "-r",
            WorktreeRegistry::SALVAGE_REVSET,
            "-T",
            "commit_id",
        ])?;
        Ok(output.trim().to_owned())
    }

    fn holds_conflicts(&self) -> Result<bool> {
        let output = self.read(&[
            "log",
            "--no-graph",
            "-r",
            "::@ & conflicts()",
            "-T",
            "commit_id.short()",
        ])?;
        Ok(!output.trim().is_empty())
    }

    /// One jj write effect in the worktree, snapshotting the working copy —
    /// land operations must see uncommitted changes, never ignore them.
    fn jj(&self, arguments: &[&str]) -> Result<()> {
        let output = Command::new("jj")
            .arg("--no-pager")
            .arg("--color")
            .arg("never")
            .arg("-R")
            .arg(&self.destination)
            .args(arguments)
            .output()
            .map_err(|error| Error::WorktreeAutoLand {
                path: self.destination.display().to_string(),
                message: format!("could not run jj: {error}"),
            })?;
        if output.status.success() {
            Ok(())
        } else {
            Err(Error::WorktreeAutoLand {
                path: self.destination.display().to_string(),
                message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            })
        }
    }

    fn read(&self, arguments: &[&str]) -> Result<String> {
        let output = Command::new("jj")
            .arg("--no-pager")
            .arg("--color")
            .arg("never")
            .arg("-R")
            .arg(&self.destination)
            .args(arguments)
            .output()
            .map_err(|error| Error::WorktreeAutoLand {
                path: self.destination.display().to_string(),
                message: format!("could not run jj: {error}"),
            })?;
        if !output.status.success() {
            return Err(Error::WorktreeAutoLand {
                path: self.destination.display().to_string(),
                message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrchestrateLayout, OrchestrateTables, StoreLocation};

    /// The reaper flags an orphaned Active worktree Abandoned without any
    /// filesystem effect; already-terminal rows and other lanes are untouched.
    #[test]
    fn flag_abandoned_flips_only_active_rows_for_the_lane() {
        let temporary = tempfile::Builder::new()
            .prefix("orchestrate-flag-abandoned")
            .tempdir()
            .expect("temp dir");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let layout = OrchestrateLayout::new(
            temporary.path().join("workspace"),
            temporary.path().join("git-index"),
        );
        let tables = OrchestrateTables::open(&store).expect("tables open");

        let row = |branch: &str, lane: &str, status: WorktreeStatus| StoredWorktree {
            repository: RepositoryName::from_text("orchestrate").expect("repository"),
            branch: BranchName::from_text(branch).expect("branch"),
            path: WirePath::from_absolute_path(format!("/tmp/wt/{branch}")).expect("path"),
            owning_lane: LaneName::from_text(lane).expect("lane"),
            status,
            purpose: PurposeText::from_text("abandonment fixture").expect("purpose"),
            last_activity: TimestampNanos::new(1),
            pushed_state: PushedState::Unpushed,
        };
        tables
            .insert_worktree(&row("orphan", "GoneLane", WorktreeStatus::Active))
            .expect("insert orphan");
        tables
            .insert_worktree(&row("already", "GoneLane", WorktreeStatus::Recycled))
            .expect("insert terminal");
        tables
            .insert_worktree(&row("other", "LiveLane", WorktreeStatus::Active))
            .expect("insert other lane");

        let registry = WorktreeRegistry::new(&tables, &layout);
        let flagged = registry
            .flag_abandoned(&LaneName::from_text("GoneLane").expect("lane"))
            .expect("flag abandoned");
        assert_eq!(flagged, 1);

        let status_of = |branch: &str| {
            tables
                .worktree_records()
                .expect("records")
                .into_iter()
                .find(|record| record.branch.as_str() == branch)
                .expect("row present")
                .status
        };
        assert_eq!(status_of("orphan"), WorktreeStatus::Abandoned);
        assert_eq!(status_of("already"), WorktreeStatus::Recycled);
        assert_eq!(status_of("other"), WorktreeStatus::Active);
    }

    #[test]
    fn conclude_refuses_an_ambiguous_legacy_lane_before_touching_a_workspace() {
        let temporary = tempfile::Builder::new()
            .prefix("orchestrate-ambiguous-worktree-lane")
            .tempdir()
            .expect("temp dir");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let layout = OrchestrateLayout::new(
            temporary.path().join("workspace"),
            temporary.path().join("git-index"),
        );
        let tables = OrchestrateTables::open(&store).expect("tables open");
        let row = |repository: &str, branch: &str| StoredWorktree {
            repository: RepositoryName::from_text(repository).expect("repository"),
            branch: BranchName::from_text(branch).expect("branch"),
            path: WirePath::from_absolute_path(format!("/tmp/{repository}/{branch}"))
                .expect("path"),
            owning_lane: LaneName::from_text("LegacyLane").expect("lane"),
            status: WorktreeStatus::Active,
            purpose: PurposeText::from_text("legacy duplicate fixture").expect("purpose"),
            last_activity: TimestampNanos::new(1),
            pushed_state: PushedState::Unpushed,
        };
        tables
            .insert_worktree(&row("first-repository", "first-feature"))
            .expect("first legacy row");
        tables
            .insert_worktree(&row("second-repository", "second-feature"))
            .expect("second legacy row");

        let error = WorktreeRegistry::new(&tables, &layout)
            .conclude(WorktreeConclusionRequest {
                owning_lane: LaneName::from_text("LegacyLane").expect("lane"),
                disposition: WorktreeConclusion::Merged,
            })
            .expect_err("ambiguous lane must fail closed");
        assert!(matches!(error, Error::WorktreeLaneAmbiguous { .. }));
    }
}
