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
    MetaOrchestrateReply, RegisterWorktree, WorktreeIndexRefreshed, WorktreeRegistered,
};
use signal_orchestrate::{
    BranchName, LaneName, OrchestrateReply, PurposeText, PushedState, RepositoryName,
    TimestampNanos, WirePath, Worktree, WorktreeStatus, WorktreesObserved,
};

use crate::{
    Error, OrchestrateLayout, OrchestrateTables, Result, StoredWorktree, layout::wire_path,
};

pub struct WorktreeRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> WorktreeRegistry<'tables> {
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
    /// [`crate::RepositoryRegistry::refresh`]. Worktrees previously registered
    /// with a richer status/purpose are re-derived from the filesystem as
    /// `Active` here; the scan is a discovery floor, registration the
    /// authoritative source.
    pub fn refresh(&self) -> Result<MetaOrchestrateReply> {
        let root = self.layout.worktree_index_root();
        std::fs::create_dir_all(root)?;
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
                worktrees.push(self.scan_worktree(repository.clone(), branch, &path)?);
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
    ) -> Result<StoredWorktree> {
        let probe = WorktreePathProbe::from_path(path);
        let derived = probe.derive()?;
        let lane = self.derive_owning_lane(&branch);
        let purpose = PurposeText::from_text(format!("scanned worktree {}", branch.as_str()))
            .unwrap_or_else(|_| {
                PurposeText::from_text("scanned worktree").expect("static purpose is valid")
            });
        Ok(StoredWorktree {
            repository,
            branch,
            path: wire_path(path)?,
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

    fn run_jj(&self, arguments: &[&str]) -> Result<String> {
        let output = Command::new("jj")
            .arg("--ignore-working-copy")
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
