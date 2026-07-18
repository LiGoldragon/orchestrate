use std::path::PathBuf;

use meta_signal_orchestrate::{MetaOrchestrateReply, RepositoryIndexRefreshed};
use signal_orchestrate::{
    OrchestrateReply, RepositoriesObserved, Repository, RepositoryIdentity, RepositoryIdentityGap,
    RepositoryIdentityState, RepositoryName, ScopeReference,
};

use crate::claim::PathRelation;
use crate::{OrchestrateLayout, OrchestrateTables, Result, StoredRepository, layout::wire_path};

pub struct RepositoryRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> RepositoryRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn refresh(&self) -> Result<MetaOrchestrateReply> {
        std::fs::create_dir_all(self.layout.git_index_root())?;
        std::fs::create_dir_all(self.layout.workspace_root().join("repos"))?;

        let refreshed_at = self.tables.current_timestamp()?;
        let mut repositories = Vec::new();
        for entry in std::fs::read_dir(self.layout.git_index_root())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name_text = entry.file_name().to_string_lossy().into_owned();
            if name_text.starts_with('.') {
                continue;
            }
            // A directory whose name cannot form a `RepositoryName` cannot be
            // indexed, mirroring the worktree scan's skip.
            let Ok(name) = RepositoryName::from_text(name_text) else {
                continue;
            };
            let path = entry.path();
            let link_path = self.layout.workspace_repository_link_path(name.as_str());
            if !link_path.exists() {
                Self::create_repository_link(&path, &link_path)?;
            }
            let identity = RepositoryIdentityProbe::new(path.clone()).derive();
            repositories.push(StoredRepository::new(
                identity,
                name,
                wire_path(&path)?,
                refreshed_at,
            ));
        }

        repositories.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
        self.tables.replace_repositories(&repositories)?;
        Ok(MetaOrchestrateReply::RepositoryIndexRefreshed(
            RepositoryIndexRefreshed::new(repositories.len().min(u32::MAX as usize) as u32),
        ))
    }

    /// The repository index with each repository's real identity — the reply
    /// to `Observe Repositories`.
    pub fn observe(&self) -> Result<OrchestrateReply> {
        let repositories = self
            .tables
            .repository_records()?
            .into_iter()
            .map(Repository::from)
            .collect();
        Ok(OrchestrateReply::RepositoriesObserved(RepositoriesObserved {
            repositories,
        }))
    }

    #[cfg(unix)]
    fn create_repository_link(
        repository_path: &std::path::Path,
        link_path: &std::path::Path,
    ) -> std::io::Result<()> {
        std::os::unix::fs::symlink(repository_path, link_path)
    }

    #[cfg(not(unix))]
    fn create_repository_link(
        repository_path: &std::path::Path,
        link_path: &std::path::Path,
    ) -> std::io::Result<()> {
        std::fs::write(link_path, repository_path.display().to_string())
    }
}

/// Reads a checkout's actual git remotes and derives the repository's real
/// identity. Never errors: every failure mode becomes a typed
/// identity-unknown gap, so a row is recorded honestly rather than dropped.
pub struct RepositoryIdentityProbe {
    checkout: PathBuf,
}

impl RepositoryIdentityProbe {
    pub fn new(checkout: PathBuf) -> Self {
        Self { checkout }
    }

    pub fn derive(&self) -> RepositoryIdentityState {
        match self.remote_url() {
            Ok(url) => match RepositoryIdentity::from_remote_url(&url) {
                Ok(identity) => RepositoryIdentityState::Identified(identity),
                Err(_) => Self::gap(format!("remote url does not name host/owner/name: {url}")),
            },
            Err(reason) => Self::gap(reason),
        }
    }

    /// The `origin` remote's URL, or the first remote when no `origin`
    /// exists. Empirical shape on the deployed jj: `jj git remote list`
    /// prints one `name url` line per remote and nothing (exit 0) for a
    /// remote-less repository.
    fn remote_url(&self) -> std::result::Result<String, String> {
        let output = std::process::Command::new("jj")
            .arg("--ignore-working-copy")
            .arg("-R")
            .arg(&self.checkout)
            .args(["git", "remote", "list"])
            .output()
            .map_err(|error| format!("jj invocation failed: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "jj git remote list failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let listing = String::from_utf8_lossy(&output.stdout).into_owned();
        let mut first = None;
        for line in listing.lines() {
            let Some((name, url)) = line.split_once(' ') else {
                continue;
            };
            if name == "origin" {
                return Ok(url.trim().to_owned());
            }
            if first.is_none() {
                first = Some(url.trim().to_owned());
            }
        }
        first.ok_or_else(|| "no git remote configured".to_owned())
    }

    fn gap(reason: String) -> RepositoryIdentityState {
        let single_line = reason.replace(['\n', '\r'], " ");
        match RepositoryIdentityGap::from_text(single_line) {
            Ok(gap) => RepositoryIdentityState::IdentityUnknown(gap),
            Err(_) => RepositoryIdentityState::IdentityUnknown(
                RepositoryIdentityGap::from_text("identity probe failed").expect("static gap text"),
            ),
        }
    }
}

/// The identity-keyed lookup surface over the repository index. A repository
/// is one entity regardless of how many local paths reach it — the git-index
/// checkout and the workspace `repos/<name>` link are the same repository —
/// so every resolution goes through this directory instead of comparing one
/// recorded path.
pub struct RepositoryDirectory<'index> {
    records: &'index [StoredRepository],
    layout: &'index OrchestrateLayout,
}

impl<'index> RepositoryDirectory<'index> {
    pub fn new(records: &'index [StoredRepository], layout: &'index OrchestrateLayout) -> Self {
        Self { records, layout }
    }

    /// Every local path this repository is reachable through: its recorded
    /// checkout path and the workspace link the index maintains for it.
    fn local_paths(&self, repository: &StoredRepository) -> [String; 2] {
        [
            repository.path.as_str().to_owned(),
            self.layout
                .workspace_repository_link_path(repository.name.as_str())
                .display()
                .to_string(),
        ]
    }

    /// The scope's owner holds this repository's whole checkout — through any
    /// of its local paths.
    pub fn scope_covers_repository(
        &self,
        scope: &ScopeReference,
        repository: &StoredRepository,
    ) -> bool {
        match scope {
            ScopeReference::Path(path) => self
                .local_paths(repository)
                .iter()
                .any(|local| PathRelation::new(path.as_str(), local).left_contains_right()),
            ScopeReference::Task(_) => false,
        }
    }

    /// The repository (if any) one of whose local paths contains the scope —
    /// the claim reaches into that repository.
    pub fn repository_covering_scope(
        &self,
        scope: &ScopeReference,
    ) -> Option<&'index StoredRepository> {
        match scope {
            ScopeReference::Path(path) => self.records.iter().find(|repository| {
                self.local_paths(repository)
                    .iter()
                    .any(|local| PathRelation::new(local, path.as_str()).left_contains_right())
            }),
            ScopeReference::Task(_) => None,
        }
    }

    /// Resolve a repository token against the index: the local alias, or the
    /// real identity's repository name when they differ.
    pub fn resolve_name(&self, name: &RepositoryName) -> Option<&'index StoredRepository> {
        self.records.iter().find(|repository| {
            repository.name.as_str() == name.as_str()
                || matches!(
                    &repository.identity,
                    RepositoryIdentityState::Identified(identity)
                        if identity.name.as_str() == name.as_str()
                )
        })
    }
}
