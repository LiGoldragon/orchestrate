use std::path::{Path, PathBuf};

use signal_orchestrate::{RoleName, WirePath};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrateLayout {
    workspace_root: PathBuf,
    git_index_root: PathBuf,
    worktree_index_root: PathBuf,
}

impl OrchestrateLayout {
    const DEFAULT_WORKTREE_INDEX_ROOT: &'static str = "/home/li/wt/github.com/LiGoldragon";

    pub fn primary_workspace() -> Self {
        Self {
            workspace_root: PathBuf::from("/home/li/primary"),
            git_index_root: PathBuf::from("/git/github.com/LiGoldragon"),
            worktree_index_root: PathBuf::from(Self::DEFAULT_WORKTREE_INDEX_ROOT),
        }
    }

    pub fn new(workspace_root: PathBuf, git_index_root: PathBuf) -> Self {
        Self {
            workspace_root,
            git_index_root,
            worktree_index_root: PathBuf::from(Self::DEFAULT_WORKTREE_INDEX_ROOT),
        }
    }

    pub fn with_worktree_index_root(mut self, worktree_index_root: PathBuf) -> Self {
        self.worktree_index_root = worktree_index_root;
        self
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn git_index_root(&self) -> &Path {
        &self.git_index_root
    }

    pub fn worktree_index_root(&self) -> &Path {
        &self.worktree_index_root
    }

    pub fn worktrees_projection_path(&self) -> PathBuf {
        self.workspace_root
            .join("orchestrate")
            .join("worktrees.nota")
    }

    pub fn report_lane_path(&self, role: &RoleName) -> PathBuf {
        self.workspace_root
            .join("reports")
            .join(role.as_wire_token())
    }

    pub fn report_repository_name(&self, role: &RoleName) -> String {
        format!("persona-role-{}-reports", role.as_wire_token())
    }

    pub fn report_repository_path(&self, role: &RoleName) -> PathBuf {
        self.git_index_root.join(self.report_repository_name(role))
    }

    pub fn workspace_repository_link_path(&self, repository_name: &str) -> PathBuf {
        self.workspace_root.join("repos").join(repository_name)
    }

    pub fn role_lock_path(&self, role: &RoleName) -> PathBuf {
        self.workspace_root
            .join("orchestrate")
            .join(format!("{}.lock", role.as_wire_token()))
    }

    pub fn role_registry_path(&self) -> PathBuf {
        self.workspace_root.join("orchestrate").join("roles.list")
    }
}

pub fn wire_path(path: &Path) -> Result<WirePath> {
    let path = path.to_str().ok_or(Error::PathIsNotUtf8)?;
    Ok(WirePath::from_absolute_path(path)?)
}
