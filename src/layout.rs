use std::path::{Path, PathBuf};

use signal_persona_orchestrate::{RoleName, WirePath};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrateLayout {
    workspace_root: PathBuf,
    git_index_root: PathBuf,
}

impl OrchestrateLayout {
    pub fn primary_workspace() -> Self {
        Self {
            workspace_root: PathBuf::from("/home/li/primary"),
            git_index_root: PathBuf::from("/git/github.com/LiGoldragon"),
        }
    }

    pub fn new(workspace_root: PathBuf, git_index_root: PathBuf) -> Self {
        Self {
            workspace_root,
            git_index_root,
        }
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn git_index_root(&self) -> &Path {
        &self.git_index_root
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
}

pub fn wire_path(path: &Path) -> Result<WirePath> {
    let path = path.to_str().ok_or(Error::PathIsNotUtf8)?;
    Ok(WirePath::from_absolute_path(path)?)
}
