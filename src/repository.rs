use meta_signal_orchestrate::{MetaOrchestrateReply, RepositoryIndexRefreshed};

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
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let path = entry.path();
            let link_path = self.layout.workspace_repository_link_path(&name);
            if !link_path.exists() {
                create_repository_link(&path, &link_path)?;
            }
            repositories.push(StoredRepository::new(name, wire_path(&path)?, refreshed_at));
        }

        repositories.sort_by(|left, right| left.name.cmp(&right.name));
        self.tables.replace_repositories(&repositories)?;
        Ok(MetaOrchestrateReply::RepositoryIndexRefreshed(
            RepositoryIndexRefreshed {
                repositories: repositories.len().min(u32::MAX as usize) as u32,
            },
        ))
    }
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
