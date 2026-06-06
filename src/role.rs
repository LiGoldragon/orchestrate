use meta_signal_orchestrate::{
    CreateRoleOrder, MetaOrchestrateReply, RetireRoleOrder, RoleCreated, RoleCreationRejected,
    RoleCreationRejectionReason, RoleRetired,
};
use signal_orchestrate::{HarnessKind, RoleName};

use crate::{OrchestrateLayout, OrchestrateTables, Result, StoredRole, layout::wire_path};

pub struct RoleRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

struct WorkspaceRoleRegistryFile<'layout> {
    layout: &'layout OrchestrateLayout,
}

struct WorkspaceRoleRegistryLine<'line> {
    text: &'line str,
}

struct WorkspaceRoleToken {
    token: String,
}

struct ReportLaneLink<'path> {
    repository_path: &'path std::path::Path,
    lane_path: &'path std::path::Path,
}

impl<'tables> RoleRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn seed_current_workspace_roles(&self) -> Result<()> {
        for token in WorkspaceRoleRegistryFile::new(self.layout).tokens()? {
            let role = RoleName::from_wire_token(token.as_str())?;
            let harness = token.harness();
            let report_repository_path = self.layout.report_repository_path(&role);
            let report_lane_path = self.layout.report_lane_path(&role);
            self.tables.insert_role_if_missing(&StoredRole::new(
                role,
                harness,
                wire_path(&report_repository_path)?,
                wire_path(&report_lane_path)?,
            ))?;
        }
        Ok(())
    }

    pub fn create_role(&self, order: CreateRoleOrder) -> Result<MetaOrchestrateReply> {
        if self.tables.role_record(&order.role)?.is_some() {
            return Ok(MetaOrchestrateReply::RoleCreationRejected(
                RoleCreationRejected {
                    role: order.role,
                    reason: RoleCreationRejectionReason::RoleAlreadyExists,
                },
            ));
        }

        let report_repository_path = self.layout.report_repository_path(&order.role);
        if report_repository_path.exists() {
            return Ok(MetaOrchestrateReply::RoleCreationRejected(
                RoleCreationRejected {
                    role: order.role,
                    reason: RoleCreationRejectionReason::ReportRepositoryAlreadyExists,
                },
            ));
        }

        let report_lane_path = self.layout.report_lane_path(&order.role);
        if report_lane_path.exists() {
            return Ok(MetaOrchestrateReply::RoleCreationRejected(
                RoleCreationRejected {
                    role: order.role,
                    reason: RoleCreationRejectionReason::ReportLaneAlreadyExists,
                },
            ));
        }

        std::fs::create_dir_all(&report_repository_path)?;
        std::fs::create_dir_all(
            report_lane_path
                .parent()
                .expect("report lane path has parent"),
        )?;
        ReportLaneLink::new(&report_repository_path, &report_lane_path).create()?;

        let report_repository = wire_path(&report_repository_path)?;
        let report_lane = wire_path(&report_lane_path)?;
        self.tables.insert_role(&StoredRole::new(
            order.role.clone(),
            order.harness,
            report_repository.clone(),
            report_lane.clone(),
        ))?;

        Ok(MetaOrchestrateReply::RoleCreated(RoleCreated {
            role: order.role,
            harness: order.harness,
            report_repository_path: report_repository,
            report_lane_path: report_lane,
        }))
    }

    pub fn retire_role(&self, order: RetireRoleOrder) -> Result<MetaOrchestrateReply> {
        self.tables.remove_role(&order.role)?;
        Ok(MetaOrchestrateReply::RoleRetired(RoleRetired {
            role: order.role,
        }))
    }
}

impl<'layout> WorkspaceRoleRegistryFile<'layout> {
    fn new(layout: &'layout OrchestrateLayout) -> Self {
        Self { layout }
    }

    fn tokens(&self) -> Result<Vec<WorkspaceRoleToken>> {
        let body = std::fs::read_to_string(self.layout.role_registry_path())?;
        let tokens = body
            .lines()
            .filter_map(|line| WorkspaceRoleRegistryLine { text: line }.token())
            .map(WorkspaceRoleToken::new)
            .collect();
        Ok(tokens)
    }
}

impl WorkspaceRoleRegistryLine<'_> {
    fn token(&self) -> Option<String> {
        let trimmed = self.text.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        trimmed
            .split_whitespace()
            .next()
            .map(std::string::ToString::to_string)
    }
}

impl WorkspaceRoleToken {
    fn new(token: String) -> Self {
        Self { token }
    }

    fn as_str(&self) -> &str {
        &self.token
    }

    fn harness(&self) -> HarnessKind {
        match self.as_str() {
            "designer" | "second-designer" | "third-designer" | "nota-designer"
            | "system-designer" | "cloud-designer" | "poet" | "counselor" => HarnessKind::Claude,
            _ => HarnessKind::Codex,
        }
    }
}

impl<'path> ReportLaneLink<'path> {
    fn new(repository_path: &'path std::path::Path, lane_path: &'path std::path::Path) -> Self {
        Self {
            repository_path,
            lane_path,
        }
    }

    fn create(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(self.repository_path, self.lane_path)
        }
        #[cfg(not(unix))]
        {
            std::fs::create_dir(self.lane_path)?;
            std::fs::write(
                self.lane_path.join("REPORT_REPOSITORY"),
                self.repository_path.display().to_string(),
            )
        }
    }
}
