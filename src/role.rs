use owner_signal_persona_orchestrate::{
    CreateRoleOrder, OwnerOrchestrateReply, RetireRoleOrder, RoleCreated, RoleCreationRejected,
    RoleCreationRejectionReason, RoleRetired,
};
use signal_persona_orchestrate::{HarnessKind, RoleName};

use crate::{OrchestrateLayout, OrchestrateTables, Result, StoredRole, layout::wire_path};

pub struct RoleRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> RoleRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
    }

    pub fn seed_current_workspace_roles(&self) -> Result<()> {
        for token in RoleName::CURRENT_WORKSPACE_ROLE_TOKENS {
            let role = RoleName::from_wire_token(token)?;
            let harness = current_workspace_harness(token);
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

    pub fn create_role(&self, order: CreateRoleOrder) -> Result<OwnerOrchestrateReply> {
        if self.tables.role_record(&order.role)?.is_some() {
            return Ok(OwnerOrchestrateReply::RoleCreationRejected(
                RoleCreationRejected {
                    role: order.role,
                    reason: RoleCreationRejectionReason::RoleAlreadyExists,
                },
            ));
        }

        let report_repository_path = self.layout.report_repository_path(&order.role);
        if report_repository_path.exists() {
            return Ok(OwnerOrchestrateReply::RoleCreationRejected(
                RoleCreationRejected {
                    role: order.role,
                    reason: RoleCreationRejectionReason::ReportRepositoryAlreadyExists,
                },
            ));
        }

        let report_lane_path = self.layout.report_lane_path(&order.role);
        if report_lane_path.exists() {
            return Ok(OwnerOrchestrateReply::RoleCreationRejected(
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
        create_report_lane(&report_repository_path, &report_lane_path)?;

        let report_repository = wire_path(&report_repository_path)?;
        let report_lane = wire_path(&report_lane_path)?;
        self.tables.insert_role(&StoredRole::new(
            order.role.clone(),
            order.harness,
            report_repository.clone(),
            report_lane.clone(),
        ))?;

        Ok(OwnerOrchestrateReply::RoleCreated(RoleCreated {
            role: order.role,
            harness: order.harness,
            report_repository_path: report_repository,
            report_lane_path: report_lane,
        }))
    }

    pub fn retire_role(&self, order: RetireRoleOrder) -> Result<OwnerOrchestrateReply> {
        self.tables.remove_role(&order.role)?;
        Ok(OwnerOrchestrateReply::RoleRetired(RoleRetired {
            role: order.role,
        }))
    }
}

fn current_workspace_harness(token: &str) -> HarnessKind {
    match token {
        "designer"
        | "designer-assistant"
        | "second-designer-assistant"
        | "poet"
        | "poet-assistant" => HarnessKind::Claude,
        _ => HarnessKind::Codex,
    }
}

#[cfg(unix)]
fn create_report_lane(
    report_repository_path: &std::path::Path,
    report_lane_path: &std::path::Path,
) -> std::io::Result<()> {
    std::os::unix::fs::symlink(report_repository_path, report_lane_path)
}

#[cfg(not(unix))]
fn create_report_lane(
    report_repository_path: &std::path::Path,
    report_lane_path: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::create_dir(report_lane_path)?;
    std::fs::write(
        report_lane_path.join("REPORT_REPOSITORY"),
        report_repository_path.display().to_string(),
    )
}
