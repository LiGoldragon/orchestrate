//! Durable role state.
//!
//! A role creation records the declared identity and its conventional paths;
//! it never creates directories, links, lock files, or a role registry file.

use meta_signal_orchestrate::{
    CreateRoleOrder, MetaOrchestrateReply, RetireRoleOrder, RoleCreated, RoleCreationRejected,
    RoleCreationRejectionReason, RoleRetired,
};

use crate::{OrchestrateLayout, OrchestrateTables, Result, StoredRole, layout::wire_path};

pub struct RoleRegistry<'tables> {
    tables: &'tables OrchestrateTables,
    layout: &'tables OrchestrateLayout,
}

impl<'tables> RoleRegistry<'tables> {
    pub fn new(tables: &'tables OrchestrateTables, layout: &'tables OrchestrateLayout) -> Self {
        Self { tables, layout }
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
        let report_repository = wire_path(&self.layout.report_repository_path(&order.role))?;
        let report_lane = wire_path(&self.layout.report_lane_path(&order.role))?;
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
        self.tables.remove_claims_for_role(&order.role)?;
        self.tables.remove_role(&order.role)?;
        Ok(MetaOrchestrateReply::RoleRetired(RoleRetired {
            role: order.role,
        }))
    }
}
