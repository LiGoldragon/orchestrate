use std::path::PathBuf;

use owner_signal_persona_orchestrate::{RoleCreationRejected, RoleCreationRejectionReason};
use persona_orchestrate::{
    CreateRoleOrder, HarnessKind, OrchestrateLayout, OrchestrateReply, OrchestrateRequest,
    OrchestrateService, OwnerOrchestrateReply, OwnerOrchestrateRequest,
    RefreshRepositoryIndexOrder, RoleClaim, RoleName, RoleRelease, ScopeReason, ScopeReference,
    StoreLocation, WirePath,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.is_empty() {
        print_usage();
        return Ok(());
    }

    let store = store_location();
    if let Some(parent) = store.as_path().parent() {
        std::fs::create_dir_all(parent)?;
    }
    let service = OrchestrateService::open_with_layout(&store, layout())?;
    match arguments.remove(0).as_str() {
        "role" => handle_role(&service, arguments)?,
        "repository" => handle_repository(&service, arguments)?,
        "claim" => handle_claim(&service, arguments)?,
        "release" => handle_release(&service, arguments)?,
        _ => print_usage(),
    }
    Ok(())
}

fn handle_role(
    service: &OrchestrateService,
    mut arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.first().map(String::as_str) {
        Some("create") => {
            arguments.remove(0);
            let role = role_argument(arguments.first(), "role create <role> <codex|claude>")?;
            let harness = harness_argument(arguments.get(1), "role create <role> <codex|claude>")?;
            let reply = service.handle_owner(OwnerOrchestrateRequest::CreateRoleOrder(
                CreateRoleOrder { role, harness },
            ))?;
            print_owner_reply(reply);
        }
        Some("list") => {
            for role in service.roles()? {
                println!(
                    "{}\t{}\t{}\t{}",
                    role.role.as_wire_token(),
                    role.harness.as_wire_token(),
                    role.report_repository_path.as_str(),
                    role.report_lane_path.as_str()
                );
            }
        }
        _ => print_usage(),
    }
    Ok(())
}

fn handle_repository(
    service: &OrchestrateService,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match arguments.first().map(String::as_str) {
        Some("refresh") => {
            let reply =
                service.handle_owner(OwnerOrchestrateRequest::RefreshRepositoryIndexOrder(
                    RefreshRepositoryIndexOrder {},
                ))?;
            print_owner_reply(reply);
        }
        Some("list") => {
            for repository in service.repositories()? {
                println!(
                    "{}\t{}\t{}",
                    repository.name,
                    repository.path.as_str(),
                    repository.active
                );
            }
        }
        _ => print_usage(),
    }
    Ok(())
}

fn handle_claim(
    service: &OrchestrateService,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.len() < 3 {
        print_usage();
        return Ok(());
    }
    let role = role_argument(arguments.first(), "claim <role> <absolute-path> <reason>")?;
    let path = WirePath::from_absolute_path(arguments[1].clone())?;
    let reason = ScopeReason::from_text(arguments[2..].join(" "))?;
    let reply = service.handle(OrchestrateRequest::RoleClaim(RoleClaim {
        role,
        scopes: vec![ScopeReference::Path(path)],
        reason,
    }))?;
    print_orchestrate_reply(reply);
    Ok(())
}

fn handle_release(
    service: &OrchestrateService,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let role = role_argument(arguments.first(), "release <role>")?;
    let reply = service.handle(OrchestrateRequest::RoleRelease(RoleRelease { role }))?;
    print_orchestrate_reply(reply);
    Ok(())
}

fn role_argument(
    value: Option<&String>,
    usage: &str,
) -> Result<RoleName, Box<dyn std::error::Error>> {
    match value {
        Some(value) => Ok(RoleName::from_wire_token(value.clone())?),
        None => Err(format!("usage: {usage}").into()),
    }
}

fn harness_argument(
    value: Option<&String>,
    usage: &str,
) -> Result<HarnessKind, Box<dyn std::error::Error>> {
    match value {
        Some(value) => Ok(HarnessKind::from_wire_token(value.clone())?),
        None => Err(format!("usage: {usage}").into()),
    }
}

fn print_owner_reply(reply: OwnerOrchestrateReply) {
    match reply {
        OwnerOrchestrateReply::RoleCreated(created) => {
            println!("created\t{}", created.role.as_wire_token());
            println!("harness\t{}", created.harness.as_wire_token());
            println!(
                "report_repository\t{}",
                created.report_repository_path.as_str()
            );
            println!("report_lane\t{}", created.report_lane_path.as_str());
        }
        OwnerOrchestrateReply::RoleRetired(retired) => {
            println!("retired\t{}", retired.role.as_wire_token());
        }
        OwnerOrchestrateReply::RoleCreationRejected(RoleCreationRejected { role, reason }) => {
            println!(
                "rejected\t{}\t{}",
                role.as_wire_token(),
                rejection_reason(reason)
            );
        }
        OwnerOrchestrateReply::RepositoryIndexRefreshed(refreshed) => {
            println!("repositories\t{}", refreshed.repositories);
        }
        OwnerOrchestrateReply::OwnerOrchestrateRequestUnimplemented(unimplemented) => {
            println!(
                "unimplemented\t{:?}\t{:?}",
                unimplemented.operation, unimplemented.reason
            );
        }
    }
}

fn print_orchestrate_reply(reply: OrchestrateReply) {
    match reply {
        OrchestrateReply::ClaimAcceptance(accepted) => {
            println!("claimed\t{}", accepted.role.as_wire_token());
        }
        OrchestrateReply::ClaimRejection(rejected) => {
            println!(
                "rejected\t{}\t{}",
                rejected.role.as_wire_token(),
                rejected.conflicts.len()
            );
        }
        OrchestrateReply::ReleaseAcknowledgment(acknowledgment) => {
            println!(
                "released\t{}\t{}",
                acknowledgment.role.as_wire_token(),
                acknowledgment.released_scopes.len()
            );
        }
        other => println!("{other:?}"),
    }
}

fn rejection_reason(reason: RoleCreationRejectionReason) -> &'static str {
    match reason {
        RoleCreationRejectionReason::RoleAlreadyExists => "role-already-exists",
        RoleCreationRejectionReason::ReportRepositoryAlreadyExists => {
            "report-repository-already-exists"
        }
        RoleCreationRejectionReason::ReportLaneAlreadyExists => "report-lane-already-exists",
    }
}

fn store_location() -> StoreLocation {
    StoreLocation::new(
        std::env::var("PERSONA_ORCHESTRATE_STORE").unwrap_or_else(|_| {
            "/home/li/primary/.persona-orchestrate/persona-orchestrate.redb".to_string()
        }),
    )
}

fn layout() -> OrchestrateLayout {
    let workspace = std::env::var("PERSONA_ORCHESTRATE_WORKSPACE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/home/li/primary"));
    let git_index = std::env::var("PERSONA_ORCHESTRATE_GIT_INDEX")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/git/github.com/LiGoldragon"));
    OrchestrateLayout::new(workspace, git_index)
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!("  persona-orchestrate-daemon role create <role> <codex|claude>");
    eprintln!("  persona-orchestrate-daemon role list");
    eprintln!("  persona-orchestrate-daemon repository refresh");
    eprintln!("  persona-orchestrate-daemon repository list");
    eprintln!("  persona-orchestrate-daemon claim <role> <absolute-path> <reason>");
    eprintln!("  persona-orchestrate-daemon release <role>");
}
