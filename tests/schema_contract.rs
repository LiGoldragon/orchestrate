use schema::{Leg, LoadedSchema, Name, Projection, RouteBody, StandardProjection};
use std::path::PathBuf;

fn schema_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schema")
        .join(name)
}

fn name(value: &str) -> Name {
    Name::new(value).expect("schema name")
}

#[test]
fn orchestrate_schema_reads_local_imports_and_lowers_both_contract_legs() {
    let loaded = LoadedSchema::read_path(schema_file("orchestrate-v0-1.schema"))
        .expect("orchestrate schema reads");
    let assembled = loaded.assembled();

    assert_eq!(assembled.routes().len(), 13);
    assert!(
        assembled
            .imports()
            .iter()
            .any(|binding| binding.binding().as_str() == "Common")
    );
    assert!(
        assembled
            .imports()
            .iter()
            .any(|binding| binding.binding().as_str() == "Storage")
    );

    let claim = assembled
        .route_for_short_header(Leg::Ordinary, u64::from_le_bytes([0, 0, 0, 0, 0, 0, 0, 0]))
        .expect("claim route");
    assert_eq!(claim.root().as_str(), "Claim");
    assert_eq!(claim.endpoint().name().as_str(), "RoleClaim");
    assert_eq!(claim.body(), &RouteBody::Type(name("RoleClaim")));

    let watch = assembled
        .route_for_short_header(Leg::Ordinary, u64::from_le_bytes([6, 0, 0, 0, 0, 0, 0, 0]))
        .expect("watch route");
    assert_eq!(watch.root().as_str(), "Watch");
    assert_eq!(watch.endpoint().name().as_str(), "ObservationSubscription");
    assert_eq!(
        watch.body(),
        &RouteBody::Type(name("ObservationSubscription"))
    );

    let create = assembled
        .route_for_short_header(Leg::Owner, u64::from_le_bytes([0, 0, 0, 0, 0, 0, 0, 0]))
        .expect("meta create route");
    assert_eq!(create.root().as_str(), "Create");
    assert_eq!(create.endpoint().name().as_str(), "CreateRoleOrder");
    assert_eq!(create.body(), &RouteBody::Type(name("CreateRoleOrder")));

    let set_authority = assembled
        .route_for_short_header(Leg::Owner, u64::from_le_bytes([4, 0, 0, 0, 0, 0, 0, 0]))
        .expect("meta set-authority route");
    assert_eq!(set_authority.root().as_str(), "SetAuthority");
    assert_eq!(
        set_authority.endpoint().name().as_str(),
        "LaneAuthorityChange"
    );
    assert_eq!(
        set_authority.body(),
        &RouteBody::Type(name("LaneAuthorityChange"))
    );

    assert!(
        assembled
            .route_for_short_header(Leg::Sema, u64::from_le_bytes([0, 0, 0, 0, 0, 0, 0, 0]))
            .is_none(),
        "orchestrate does not expose a sema socket in v0.1"
    );
}

#[test]
fn orchestrate_next_schema_plans_no_downtime_mainline_upgrade() {
    let previous = LoadedSchema::read_path(schema_file("orchestrate-v0-1.schema"))
        .expect("previous schema reads");
    let current = LoadedSchema::read_path(schema_file("orchestrate-v0-1-1.schema"))
        .expect("current schema reads");

    let plan = current
        .assembled()
        .plan_upgrade_from(previous.assembled())
        .expect("upgrade plans");

    assert!(
        plan.projections().iter().any(|projection| matches!(
            projection,
            Projection::Standard {
                name,
                kind: StandardProjection::AdditiveEnumVariant,
            } if name.as_str() == "BranchTopology"
        )),
        "the v0.1.1 schema carries the mainline/worktree topology as an additive upgrade"
    );
}

#[test]
fn orchestrate_concept_schema_is_real_schema_text() {
    LoadedSchema::read_path(schema_file("orchestrate.concept.schema"))
        .expect("concept schema remains parser-backed");
}
