use schema_next::{ImportResolver, SchemaEngine, SchemaIdentity, SchemaSourceArtifact};
use std::path::PathBuf;

fn schema_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schema")
        .join(name)
}

fn dependency_schema_directory(environment_variable: &str, checkout_name: &str) -> PathBuf {
    std::env::var_os(environment_variable)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("orchestrate has a parent directory")
                .join(checkout_name)
                .join("schema")
        })
}

fn resolver() -> ImportResolver {
    ImportResolver::new()
        .with_dependency(
            "signal-orchestrate",
            dependency_schema_directory(
                "ORCHESTRATE_TEST_SIGNAL_ORCHESTRATE_SCHEMA_DIR",
                "signal-orchestrate",
            ),
            "0.2.0",
        )
        .with_dependency(
            "meta-signal-orchestrate",
            dependency_schema_directory(
                "ORCHESTRATE_TEST_META_SIGNAL_ORCHESTRATE_SCHEMA_DIR",
                "meta-signal-orchestrate",
            ),
            "0.2.0",
        )
        .with_package(schema_next::SchemaPackage::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            "orchestrate",
            "0.3.1",
        ))
}

fn lower_schema(name: &str, module: &str) -> schema_next::Schema {
    let source = std::fs::read_to_string(schema_file(name)).expect("read schema source");
    let artifact = SchemaSourceArtifact::from_schema_text(&source).expect("schema source decodes");
    SchemaEngine::default()
        .lower_schema_source_with_resolver(
            artifact.source(),
            SchemaIdentity::new(format!("orchestrate:{module}"), "0.3.1"),
            &resolver(),
        )
        .expect("schema lowers")
}

#[test]
fn orchestrate_runtime_schemas_import_current_signal_contracts() {
    let sema = lower_schema("sema.schema", "sema");
    let nexus = lower_schema("nexus.schema", "nexus");

    assert_eq!(sema.input().variants.len(), 2);
    assert_eq!(sema.output().variants.len(), 2);
    assert_eq!(nexus.input().variants.len(), 1);
    assert_eq!(nexus.output().variants.len(), 1);
    assert!(nexus.resolved_imports().iter().any(|import| {
        import
            .use_item()
            .contains("signal_orchestrate::schema::lib::Input")
    }));
    assert!(nexus.resolved_imports().iter().any(|import| {
        import
            .use_item()
            .contains("meta_signal_orchestrate::schema::lib::Input")
    }));
}

#[test]
fn orchestrate_generated_runtime_schema_modules_compile() {
    let _ = std::mem::size_of::<orchestrate::schema::nexus::NexusWork>();
    let _ = std::mem::size_of::<orchestrate::schema::nexus::NexusAction>();
    let _ = std::mem::size_of::<orchestrate::schema::sema::SemaReadInput>();
    let _ = std::mem::size_of::<orchestrate::schema::sema::SemaWriteInput>();
}
