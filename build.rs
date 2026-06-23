use std::{env, path::PathBuf};

use schema_rust_next::{
    MetaListenerTier, NexusDaemonShape, SocketModeBits, UpgradeListenerTier, WorkingListenerTier,
    build::{DependencySchema, GenerationDriver, GenerationPlan, ModuleEmission},
};

const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;

fn main() {
    SchemaBuild::from_environment().run();
}

struct SchemaBuild {
    crate_root: PathBuf,
}

impl SchemaBuild {
    fn from_environment() -> Self {
        Self {
            crate_root: PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir set")),
        }
    }

    fn run(&self) {
        println!("cargo:rerun-if-changed=schema/nexus.schema");
        println!("cargo:rerun-if-changed=schema/sema.schema");
        println!("cargo:rerun-if-env-changed=DEP_SIGNAL_ORCHESTRATE_SCHEMA_DIR");
        println!("cargo:rerun-if-env-changed=DEP_META_SIGNAL_ORCHESTRATE_SCHEMA_DIR");
        println!("cargo:rerun-if-env-changed=DEP_SIGNAL_AGENT_SCHEMA_DIR");

        let signal_orchestrate_schema = self.signal_orchestrate_schema();
        let meta_signal_orchestrate_schema = self.meta_signal_orchestrate_schema();
        let signal_agent_schema = self.signal_agent_schema();
        println!(
            "cargo:rustc-env=ORCHESTRATE_TEST_SIGNAL_ORCHESTRATE_SCHEMA_DIR={}",
            signal_orchestrate_schema.schema_directory().display()
        );
        println!(
            "cargo:rustc-env=ORCHESTRATE_TEST_META_SIGNAL_ORCHESTRATE_SCHEMA_DIR={}",
            meta_signal_orchestrate_schema.schema_directory().display()
        );
        println!(
            "cargo:rustc-env=ORCHESTRATE_TEST_SIGNAL_AGENT_SCHEMA_DIR={}",
            signal_agent_schema.schema_directory().display()
        );

        GenerationDriver::new(
            GenerationPlan::daemon_runtime(&self.crate_root, "orchestrate", "0.3.1")
                .with_dependency_schema(signal_orchestrate_schema)
                .with_dependency_schema(meta_signal_orchestrate_schema)
                .with_dependency_schema(signal_agent_schema)
                .with_module(ModuleEmission::daemon_module("nexus", Self::daemon_shape())),
        )
        .generate()
        .expect("generate orchestrate runtime schema artifacts")
        .write_or_check("ORCHESTRATE_UPDATE_SCHEMA_ARTIFACTS")
        .expect("checked-in orchestrate runtime schema artifacts are fresh");
    }

    fn signal_orchestrate_schema(&self) -> DependencySchema {
        DependencySchema::from_cargo_metadata("signal-orchestrate", "signal-orchestrate", "0.2.0")
            .expect("read signal-orchestrate schema metadata")
            .expect("signal-orchestrate must emit schema metadata")
    }

    fn meta_signal_orchestrate_schema(&self) -> DependencySchema {
        DependencySchema::from_cargo_metadata(
            "meta-signal-orchestrate",
            "meta-signal-orchestrate",
            "0.2.0",
        )
        .expect("read meta-signal-orchestrate schema metadata")
        .expect("meta-signal-orchestrate must emit schema metadata")
    }

    fn signal_agent_schema(&self) -> DependencySchema {
        DependencySchema::from_cargo_metadata("signal-agent", "signal-agent", "0.2.0")
            .expect("read signal-agent schema metadata")
            .expect("signal-agent must emit schema metadata")
    }

    fn daemon_shape() -> NexusDaemonShape {
        NexusDaemonShape::new(
            "orchestrate-daemon",
            WorkingListenerTier::dependency("signal_orchestrate::schema::lib"),
        )
        .with_meta_tier(MetaListenerTier::new(SocketModeBits::new(
            OWNER_ONLY_SOCKET_MODE,
        )))
        .with_upgrade_tier(UpgradeListenerTier::new(SocketModeBits::new(
            OWNER_ONLY_SOCKET_MODE,
        )))
    }
}
