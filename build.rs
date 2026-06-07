use std::{env, path::PathBuf};

use schema_rust_next::{
    build::{DependencySchema, GenerationDriver, GenerationPlan, ModuleEmission},
    MetaListenerTier, NexusDaemonShape, SocketModeBits, WorkingListenerTier,
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

        GenerationDriver::new(
            GenerationPlan::daemon_runtime(&self.crate_root, "orchestrate", "0.3.0")
                .with_dependency_schema(self.signal_orchestrate_schema())
                .with_dependency_schema(self.meta_signal_orchestrate_schema())
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

    fn daemon_shape() -> NexusDaemonShape {
        NexusDaemonShape::new(
            "orchestrate-daemon",
            WorkingListenerTier::dependency("signal_orchestrate::schema::lib"),
        )
        .with_meta_tier(MetaListenerTier::new(SocketModeBits::new(
            OWNER_ONLY_SOCKET_MODE,
        )))
    }
}
