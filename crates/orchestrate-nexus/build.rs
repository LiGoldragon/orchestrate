//! The Nexus's Memory is generated from memory.ethos by ethos-zero; the
//! committed Rust must equal fresh generation.

use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    println!("cargo:rerun-if-changed=memory.ethos");
    println!("cargo:rerun-if-changed=src/memory.rs");
    let source = std::fs::read_to_string(root.join("memory.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .unwrap_or_else(|_| panic!("read memory.ethos"));
    let generated = file
        .generate()
        .unwrap_or_else(|_| panic!("generate memory.ethos"));
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/memory.rs")).expect("generated"),
        "src/memory.rs is stale; regenerate it from memory.ethos"
    );
}
