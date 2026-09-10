use ethos_zero::{Actualizing, File, Generating, Potential};
use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    println!("cargo:rerun-if-changed=client.ethos");
    println!("cargo:rerun-if-changed=src/generated/client.rs");
    let source = fs::read_to_string(root.join("client.ethos")).expect("read client Ethos");
    let potential = Potential::<File>::from(source);
    let file = potential
        .actualize()
        .unwrap_or_else(|_| panic!("read client Ethos"));
    let generated = file
        .generate()
        .unwrap_or_else(|_| panic!("generate client contract"));
    let committed = fs::read_to_string(root.join("src/generated/client.rs"))
        .expect("read committed client contract");
    assert_eq!(generated, committed, "committed client.rs is stale");
}
