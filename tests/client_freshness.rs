//! Freshness test: committed generated modules match ethos-zero output.

use ethos_zero::{Actualizing, Emitting, Potential};
use std::io::Write;
use std::process::{Command, Stdio};

fn format_rust(source: &str) -> String {
    let mut child = Command::new("rustfmt")
        .arg("--edition=2024")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("rustfmt");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(source.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "rustfmt failed");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn client_module_is_fresh() {
    let source = std::fs::read_to_string("ethos/client.ethos").expect("read client.ethos");
    let concept = Potential::from(source.as_str())
        .actualize()
        .expect("actualize client.ethos");
    let emitted = format_rust(&concept.emit().expect("emit client.ethos"));
    let committed =
        std::fs::read_to_string("src/generated/client.rs").expect("read committed client.rs");
    assert_eq!(
        emitted, committed,
        "src/generated/client.rs is stale: regenerate from ethos/client.ethos"
    );
}

#[test]
fn meta_client_module_is_fresh() {
    let source =
        std::fs::read_to_string("ethos/meta_client.ethos").expect("read meta_client.ethos");
    let concept = Potential::from(source.as_str())
        .actualize()
        .expect("actualize meta_client.ethos");
    let emitted = format_rust(&concept.emit().expect("emit meta_client.ethos"));
    let committed = std::fs::read_to_string("src/generated/meta_client.rs")
        .expect("read committed meta_client.rs");
    assert_eq!(
        emitted, committed,
        "src/generated/meta_client.rs is stale: regenerate from ethos/meta_client.ethos"
    );
}
