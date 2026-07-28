use std::fs;

use orchestrate::{OrchestrateService, StoreLocation};
use signal_orchestrate::{ActivityQuery, Observation, OrchestrateRequest};

fn store_bytes(path: &std::path::Path) -> Vec<u8> {
    fs::read(path).expect("read durable store")
}

#[tokio::test]
async fn observe_and_query_leave_the_durable_store_byte_identical() {
    let temporary = tempfile::tempdir().expect("temporary store directory");
    let store = temporary.path().join("orchestrate.sema");
    let mut service = OrchestrateService::open(&StoreLocation::new(store.to_str().expect("utf8")))
        .expect("open service");

    let before_observe = store_bytes(&store);
    service
        .handle(OrchestrateRequest::Observe(Observation::Lanes))
        .await
        .expect("observe lanes");
    assert_eq!(
        store_bytes(&store),
        before_observe,
        "observe must not write"
    );

    let before_query = store_bytes(&store);
    service
        .handle(OrchestrateRequest::Query(ActivityQuery {
            limit: 64,
            filters: Vec::new(),
        }))
        .await
        .expect("query activity");
    assert_eq!(store_bytes(&store), before_query, "query must not write");
}

#[test]
fn production_source_has_no_repository_or_process_probe_path() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in walk(&root) {
        let path = entry.expect("source path");
        let text = fs::read_to_string(&path).expect("source text");
        for forbidden in [
            "Command::new",
            "flock",
            "canonicalize",
            "\"/proc\"",
            "pidfd",
        ] {
            assert!(
                !text.contains(forbidden),
                "{} retains forbidden host access {forbidden}",
                path.display()
            );
        }
    }
}

fn walk(root: &std::path::Path) -> Vec<std::io::Result<std::path::PathBuf>> {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("source directory") {
            let entry = entry.map(|entry| entry.path());
            if let Ok(path) = &entry
                && path.is_dir()
            {
                pending.push(path.clone());
                continue;
            }
            paths.push(entry);
        }
    }
    paths
}
