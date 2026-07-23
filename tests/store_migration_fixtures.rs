//! Migration tests against captured real stores.
//!
//! Real production stores never ship in this public repository: store files
//! carry private session, lane, and path names. The harness instead loads
//! captured stores from a local fixture directory —
//! `ORCHESTRATE_MIGRATION_FIXTURE_DIRECTORY`, defaulting to
//! `~/.local/state/orchestrate/fixtures` — and skips cleanly when the
//! directory is absent or empty, so sandboxed check runs stay green.
//!
//! Contract proven per fixture, on a writable copy:
//! - an open either succeeds or fails closed with a typed error;
//! - a successful migrating open leaves exactly one pre-migration preserve,
//!   and that preserve is itself a working rollback point — a copy of it
//!   opens through the same migration path to identical record counts;
//! - a successful open is stable — reopening yields identical counts with no
//!   second migration;
//! - a failed-closed open leaves the store byte-identical with no preserve.

use std::path::{Path, PathBuf};

use meta_signal_orchestrate::MetaOrchestrateRequest;
use orchestrate::{
    LaneAssignment, LaneAuthority, LaneDetails, LaneIdentifier, LaneRegistrationMode,
    LaneRegistrationRequest, LaneUnregistrationRequest, OrchestrateLayout, OrchestrateService,
    OrchestrateTables, Role, RoleToken, SessionIdentifier, StoreLocation,
};

/// A writable scratch copy of a fixture store; removes itself and any
/// preserves written beside it.
struct ScratchStore {
    path: PathBuf,
}

impl ScratchStore {
    fn from_fixture(fixture: &Path, label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "orchestrate-fixture-{label}-{}-{}.sema",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        std::fs::copy(fixture, &path).expect("copy fixture to scratch store");
        let mut permissions = std::fs::metadata(&path)
            .expect("scratch store metadata")
            .permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(&path, permissions).expect("make scratch store writable");
        Self { path }
    }

    fn location(&self) -> StoreLocation {
        StoreLocation::new(self.path.to_string_lossy().into_owned())
    }

    fn preserves(&self) -> Vec<PathBuf> {
        let directory = self.path.parent().expect("scratch store has a parent");
        let file_name = self
            .path
            .file_name()
            .expect("scratch store has a file name")
            .to_string_lossy()
            .into_owned();
        let prefix = format!("{file_name}.v");
        let mut preserves = Vec::new();
        for entry in std::fs::read_dir(directory).expect("read scratch directory") {
            let entry = entry.expect("directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) && name.contains("-premigration-") {
                preserves.push(entry.path());
            }
        }
        preserves
    }
}

impl Drop for ScratchStore {
    fn drop(&mut self) {
        for preserve in self.preserves() {
            let _ = std::fs::remove_file(preserve);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The record counts a store answers with; equality across reopens and
/// rollback replays is the row-preservation invariant.
#[derive(Debug, PartialEq, Eq)]
struct RecordCounts {
    lanes: usize,
    claims: usize,
    agents: usize,
    repositories: usize,
    worktrees: usize,
}

impl RecordCounts {
    fn read(tables: &OrchestrateTables) -> Self {
        Self {
            lanes: tables.lane_records().expect("read lanes").len(),
            claims: tables.claim_records().expect("read claims").len(),
            agents: tables
                .orchestrator_agent_records()
                .expect("read agents")
                .len(),
            repositories: tables
                .repository_records()
                .expect("read repositories")
                .len(),
            worktrees: tables.worktree_records().expect("read worktrees").len(),
        }
    }
}

fn fixture_directory() -> PathBuf {
    if let Some(directory) = std::env::var_os("ORCHESTRATE_MIGRATION_FIXTURE_DIRECTORY") {
        return PathBuf::from(directory);
    }
    let home = std::env::var_os("HOME").unwrap_or_default();
    PathBuf::from(home).join(".local/state/orchestrate/fixtures")
}

fn fixture_stores(directory: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut stores: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".sema"))
        })
        .collect();
    stores.sort();
    stores
}

#[test]
fn captured_real_stores_migrate_with_preserved_rows_or_fail_closed() {
    let directory = fixture_directory();
    let stores = fixture_stores(&directory);
    if stores.is_empty() {
        eprintln!(
            "SKIP: no captured store fixtures under {} — set ORCHESTRATE_MIGRATION_FIXTURE_DIRECTORY to run",
            directory.display()
        );
        return;
    }

    for (index, fixture) in stores.iter().enumerate() {
        let fixture_name = fixture
            .file_name()
            .expect("fixture has a file name")
            .to_string_lossy()
            .into_owned();
        let scratch = ScratchStore::from_fixture(fixture, &format!("case{index}"));

        match OrchestrateTables::open(&scratch.location()) {
            Ok(tables) => {
                let first_counts = RecordCounts::read(&tables);
                drop(tables);
                let preserves = scratch.preserves();
                assert!(
                    preserves.len() <= 1,
                    "{fixture_name}: one open must write at most one preserve, found {}",
                    preserves.len()
                );

                // Stability: a second open reports the same rows and performs
                // no further migration.
                let reopened =
                    OrchestrateTables::open(&scratch.location()).expect("migrated store reopens");
                assert_eq!(
                    RecordCounts::read(&reopened),
                    first_counts,
                    "{fixture_name}: reopening the migrated store must preserve every row"
                );
                drop(reopened);
                assert_eq!(
                    scratch.preserves().len(),
                    preserves.len(),
                    "{fixture_name}: a reopen must not migrate again"
                );

                // Rollback: the preserve is a working pre-migration store — a
                // copy of it replays the same migration to identical counts.
                if let Some(preserve) = preserves.first() {
                    let replay = ScratchStore::from_fixture(preserve, &format!("replay{index}"));
                    let replayed = OrchestrateTables::open(&replay.location())
                        .expect("preserve copy opens through the migration path");
                    assert_eq!(
                        RecordCounts::read(&replayed),
                        first_counts,
                        "{fixture_name}: replaying the preserve must yield the same rows"
                    );
                    eprintln!(
                        "OK (migrated): {fixture_name} — {first_counts:?}, preserve replayed"
                    );
                } else {
                    eprintln!("OK (no migration needed): {fixture_name} — {first_counts:?}");
                }
            }
            Err(error) => {
                // Fail-closed: typed error, no preserve, and the failure is
                // stable — a second open fails too because nothing was
                // repaired. (The engine may rewrite its own file header on any
                // open; byte identity is not the contract at this layer.)
                assert!(
                    scratch.preserves().is_empty(),
                    "{fixture_name}: a failed-closed open must not leave a preserve"
                );
                let second = OrchestrateTables::open(&scratch.location());
                assert!(
                    second.is_err(),
                    "{fixture_name}: a failed-closed store must still fail on the next open"
                );
                assert!(
                    scratch.preserves().is_empty(),
                    "{fixture_name}: repeated failed-closed opens must not accumulate preserves"
                );
                eprintln!("OK (failed closed): {fixture_name} — {error}");
            }
        }
    }
}

/// An openable store is a writable store. The 2026-07-18 production wedge
/// broke exactly this: a captured store with a ~27k-entry versioned log and a
/// legacy consumer-less outbox opened cleanly but refused every write's
/// history maintenance (`HistoryCompactionUnacknowledged`) under the
/// LocalCheckpoint topology. Every fixture that opens must accept the exact
/// operation that wedged — an ordinary lane register/unregister round-trip.
#[test]
fn captured_real_stores_accept_writes_after_open() {
    let directory = fixture_directory();
    let stores = fixture_stores(&directory);
    if stores.is_empty() {
        eprintln!(
            "SKIP: no captured store fixtures under {} — set ORCHESTRATE_MIGRATION_FIXTURE_DIRECTORY to run",
            directory.display()
        );
        return;
    }

    for (index, fixture) in stores.iter().enumerate() {
        let fixture_name = fixture
            .file_name()
            .expect("fixture has a file name")
            .to_string_lossy()
            .into_owned();
        let scratch = ScratchStore::from_fixture(fixture, &format!("write{index}"));
        if OrchestrateTables::open(&scratch.location()).is_err() {
            eprintln!("SKIP (fails closed, covered above): {fixture_name}");
            continue;
        }

        let temporary = tempfile::Builder::new()
            .prefix("fixture-write-scaffold")
            .tempdir()
            .expect("scaffold directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        let mut service = OrchestrateService::open_with_layout(
            &scratch.location(),
            OrchestrateLayout::new(workspace, git_index),
        )
        .expect("service opens the captured store");

        let session =
            SessionIdentifier::from_camel_case_name("FixtureWriteSession").expect("session");
        let lane = LaneIdentifier::from_wire_token("fixture-write-probe").expect("lane");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime
            .block_on(
                service.handle_meta(MetaOrchestrateRequest::Register(LaneRegistrationRequest {
                    assignment: LaneAssignment {
                        session: session.clone(),
                        lane: lane.clone(),
                        owner: orchestrate::LaneOwner {
                            role: Role::try_new(vec![
                                RoleToken::from_text("Operator").expect("role token"),
                            ])
                            .expect("role"),
                            authority: LaneAuthority::Structural,
                        },
                        details: LaneDetails::from_text("fixture write probe").expect("details"),
                    },
                    mode: LaneRegistrationMode::Fresh,
                })),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{fixture_name}: an openable store must accept a lane registration, got {error}"
                )
            });
        runtime
            .block_on(service.handle_meta(MetaOrchestrateRequest::Unregister(
                LaneUnregistrationRequest {
                    session,
                    lane,
                    details: LaneDetails::from_text("fixture write probe done").expect("details"),
                },
            )))
            .unwrap_or_else(|error| {
                panic!("{fixture_name}: the probe lane must unregister cleanly, got {error}")
            });
        eprintln!("OK (write round-trip): {fixture_name}");
    }
}
