//! Smoke test for the worktree registry (Spirit eh5a): register a worktree
//! against a TEMP store, observe the list, and confirm `worktrees.nota` is
//! projected. Exercises the daemon end-to-end through `handle` / `handle_meta`
//! without touching the live store.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;

use orchestrate::{
    BranchName, LaneName, MetaOrchestrateReply, MetaOrchestrateRequest, Observation,
    OrchestrateLayout, OrchestrateReply, OrchestrateRequest, OrchestrateService, PurposeText,
    PushedState, RegisterWorktree, RepositoryName, StoreLocation, TimestampNanos, WirePath,
    Worktree, WorktreeConclusion, WorktreeConclusionRequest, WorktreeRequest, WorktreeStatus,
};
use tempfile::TempDir;

struct WorktreeFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    git_index: PathBuf,
    worktree_root: PathBuf,
    service: OrchestrateService,
}

impl WorktreeFixture {
    fn new(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let git_index = temporary.path().join("git-index");
        let worktree_root = temporary.path().join("worktrees");
        Self::with_temporary(temporary, git_index, worktree_root)
    }

    fn new_with_indexes(name: &str, git_index: PathBuf, worktree_root: PathBuf) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        Self::with_temporary(temporary, git_index, worktree_root)
    }

    fn with_temporary(temporary: TempDir, git_index: PathBuf, worktree_root: PathBuf) -> Self {
        let workspace = temporary.path().join("workspace");
        std::fs::create_dir_all(workspace.join("orchestrate")).expect("orchestrate directory");
        std::fs::write(
            workspace.join("orchestrate").join("roles.list"),
            "operator\ndesigner\nsystem-operator\n",
        )
        .expect("role registry");
        std::fs::create_dir_all(&git_index).expect("git index directory");
        std::fs::create_dir_all(&worktree_root).expect("worktree root");
        let store = StoreLocation::new(
            temporary
                .path()
                .join("orchestrate.sema")
                .to_string_lossy()
                .into_owned(),
        );
        let service = OrchestrateService::open_with_layout(
            &store,
            OrchestrateLayout::new(workspace.clone(), git_index.clone())
                .with_worktree_index_root(worktree_root.clone()),
        )
        .expect("service opens");
        Self {
            _temporary: temporary,
            workspace,
            git_index,
            worktree_root,
            service,
        }
    }

    /// A source checkout at `<git_index>/<repository>` with a `main` bookmark
    /// and repo-local jj user config, so the daemon's own `jj workspace add`
    /// (which sets no author env) can commit a working copy.
    fn make_source_repository(&self, repository: &str) -> PathBuf {
        let path = self.git_index.join(repository);
        std::fs::create_dir_all(&path).expect("source repo path");
        let status = Command::new("jj")
            .arg("--no-pager")
            .arg("git")
            .arg("init")
            .arg("--colocate")
            .arg(&path)
            .env("JJ_USER", "smoke")
            .env("JJ_EMAIL", "smoke@example.invalid")
            .output()
            .expect("run jj git init");
        assert!(
            status.status.success(),
            "jj git init failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        run_jj(&path, &["config", "set", "--repo", "user.name", "smoke"]);
        run_jj(
            &path,
            &[
                "config",
                "set",
                "--repo",
                "user.email",
                "smoke@example.invalid",
            ],
        );
        std::fs::write(path.join("base.txt"), "base\n").expect("base file");
        run_jj(&path, &["describe", "-m", "base commit"]);
        run_jj(&path, &["bookmark", "create", "main", "-r", "@"]);
        run_jj(&path, &["new"]);
        path
    }

    /// A Git-only source checkout with a real `main` commit. It deliberately
    /// lacks `.jj` so `RequestWorktree` must bootstrap colocated metadata.
    fn make_git_source_repository_without_jj(&self, repository: &str) -> PathBuf {
        let path = self.git_index.join(repository);
        std::fs::create_dir_all(&path).expect("Git source repo path");
        let init = Command::new("git")
            .arg("init")
            .arg("--initial-branch=main")
            .arg(&path)
            .output()
            .expect("initialize Git source repo");
        assert!(
            init.status.success(),
            "git init failed: {}",
            String::from_utf8_lossy(&init.stderr)
        );
        run_git(&path, &["config", "user.name", "smoke"]);
        run_git(&path, &["config", "user.email", "smoke@example.invalid"]);
        std::fs::write(path.join("base.txt"), "base\n").expect("Git source content");
        run_git(&path, &["add", "base.txt"]);
        run_git(&path, &["commit", "-m", "base commit"]);
        assert!(
            !path.join(".jj").exists(),
            "Git-only fixture must not already hold Jujutsu metadata"
        );
        path
    }

    fn request(&mut self, repository: &str, branch: &str, lane: &str) -> OrchestrateReply {
        self.handle(OrchestrateRequest::RequestWorktree(WorktreeRequest {
            repository: RepositoryName::from_text(repository).expect("repository name"),
            branch: BranchName::from_text(branch).expect("branch name"),
            owning_lane: LaneName::from_text(lane).expect("lane name"),
            purpose: PurposeText::from_text("worktree lifecycle protocol").expect("purpose"),
        }))
        .expect("request worktree")
    }

    fn conclude(&mut self, lane: &str, disposition: WorktreeConclusion) -> OrchestrateReply {
        self.handle(OrchestrateRequest::ConcludeWorktree(
            WorktreeConclusionRequest {
                owning_lane: LaneName::from_text(lane).expect("lane name"),
                disposition,
            },
        ))
        .expect("conclude worktree")
    }

    fn handle(&mut self, request: OrchestrateRequest) -> orchestrate::Result<OrchestrateReply> {
        block_on(self.service.handle(request))
    }

    fn handle_meta(
        &mut self,
        request: MetaOrchestrateRequest,
    ) -> orchestrate::Result<MetaOrchestrateReply> {
        block_on(self.service.handle_meta(request))
    }

    /// A colocated jj repo at `<worktree_root>/<repository>/<branch>` with one
    /// committed change, so the daemon's jj probe derives a real pushed-state
    /// and last-activity.
    fn make_worktree_repository(&self, repository: &str, branch: &str) -> PathBuf {
        let path = self.worktree_root.join(repository).join(branch);
        std::fs::create_dir_all(&path).expect("worktree path");
        // `jj git init` takes the destination positionally, not via `-R`.
        let status = Command::new("jj")
            .arg("--no-pager")
            .arg("git")
            .arg("init")
            .arg("--colocate")
            .arg(&path)
            .env("JJ_USER", "smoke")
            .env("JJ_EMAIL", "smoke@example.invalid")
            .output()
            .expect("run jj git init");
        assert!(
            status.status.success(),
            "jj git init failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        std::fs::write(path.join("seed.txt"), "seed\n").expect("seed file");
        run_jj(&path, &["describe", "-m", "seed worktree commit"]);
        run_jj(&path, &["new"]);
        path
    }
}

fn block_on<Future: std::future::Future>(future: Future) -> Future::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn run_jj(repository: &std::path::Path, arguments: &[&str]) {
    let status = Command::new("jj")
        .arg("--no-pager")
        .arg("-R")
        .arg(repository)
        .args(arguments)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj");
    assert!(
        status.status.success(),
        "jj {arguments:?} failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
}

fn run_git(repository: &std::path::Path, arguments: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn read_jj(repository: &std::path::Path, arguments: &[&str]) -> String {
    let output = Command::new("jj")
        .arg("--no-pager")
        .arg("-R")
        .arg(repository)
        .args(arguments)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj");
    assert!(
        output.status.success(),
        "jj {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn register_worktree_observe_and_project_manifest() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-smoke");
    let path = fixture.make_worktree_repository("orchestrate", "worktree-registry");
    let wire_path = WirePath::from_absolute_path(path.to_string_lossy().into_owned())
        .expect("absolute worktree path");

    let order = RegisterWorktree {
        worktree: Worktree {
            repository: RepositoryName::from_text("orchestrate").expect("repository name"),
            branch: BranchName::from_text("worktree-registry").expect("branch name"),
            path: wire_path,
            owning_lane: LaneName::from_text("designer").expect("lane name"),
            status: WorktreeStatus::Active,
            purpose: PurposeText::from_text("prototype the worktree registry")
                .expect("purpose text"),
            // Agent-supplied last_activity / pushed_state are re-derived by the
            // daemon, so the seed values here must not survive.
            last_activity: TimestampNanos::new(0),
            pushed_state: PushedState::AncestorOfMain,
        },
    };

    let reply = fixture
        .handle_meta(MetaOrchestrateRequest::RegisterWorktree(order))
        .expect("register worktree");
    let MetaOrchestrateReply::WorktreeRegistered(registered) = reply else {
        panic!("expected WorktreeRegistered, got {reply:?}");
    };
    assert_eq!(registered.worktree.repository.as_str(), "orchestrate");
    assert_eq!(registered.worktree.branch.as_str(), "worktree-registry");
    assert_eq!(registered.worktree.owning_lane.as_str(), "designer");
    // The daemon re-derived last_activity from the worktree commit; the seed
    // zero must have been replaced.
    assert!(registered.worktree.last_activity.value() > 0);
    // A fresh colocated repo with no remote is Unpushed (not AncestorOfMain).
    assert_eq!(registered.worktree.pushed_state, PushedState::Unpushed);

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::Worktrees))
        .expect("observe worktrees");
    let OrchestrateReply::WorktreesObserved(snapshot) = observed else {
        panic!("expected WorktreesObserved, got {observed:?}");
    };
    assert_eq!(snapshot.worktrees.len(), 1);
    assert_eq!(snapshot.worktrees[0].branch.as_str(), "worktree-registry");

    let manifest = fixture.workspace.join("orchestrate").join("worktrees.nota");
    let body = std::fs::read_to_string(&manifest).expect("worktrees.nota written");
    assert!(
        body.contains("orchestrate") && body.contains("worktree-registry"),
        "manifest body: {body}"
    );
    assert!(body.contains("designer"), "manifest body: {body}");
    // Positional NOTA record: one parenthesised tuple per worktree, fields in
    // declared order, whitespace-bearing strings bracketed, and quote-free.
    assert!(body.starts_with('('), "manifest body: {body}");
    assert!(
        body.contains("[prototype the worktree registry]"),
        "purpose must be bracketed: {body}"
    );
    assert!(
        body.contains("Active") && body.contains("Unpushed"),
        "manifest body: {body}"
    );
    assert!(!body.contains('"'), "manifest must be quote-free: {body}");
}

#[test]
fn refresh_scans_worktree_index_into_manifest() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-refresh");
    fixture.make_worktree_repository("orchestrate", "worktree-registry");
    fixture.make_worktree_repository("signal-orchestrate", "worktree-registry");

    let reply = fixture
        .handle_meta(MetaOrchestrateRequest::RefreshWorktreeIndex(
            orchestrate::RefreshWorktreeIndexOrder {},
        ))
        .expect("refresh worktree index");
    let MetaOrchestrateReply::WorktreeIndexRefreshed(refreshed) = reply else {
        panic!("expected WorktreeIndexRefreshed, got {reply:?}");
    };
    assert_eq!(refreshed.worktrees(), 2);

    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::Worktrees))
        .expect("observe worktrees");
    let OrchestrateReply::WorktreesObserved(snapshot) = observed else {
        panic!("expected WorktreesObserved, got {observed:?}");
    };
    assert_eq!(snapshot.worktrees.len(), 2);
}

fn observe_worktrees(fixture: &mut WorktreeFixture) -> Vec<Worktree> {
    let observed = fixture
        .handle(OrchestrateRequest::Observe(Observation::Worktrees))
        .expect("observe worktrees");
    let OrchestrateReply::WorktreesObserved(snapshot) = observed else {
        panic!("expected WorktreesObserved, got {observed:?}");
    };
    snapshot.worktrees
}

#[test]
fn refresh_preserves_registered_worktree_ownership_purpose_and_status() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-refresh-metadata");
    let path = fixture.make_worktree_repository("orchestrate", "preserved-metadata");
    let expected_lane = LaneName::from_text("MetadataOwner").expect("lane");
    let expected_purpose =
        PurposeText::from_text("preserve registered worktree meaning").expect("purpose");

    fixture
        .handle_meta(MetaOrchestrateRequest::RegisterWorktree(RegisterWorktree {
            worktree: Worktree {
                repository: RepositoryName::from_text("orchestrate").expect("repository"),
                branch: BranchName::from_text("preserved-metadata").expect("branch"),
                path: WirePath::from_absolute_path(path.to_string_lossy().into_owned())
                    .expect("absolute worktree path"),
                owning_lane: expected_lane.clone(),
                status: WorktreeStatus::Abandoned,
                purpose: expected_purpose.clone(),
                last_activity: TimestampNanos::new(0),
                pushed_state: PushedState::Unpushed,
            },
        }))
        .expect("register worktree");

    fixture
        .handle_meta(MetaOrchestrateRequest::RefreshWorktreeIndex(
            orchestrate::RefreshWorktreeIndexOrder {},
        ))
        .expect("refresh worktree index");

    let refreshed = observe_worktrees(&mut fixture);
    assert_eq!(refreshed.len(), 1);
    assert_eq!(refreshed[0].owning_lane, expected_lane);
    assert_eq!(refreshed[0].purpose, expected_purpose);
    assert_eq!(refreshed[0].status, WorktreeStatus::Abandoned);
}

#[test]
fn request_initializes_missing_colocated_jj_metadata_for_git_source() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-git-bootstrap");
    let source = fixture.make_git_source_repository_without_jj("git-only");

    let reply = fixture.request("git-only", "bootstrap-feature", "designer");
    let OrchestrateReply::WorktreeScaffolded(scaffolded) = reply else {
        panic!("expected WorktreeScaffolded")
    };
    let destination = fixture
        .worktree_root
        .join("git-only")
        .join("bootstrap-feature");
    assert!(source.join(".jj").is_dir(), "source metadata initialized");
    assert!(
        destination.join(".jj").exists(),
        "workspace metadata exists"
    );
    assert_eq!(
        scaffolded.worktree.path.as_str(),
        destination.to_string_lossy()
    );
}

#[test]
fn request_reuses_existing_jj_metadata_and_registers_row() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-request");
    let source = fixture.make_source_repository("orchestrate");
    let main_before = read_jj(&source, &["log", "-r", "main", "-T", "description"]);

    let reply = fixture.request("orchestrate", "feature-lifecycle", "designer");
    let OrchestrateReply::WorktreeScaffolded(scaffolded) = reply else {
        panic!("expected WorktreeScaffolded, got {reply:?}");
    };
    assert_eq!(scaffolded.worktree.status, WorktreeStatus::Active);
    assert_eq!(scaffolded.worktree.owning_lane.as_str(), "designer");

    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("feature-lifecycle");
    assert!(destination.join(".jj").exists(), "workspace .jj must exist");
    assert_eq!(
        scaffolded.worktree.path.as_str(),
        destination.to_string_lossy()
    );
    assert!(source.join(".jj").is_dir(), "existing metadata remains");
    assert_eq!(
        read_jj(&source, &["log", "-r", "main", "-T", "description"]),
        main_before,
        "request must not reinitialize or rewrite the source repository"
    );

    let worktrees = observe_worktrees(&mut fixture);
    assert_eq!(worktrees.len(), 1);
    assert_eq!(worktrees[0].branch.as_str(), "feature-lifecycle");

    // A second request for the same identity is refused without scaffolding.
    let again = fixture.request("orchestrate", "feature-lifecycle", "designer");
    assert!(
        matches!(again, OrchestrateReply::WorktreeRequestRejected(_)),
        "duplicate request must be rejected, got {again:?}"
    );

    // An unknown repository is refused before any filesystem work.
    let unknown = fixture.request("no-such-repo", "feature-lifecycle", "designer");
    let OrchestrateReply::WorktreeRequestRejected(rejected) = unknown else {
        panic!("expected WorktreeRequestRejected, got {unknown:?}");
    };
    assert_eq!(
        rejected.reason,
        orchestrate::WorktreeRequestRejection::RepositoryNotFound
    );
}

#[test]
fn request_rejects_non_git_source_with_existing_typed_refusal() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-non-git-source");
    let source = fixture.git_index.join("plain-directory");
    std::fs::create_dir_all(&source).expect("plain source directory");
    std::fs::write(source.join("note.txt"), "not a checkout\n").expect("plain source content");

    let reply = fixture.request("plain-directory", "rejected-feature", "designer");
    let OrchestrateReply::WorktreeRequestRejected(rejected) = reply else {
        panic!("expected WorktreeRequestRejected, got {reply:?}");
    };
    assert_eq!(
        rejected.reason,
        orchestrate::WorktreeRequestRejection::RepositoryNotFound
    );
    assert!(
        !source.join(".jj").exists(),
        "a non-Git source must not be initialized"
    );
    assert!(
        !fixture
            .worktree_root
            .join("plain-directory")
            .join("rejected-feature")
            .exists(),
        "a rejected request must not create a workspace"
    );
}

#[test]
fn concurrent_requests_bootstrap_git_source_safely() {
    let source_fixture = WorktreeFixture::new("orchestrate-worktree-bootstrap-source");
    let source = source_fixture.make_git_source_repository_without_jj("git-only");
    let mut first = WorktreeFixture::new_with_indexes(
        "orchestrate-worktree-bootstrap-first",
        source_fixture.git_index.clone(),
        source_fixture.worktree_root.clone(),
    );
    let mut second = WorktreeFixture::new_with_indexes(
        "orchestrate-worktree-bootstrap-second",
        source_fixture.git_index.clone(),
        source_fixture.worktree_root.clone(),
    );
    let start = Arc::new(Barrier::new(3));

    let ((_first_fixture, first), (_second_fixture, second)) = thread::scope(|scope| {
        let first_start = Arc::clone(&start);
        let first = scope.spawn(move || {
            first_start.wait();
            let reply = first.request("git-only", "first-bootstrap-feature", "designer");
            (first, reply)
        });
        let second_start = Arc::clone(&start);
        let second = scope.spawn(move || {
            second_start.wait();
            let reply = second.request("git-only", "second-bootstrap-feature", "operator");
            (second, reply)
        });
        start.wait();
        (
            first.join().expect("first request thread"),
            second.join().expect("second request thread"),
        )
    });

    let OrchestrateReply::WorktreeScaffolded(first) = first else {
        panic!("first request must scaffold, got {first:?}");
    };
    let OrchestrateReply::WorktreeScaffolded(second) = second else {
        panic!("second request must scaffold, got {second:?}");
    };
    assert!(source.join(".jj").is_dir(), "source bootstrap completed");
    assert!(
        PathBuf::from(first.worktree.path.as_str())
            .join(".jj")
            .exists(),
        "first workspace exists"
    );
    assert!(
        PathBuf::from(second.worktree.path.as_str())
            .join(".jj")
            .exists(),
        "second workspace exists"
    );
}

#[test]
fn concurrent_same_workspace_request_returns_typed_refusal() {
    let source_fixture = WorktreeFixture::new("orchestrate-worktree-same-workspace-source");
    let source = source_fixture.make_git_source_repository_without_jj("git-only");
    let mut first = WorktreeFixture::new_with_indexes(
        "orchestrate-worktree-same-workspace-first",
        source_fixture.git_index.clone(),
        source_fixture.worktree_root.clone(),
    );
    let mut second = WorktreeFixture::new_with_indexes(
        "orchestrate-worktree-same-workspace-second",
        source_fixture.git_index.clone(),
        source_fixture.worktree_root.clone(),
    );
    let start = Arc::new(Barrier::new(3));

    let ((_first_fixture, first), (_second_fixture, second)) = thread::scope(|scope| {
        let first_start = Arc::clone(&start);
        let first = scope.spawn(move || {
            first_start.wait();
            let reply = first.request("git-only", "same-bootstrap-feature", "designer");
            (first, reply)
        });
        let second_start = Arc::clone(&start);
        let second = scope.spawn(move || {
            second_start.wait();
            let reply = second.request("git-only", "same-bootstrap-feature", "operator");
            (second, reply)
        });
        start.wait();
        (
            first.join().expect("first request thread"),
            second.join().expect("second request thread"),
        )
    });

    match (first, second) {
        (
            OrchestrateReply::WorktreeScaffolded(_),
            OrchestrateReply::WorktreeRequestRejected(rejected),
        )
        | (
            OrchestrateReply::WorktreeRequestRejected(rejected),
            OrchestrateReply::WorktreeScaffolded(_),
        ) => assert_eq!(
            rejected.reason,
            orchestrate::WorktreeRequestRejection::WorktreeAlreadyExists
        ),
        replies => panic!("expected one scaffold and one typed rejection, got {replies:?}"),
    }
    assert!(source.join(".jj").is_dir(), "source bootstrap completed");
    assert!(
        source_fixture
            .worktree_root
            .join("git-only")
            .join("same-bootstrap-feature")
            .join(".jj")
            .exists(),
        "the successful workspace remains"
    );
}

#[test]
fn conclude_merged_fast_forwards_real_work_onto_unmoved_main() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-fastforward");
    let source = fixture.make_source_repository("orchestrate");
    fixture.request("orchestrate", "unmerged-feature", "operator");

    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("unmerged-feature");
    // Real work not yet on main: under the MVP ruling the daemon lands it
    // itself — main has not moved, so the land is a fast-forward.
    run_jj(&destination, &["describe", "-m", "real unmerged work"]);
    run_jj(&destination, &["new"]);

    let reply = fixture.conclude("operator", WorktreeConclusion::Merged);
    let OrchestrateReply::WorktreeConcluded(concluded) = reply else {
        panic!("expected WorktreeConcluded, got {reply:?}");
    };
    assert_eq!(
        concluded.integration,
        orchestrate::MainIntegration::FastForwarded
    );
    assert_eq!(concluded.worktree.status, WorktreeStatus::Recycled);
    assert!(
        !destination.exists(),
        "landed teardown removes the worktree"
    );
    let main_description = read_jj(
        &source,
        &[
            "log",
            "--no-graph",
            "-r",
            "main",
            "-T",
            "description.first_line()",
        ],
    );
    assert_eq!(main_description.trim(), "real unmerged work");
}

#[test]
fn conclude_merged_rebases_work_when_main_moved() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-rebase");
    let source = fixture.make_source_repository("orchestrate");
    fixture.request("orchestrate", "rebase-feature", "operator");

    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("rebase-feature");
    std::fs::write(destination.join("feature.txt"), "feature\n").expect("feature file");
    run_jj(&destination, &["describe", "-m", "feature work"]);
    run_jj(&destination, &["new"]);

    // Someone else lands on main first (same repo — the worktree is a jj
    // workspace of it, so the bookmark move is immediately visible).
    run_jj(&source, &["new", "main"]);
    std::fs::write(source.join("other.txt"), "other\n").expect("other file");
    run_jj(&source, &["describe", "-m", "other landed work"]);
    run_jj(&source, &["bookmark", "set", "main", "-r", "@"]);
    run_jj(&source, &["new"]);

    let reply = fixture.conclude("operator", WorktreeConclusion::Merged);
    let OrchestrateReply::WorktreeConcluded(concluded) = reply else {
        panic!("expected WorktreeConcluded, got {reply:?}");
    };
    assert_eq!(concluded.integration, orchestrate::MainIntegration::Rebased);
    assert_eq!(concluded.worktree.status, WorktreeStatus::Recycled);
    // main now carries both lines of work, feature rebased on top.
    let main_history = read_jj(
        &source,
        &[
            "log",
            "--no-graph",
            "-r",
            "::main",
            "-T",
            "description.first_line() ++ \"\\n\"",
        ],
    );
    assert!(main_history.contains("feature work"));
    assert!(main_history.contains("other landed work"));
}

#[test]
fn conclude_merged_refuses_conflicted_rebase_and_preserves_work() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-conflict");
    let source = fixture.make_source_repository("orchestrate");
    fixture.request("orchestrate", "conflict-feature", "operator");

    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("conflict-feature");
    // Both sides edit base.txt: the auto-rebase must hit a real conflict,
    // fully unwind, and refuse typed — the seam where review gets built later.
    std::fs::write(destination.join("base.txt"), "mine\n").expect("conflicting edit");
    run_jj(
        &destination,
        &["describe", "-m", "conflicting feature edit"],
    );
    run_jj(&destination, &["new"]);

    run_jj(&source, &["new", "main"]);
    std::fs::write(source.join("base.txt"), "theirs\n").expect("other edit");
    run_jj(&source, &["describe", "-m", "conflicting landed work"]);
    run_jj(&source, &["bookmark", "set", "main", "-r", "@"]);
    run_jj(&source, &["new"]);

    let reply = fixture.conclude("operator", WorktreeConclusion::Merged);
    let OrchestrateReply::WorktreeTeardownRefused(refused) = reply else {
        panic!("expected WorktreeTeardownRefused, got {reply:?}");
    };
    assert_eq!(
        refused.reason,
        orchestrate::TeardownRefusal::AutoRebaseConflicted
    );
    assert!(
        destination.join(".jj").exists(),
        "refused teardown keeps the worktree"
    );
    let worktrees = observe_worktrees(&mut fixture);
    assert_eq!(worktrees[0].status, WorktreeStatus::Active);
    // The unwind restored the pre-rebase graph: the feature commit is intact
    // and conflict-free.
    let conflicted = read_jj(
        &destination,
        &[
            "log",
            "--no-graph",
            "-r",
            "::@ & conflicts()",
            "-T",
            "commit_id.short()",
        ],
    );
    assert!(conflicted.trim().is_empty(), "no conflicted commits remain");
    let feature = read_jj(
        &destination,
        &[
            "log",
            "--no-graph",
            "-r",
            "@-",
            "-T",
            "description.first_line()",
        ],
    );
    assert_eq!(feature.trim(), "conflicting feature edit");
}

#[test]
fn conclude_merged_tears_down_when_ancestor_of_main() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-merged");
    fixture.make_source_repository("orchestrate");
    fixture.request("orchestrate", "merged-feature", "operator");

    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("merged-feature");
    // No divergence: the scaffolded working-copy parent is `main` itself, so the
    // work is trivially an ancestor of main and the safety gate opens.
    let reply = fixture.conclude("operator", WorktreeConclusion::Merged);
    let OrchestrateReply::WorktreeConcluded(concluded) = reply else {
        panic!("expected WorktreeConcluded, got {reply:?}");
    };
    assert_eq!(
        concluded.integration,
        orchestrate::MainIntegration::AlreadyAncestor
    );
    assert_eq!(concluded.worktree.status, WorktreeStatus::Recycled);
    assert!(
        !destination.exists(),
        "merged teardown removes the worktree directory"
    );
    // The next ordinary turn reconciles the registry. A concluded row whose
    // checkout was just removed is stale state, so it is immediately reaped
    // rather than retained as a tombstone.
    assert!(observe_worktrees(&mut fixture).is_empty());
}

#[test]
fn conclude_rejected_salvages_to_remote_then_tears_down() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-rejected");
    let source = fixture.make_source_repository("orchestrate");
    // A throwaway colocated repo used purely as the push remote for salvage.
    let remote = fixture._temporary.path().join("remote");
    let status = Command::new("jj")
        .arg("--no-pager")
        .arg("git")
        .arg("init")
        .arg("--colocate")
        .arg(&remote)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj git init remote");
    assert!(
        status.status.success(),
        "remote init: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    run_jj(
        &source,
        &[
            "git",
            "remote",
            "add",
            "origin",
            &format!("{}/.git", remote.to_string_lossy()),
        ],
    );

    fixture.request("orchestrate", "rejected-feature", "designer");
    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("rejected-feature");
    run_jj(&destination, &["describe", "-m", "rejected work"]);

    let reply = fixture.conclude("designer", WorktreeConclusion::Rejected);
    let OrchestrateReply::WorktreeConcluded(concluded) = reply else {
        panic!("expected WorktreeConcluded, got {reply:?}");
    };
    assert_eq!(
        concluded.integration,
        orchestrate::MainIntegration::Discarded
    );
    assert_eq!(concluded.worktree.status, WorktreeStatus::Recycled);
    assert!(!destination.exists(), "rejected teardown removes the dir");

    // The salvage bookmark lives on the remote repository itself.
    let remote_bookmarks = jj_stdout(&remote, &["bookmark", "list"]);
    assert!(
        remote_bookmarks.contains("discard/rejected-feature"),
        "remote must hold the salvage bookmark: {remote_bookmarks}"
    );
    // Nothing local survives: neither the feature bookmark nor a live (undeleted)
    // discard bookmark remains in the source repository.
    let source_bookmarks = jj_stdout(&source, &["bookmark", "list"]);
    assert!(
        source_bookmarks.lines().all(|line| {
            let live = !line.trim_start().starts_with('@') && !line.contains("(deleted)");
            !(live && line.contains("rejected-feature"))
        }),
        "no live local rejected-feature bookmark should remain: {source_bookmarks}"
    );
}

fn jj_stdout(repository: &std::path::Path, arguments: &[&str]) -> String {
    let output = Command::new("jj")
        .arg("--no-pager")
        .arg("-R")
        .arg(repository)
        .args(arguments)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj");
    assert!(
        output.status.success(),
        "jj {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// A throwaway colocated repo wired as `origin` of `source`, used purely as
/// the push remote for salvage assertions.
fn add_salvage_remote(fixture: &WorktreeFixture, source: &std::path::Path) -> PathBuf {
    let remote = fixture._temporary.path().join("salvage-remote");
    let status = Command::new("jj")
        .arg("--no-pager")
        .arg("git")
        .arg("init")
        .arg("--colocate")
        .arg(&remote)
        .env("JJ_USER", "smoke")
        .env("JJ_EMAIL", "smoke@example.invalid")
        .output()
        .expect("run jj git init remote");
    assert!(
        status.status.success(),
        "remote init: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    run_jj(
        source,
        &[
            "git",
            "remote",
            "add",
            "origin",
            &format!("{}/.git", remote.to_string_lossy()),
        ],
    );
    remote
}

fn remote_salvage_description(remote: &std::path::Path, branch: &str) -> String {
    jj_stdout(
        remote,
        &[
            "log",
            "--no-graph",
            "-r",
            &format!("discard/{branch}"),
            "-T",
            "description.first_line()",
        ],
    )
    .trim()
    .to_owned()
}

/// The production wedge: a scaffolded worktree rejected untouched, its working
/// copy still the empty description-less placeholder `jj` parks on. Salvage
/// must skip the placeholder and land the discard bookmark on the last real
/// commit instead of failing the push.
#[test]
fn conclude_rejected_salvages_untouched_scaffold_placeholder() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-placeholder");
    let source = fixture.make_source_repository("orchestrate");
    let remote = add_salvage_remote(&fixture, &source);
    fixture.request("orchestrate", "placeholder-feature", "operator");
    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("placeholder-feature");

    let reply = fixture.conclude("operator", WorktreeConclusion::Rejected);
    let OrchestrateReply::WorktreeConcluded(concluded) = reply else {
        panic!("expected WorktreeConcluded, got {reply:?}");
    };
    assert_eq!(concluded.worktree.status, WorktreeStatus::Recycled);
    assert!(!destination.exists(), "rejected teardown removes the dir");
    assert_eq!(
        remote_salvage_description(&remote, "placeholder-feature"),
        "base commit"
    );
}

/// A rejected working copy holding real undescribed changes: salvage must
/// describe the work instead of dropping it, so the remote discard bookmark
/// carries the changes.
#[test]
fn conclude_rejected_describes_and_salvages_undescribed_changes() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-undescribed");
    let source = fixture.make_source_repository("orchestrate");
    let remote = add_salvage_remote(&fixture, &source);
    fixture.request("orchestrate", "undescribed-feature", "designer");
    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("undescribed-feature");
    std::fs::write(destination.join("finding.txt"), "real work\n").expect("write change");

    let reply = fixture.conclude("designer", WorktreeConclusion::Rejected);
    assert!(
        matches!(reply, OrchestrateReply::WorktreeConcluded(_)),
        "expected WorktreeConcluded, got {reply:?}"
    );
    assert_eq!(
        remote_salvage_description(&remote, "undescribed-feature"),
        "salvaged rejected working copy"
    );
}

/// A retried rejection after a failed attempt left the salvage bookmark parked
/// on the placeholder commit: the bookmark must move backwards onto the real
/// commit and the teardown complete.
#[test]
fn conclude_rejected_retries_over_leftover_salvage_bookmark() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-retried");
    let source = fixture.make_source_repository("orchestrate");
    let remote = add_salvage_remote(&fixture, &source);
    fixture.request("orchestrate", "retried-feature", "operator");
    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("retried-feature");
    run_jj(
        &destination,
        &["bookmark", "create", "discard/retried-feature", "-r", "@"],
    );

    let reply = fixture.conclude("operator", WorktreeConclusion::Rejected);
    assert!(
        matches!(reply, OrchestrateReply::WorktreeConcluded(_)),
        "expected WorktreeConcluded, got {reply:?}"
    );
    assert_eq!(
        remote_salvage_description(&remote, "retried-feature"),
        "base commit"
    );
}

/// When the salvage push fails (no remote configured), teardown reports the
/// error, removes nothing, and leaves no local salvage bookmark behind for a
/// retry to trip over.
#[test]
fn conclude_rejected_failed_push_leaves_no_salvage_residue() {
    let mut fixture = WorktreeFixture::new("orchestrate-worktree-pushfail");
    let source = fixture.make_source_repository("orchestrate");
    fixture.request("orchestrate", "pushless-feature", "operator");
    let destination = fixture
        .worktree_root
        .join("orchestrate")
        .join("pushless-feature");

    let result = fixture.handle(OrchestrateRequest::ConcludeWorktree(
        WorktreeConclusionRequest {
            owning_lane: LaneName::from_text("operator").expect("lane name"),
            disposition: WorktreeConclusion::Rejected,
        },
    ));
    assert!(result.is_err(), "push without a remote must fail teardown");
    assert!(
        destination.join(".jj").exists(),
        "failed teardown keeps the dir"
    );
    let bookmarks = jj_stdout(&source, &["bookmark", "list"]);
    assert!(
        !bookmarks.contains("discard/pushless-feature"),
        "no salvage bookmark residue: {bookmarks}"
    );
    assert_eq!(
        observe_worktrees(&mut fixture)[0].status,
        WorktreeStatus::Active
    );
}
