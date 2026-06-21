//! Smoke test for the worktree registry (Spirit eh5a): register a worktree
//! against a TEMP store, observe the list, and confirm `worktrees.nota` is
//! projected. Exercises the daemon end-to-end through `handle` / `handle_meta`
//! without touching the live store.

use std::path::PathBuf;
use std::process::Command;

use orchestrate::{
    BranchName, LaneName, MetaOrchestrateReply, MetaOrchestrateRequest, Observation,
    OrchestrateLayout, OrchestrateReply, OrchestrateRequest, OrchestrateService, PurposeText,
    PushedState, RegisterWorktree, RepositoryName, StoreLocation, TimestampNanos, WirePath,
    Worktree, WorktreeStatus,
};
use tempfile::TempDir;

struct WorktreeFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    worktree_root: PathBuf,
    service: OrchestrateService,
}

impl WorktreeFixture {
    fn new(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        let worktree_root = temporary.path().join("worktrees");
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
            OrchestrateLayout::new(workspace.clone(), git_index)
                .with_worktree_index_root(worktree_root.clone()),
        )
        .expect("service opens");
        Self {
            _temporary: temporary,
            workspace,
            worktree_root,
            service,
        }
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
