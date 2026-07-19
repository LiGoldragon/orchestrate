# Non-Ideal Agents Registry

Known non-idealities in this repository: sanctioned workarounds and deferred
proper fixes. Ordinary rules live in `AGENTS.md`; the ideal shape lives in
`ARCHITECTURE.md`. This file is debt with a named future fix target.

## Family schema-hash evolution — resolved by the sema-engine primitive

Resolved 2026-07-18: sema-engine ≥ 0.11.0 carries a first-class family-evolution
primitive. `TableDescriptor::with_prior` declares a family's prior stored
generations (per-generation schema hash plus typed carry-forward), and
registration against a store whose catalog names a declared prior migrates the
rows, the catalog identity, and the log entries in one engine write transaction
— logged as row history (retract under the prior identity, assert under the
evolved one), so folds and rebuilds never need the retired shape. Orchestrate's
consumer-side workaround (prior-identity row reads, raw
`__sema_engine_catalog` edits, drop-and-reinsert) is deleted; the migration
path re-opens with `OrchestrateTables::evolving_agent_descriptor()` after the
pre-migration preserve is taken. A future field bump on an orchestrate family
needs only a new `with_prior` step on its descriptor, not a hand-written
migration. Historical stale-family entries already orphaned in the versioned
log before this primitive remain orphan links, harmless for the same reasons
as before (full-chain refold, no checkpoints).

## Store-format stamp is not atomic with family registration

Symptom: a startup open is logically "engine open + register tables", but the two
are separate steps. sema-engine commits its `STORAGE_LAYOUT` stamp (and, for a
genuine prior-version file, orchestrate's own `stamp_current_schema_version`
commits the sema schema-version bump) before family registration runs, and family
validation happens later in `register_table`. So an open that ultimately fails on
`FamilyIdentityMismatch` can leave the file already stamped forward — which is how
the original v0.7.0 deploy left the production store stamped to the current version
yet still carrying the stale agent family, locking out both the new binary and a
rollback.

Current workaround: the repair loop in `open_after_migration` now completes every
recognised repair (version stamp + agent-family migration) so a real store ends
fully migrated and opens cleanly rather than half-stamped. The residual
non-atomicity — a stamp persisting when some *unrecognised* later error aborts the
open — is not orchestrate's to fix: the eager `STORAGE_LAYOUT` commit lives in
sema-engine's `Engine::open` (`apply_layout_plan`), and open cannot see family
descriptors, so it cannot validate identity before stamping.

Proper fix / design question: sema-engine should make the open-and-register
sequence atomic (defer or roll back the format/layout stamp until family
registration succeeds), or accept family descriptors at open so identity is
validated before any write. This is a sema-engine finding, reported upstream, not
patched around here.

## Worktree scaffolding assumes existing Jujutsu metadata

Symptom: `RequestWorktree` on an indexed checkout that is a Git repository but
has no colocated `.jj` metadata invokes `jj workspace add` and fails. The
orchestrator therefore does not yet fulfill the mechanical worktree setup
promise for every indexed Git checkout.

Current workaround: initialize Jujutsu colocated in the known source checkout
with `jj git init --colocate`, track its existing `main` remote bookmark, then
request the worktree again.

Proper fix: `WorktreeRegistry` should explicitly recognize a Git-only indexed
checkout and either perform the safe colocated Jujutsu bootstrap before
scaffolding or return a dedicated typed refusal that names the required
bootstrap. The intended automatic lifecycle needs a deliberate choice between
those behaviors rather than leaking a `jj` subprocess failure.

## Upgrade tier still closes without a reply on engine error

The working tier (generated spine, schema-rust 0.7.1) and the meta tier
(hand-written handler) now answer every decoded request with a complete frame —
ordinary output or the typed `EngineRefusal` under the reserved all-ones short
header — so callers can distinguish an engine error from daemon death. The
upgrade tier still propagates engine errors without writing a reply because it
speaks the shared version-handover *contract* wire, not a schema-emitted frame;
its refusal shape belongs to that shared contract. Fix belongs in the
version-handover contract crate, not here.

## Lane-selected worktree conclusion awaits an exact public identity

Symptom: the current `ConcludeWorktree` wire request selects only an owning lane,
while the durable worktree identity is `(repository, branch)`. Selecting the first
matching row would make a destructive conclusion ambiguous when legacy or manually
registered data contains two live worktrees for one lane.

Current workaround: `ConcludeWorktree` fails closed with a typed engine refusal
when it finds ambiguity; it does not choose a first matching row. Refresh preserves
registered metadata but cannot infer ownership for a previously unknown filesystem
checkout.

Proper fix / producer contract: after the new Protos/schema generator publishes a
stable, integration-tested replacement for the current `schema-rust` contract and
daemon generation APIs, evolve `signal-orchestrate` with an exact
`WorktreeIdentity { RepositoryName BranchName }` in `ConcludeWorktree`. In the same
producer migration, define a PascalCase `WorkSubjectKey` relation record that links
lanes, worktrees, and messenger named threads explicitly; Mint must consume that
relation through its own later boundary, never infer it from matching strings.

## Migration fixture is outside current quality gates

Symptom: `nix build .#checks.x86_64-linux.clippy` fails in
`tests/store_migration_fixtures.rs` because `Permissions::set_readonly(false)` is
denied by the current Clippy lint set: on Unix it makes the file world-writable.
`nix build .#checks.x86_64-linux.fmt` also reports pre-existing Rust formatting
drift in the same fixture. Neither line is changed by the worktree and human-time
integration.

Current workaround: the focused Nix build and behavior checks remain the evidence
for this integration; do not suppress the lint or reformat an unrelated migration
fixture in a feature change that does not own it.

Proper fix: use the platform-appropriate explicit mode API to restore the fixture
file's intended writable permissions, with a portable non-Unix branch where needed,
then format the fixture and restore the full Clippy and format checks as green
repository gates.
