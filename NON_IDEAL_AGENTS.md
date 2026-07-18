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

## Upgrade tier still closes without a reply on engine error

The working tier (generated spine, schema-rust 0.7.1) and the meta tier
(hand-written handler) now answer every decoded request with a complete frame —
ordinary output or the typed `EngineRefusal` under the reserved all-ones short
header — so callers can distinguish an engine error from daemon death. The
upgrade tier still propagates engine errors without writing a reply because it
speaks the shared version-handover *contract* wire, not a schema-emitted frame;
its refusal shape belongs to that shared contract. Fix belongs in the
version-handover contract crate, not here.
