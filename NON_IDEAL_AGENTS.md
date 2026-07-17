# Non-Ideal Agents Registry

Known non-idealities in this repository: sanctioned workarounds and deferred
proper fixes. Ordinary rules live in `AGENTS.md`; the ideal shape lives in
`ARCHITECTURE.md`. This file is debt with a named future fix target.

## Family schema-hash evolution has no sema-engine primitive

Symptom: sema-engine keys a table's identity on a schema hash and validates it by
exact equality at `register_table`; a family whose layout changes (here the
orchestrator-agent registry gaining `last_activity`, bumping its family label from
`v7` to `v8`) is rejected with `FamilyIdentityMismatch`, and the engine exposes no
supported path to evolve a family's identity or migrate its rows in place. There
is no register-prior-shape, retire-family, or rewrite-catalog API; `replay_versioned`
skips entries whose hash differs; checkpoint import requires a fresh store.

Current workaround: orchestrate migrates the agent family consumer-side in
`OrchestrateStoreMigration::migrate_agent_registry` (`src/tables.rs`). It reads the
prior-shape rows by registering the table under its old (`v7`) identity, retires
the stale family with a raw redb edit of the engine-internal `__sema_engine_catalog`
plus a data-table drop (`retire_stale_agent_family`), then re-opens and re-inserts
the rows in the current shape through the ordinary write path. Records are carried
forward, and the re-inserted rows are logged under the current family, so the live
inventory stays consistent. The stale family's historical entries remain in the
versioned commit log as orphan links; the log is a hash chain verified by
full-chain refold, orchestrate takes no checkpoints and never materializes the log
against the live catalog, so those entries never resolve and never surface.

Proper fix / design question: reaching into `__sema_engine_catalog` from a consumer
is a layering violation forced by the missing engine primitive. The clean fix is a
first-class sema-engine family-evolution operation that, under engine control,
rewrites a family's catalog identity, migrates its rows to the new layout, and
rechains the versioned log together while preserving chain integrity — so every
consumer's additive field bump stops bricking existing stores. This needs a
sema-engine feature and a psyche design decision on whether family evolution /
retirement is a first-class engine operation. Until then, every future field added
to any orchestrate family will need a matching consumer-side migration here.

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
