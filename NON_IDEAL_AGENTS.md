# Non-Ideal Agents Registry

Known non-idealities in this repository: sanctioned workarounds and deferred
proper fixes. Ordinary rules live in `AGENTS.md`; the ideal shape lives in
`ARCHITECTURE.md`. This file is debt with a named future fix target.

## Stale-family drop leaves orphan versioned-log entries

Symptom: when the store migration drops the ephemeral orchestrator-agent registry
under a stale family hash (`OrchestrateStoreMigration::drop_stale_agent_registry`
in `src/tables.rs`), it removes the family's catalog registration and data table
but leaves that family's historical entries in the sema-engine versioned commit
log.

Current workaround: leave the log intact. The versioned log is a hash chain whose
integrity is verified by full-chain refold, and the orphan entries stay valid
links, so the chain and every normal open stay correct. Orchestrate takes no
sema-engine checkpoints and never materializes the versioned log against the live
family inventory, so the orphan entries — which name a family identity no longer
in the catalog — are never resolved and never surface.

Proper fix / design question: if orchestrate ever adopts sema-engine checkpoints
or version-history materialization, a dropped family's orphan log entries would
fail `into_rows` identity resolution. The clean fix is a sema-engine primitive
that retires a family from the catalog, the data table, and the versioned log
together while preserving the chain (a rechaining compaction), rather than a
consumer-side redb edit. This needs a sema-engine feature and a psyche design
decision on whether family retirement is a first-class engine operation.
