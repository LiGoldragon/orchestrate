# persona-orchestrate — architecture

*Typed workspace coordination for agents working in the Persona ecosystem.*

`persona-orchestrate` models the workspace coordination protocol as Rust state:
roles, claims, role-visible lock projections, and handoff tasks. It is the
typed successor to `~/primary/tools/orchestrate`.

---

## 0 · TL;DR

This repo coordinates agents and workspace scopes. It is not the Persona
runtime router, harness delivery engine, or main database owner.

```mermaid
flowchart LR
    "agent" -->|"claim command"| "OrchestrationState"
    "OrchestrationState" -->|"lock projection"| "role lock files"
    "OrchestrationState" -->|"handoff task"| "task projection"
    "OrchestrationState" -->|"orchestrate-owned state"| "persona-sema"
```

## 1 · Component Surface

`persona-orchestrate` exposes:

- a **library crate** (`persona-orchestrate`) that consumes
  the `signal-persona-orchestrate` contract, opens
  `orchestrate.redb` through `persona-sema`, and dispatches
  typed requests to handlers;
- a **binary crate** (`orchestrate`) — the CLI agents
  invoke per call; takes one Nota record on argv (per
  `lojix-cli`'s discipline), prints one Nota record on
  stdout;
- typed `sema::Table<K, V>` constants for the runtime
  state (`CLAIMS`, `ACTIVITIES`, `META`);
- claim/release/handoff handlers with overlap detection;
- the activity log: `ActivitySubmission` writers,
  `ActivityQuery` readers;
- lock-file projection writers (regenerate `<role>.lock`
  files on every claim/release/handoff for backward
  compatibility);
- the `RoleObservation` snapshot builder.

Channel: `signal-persona-orchestrate` (request/reply,
six request kinds + eight reply kinds; see that crate's
`ARCHITECTURE.md`).

The contract repo lands first; this component implements
against it. Per
`~/primary/reports/designer/93-persona-orchestrate-rust-rewrite-and-activity-log.md`,
designer creates the contract, operator/assistant fills
the handlers.

## 2 · State and Ownership

This component owns workspace coordination state — roles, claims, handoff
tasks, lock projections. The state lives in this component's **own redb
file** (e.g. `orchestrate.redb`), opened through `persona-sema` (which uses
the workspace's `sema` database library underneath). Lock files on disk are
projections of the typed records, regenerated from the database on commit.

While primary still uses plain lock files (`tools/orchestrate`), this repo
models the typed replacement. BEADS remains a transitional shared task
substrate and is never modeled as an exclusive lock.

Per `~/primary/reports/designer/92-sema-as-database-library-architecture-revamp.md`:
sema is a library; this component owns its own sema-managed database, the
same way every other state-bearing component does (criome, persona-router,
persona-harness, future mentci).

## 3 · Boundaries

This repo owns:

- agent role and claim state;
- claim/release command surfaces;
- workspace handoff tasks;
- projections compatible with the current orchestration protocol.

This repo does not own:

- runtime Persona delivery (`persona-router`);
- harness lifecycle (`persona-harness`);
- typed table mechanics (`persona-sema` for table layouts; `sema`
  for the kernel underneath);
- BEADS internals or BEADS exclusivity.

## 4 · Invariants

- Every agent knows its role before claiming.
- Claims prevent overlapping file ownership; BEADS is never claimed.
- Lock files are projections, not the source of durable typed truth.
- Open task checks are coordination visibility, not locking.
- The CLI takes **one Nota record on argv** (lojix-cli
  discipline). No flags, no subcommands, no env-var dispatch.
  New behavior lands as a typed positional field on
  `OrchestrateRequest`, never as a flag.
- `Activity::stamped_at` is **store-supplied** at commit
  time, never agent-supplied (per ESSENCE
  infrastructure-mints rule).
- Subscriptions emit only **after** redb commit completes
  (per assistant/90 §"Emit After Commit"; v1 has no
  subscriptions yet — request/reply only).
- Concurrent CLI invocations serialize cleanly through
  redb's MVCC; multiple readers run in parallel.

## 5 · Runtime tables

| Table | Key | Value | Purpose |
|---|---|---|---|
| `CLAIMS` | `(RoleName, ScopeReference)` byte-encoded | `ClaimEntry` | Active claims, one row per (role, scope) pair |
| `ACTIVITIES` | `u64` (slot) | `Activity` | Append-only activity log |
| `META` | `&str` | `u64` | Slot counter for activities; future schema-version meta |

Composite keys are byte-encoded with explicit ordering
(per `~/primary/reports/assistant/90-rkyv-redb-design-research.md`
§"Do Not Store Arbitrary rkyv Archives as redb Keys" — keys
are designed, not rkyv-encoded).

## Code Map

```text
src/lib.rs            module entry; library surface
src/error.rs          typed Error enum (thiserror)
src/state.rs          OrchestrateState handle (opens orchestrate.redb)
src/tables.rs         typed sema::Table<K, V> constants
src/claim.rs          RoleClaim / Release / Handoff handlers
src/observation.rs    RoleObservation handler (build snapshot)
src/activity.rs       ActivitySubmission / ActivityQuery handlers
src/projection.rs     lock-file projection writer
src/service.rs        frame dispatch (request → handler → reply)
src/main.rs           CLI entry: parse Nota argv, dispatch, print Nota reply
tests/claim_release_handoff.rs
tests/activity_log.rs
tests/lock_projection.rs
```

## See Also

- `~/primary/reports/designer/93-persona-orchestrate-rust-rewrite-and-activity-log.md`
  — design report grounding this rewrite.
- `~/primary/protocols/orchestration.md` — the current
  protocol; updated post-Rust-impl.
- `../signal-persona-orchestrate/ARCHITECTURE.md` — the
  contract this component consumes.
- `../persona-sema/ARCHITECTURE.md` — typed table layer.
- `../persona/ARCHITECTURE.md` — apex.
- `~/primary/reports/assistant/90-rkyv-redb-design-research.md`
  — production sema-interface research informing table
  design.
