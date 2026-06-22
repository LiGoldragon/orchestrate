# INTENT - orchestrate

*What the psyche has explicitly intended for this project.
Synthesised from psyche statements; not embellished.*

## Goals

- `orchestrate` is a real triad component: daemon, ordinary thin CLI
  named `orchestrate` for `signal-orchestrate`, and meta-policy thin
  CLI named `meta-orchestrate` for `meta-signal-orchestrate`.
- `orchestrate` should move forward now so the workspace can
  replace the old shell-script orchestration helper with the real
  component.
- The `orchestrate` component CLI is the production surface for agents:
  it takes one NOTA request, sends typed Signal requests to
  `orchestrate-daemon`, and prints one NOTA reply. Lock files are only
  daemon projections.
- The replacement target is **not** legacy argv compatibility. The two
  one-argument NOTA clients talking to `orchestrate-daemon` are the
  complete production surface: `orchestrate` for ordinary working
  operations and `meta-orchestrate` for meta-policy operations. Callers
  migrate to NOTA operations instead of preserving `claim/release/status`
  argv grammar, and the old compatibility helper is retired.
- The immediate MVP should create dynamic roles named by the work they
  own, create report lanes for those roles, and track enough typed
  claim state to replace fixed assistant-lane lock files.

## Boundaries

- `mind` owns state: work graph, memory, thoughts, durable
  policy truth, and channel-grant authority decisions.
- `orchestrate` owns machinery: role claims, activity log,
  agent-run lifecycle, spawn plans, scope-acquisition workflow,
  executor capacity, scheduling, escalation, and lane registry.
- `orchestrate` is not folded into `mind`.

## Principles

- Lane definitions are data, not permanent enum variants. The
  runtime registry belongs in `orchestrate` state, and meta
  authority mutates it.
- Harness assignment is a typed role field (`Codex` or `Claude` in
  the MVP), not information hidden inside the role string.
- Repository management starts from local checkouts: refresh local
  repository state, link checkouts into the workspace, and add
  GitHub/ghq remote creation after the raw shape is useful.
- Components can ship in raw form first. They do not need full
  cross-component wiring before agents can use them directly.
- Claiming a directory claims everything in that directory. If an
  agent wants only specific files, the claim names those files
  explicitly; there is no "directory minus file" handoff shape.

## Anti-Patterns

- Do not deepen the transitional shell helper as the final
  orchestration surface.
- Do not let compatibility lock files become a second state model
  after daemon cutover. The daemon store is the source of truth.
- Do not put orchestration machinery in `mind`.
- Do not model lane churn by recompiling closed role enums as the
  long-term solution.

## Current schema-engine shape

`orchestrate` now carries authored Nexus and SEMA schemas in
`schema/nexus.schema` and `schema/sema.schema`, imports the ordinary
`signal-orchestrate` and `meta-signal-orchestrate` wire contracts, and
emits checked-in `src/schema/{nexus,sema,daemon}.rs` through
`schema-rust-next`. The emitted daemon module uses
`triad-runtime`'s multi-listener runtime for the ordinary and meta
sockets. Runtime execution enters the generated Nexus/SEMA schema
surface: ordinary and meta Signal inputs become `SignalArrived`
Nexus work, Nexus commands SEMA writes/reads, and SEMA owns the
storage-backed claim, lane, role, repository, activity, lock-file
projection, and handover mutation surface. The old
`signal-executor` lowering/command-executor path is no longer a
runtime dependency.

Lane definitions stay data, not closed role enums. The schema-backed
runtime must preserve dynamic role registry persistence without baking
the live role set into the wire.
