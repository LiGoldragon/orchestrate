# INTENT - orchestrate

*What the psyche has explicitly intended for this project.
Synthesised from psyche statements; not embellished.*

## Goals

- `orchestrate` is a real triad component: daemon, thin CLI,
  ordinary `signal-orchestrate` contract, and owner-only
  `owner-signal-orchestrate` contract.
- `orchestrate` should move forward now so the workspace can
  replace the old shell-script orchestration helper with the real
  component.
- The immediate MVP should create dynamic roles named by the work they
  own, create report lanes for those roles, and track enough typed
  claim state to replace fixed assistant-lane lock files.

## Boundaries

- `persona-mind` owns state: work graph, memory, thoughts, durable
  policy truth, and channel-grant authority decisions.
- `orchestrate` owns machinery: role claims, activity log,
  agent-run lifecycle, spawn plans, scope-acquisition workflow,
  executor capacity, scheduling, escalation, and lane registry.
- `orchestrate` is not folded into `persona-mind`.

## Principles

- Lane definitions are data, not permanent enum variants. The
  runtime registry belongs in `orchestrate` state, and owner
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
- Do not put orchestration machinery in `persona-mind`.
- Do not model lane churn by recompiling closed role enums as the
  long-term solution.

## Pending schema-engine upgrade

**Status:** scheduled for migration to schema-language-based contract per
`primary/reports/designer/326-v13-spirit-complete-schema-vision.md` +
`primary/reports/designer/324-migration-mvp-spirit-handover-re-specification.md`.
The reader model is multi-pass NOTA-first per spirit record 549; macro
application iterates to a fixed point per record 569.

**Target:** this component's hand-written `signal_channel!` invocation +
Layer 2 Component Commands + storage types convert to a single
`orchestrate/orchestrate.schema` file. The brilliant macro library
(`primary-ezqx.1`) reads the schema + emits all the wire types +
ShortHeader projection + dispatcher + VersionProjection + storage
descriptors.

**Sequence:** Spirit is the MVP pilot landing first via
`primary-ezqx.1`. Orchestrate cuts over after Spirit and mind because
the authority chain `mind -> orchestrate -> router/harness` means
orchestrate's outbound owner calls should land on the schema engine
after the contracts at both ends.

**Per-component concerns:** Cluster/lifecycle orchestration; schema
cutover after Spirit + mind. Lane definitions stay data (not closed
role enums) under the schema — the schema must enable dynamic-role
registry persistence without baking the live role set into the wire.
Per spirit record 562 enums place data-carrying variants first; adding
a new unit lane outcome is a no-op upgrade. Mirror phase ordering is
orchestrate's load-bearing question: in-memory critical state (active
claims, lane bindings) needs to transfer BEFORE cutover, not after. Per
/333-v2 §4.1.

**References:**
- `primary/reports/designer/326-v13-spirit-complete-schema-vision.md` —
  uniform header form + schema-language design
- `primary/reports/designer/333-upgrade-mechanism-full-design-explained.md`
  + `333-v2` — upgrade mechanism design + corrections
- `primary/reports/designer/334-v2-multi-pass-nota-first-schema-reader.md`
  — multi-pass reader model (record 549)
- `primary/reports/designer/324-migration-mvp-spirit-handover-re-specification.md`
  — migration MVP + handover state
- `primary/reports/operator/174-schema-import-header-design-critique-2026-05-24.md`
  — header/body/feature separation + lowering rules

*Source statements live in `/home/li/primary/intent/persona.nota` and
`/home/li/primary/intent/component-shape.nota`.*
