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

*Source statements live in `/home/li/primary/intent/persona.nota` and
`/home/li/primary/intent/component-shape.nota`.*
