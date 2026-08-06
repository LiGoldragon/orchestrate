# orchestrate skill

Work here when the change concerns typed workspace coordination: roles, claims,
handoffs, lanes, agents, worktrees, or either `orchestrate` CLI.

Rules for work here:

- Never model BEADS as exclusively locked. Any agent may write BEADS while it
  remains the transitional task substrate.
- Keep runtime message delivery in `persona-router`.
- Keep harness lifecycle in `persona-harness`.
- Keep the ordinary and meta policy clients separate: `orchestrate`
  accepts `signal-orchestrate` Dotos and `meta-orchestrate` accepts
  `meta-signal-orchestrate` Dotos. Do not add tier auto-routing back to
  either client.
- This component owns **its own** `sema-engine` database file
  (`orchestrate.sema`). `OrchestrateService` serializes today's
  request handling through the daemon's one service mutex. There is no shared
  cross-component database and no second claim-file state owner.
