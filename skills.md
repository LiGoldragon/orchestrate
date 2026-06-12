# orchestrate skill

Work here when the change concerns typed workspace coordination: roles, claims,
handoff tasks, lock projections, or the `orchestrate` CLI.

Rules for work here:

- Never model BEADS as exclusively locked. Any agent may write BEADS while it
  remains the transitional task substrate.
- Keep runtime message delivery in `persona-router`.
- Keep harness lifecycle in `persona-harness`.
- Keep the ordinary and meta policy clients separate: `orchestrate`
  accepts `signal-orchestrate` NOTA and `meta-orchestrate` accepts
  `meta-signal-orchestrate` NOTA. Do not add tier auto-routing back to
  either client.
- This component owns **its own** `sema-engine` database file
  (`orchestrate.sema`). `OrchestrateService` serializes today's
  request handling; the orchestration state actor becomes the long-lived
  sequencer when the daemon is wired. There is no shared cross-component DB.
- Lock files are projections for human and cross-harness visibility,
  regenerated from the typed records on commit.
