# persona-orchestrate skill

Work here when the change concerns typed workspace coordination: roles, claims,
handoff tasks, lock projections, or the `orchestrate` CLI.

Rules for work here:

- Never model BEADS as exclusively locked. Any agent may write BEADS while it
  remains the transitional task substrate.
- Keep runtime message delivery in `persona-router`.
- Keep harness lifecycle in `persona-harness`.
- This component owns **its own** `persona-sema`-backed redb file (e.g.
  `orchestrate.redb`). The orchestration state actor sequences writes through
  that database; no shared cross-component DB.
- Lock files are projections for human and cross-harness visibility,
  regenerated from the typed records on commit.

