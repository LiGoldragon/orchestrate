# orchestrate

Typed workspace orchestration state for Persona agents.

This crate models role ownership, claimed scopes, handoffs, and the activity
log that replaces primary workspace lock files over time.

The runtime surface is a triad: `orchestrate-daemon` owns the
`orchestrate.sema` store, `orchestrate` is the one-argument ordinary
`signal-orchestrate` CLI, and `meta-orchestrate` is the one-argument
`meta-signal-orchestrate` policy CLI.

## Ordinary CLI presentation

Ordinary contract input is shorthand for a typed human presentation:

```text
orchestrate '(Observe Lanes)'
```

Lane elapsed ages encode as closed `HumanReadableTime` values, not text. For
example, a whole minute value is `Minutes.10`; a fractional day value is
`Days.(3.2)`. The human lane projection retains timestamps as exact nanosecond
values, distinct from those elapsed-time units.

Use the explicit form when a program needs the unchanged daemon contract
output:

```text
orchestrate '(Explicit (Canonical (Observe Lanes)))'
```

Both forms lower to the same ordinary Signal request. `Canonical` preserves the
existing `signal-orchestrate` NOTA reply exactly; only the CLI-side `Human`
presentation converts elapsed `DurationNanos` values. An explicit human form,
`(Explicit (Human (Observe Lanes)))`, is equivalent to shorthand.

It is not Persona's central mind database. Work graph state, thoughts,
relations, and policy truth belong in `mind`; this crate owns collaborative
orchestration machinery in `orchestrate.sema`.
