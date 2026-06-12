# orchestrate

Typed workspace orchestration state for Persona agents.

This crate models role ownership, claimed scopes, handoffs, and the activity
log that replaces primary workspace lock files over time.

The runtime surface is a triad: `orchestrate-daemon` owns the
`orchestrate.sema` store, `orchestrate` is the one-argument ordinary
`signal-orchestrate` CLI, and `meta-orchestrate` is the one-argument
`meta-signal-orchestrate` policy CLI.

It is not Persona's central mind database. Work graph state, thoughts,
relations, and policy truth belong in `mind`; this crate owns
collaborative orchestration machinery in `orchestrate.sema`.
