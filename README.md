# persona-orchestrate

Typed workspace orchestration state for Persona agents.

This crate models role ownership, claimed scopes, handoffs, and the activity
log that replaces primary workspace lock files over time.

It is not Persona's central mind database. Work graph state, thoughts,
relations, and policy truth belong in `persona-mind`; this crate owns
collaborative orchestration machinery in `persona-orchestrate.redb`.
