# persona-orchestrate

Typed workspace orchestration state for Persona agents.

This crate models role ownership, claimed scopes, handoff tasks, and the typed
replacement for primary workspace lock files.

It is not Persona's main daemon database. Runtime Persona state belongs behind
`persona-store`; this crate models collaborative workspace coordination.
