# Orchestrate — Agent Instructions

## Purpose

`orchestrate` is a durable, metadata-only registry of named native Datom path
locks. It neither locks nor mutates filesystem paths.

## Local Rules

- Use Jujutsu for version control.
- Keep repositories public unless the human gives a specific reason otherwise.
- Use Nix for build and test entry points.
- BEADS is shared coordination state; do not treat it as exclusive ownership.
- No polling in tests; wait for the event under test.
- Durable registry metadata uses `sema-engine` over the redb + rkyv substrate.
- The ordinary text boundary is native Datom. The daemon speaks binary Signal.

## Protos estate status

Stack: correct-new destination
Status: active component
