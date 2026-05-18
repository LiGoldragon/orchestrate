# Persona Orchestrate — Agent Instructions

Read `/home/li/primary/AGENTS.md` first, then `/home/li/primary/lore/AGENTS.md`.
This repository follows the primary workspace orchestration protocol.

## Purpose

`persona-orchestrate` is the typed successor to the current primary workspace
orchestration helper. It models roles, scopes, claims, and handoff tasks without
deepening the transitional BEADS dependency.

## Local Rules

- Use Jujutsu for version control.
- Keep repositories public unless the human gives a specific reason otherwise.
- Use Nix for build and test entry points.
- BEADS is shared coordination state and is never claimed or exclusively locked.
- No polling. Orchestration status is pushed through explicit writes and future
  Persona messages.
- Durable orchestration state uses `redb + rkyv`.
