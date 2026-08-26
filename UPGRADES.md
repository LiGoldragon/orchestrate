# Upgrades

## 0.23.0 — Orchestrate Nexus replacement

This is a breaking deployment replacement. Stop and remove the
`orchestrate-daemon` service, then deploy `orchestrate-nexus` with one encoded
meta `Configure` Signal frame as its sole argument. Keep the ordinary
`orchestrate` and privileged `meta-orchestrate` clients; they retain their
single-Datom-argument interfaces.

Discard the old lane, worktree, claim, and lock-projection state. Do not migrate
it, import it, or add a compatibility path. Remove the old durable store and
its projected lock files before starting the fresh Nexus. Start Orchestrate
Nexus with an empty configured Sema store, then verify a PathLock registration
and release through the ordinary client.

The 0.23.0 Nexus preserves only PathLock registration and release plus meta
configuration. Former orchestration operations are not available after the
replacement.
