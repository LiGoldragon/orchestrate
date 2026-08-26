# Upgrades

## 0.24.0 — zero-argument default Nexus

This breaking replacement removes the startup `Configure` Signal argv. Start
`orchestrate-nexus` with zero arguments. It creates or resumes only
`$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema` (or
`$HOME/.local/state/orchestrate-nexus/orchestrate-nexus.sema` when the XDG
state root is unset), with runtime sockets under
`$XDG_RUNTIME_DIR/orchestrate-nexus/`.

Do not point the Nexus at, import, move, or migrate the legacy
`$XDG_STATE_HOME/orchestrate/` state. It is deliberately untouched. The fresh
`orchestrate-nexus` namespace is the only store opened by this release.

The meta client now sends `Configure.{<ordinary-socket> <meta-socket>}`. That
configuration persists for the next start; it does not rebind a running Nexus.

## 0.23.0 — Orchestrate Nexus replacement

This is a breaking deployment replacement. Stop and remove the
`orchestrate-daemon` service, then deploy `orchestrate-nexus`. Keep the ordinary
`orchestrate` and privileged `meta-orchestrate` clients; they retain their
single-Datom-argument interfaces.

Discard the old lane, worktree, claim, and lock-projection state. Do not migrate
it, import it, or add a compatibility path. Remove the old durable store and
its projected lock files before starting the fresh Nexus. Start Orchestrate
Nexus with an empty default Sema store, then verify a PathLock registration
and release through the ordinary client.

The 0.23.0 Nexus preserves only PathLock registration and release plus meta
configuration. Former orchestration operations are not available after the
replacement.
