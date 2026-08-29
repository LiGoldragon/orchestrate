# Upgrades

## 0.26.0 — WireContract and Datomic roots

This is a breaking socket-contract upgrade. Stop the 0.25 Nexus before
activation: ordinary frames change from the legacy routed envelope to
WireContract contract `1/6`, and privileged frames change to `2/5`. Replace
every client at the same time; there is no old frame, Dotos parser, Datom
parser, route/exchange, or text compatibility path.

The durable Sema families, hashes, and schema version remain unchanged because
the persisted configuration, complete Lock facts, and allocator are the same
records. Keep the store in place, but run the existing zero-argument preflight
against the same XDG roots before starting 0.26. It still refuses a nonempty
pre-0.25 `active_path_locks` family and does not mutate the store. Do not
activate if it reports active legacy rows.

After the coordinated restart, verify `Observe.Locks` yields
`Observed.Locks.[]`, acquire a Lock, observe the typed duplicate-name refusal,
and release the returned ID. Verify meta `Configure.{<ordinary> <meta>}`
returns `Configured.{{<ordinary> <meta>}}`; the change takes effect after the
next restart. This release records an upgrade procedure only: it does not
deploy or cut over any live Nexus.

## 0.25.0 — ordinary Lock contract

This is a breaking ordinary-socket upgrade from `PathLock` registration to
`Lock`, `Release(LockId)`, and `Observe.Locks`. Stop the old Nexus
and release every active old PathLock before installing 0.25.0. A nonempty old
active-row store is refused; 0.25.0 never guesses Flow attribution for an old
row. With the old lock set quiescent, the new Nexus retains its durable
configuration and initializes its Lock rows and ID allocator cleanly.

Before activation, run the zero-argument preflight against the same XDG state
root that the Nexus uses:

```text
orchestrate-upgrade-preflight
```

It opens only the legacy `active_path_locks` family under its exact 0.24
identity and prints its row count. It does not open or mutate configuration,
new Lock rows, or the ID allocator; it does not convert old rows. Proceed only
when it reports `active legacy PathLock rows: 0`. A nonzero count means start
the old Nexus, release those Locks with the old client, stop it, and rerun this
preflight. The new runtime checks the same condition again at startup.

Deploy the matching `signal-orchestrate` 1/5 producer and its generated Datom
projection with the Nexus. Replace every old ordinary client invocation and
wire frame; `Register`, `PathLock`, `PathLockRelease`, their reply names, and
the Dotos fallback are not accepted. The meta contract remains unchanged.

After starting the new Nexus, verify one atomic Lock over more than one path,
a typed duplicate-name refusal, `Observe.Locks`, and Release by
the returned Lock ID. Do not force-release or automatically release Locks:
those operations are outside this revision.

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
