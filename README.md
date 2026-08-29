# Orchestrate Nexus

Orchestrate Nexus is the durable Lock state owner. It owns a Sema store and
serves two separate Unix-domain Signal sockets:

- `orchestrate` sends ordinary Lock, Release, and Observe requests.
- `meta-orchestrate` sends owner-only configuration requests.

The Nexus is the sole durable state owner. The clients neither open the store
nor implement a second wire protocol: they parse and render generated Datomic
values and exchange the generated framed Signal values directly.

## Operations

The ordinary client accepts exactly one positional, type-directed Datomic value,
with no flags. Its generated `Request` root selects `Lock`, `Release`, or
`Observe`; it prints the corresponding canonical `Reply` or `Refusal` Datomic
root. Set
`ORCHESTRATE_SOCKET` to select the ordinary socket. The CLI is only a Datom to
Signal boundary: it has no old Dotos parser or compatibility grammar.

The meta client also accepts exactly one positional Datomic value:

```text
meta-orchestrate 'Configure.{/run/user/1000/orchestrate-nexus/orchestrate.sock /run/user/1000/orchestrate-nexus/meta-orchestrate.sock}'
```

It prints `Configured.{...}` or `ConfigurationRejected.{...}`. Set
`ORCHESTRATE_META_SOCKET` to select the meta socket.

Active Lock names are unique. Each Lock holds a Flow attribution, absolute
normalized paths, and a reason. A duplicate name or overlapping path is a
typed rejection. The Nexus assigns a durable non-reused integer Lock ID;
releasing that ID returns the complete released Lock. `Observe.Locks` returns
the complete current snapshot in canonical name-then-ID
order. Flow attribution does not authorize an operation, and this version has
neither force release nor automatic release.

For example, with `ORCHESTRATE_SOCKET` set, the live generated Datomic roots are:

```text
orchestrate 'Lock.{cli-lock 01a03eda [/absolute/path] cli-reason}'
orchestrate 'Observe.Locks'
orchestrate 'Release.1'
```

## Nexus startup and defaults

`orchestrate-nexus` takes zero arguments. It derives and owns these per-user
locations:

- Store: `$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema`, or
  `$HOME/.local/state/orchestrate-nexus/orchestrate-nexus.sema` when
  `XDG_STATE_HOME` is unset.
- Ordinary socket: `$XDG_RUNTIME_DIR/orchestrate-nexus/orchestrate.sock`.
- Meta socket: `$XDG_RUNTIME_DIR/orchestrate-nexus/meta-orchestrate.sock`.

`XDG_RUNTIME_DIR` is required and all XDG roots must be absolute. First start
creates the store and persists the derived socket configuration. Later starts
resume the configuration from that store. A meta `Configure` changes the
durable socket configuration and takes effect on the next Nexus start; it does
not rebind sockets under a running Nexus.

Before upgrading an existing pre-0.25 store, run `orchestrate-upgrade-preflight`
with the same XDG roots. It is a zero-argument, read-only check that reports
the number of active legacy rows; its exact deployment procedure is in
[`UPGRADES.md`](UPGRADES.md).

## Development proof

`cargo test` runs the durable-store tests and starts real zero-argument
Orchestrate Nexus processes under isolated XDG roots. The proof covers
first-store default persistence, argument rejection, meta configuration
persistence, restart-resume, atomic Lock behavior, typed conflict replies,
durable ID release, canonical current observation, malformed-frame rejection,
and clean old-store transition.

`nix build .#checks.x86_64-linux.live-nexus` exposes the same live-process
proof as a Nix check.
