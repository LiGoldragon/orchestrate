# Orchestrate Nexus

Orchestrate Nexus is the durable PathLock state owner. It owns a Sema store and
serves two separate Unix-domain Signal sockets:

- `orchestrate` sends ordinary PathLock registration and release requests.
- `meta-orchestrate` sends owner-only configuration requests.

The Nexus is the sole durable state owner. The clients neither open the store
nor implement a second wire protocol: they parse and render generated Datom
values and exchange the generated framed Signal values directly.

## Operations

The ordinary client accepts exactly one positional Datom value, with no flags:

```text
orchestrate 'PathLock.{my-lock [/absolute/path] (short purpose)}'
orchestrate 'PathLockRelease.{my-lock}'
```

It prints the corresponding concrete generated reply carrier, such as
`PathLockRegistered.{...}`, `PathLockRegistrationRejected.{...}`, or
`PathLockReleased.{...}`. Set `ORCHESTRATE_SOCKET` to select the ordinary
socket.

The meta client also accepts exactly one positional Datom value:

```text
meta-orchestrate 'Configure.{/run/user/1000/orchestrate-nexus/orchestrate.sock /run/user/1000/orchestrate-nexus/meta-orchestrate.sock}'
```

It prints `Configured.{...}` or `ConfigurationRejected.{...}`. Set
`ORCHESTRATE_META_SOCKET` to select the meta socket.

Active PathLock names are unique. Each locked path must be absolute and
normalized; an overlapping active path or a duplicate active name is a typed
rejection. Releasing removes the active lock, so the name and paths can be
registered again.

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

## Development proof

`cargo test` runs the durable-store tests and starts real zero-argument
Orchestrate Nexus processes under isolated XDG roots. The live proof covers
first-store default persistence, argument rejection, meta configuration
persistence, restart-resume, PathLock registration and release.

`nix build .#checks.x86_64-linux.live-nexus` exposes the same live-process
proof as a Nix check.
