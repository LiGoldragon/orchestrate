# Orchestrate Nexus

Orchestrate Nexus is the durable PathLock state owner. It owns a Sema store and
serves two separate Unix-domain Signal sockets:

- `orchestrate` sends ordinary PathLock registration and release requests.
- `meta-orchestrate` sends owner-only live configuration requests.

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
meta-orchestrate 'Configure.{/absolute/store.sema /absolute/orchestrate.sock /absolute/meta-orchestrate.sock}'
```

It prints `Configured.{...}` or `ConfigurationRejected.{...}`. Set
`ORCHESTRATE_META_SOCKET` to select the meta socket.

Active PathLock names are unique. Each locked path must be absolute and
normalized; an overlapping active path or a duplicate active name is a typed
rejection. Releasing removes the active lock, so the name and paths can be
registered again.

## Nexus startup

`orchestrate-nexus` takes exactly one argument: URL-safe, unpadded base64 of
one generated framed meta `Configure` request. This is an argv-safe envelope
for the typed Signal frame, not a socket protocol. The Nexus rejects malformed
frames and any startup operation other than `Configure`.

The configured store and socket paths are therefore in the typed startup
configuration. A later meta `Configure` must name that same configuration;
an attempted store change receives `StorePathImmutable`, while another
configuration change receives `InvalidConfiguration`.

## Development proof

`cargo test` runs the durable-store tests and starts a real Orchestrate Nexus process for
the live Nexus test. That test uses the actual two client binaries to prove
registration, duplicate-name rejection, path-overlap rejection, release,
re-registration, and a meta Configure round trip.

`nix build .#checks.x86_64-linux.live-nexus` exposes the same live-process
proof as a Nix check.
