# Orchestrate

Orchestrate is a durable Lock Nexus. It owns coordination locks --
who holds which paths, under which flow, for what reason -- in a
single Sema store served over two Unix-domain sockets.

## The Nexus and its sockets

`orchestrate-nexus` is the long-running Nexus. It opens two sockets:

- **Ordinary** (`orchestrate.sock`) -- Lock, Release, Observe.
- **Meta** (`meta-orchestrate.sock`) -- Configure (privileged).

The Nexus starts with zero arguments. It derives per-user locations
from XDG roots and persists them in its Sema store. A populated store
resumes its configuration on the next start.

## The CLIs

`orchestrate` and `meta-orchestrate` are datom-converting edges. Each
takes exactly one inline datom value and no flags:

```
orchestrate 'Lock.{ MyLock 6329f1 [ /absolute/path ] "why I hold it" }'
orchestrate 'Observe.Locks'
orchestrate 'Release.442'
meta-orchestrate 'Configure.{ /o.sock /m.sock }'
```

A string containing a space or a delimiter character is written in
curly quotes \u{201C} \u{201D}. A word without them is bare.

With no argument, each CLI prints its signal contract (the ethos
source) and its client failure vocabulary, then exits 0.

## Requests and replies

Every request is one datom value. Every reply is one datom value on
stdout with exit 0.

### Ordinary socket

| Request | Reply | Rejection |
|---|---|---|
| `Lock.{ MyLock 6329f1 [ /abs/path ] "why I hold it" }` | `Locked.{ 442 MyLock 6329f1 [ /abs/path ] "why I hold it" }` | `LockRejected.DuplicateName.{ ... }` or `LockRejected.PathOverlap.{ ... }` |
| `Release.442` | `Released.{ 442 MyLock 6329f1 [ /abs/path ] "why I hold it" }` | `ReleaseRejected.UnknownLockId` |
| `Observe.Locks` | `Observed.Locks.[]` or `Observed.Locks.[ { 442 MyLock 6329f1 [ /abs/path ] "why I hold it" } ]` | -- |

### Meta socket

| Request | Reply | Rejection |
|---|---|---|
| `Configure.{ /o.sock /m.sock }` | `Configured.{ /o.sock /m.sock }` | `ConfigurationRejected.{ ... }` |

## Faults

A client fault prints one datom value on stderr and exits 1:

```
Unreadable.{ Some.{ 5 13 } Structural.{ { 5 13 } Unclosed.Braced } }
Unreachable.{ /no/such.sock \u{201C}No such file or directory (os error 2)\u{201D} }
Refused.VersionMismatch.{ { 1 0 0 } { 0 9 0 } }
```

`Unreadable` -- the argument could not be actualized as a request.
`Unreachable` -- the socket path or the Nexus is not reachable.
`Refused` -- the Nexus sent a wire-level refusal.

## Wire

The wire is binary rkyv. A frame is `Frame.{ Version Body }` where
Version is the Signal contract's semver triple (e.g. `{ 1 0 0 }`)
and Body is a Request, Reply, or Refusal. Frames are
length-prefixed on the socket. The Signal's version is the wire
version. A version mismatch produces a `Refusal`, not a silent
failure.

## The three repositories

| Repository | Role |
|---|---|
| `orchestrate` | The Nexus, its store, its transport, and the two CLIs. |
| `signal-orchestrate` | The ordinary wire contract: request, reply, and refusal vocabulary. |
| `meta-signal-orchestrate` | The meta wire contract: configuration vocabulary. |

A contract change flows: edit the ethos source in the signal crate,
regenerate through ethos-zero, run the freshness test
(`tests/regeneration.rs`), then pin the new signal crate rev in
orchestrate's `Cargo.toml`.

## Store

The Sema store persists at
`$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema` (or
`$HOME/.local/state/orchestrate-nexus/orchestrate-nexus.sema`). It
holds the Configure value, every active Lock, and the Lock ID
allocator. Lock IDs are durable and never reused.

## Build, test, deploy

Build:

```
nix build
```

Test:

```
cargo test
```

The test suite starts real Nexus processes under isolated XDG roots.
It covers default-store creation, meta configuration persistence,
restart-resume, atomic Lock behavior, typed conflict replies, durable
ID release, canonical observation ordering, malformed-frame rejection,
and CLI fault output.

Deploy (CriomOS):

1. Bump the `orchestrate` flake input in CriomOS-home to the new rev.
2. Rebuild: `nixos-rebuild switch --flake ...`
3. Restart: `systemctl --user restart orchestrate-nexus`

Verify after deployment:

```
orchestrate 'Observe.Locks'
```

The reply must use spaced delimiters and curly-quoted reasons.
The CriomOS-home check `checks/orchestrate-service-path` asserts this.

Upgrade history is in [`UPGRADES.md`](UPGRADES.md).
