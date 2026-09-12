# Orchestrate

Orchestrate is a durable Lock Nexus. It owns coordination locks in one Sema
store and serves two typed Unix sockets.

## Packages

The repository is a Cargo workspace with one package for each process:

| Package | Binary | Role |
|---|---|---|
| `orchestrate-nexus` | `orchestrate-nexus` | zero-argument Nexus, store, and two sockets |
| `orchestrate` | `orchestrate` | ordinary Datom client |
| `orchestrate-meta` | `orchestrate-meta` | privileged Datom client |

The Nexus packages both Signal contracts without Datom. Each client enables
Datom only for the contract it textualizes.

## Clients

Each client takes exactly one inline Datom query and no flags:

```sh
orchestrate 'Lock.{ MyLock 6329f1 [ /absolute/path ] «why I hold it» }'
orchestrate 'Observe.Locks'
orchestrate 'Release.442'
orchestrate-meta 'Configure.{ «/run/user/1001/orchestrate-nexus/orchestrate.sock» «/run/user/1001/orchestrate-nexus/meta-orchestrate.sock» }'
```

`ORCHESTRATE_SOCKET` selects the ordinary socket and
`ORCHESTRATE_META_SOCKET` selects the privileged socket. The installed
wrappers supply them. With no argument, a client prints its Signal Ethos and
its client-failure Ethos.

The ordinary replies are `ConfigurationAccepted`, `ConfigurationRefused`,
`Locked`, `Released`, `Observed`, `LockRejected`, and `ReleaseRejected`. The
meta replies are `Configured`, `OrdinaryConfigurationReopened`,
`ConfigurationRejected`, and `PeerRefused`. Local client failures are Datom
values:
`Unreadable` for a query that cannot actualize and `Unreachable` for a failed
Signal exchange.

## Wire and store

Framing is the `signal` crate's and only its: a four-byte big-endian length
prefix and one validated rkyv archive. This Nexus owns no length prefix of its
own, and neither do its clients.

Every query is answered with one frame, with one exception. `Observe` opens a
subscription: the Nexus writes the state on open and one further `Observed`
frame for every later change, on the same connection, until the peer closes
it. The subscription is the connection, so there is no token and no `Unwatch`.
The default CLI takes one argument and prints one value, so it ends at the
state on open; a peer that wants the stream reads the socket itself.

The meta socket is bound `0600` and answers only a peer the kernel reports as
its own user; anyone else is answered `PeerRefused` carrying the user id. The
ordinary socket is bound `0660`.

`Configure` is accepted on the ordinary socket only while the privileged
`Configure` has never been done. Whether it was done is a durable record in
the standard Nexus metadata tree, and only `ReverseMetaConfiguration` on the
meta socket unsets it.

The store is
`$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema`, falling back to
`$HOME/.local/state/orchestrate-nexus/orchestrate-nexus.sema`. It persists
configuration, every active Lock, and the next monotonic Lock ID.

`orchestrate-upgrade-preflight` reports how many pre-0.25 PathLock rows a
store still carries. The Nexus refuses to start while that count is nonzero
and never converts such a row into a Lock: a PathLock has no Flow, and
inventing one would attribute a coordination fact to a flow that never
claimed it.

A 0.30 or 0.31 store needs no migration tool. Its Lock and allocator families
are identical to this one's, and its separate configuration family is read
once on first open to seed the metadata tree, then cleared in the same commit.

## Verification

```sh
cargo test --workspace --all-targets
nix flake check -L
```

The durable gates cover both clients' Datom conversion, both live typed
sockets, restart persistence, Lock behavior, the socket modes, the
subscription, the configuration authority and its durability, malformed
archive rejection, refusal of a byte-swapped frame prefix, and — in
`datom-free-nexus` — that the Nexus resolution the package is built from
links neither `datom-codec` nor `protos`.

Upgrade details are in [UPGRADES.md](UPGRADES.md).
