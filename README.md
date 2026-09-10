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

The ordinary replies are `Locked`, `Released`, `Observed`, `LockRejected`,
and `ReleaseRejected`. The meta replies are `Configured` and
`ConfigurationRejected`. Local client failures are Datom values:
`Unreadable` for a query that cannot actualize and `Unreachable` for a failed
Signal exchange.

## Wire and store

Each connection carries one Query and one Response. The transport writes a
little-endian `u32` byte length followed by the contract crate's portable rkyv
Signal bytes. `signal-orchestrate` and `meta-signal-orchestrate` own the typed
archive and restoration operations. The Nexus validates the Query archive
before dispatching it to the store.

The store is
`$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema`, falling back to
`$HOME/.local/state/orchestrate-nexus/orchestrate-nexus.sema`. It persists
configuration, every active Lock, and the next monotonic Lock ID.

`orchestrate-store-migrate <absolute-store-path>` is the offline one-shot
importer for the previous tuple-record families. It validates the old
configuration and allocator, copies configuration, every Lock field, and the
allocator into the current families in one atomic commit, then retracts the
old rows. The Nexus refuses an unmigrated store.

## Verification

```sh
cargo test --workspace --all-targets
nix flake check -L
```

The durable gates cover both clients' Datom conversion, both live typed
sockets, restart persistence, Lock behavior, malformed archive rejection,
and exact-layout synthetic migration from the old tuple records.

Upgrade details are in [UPGRADES.md](UPGRADES.md).
