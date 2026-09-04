# Orchestrate Nexus -- architecture

Orchestrate is a small Lock Nexus. It has one durable state owner,
two sockets, two CLIs, and no lane, claim-file, worktree, workflow,
or peer-coordination machinery.

## Process boundary

```
orchestrate                 meta-orchestrate
(one datom argument)        (one datom argument)
       |                           |
       | Frame.{ Version Body }    | Frame.{ Version Body }
       v                           v
  ordinary socket             meta socket
       |                           |
       +------ orchestrate-nexus --+
                     |
              orchestrate-nexus.sema
```

The ordinary and meta sockets carry different signal contracts:
`signal-orchestrate` (ordinary) and `meta-signal-orchestrate` (meta).
Each accepted Unix connection carries one length-prefixed binary rkyv
frame and receives one reply frame.

The frame envelope is `Frame.{ Version Body }`. Version is the signal
contract's semver triple. Body is a Request, Reply, Refusal, or (on
the wire only) a version-mismatch Refusal. There is no contract id or
wire revision field; the Signal's version is the wire version.

## Ethos

The wire vocabulary is declared in ethos. Each signal crate owns an
`ethos/signal.ethos` file and a generated `src/generated/signal.rs`.

Ordinary signal ethos (signal-orchestrate):

```
Signal.{ 1 0 0 }

[]

[ Lock.LockRequest  Release.LockId  Observe.ObserveSelection ]

[ Locked.Lock  Released.Lock  Observed.Observation
  LockRejected.LockRejection  ReleaseRejected.ReleaseRejection ]

[ LockId.Integer
  LockName.Text
  FlowId.Text
  LockPath.Text
  LockReason.Text
  LockRequest.{ LockName FlowId Vector<LockPath> LockReason }
  Lock.{ LockId LockName FlowId Vector<LockPath> LockReason }
  LockOverlap.{ LockPath Lock }
  LockRejection.[ DuplicateName.Lock  PathOverlap.LockOverlap ]
  ReleaseRejection.[ UnknownLockId ]
  ObserveSelection.[ Locks ]
  Observation.[ Locks.Vector<Lock> ] ]
```

Meta signal ethos (meta-signal-orchestrate):

```
Signal.{ 1 0 0 }

[]

[ Configure.Configure ]

[ Configured.Configure  ConfigurationRejected.ConfigurationRejection ]

[ OrdinarySocketPath.Text
  MetaSocketPath.Text
  Configure.{ OrdinarySocketPath MetaSocketPath }
  ConfigurationRefusal.[ InvalidConfiguration ]
  ConfigurationRejection.{ Configure ConfigurationRefusal } ]
```

The generated Rust is the single source of truth for frame encoding,
decoding, and validation. Regeneration through ethos-zero is checked
by `tests/regeneration.rs` in each signal crate.

## Traits first, no free functions

Every method lives under a trait. The ordinary ontology:

- **`Locks`** -- atomically records one complete Lock or returns a
  typed rejection (`DuplicateName` or `PathOverlap`).
- **`Releases`** -- removes the Lock named by its durable non-reusable
  `LockId`, or returns `UnknownLockId`.
- **`Observes`** -- captures one complete point-in-time observation
  (currently `Locks`, ordered by name then ID).

These traits are implemented by `OrchestrateStore`, the sole durable
owner. Transport dispatches to it; it never duplicates domain
decisions.

`fn main()` is the only free function. When no owning type exists,
the model is incomplete -- name the missing type.

## State and rules

The Sema store persists:

- The `Configure` value (socket paths).
- Every active `Lock` row.
- The next `LockId` (monotonic, never reused).

A Lock carries five positional fields: `LockId`, `LockName`, `FlowId`,
`Vector<LockPath>`, `LockReason`. Paths are absolute and normalized.
`FlowId` is attribution, not authorization; release is cooperative.
There is no force release and no automatic release.

The startup boundary: the executable owns the default XDG-derived
configuration. `DefaultConfiguration::from_process()` reads per-user
XDG roots and rejects every startup argument. A new store persists
those defaults; a populated store resumes them.

## CLI boundaries

Both CLIs accept exactly one datom value and no flags. The ordinary
CLI accepts a `Request` root (`Lock`, `Release`, or `Observe`) and
prints the canonical `Reply` or `Refusal` datom root on stdout.
`meta-orchestrate` accepts `Configure` and prints `Configured` or
`ConfigurationRejected`.

A client fault is a `ClientFailure` enum with three variants:

- `Unreadable` -- the argument failed actualization.
- `Unreachable` -- the socket is unreachable.
- `Refused` -- the Nexus sent a wire-level refusal.

Each fault is printed as datom on stderr with exit 1.

With no argument, each CLI prints its signal contract ethos source
and the `ClientFailure` ethos, then exits 0. This is the
no-argument self-description.

## Code map

```
src/defaults.rs              XDG default derivation (DefaultConfiguration)
src/main.rs                  zero-argument startup and runtime launch
src/ordinary.rs              Locks / Releases / Observes trait ontology
src/store.rs                 durable Lock / configuration transitions (OrchestrateStore)
src/transport.rs             two-socket Unix transport (TransportRuntime)
src/bin/orchestrate.rs       ordinary datom CLI
src/bin/meta_orchestrate.rs  meta datom CLI
tests/ordinary_lock_contract.rs  store-level Lock behavioral proof
tests/live_nexus.rs          live Nexus process and CLI behavioral proof
```

## Verification

`cargo test` starts real Nexus processes under isolated XDG roots,
then checks: first-store default persistence, zero-argument startup,
argument rejection, meta configuration persistence, restart-resume,
Lock/Release/Observe behavior, typed conflict replies, durable ID
non-reuse, canonical observation ordering, malformed-frame rejection,
CLI fault datom output, and no-argument ethos self-description.

`nix build .#checks.x86_64-linux.live-nexus` runs the same proof as
a Nix check.
