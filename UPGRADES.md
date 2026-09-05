# Upgrades

## 0.29.1 to 0.29.2 -- Import datomic::Situated<datomic::Fault> from datomic

### What changed

`ethos/client.ethos` and `ethos/meta_client.ethos` now import
`datomic:[ Situated Fault ]` and declare
`ClientFailure.[ Unreadable.Situated<Fault> ... ]`. The local `Situated`
struct is removed. Generated code references `datomic::Situated<datomic::Fault>`
directly; the blanket `impl<F: Datomic> Corporal<Datom> for Situated<F>` added
in datomic 0.9.1 makes this compile without orphan violations. Wire format and
all CLI stderr lines are byte-identical to 0.29.1.

### Rollout

Bump the CriomOS `orchestrate` input to the new rev, deploy through Lojix,
and restart orchestrate-nexus. No store migration.

## 0.29.0 to 0.29.1 -- Pin protos 0.15.1, datomic 0.9.1, ethos-zero 1.3.1

### What changed

Dependency pins updated: protos 0.15.1 (Situated<F> gains
Clone/Debug/PartialEq/Eq), datomic 0.9.1 (re-exports the new derives),
ethos-zero 1.3.1 (Copy for unit-only enums, pin fixes), signal-orchestrate
0.20.1 and meta-signal-orchestrate 0.14.1. No wire change, no store change.
Generated code and all CLI stderr lines are byte-identical.

### Rollout

Bump the CriomOS `orchestrate` input to the new rev, deploy through Lojix,
and restart orchestrate-nexus. No store migration.

## 0.28.0 to 0.29.0 -- Pin datomic 0.9.0, ethos-zero 1.2.0

### What changed

Dependency pins updated: datomic 0.9.0 (Situated<F> bears Corporal/Datomic;
impl_datomic_box!), ethos-zero 1.2.0 (Library derives Clone/Debug/PartialEq/Eq;
Meaning intrinsic; recursive positions boxed). Signal crates updated to
signal-orchestrate 0.20.0 and meta-signal-orchestrate 0.14.0.

Generated Library types now carry `#[derive(Clone, Debug, PartialEq, Eq)]`.
Situated remains locally declared because `protos::Situated<F>` lacks
PartialEq/Eq; datomic 0.9.0 provides Corporal/Datomic for Situated<F>
but the Library derives prevent importing it directly.

No wire change. The datom shape and all CLI stderr lines are byte-identical.

### Rollout

Bump the CriomOS `orchestrate` input to the new rev, deploy through Lojix,
and restart orchestrate-nexus. No store migration.

## 0.27.0 to 0.28.0 -- Generated ClientFailure

### What changed

The hand-written `ClientFailure` enum and its `Corporal`/`Datomic` impls
are replaced by a generated Library ethos file. Each CLI has its own
ethos file (`ethos/client.ethos`, `ethos/meta_client.ethos`) that imports
`Fault` from datomic, `Extent` from protos, and `Refusal` from its
signal crate. The generated Rust is committed at `src/generated/` with
a freshness test.

`Situated` is defined locally in the ethos file (not imported as a
generic) because datomic does not yet carry blanket `Corporal`/`Datomic`
impls for `Situated<F>`. The local struct has an identical datom shape.

The no-argument self-description now prints the client Library's
canonical text from its ethos concept (actualized and protosized through
ethos-zero) instead of a hand-written commented block.

### Wire and store compatibility

No wire or store changes. The datom text output of every client fault
is byte-identical to 0.27.0.

### Rollout

Same as 0.27.0: bump the CriomOS flake input `orchestrate` to the
0.28.0 rev and deploy via Lojix `Deploy.UserEnvironment` with
`ActivateNow`.

## 0.26.0 to 0.27.0 -- ProtoformStack

### What changed

The entire datom pipeline is rewritten. The signal wire, the CLIs, and
all domain types now use the ProtoformStack generation of protos,
datomic, and ethos-zero.

**Wire**: Frame envelope is now `Frame.{ Version Body }` -- contract id
and wire revision fields are removed. All domain types are positional
tuple structs. Single-field newtypes (`LockName`, `FlowId`, `LockPath`,
`LockReason`, `LockPaths`, `Configured`) are removed; named type
aliases remain in the ethos but generate as Rust `pub type` aliases.

**Datom text**: canonical output uses spaced delimiters (`{ a b }` not
`{a b}`). A reason with spaces is curly-quoted (`\u{201C}...\u{201D}`).
Empty enclosures are tight (`[]`).

**CLIs**: both CLIs now print replies and refusals as canonical datom
text (not Rust Debug). Client faults (`Unreadable`, `Unreachable`,
`Refused`) print datom on stderr with exit 1, no prefix. With no
argument, each CLI prints its signal ethos source and its client
failure ethos, then exits 0.

**Ethos**: both signal crates carry an `ethos/signal.ethos` file and a
`tests/regeneration.rs` freshness test that proves the committed
generated module matches ethos-zero output.

**API**: `datomic::Text::<T>::from(text).embody()` is replaced by
`protos::Potential::<T, datomic::Datom>::from(text).actualize()`.
`reply.textualize().as_ref()` is replaced by
`datomic::Textualizable::textualize(&reply)` (returns `String`).

### Store compatibility

The Sema store schema version, table names, table descriptors, and
record key shapes are unchanged between 0.26 and 0.27. The persisted
rkyv archives of `Configure` and `Lock` use the same field layout
(positional tuple structs in both versions). The `LockId` allocator
record is an `i64` in both. The 0.27 Nexus opens a 0.26 store without
migration.

This was verified in flow 6329f1's final witness: the test suite
includes `released_ids_never_reach_a_later_lock_after_restart`, which
creates a store, stops, and resumes from it. No store migration code
exists or is needed.

### Rollout

1. Bump CriomOS flake input `orchestrate` to the 0.27 rev. CriomOS
   carries `criomos-home.inputs.orchestrate.follows = "orchestrate"`,
   so the pin propagates to CriomOS-home without a separate bump.
2. Deploy via Lojix `Deploy.UserEnvironment` with `ActivateNow` at the
   new CriomOS rev. The activation sets the home-manager profile and
   restarts `orchestrate-nexus` automatically.
3. Verify: `orchestrate 'Observe.Locks'` -- the reply must use spaced
   delimiters and curly-quoted reasons.
4. The CriomOS-home check `checks/orchestrate-service-path` asserts
   the new spaced-delimiter text.

Existing locks survive the restart. The 0.27 Nexus reads the 0.26
store as-is. New replies use spaced canonical datom.

## 0.25.0 to 0.26.0 -- WireContract and Datomic roots

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
next restart.

## 0.24.0 to 0.25.0 -- ordinary Lock contract

This is a breaking ordinary-socket upgrade from `PathLock` registration to
`Lock`, `Release(LockId)`, and `Observe.Locks`. Stop the old Nexus
and release every active old PathLock before installing 0.25.0. A nonempty old
active-row store is refused; 0.25.0 never guesses Flow attribution for an old
row. With the old lock set quiescent, the new Nexus retains its durable
configuration and initializes its Lock rows and ID allocator cleanly.

Before activation, run the zero-argument preflight against the same XDG state
root that the Nexus uses:

```
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
the returned Lock ID.

## 0.23.0 to 0.24.0 -- zero-argument default Nexus

This breaking replacement removes the startup `Configure` Signal argv. Start
`orchestrate-nexus` with zero arguments. It creates or resumes only
`$XDG_STATE_HOME/orchestrate-nexus/orchestrate-nexus.sema` (falling back to
`$HOME/.local/state`), with runtime sockets under
`$XDG_RUNTIME_DIR/orchestrate-nexus/`.

Do not point the Nexus at, import, move, or migrate the legacy
`$XDG_STATE_HOME/orchestrate/` state. The fresh `orchestrate-nexus` namespace
is the only store opened by this release.

The meta client now sends `Configure.{<ordinary-socket> <meta-socket>}`. That
configuration persists for the next start; it does not rebind a running Nexus.

## 0.22.0 to 0.23.0 -- Orchestrate Nexus replacement

This is a breaking deployment replacement. Stop and remove the
`orchestrate-daemon` service, then deploy `orchestrate-nexus`. Keep the ordinary
`orchestrate` and privileged `meta-orchestrate` clients.

Discard the old lane, worktree, claim, and lock-projection state. Do not migrate
it. Remove the old durable store and its projected lock files before starting the
fresh Nexus. Start with an empty default Sema store, then verify a PathLock
registration and release through the ordinary client.
# Orchestrate 0.30.0: Signal frame and durable-record v2

This release replaces the generated producer-owned frame with signal-frame's
bound structural archives. Stop the Nexus before upgrading; this document does
not authorize a service restart or deployment.

Existing v1 configuration, Lock, and allocator records require the explicit
offline `orchestrate-store-migrate <absolute-store-path>` operation. It copies
all three record classes into v2 tables and retracts the v1 rows in the same
atomic durable commit. The normal daemon refuses any remaining v1 rows with
`PreviousSignalMigrationRequired`; it never resets, drops, or reads those rows
at runtime.

Stop the daemon and preserve a backup copy before running the migration. Run
`orchestrate-store-migrate <absolute-store-path>` once while the daemon is
offline. It refuses a nonempty v2 target, so a repeated invocation cannot
overwrite a completed migration. Start the new daemon only after it succeeds,
then verify the retained configuration, active locks, and next allocated Lock
identifier.
