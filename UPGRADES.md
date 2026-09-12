# Upgrades

## 0.33.0 to 0.33.1 -- Pin signal-orchestrate 3.0.2, meta-signal-orchestrate 3.0.2, protos 0.30.1, datom-codec 0.26.3, ethos-zero 8.0.1, signal 3.0.2

### What changed

Dependency pins updated: signal-orchestrate 3.0.2, meta-signal-orchestrate
3.0.2, protos 0.30.1, datom-codec 0.26.3, ethos-zero 8.0.1, signal 3.0.2.
`nexus` is unchanged, still pinned at 0.1.1. No wire change, no store change.
Both `orchestrate` and `orchestrate-meta` regenerate their client contract
from `client.ethos` against ethos-zero 8.0.1 in their `build.rs`; the
regenerated `src/generated/client.rs` is byte-identical to the committed one
in both crates.

### Rollout

Bump the CriomOS `orchestrate` input to the new rev, deploy through Lojix,
and restart orchestrate-nexus. No store migration.

## 0.32.0 to 0.33.0 -- a carried store keeps ordinary Configure shut

No wire change. Both contracts are pinned exactly as 0.32.0 pinned them, and
0.32.0's clients speak to this Nexus unchanged. The change is in the store's
first open and in the tests.

### A store carried across the cutover is not a fresh Nexus

0.32.0 seeded the metadata tree of a carried 0.30 or 0.31 store with the
privileged Configure recorded as *not* done, on the correct reading that those
generations kept no such record. The consequence was not correct: while that
record is unset, `Vision/nexus.md` makes `Configure` accessible on the ordinary
socket, so after the cutover any ordinary peer could repoint both socket paths,
and the next restart would bind them — stranding every client at a path no
wrapper and no unit file knows.

A carried store now resumes with the record set. 0.30 and 0.31 had no ordinary
`Configure` at all: the value in a carried configuration row came from the
executable's own constant or from a meta `Configure`, both privileged. The
ordinary bootstrap window was never open in that store's life, and Vision gives
it to a Nexus that has never been in service. A fresh store is unaffected — it
seeds from the defaults with ordinary `Configure` open, exactly as before.

A deployment cutting over from 0.30 or 0.31 therefore no longer depends on a
meta `Configure` being the first act after start. Ordinary `Configure` on such
a store answers `ConfigurationRefused.MetaConfigureOccurred` until
`ReverseMetaConfiguration` is sent on the meta socket.

Witnessed in `store::cutover::tests`, and through the sockets of the real
executable over a store written in the deployed 0.30.0 shape, in
`tests/live_nexus.rs`.

### The meta socket's refusing branch is witnessed end to end

0.32.0 tested the admission rule and the admitting wire path, and recorded the
refusing wire path as having no end-to-end witness. It has two now.

`transport::session::tests` binds a real privileged socket, accepts a real
connection, reads the peer's credential with `SO_PEERCRED`, and asserts that a
peer who is not the socket's owner receives `PeerRefused.PeerRejection` naming
it, before it has written anything, and that the connection then closes. The
socket's owner is named by the test rather than read from the file, because a
single-user host has no second user to borrow; its companion test drives the
same path with the owner admitted, so the refusal is the rule's doing and not
the harness's.

`tests/second_user_peer.rs` closes the last step — that the kernel's own answer
for a genuinely different user reaches the rule — by connecting from a process
mapped to the host's first subordinate uid. It requires `/etc/subuid`,
`newuidmap`, `unshare` and `setpriv`; a Nix build sandbox grants none of them,
so where the requirement is unmet the test says so and ends rather than
reporting a refusal it did not see. The new `peer-authority` check runs it.

### The CLI string delimiter, which no entry had recorded

Not a change of this release, and a deploy-day break all the same. Since
0.31.0 the clients have been built against a Protos generation whose opaque
string is bounded by guillemets, `\u{00AB}like this\u{00BB}`; the deployed
0.30.0 clients bound it with curly quotes, `\u{201C}like this\u{201D}`, and
still accept them. So the first multi-word Lock reason sent through a client
from this package fails, with `Unreadable.Error.{ Composition [ 1 ]
Arity.{ 4 5 } }` — an error that names neither quotes nor the reason field.
`AGENTS.md` said curly quotes until this release; it now says guillemets and
says what the old form produces. Any document outside this repository that
teaches the CLI — the `orchestrate` skill among them — needs the same
correction before a deploy.

## 0.31.0 to 0.32.0 -- shared frame, real authority, no migration tool

Breaking on the wire, in the store, and in the package. Not deployed by this
change.

### The frame is the shared one

Orchestrate wrote a little-endian `u32` length prefix at three production
sites and in five tests, while `signal/src/frame.rs` — the crate created to
end exactly this disagreement — is big-endian. The mistake was invisible
because Orchestrate spoke only to Orchestrate, and its own tests shared it.
Orchestrate now frames through `signal` and carries no framing of its own.
`live_nexus::a_little_endian_prefix_is_not_the_shared_frame` witnesses that
the old prefix is now refused.

Any client built before 0.32.0 will not be understood, and must be replaced.

### The Nexus is Datom-free as built, not merely by manifest

The Nix package built the whole workspace in one Cargo resolution, so feature
unification compiled the contract crates with the clients' `datom` feature on
and linked `datom-codec` and `protos` into `orchestrate-nexus`. The Nexus and
the clients are now two separate resolutions joined at install, and the
`datom-free-nexus` check runs `cargo tree` on the Nexus resolution the package
is built from.

`packages.nexus` and `packages.clients` are the two halves; `packages.default`
is their join and still contains every binary the previous package did, minus
`orchestrate-store-migrate`.

### Authority on the meta socket

The meta socket was privileged in name only: it bound exactly like the
ordinary socket and authorised nobody. It is now bound `0600` and answers only
a peer the kernel reports as its own user. A refused peer receives
`PeerRefused.PeerRejection` carrying the user id. The ordinary socket is bound
`0660`.

### A durable record of whether the privileged Configure occurred

There was none. The standard Nexus metadata tree now holds the desired
configuration together with `meta_configure_done`, using
`nexus::ConfigurationState` so the lifecycle rule is the shared one. Ordinary
`Configure` is accepted while that record is unset and refused with
`MetaConfigureOccurred` afterwards; `ReverseMetaConfiguration` on the meta
socket unsets it again.

### Observe is a subscription

`Observe` no longer answers once and closes. The Nexus writes the state on
open and one further `Observed` frame for every later change, on the same
connection, until the peer closes it. No vocabulary changed: the subscription
is the connection, so there is no token and no `Unwatch`. The default CLI
still takes one argument and prints one value, so it ends at the state on
open.

### `orchestrate-store-migrate` is deleted, with its migration path

It was written for the wrong generation. It required exactly one row in the
*pre-0.30* configuration and allocator families; 0.30 emptied those families
before it was ever deployed, so the tool would have refused the live store it
was written for. The families, the `Previous*` record types, the
`PreviousSignalMigrationRequired` and `Migration*` refusals, and the two tests
that exercised them are gone.

What a real 0.30-or-0.31 to 0.32 cutover needs is one read, and that is all
that remains (`store::cutover`): Locks and the allocator carry across
untouched, and a store that still has the separate configuration family has
its metadata tree seeded from that row on first open, the row retracted in the
same commit.

`orchestrate-upgrade-preflight` and the pre-0.25 `active_path_locks` guard
stay. They are a different generation and a refusal rather than a migration,
and the deployed `orchestrate-service-path` check names the binary.

### Corrections to the 0.30.0 and 0.31.0 entries below

Both entries describe a tuple-record migration as a live requirement. It was
not one. `5f016531`, the deployed 0.30.0, and `1bc55af1`, the released 0.31.0,
carry textually identical `StoredConfiguration`, `StoredLock` and
`StoredAllocator` types and identical family constants; the tuple types both
revisions carry read the *pre-0.30* families, which 0.30 itself emptied on
2026-09-08. Read those entries as history, not as instructions.

## 0.30.0 to 0.31.0 -- separate Nexus and Datom clients

This is a coordinated socket and durable-archive cutover. The workspace now
contains the Datom-free `orchestrate-nexus` daemon, the ordinary `orchestrate`
client, and the privileged `orchestrate-meta` client. The clients actualize
the generated `Query` roots through Datom; the daemon exchanges the generated
`Query` and `Response` roots as length-prefixed portable rkyv bytes. There is
no routed wire envelope or old-contract compatibility path.

The named generated records change the archives stored by Sema. Before
installing 0.31.0, stop the old Nexus, copy the latest
`orchestrate-nexus.sema` into isolated state, and run
`orchestrate-store-migrate` against that copy. The migration reads the exact
0.30 tuple-shaped `Configure`, `Lock`, and allocator records and writes the
0.31 named records while preserving configuration, every lock field, and the
next monotonic ID. Validate the migrated copy by starting the replacement with
isolated `XDG_RUNTIME_DIR` and `XDG_STATE_HOME`, exercising both clients, and
restarting it before installing the new package. If migration or validation
fails, leave the old store in place and restart the old Nexus.

The installed clients and Nexus must come from the same 0.31.0 package. Point
`ORCHESTRATE_SOCKET` and `ORCHESTRATE_META_SOCKET` at the generated unit's
ordinary and privileged sockets. After activation, verify `Observe.Locks`, a
Lock and Release cycle, and privileged `Configure` through `orchestrate-meta`.

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
offline. The Sema/redb store takes the native exclusive writable file lock, so
the importer refuses a second concurrent owner instead of guessing from a PID
or socket path. It validates v1 configuration and allocator rows before it
registers any v2 table; a malformed v1 source therefore leaves no empty v2
catalogue registrations. After source validation, the v2 record assertions and
v1 retractions land in one atomic durable commit. A repeated invocation finds
no longer a valid v1 source and refuses without changing the completed v2
records. Start the new daemon only after it succeeds, then verify the retained
configuration, active locks, and next allocated Lock identifier.
