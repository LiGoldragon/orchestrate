# Orchestrate Nexus architecture

Orchestrate has three process crates and two contract repositories.

```text
orchestrate                  orchestrate-meta
ordinary Datom CLI           privileged Datom CLI
       |                           |
       | length + typed rkyv       | length + typed rkyv
       v                           v
 ordinary socket               meta socket
       |                           |
       +------ orchestrate-nexus --+
                     |
              orchestrate-nexus.sema
```

## Process boundaries

`orchestrate-nexus` starts with zero arguments. It derives its initial socket
and state locations from XDG roots, persists the configuration on first open,
and resumes the stored configuration afterward. The ordinary and meta
listeners share one serialized `OrchestrateStore` owner.

The clients open no database. They actualize one Query from Datom, form the
contract crate's typed Signal, exchange it over one socket, restore a typed
Response, and textualize it through the open Datom → Protos → text chain.

## Contract boundary

`signal-orchestrate` declares the ordinary Query and Response roots.
`meta-signal-orchestrate` declares the privileged roots. Generated records use
named Rust fields while Datom stays positional. The `signal` crate provides the
trait-borne `Signalizable`, `ByteViewable`, and `Restorable` operations over a
typed `Signal<T>`, and the frame that carries them: a four-byte big-endian
length prefix. One implementation, shared by every Nexus. There is no route
header, exchange envelope, textual wire form, or alternate protocol, and no
framing of this repository's own.

## Nexus Core

`NexusCore` owns the store and the change stream. `Applies<Entering>` is
implemented once per contract this Nexus speaks: an object enters for the
effect, and the response follows as an effect of it. `Announcing` carries the
other half — a subscriber receives the state on open, then every change.

Locks change only through `Applies`, so the core is the only place that can
announce a change, and it announces the whole observation rather than a
delta: a subscriber that joins mid-stream and one that has followed from the
start then hold the same value.

Serialization is `Arc<Mutex<_>>`, which Vision permits while the Kameo
standards are undesigned. The boundary is drawn so that becoming an actor is
a change of `core.rs` alone: nothing outside it touches the store.

## Store ontology

`Locks`, `Releases`, `Observes` and `Configures` state the transitions.
`OrchestrateStore` bears those traits and owns:

- the standard Nexus metadata tree — the desired configuration together with
  whether the privileged Configure was ever done, held in
  `nexus::ConfigurationState`, whose lifecycle rule is the shared one;
- one row per active Lock;
- one monotonic next-Lock-ID row.

The public contract types are not persisted directly. Stable storage records
hold plain named fields, so wire generation changes do not silently redefine
the database layout.

## Cutover boundary

There is no migration tool and no importer. 0.30, 0.31 and 0.32 keep Locks and
the id allocator in identical families with identical record layouts, so a
deployed store carries them across untouched.

What changed is where the configuration lives: 0.30 and 0.31 kept it in its
own family and kept no record of whether the privileged Configure had ever
happened — that record did not exist. 0.32 keeps both in the standard Nexus
metadata tree. So the whole cutover is one read, in `store::cutover`: a store
that already has a configuration row seeds its metadata tree from that row
rather than from the executable's defaults, and the row is retracted in the
same commit. After one open the reader is never reached again.

The seeded record says the privileged Configure has been done. 0.30 and 0.31
had no ordinary `Configure` at all, so every value in a carried row came from
the privileged path — the executable's own constant or the meta socket — and
the ordinary bootstrap window was never open in that store's life. Seeding it
open would let any ordinary peer repoint both sockets, which the next restart
would obey. The window belongs to a Nexus that has never been in service.

Nothing older is read as data. The pre-0.25 `active_path_locks` family is
counted and refused, never converted.

## Datom-free by construction

The Nexus and the Datom clients are built as two separate Cargo resolutions.
Cargo unifies features across the members of one build, so a `--workspace`
build compiles the contract crates with the clients' `datom` feature on and
links `datom-codec` and `protos` into the Nexus as well — a clean manifest
and an unclean artefact. Resolving the Nexus alone is what makes the
Datom-free Nexus a fact about the binary, and the `datom-free-nexus` check
witnesses it on exactly that resolution.
