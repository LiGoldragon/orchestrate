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
named Rust fields while Datom stays positional. Both crates provide
trait-borne `Signalizable`, `ByteViewable`, and `Restorable` operations over a
typed `Signal<T>`.

The runtime adds only transport framing: a little-endian `u32` payload length.
There is no route header, exchange envelope, textual wire form, or alternate
protocol.

## Store ontology

`Locks`, `Releases`, and `Observes` state the ordinary transitions.
`HandlesOrdinary` and `HandlesMeta` dispatch the two Query roots.
`OrchestrateStore` bears those traits and owns:

- one stored Configure value;
- one row per active Lock;
- one monotonic next-Lock-ID row.

The public contract types are not persisted directly. Stable storage records
hold plain named fields, so wire generation changes do not silently redefine
the database layout.

## Migration boundary

The normal open path never reads old tuple records. The offline migration
registers their exact old family names and schema hashes, validates the source
before registering targets, then moves configuration, all Locks, and the
allocator in one Sema atomic commit. Tests write the audited historical tuple
archives through distinct mirror types and read them through the migration's
types, which proves the rkyv layout rather than merely reusing the reader type
as its fixture writer.

The unversioned Sema engine has no online backup API, and the live store sits
on ext4 without filesystem snapshots. A latest-state deployment snapshot
therefore requires controlled quiescence. Production remains on the old Nexus
until the replacement and protected declarative pin are ready for that final
cutover.
