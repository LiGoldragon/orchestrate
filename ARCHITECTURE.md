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

`NexusCore` is the one actor in the process and owns `OrchestrateStore` by
value. What an actor is for is owning exclusive mutable state and serialising
access to it, so the boundary is drawn at the state: one Lock table is one
unit of consistency, so it gets one actor, and the only way anything reaches
the store is a message.

`Message<T>` is implemented once per contract this Nexus speaks: an object
enters for the effect, and the response follows as an effect of it. That is
what `Applies<Entering>` used to name in this repository; Kameo's `Message`
is the same kind from the library the actor layer already is, so it is not
named twice. `Attending` and `Overtaking` carry the other half — a subscriber
receives the state on open, then every change.

Locks change only through the core, so the core is the only place that can
announce a change, and it announces the whole observation rather than a delta:
a subscriber that joins mid-stream and one that has followed from the start
then hold the same value. Joining and reading the opening state are one step
of the core, so nothing can commit between them.

The mailbox is bounded at 64. The core is the throughput of the durable store,
and an unbounded queue in front of it would turn a slow disk into unbounded
memory rather than into the backpressure it is. Announcement fan-out is a
broadcast channel, which never makes the core wait on a subscriber: one that
falls too far behind is told it lagged and re-reads the current state.

## Sockets, sessions, and authority

Sockets are not actors and sessions are not actors. A listener is a task and
an accepted connection is a task holding a reference to the one core, so a
session that fails ends its own connection and is recorded against the socket
it arrived on while the listener keeps listening.

What separates the two doors is not the actor: it is the message type the
session behind each can construct. A session serving the ordinary contract can
only ever build an `OrdinaryQuery`, and the meta contract's operations are
unreachable from it. Authority is therefore checked by the type system rather
than by a runtime branch.

The same rule decides what a refused peer is told. A refusal is a value of the
contract the socket bears — a frame written on a socket must be one its peer
can restore — so `Refusing` is implemented on the response types. The meta
response names the peer; the ordinary response has nothing to name it with,
because the ordinary authority admits whoever the filesystem let through and
so never refuses.

`SocketAuthority` and `Permissive` — the mode a socket is bound with and the
peers it admits — are the `nexus` library's, because they are the same in
every Nexus.

## Holding a socket path

A socket path is claimed before it is bound, by an advisory lock on
`<socket>.claim` beside it, held for the life of the process. Whatever is at
the socket path is then this process's to remove, because nothing else holds
the claim.

This replaces a connect-probe that asked whether anyone was listening and
deleted the file when nobody answered. The probe had a window between the
answer and the bind, and it read the wrong thing: remove the socket file from
under a serving Nexus and the path looked free, so a second Nexus would bind
it while the first ran on the unlinked inode.

## Where the Nexus actually is

The metadata tree says where the Nexus intends to listen. `nexus::Situation`,
kept in its own family, says where one actually did: the store file opened,
the paths bound, and the process, boot and host that bound them. It is written
once both binds succeed and is never read as configuration.

It exists because the store is portable and its configuration is not. A copy
of a populated store, opened by the same user on the same machine, carries the
production socket paths and would bind them. A store found anywhere other than
where its own record says it lives is a copy, and the Nexus exits naming both
paths.

Its own family rather than a second field on the metadata tree: the two are
different kinds of fact, and a store written before the family existed simply
has no row in it.

## Stopping

`SIGTERM` and `SIGINT` are the two ways a service manager asks a daemon to go,
and both arrive as the one shutdown the transport waits on. Sessions are
aborted first — a session still holding a reference would keep the core alive
past its own stop — then the core is stopped gracefully and waited on, so that
by the time serving returns the store is closed and both socket claims are the
next process's to take.

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
