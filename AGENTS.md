# Orchestrate -- Agent Instructions

## Purpose

Orchestrate Nexus is the durable owner of Lock coordination. The
`orchestrate` and `orchestrate-meta` CLIs are its ordinary and
privileged datom boundaries.

## Local rules

- Use Jujutsu for version control.
- Use Nix for build and test entry points.
- Durable state uses `sema-engine` over the redb + rkyv substrate.
- Preserve the small Lock surface: no lanes, claims, worktrees, roles,
  or compatibility layer belong here.
- The long-running executable is `orchestrate-nexus`; do not
  reintroduce `orchestrate-daemon`.
- `orchestrate-nexus` owns its XDG defaults and takes zero arguments.
  Do not add a startup frame, configuration writer, or bootstrap
  binary.

## CLI shape

Each CLI takes exactly one inline datom value and no flags. Flag-style
arguments (`--anything`) are rejected. With no argument, the CLI
prints its signal contract ethos and its client failure ethos, then
exits 0.

A string containing a space or a delimiter character is written in
guillemets \u{00AB} \u{00BB}, as the examples below are. A word without
them is bare. Curly quotes \u{201C} \u{201D} were the delimiter of an
earlier Protos generation and are refused by these clients:
`Unreadable.Error.{ Composition [ 1 ] Arity.{ 4 5 } }` is what a
curly-quoted Lock reason produces, naming neither quotes nor the field.

## Ordinary operations

```
orchestrate 'Lock.{ MyLock 6329f1 [ /absolute/path ] «reason with spaces» }'
orchestrate 'Observe.Locks'
orchestrate 'Release.442'
```

`Locked` returns the complete Lock with its integer ID.
`Released` returns the complete Lock.
`LockRejected` and `ReleaseRejected` are typed refusals.
`Observed.Locks.[]` is the empty snapshot; `Observed.Locks.[ { ... } ]`
carries locks.

## Meta operations

```
orchestrate-meta 'Configure.{ «/o.sock» «/m.sock» }'
```

## Faults

A client fault prints one datom value on stderr and exits 1:

- `Unreadable.{ ... }` -- argument failed actualization.
- `Unreachable.{ ... }` -- socket unreachable.

## Code shape

Every method lives under a trait. `fn main()` is the only free
function. The ordinary ontology is five traits on `OrchestrateStore`:
`Locks`, `Releases`, `Observes`, `Configures`, `Situates`. `NexusCore`
is a Kameo actor bearing `Message<T>` once per contract, plus
`Attending` and `Overtaking` for subscription; the transport carries
frames and decides nothing.

`NexusCore` owns the store by value. Do not reintroduce a lock around
it or a second reference to it: the only way to the store is a message.
Sockets and sessions are tasks, never actors.

## Wire

Framing is `signal`'s and only `signal`'s: a four-byte big-endian length
prefix and one validated rkyv archive. Never hand-roll a prefix here — a
hand-rolled one disagreed with every other component for a full generation
and the tests shared the mistake, so they could not catch it.

Every query is answered with one frame except `Observe`, which opens a
subscription on the connection.

The meta socket is bound `0600` and admits only its own user; the ordinary
socket is bound `0660`. Each path carries an advisory claim on
`<socket>.claim` beside it, held for the life of the process; never
replace it with a connect-probe.

A refusal is a value of the contract the socket bears. Never write one
contract's frame onto a socket bearing another.

## Contract changes

Edit the ethos in `signal-orchestrate` or `meta-signal-orchestrate`,
regenerate through ethos-zero, run the freshness test, then pin the
new signal crate rev in orchestrate's `Cargo.toml`.

## Deployment

Bump the `orchestrate` flake input in CriomOS-home, rebuild, restart
`orchestrate-nexus` with `systemctl --user restart orchestrate-nexus`.
The restart is graceful: the Nexus stops on `SIGTERM`, finishes the work
it took, and releases its store and its socket claims before exiting.

The Nexus records which store *file* it opened, not merely the path, so
moving the store within a filesystem needs nothing: the file is the same
one and the Nexus recognises it. A store whose bytes arrived in a new
file — a cross-filesystem `mv`, a `tar`, a snapshot restore — cannot be
told from a copy, and is refused until the owner declares the move.

To declare it, with the service stopped and in the service's own
environment:

    systemctl --user stop orchestrate-nexus
    orchestrate-relocate
    systemctl --user start orchestrate-nexus

`orchestrate-relocate` takes no arguments; it derives the store path the
same way the Nexus does. It refuses unless nothing remains at the
address the store records and nothing holds the sockets it is about to
bind, so a copy cannot be declared a move while the original exists. The
declaration names that one move and is spent by the start that completes
it.
