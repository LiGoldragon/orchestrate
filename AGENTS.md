# Orchestrate -- Agent Instructions

## Purpose

Orchestrate Nexus is the durable owner of Lock coordination. The
`orchestrate` and `meta-orchestrate` CLIs are its ordinary and
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
curly quotes \u{201C} \u{201D}. A word without them is bare.

## Ordinary operations

```
orchestrate 'Lock.{ MyLock 6329f1 [ /absolute/path ] "reason" }'
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
meta-orchestrate 'Configure.{ /o.sock /m.sock }'
```

## Faults

A client fault prints one datom value on stderr and exits 1:

- `Unreadable.{ ... }` -- argument failed actualization.
- `Unreachable.{ ... }` -- socket unreachable.
- `Refused.{ ... }` -- wire-level refusal.

## Code shape

Every method lives under a trait. `fn main()` is the only free
function. The ordinary ontology is three traits on `OrchestrateStore`:
`Locks`, `Releases`, `Observes`. The transport dispatches to them.

## Wire

Binary rkyv frames: `Frame.{ Version Body }`. Version is the signal
contract's semver. The Signal's version is the wire version.

## Contract changes

Edit the ethos in `signal-orchestrate` or `meta-signal-orchestrate`,
regenerate through ethos-zero, run the freshness test, then pin the
new signal crate rev in orchestrate's `Cargo.toml`.

## Deployment

Bump the `orchestrate` flake input in CriomOS-home, rebuild, restart
`orchestrate-nexus` with `systemctl --user restart orchestrate-nexus`.
