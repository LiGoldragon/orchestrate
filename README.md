# orchestrate

Typed workspace orchestration state for Persona agents.

The daemon's authority stops at its own durable Sema state and Unix sockets.
It does not scan or mutate repositories, worktrees, role directories, lock
files, terminals, or process state. Worktree registration records the complete
caller-supplied row; a worktree scaffold request is typed-refused because it
does not carry a row to record. Observations and activity queries are read-only.

This crate models role ownership, claimed scopes, handoffs, and the activity
log that replaces primary workspace lock files over time.

The runtime surface is a triad: `orchestrate-daemon` owns the
`orchestrate.sema` store, `orchestrate` is the one-argument ordinary
`signal-orchestrate` CLI, and `meta-orchestrate` is the one-argument
`meta-signal-orchestrate` policy CLI.

## Daemon startup contract

`orchestrate-daemon` receives its configuration as typed positional startup
arguments, not through a configuration file or environment variables:

```text
orchestrate-daemon \
  <sema-store> <ordinary-socket> <meta-socket> <upgrade-socket> \
  <workspace-root> <git-index-root> \
  [router=<socket>] [messenger=<socket>]
```

All six required paths and either optional socket value must be absolute and
must not contain a parent-directory component. The daemon opens only the
configured Sema store and binds the three configured Unix sockets. Its service
manager must create their parent directories. Sema's pre-migration preserve
remains beside the configured store, and the existing public-socket retirement
on handover remains unchanged.

The removed `orchestrate-write-configuration` program has no replacement: the
service manager starts the daemon directly with that contract. For the current
downstream declarative module,
`CriomOS-home/modules/home/profiles/min/orchestrate.nix`, make these changes
when updating its Orchestrate package pin:

- Remove `signalPath` and the `ExecStartPre` invocation of
  `orchestrate-write-configuration`.
- Set `ExecStart` to the daemon followed by `storePath`, `ordinarySocketPath`,
  `metaSocketPath`, `upgradeSocketPath`, `workspaceRoot`, `gitIndexRoot`, and
  `messenger=${messengerSocketPath}` in that order.
- Keep `StateDirectory`, `RuntimeDirectory`, the Sema store path, and all
  three Unix socket paths; they now directly serve the daemon's startup
  contract.
- Remove the daemon `PATH` entries for Jujutsu and Git. This source boundary
  does not execute VCS programs or scan host repositories.

## Ordinary CLI presentation

Ordinary contract input is shorthand for a typed human presentation:

```text
orchestrate '(Observe Lanes)'
```

Lane elapsed ages encode as closed `HumanReadableTime` values, not text. For
example, a whole minute value is `Minutes.10`; a fractional day value is
`Days.(3.2)`. The human lane projection retains timestamps as exact nanosecond
values, distinct from those elapsed-time units.

Use the explicit form when a program needs the unchanged daemon contract
output:

```text
orchestrate '(Explicit (Canonical (Observe Lanes)))'
```

Both forms lower to the same ordinary Signal request. `Canonical` preserves the
existing `signal-orchestrate` NOTA reply exactly; only the CLI-side `Human`
presentation converts elapsed `DurationNanos` values. An explicit human form,
`(Explicit (Human (Observe Lanes)))`, is equivalent to shorthand.

It is not Persona's central mind database. Work graph state, thoughts,
relations, and policy truth belong in `mind`; this crate owns collaborative
orchestration machinery in `orchestrate.sema`.
