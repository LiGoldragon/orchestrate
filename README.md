# orchestrate

Typed workspace orchestration state for Persona agents.

The daemon's authority stops at its own durable Sema state, Unix sockets, and
configured component exchanges. It does not scan or mutate repositories,
worktrees, role directories, claim files, terminals, or process state.
Worktree registration records the complete
caller-supplied row; a worktree scaffold request is typed-refused because it
does not carry a row to record. Observations and activity queries are read-only.

This crate models role ownership, claimed scopes, handoffs, and activity
directly in typed durable state; claim files are not part of the runtime.

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
manager must create their parent directories. A handover retires the two public
sockets only after the replacement generation proves it is ready; the private
upgrade socket remains available to complete that exchange.

The service manager starts the daemon directly with this contract. Its service
declaration supplies `StateDirectory`, `RuntimeDirectory`, the Sema store path,
all three Unix socket paths, and any downstream messenger or router socket. The
daemon needs no VCS programs in `PATH`: this source boundary neither executes
them nor scans host repositories.

## Ordinary CLI presentation

Ordinary contract input is shorthand for a typed human presentation:

```text
orchestrate '(Observe Lanes)'
```

Lane elapsed ages encode as closed `HumanReadableTime` values, not text. For
example, a whole minute value is `Minutes.10`; a fractional day value is
`Days.(3.2)`. Exact nanosecond values remain available in canonical output.

Use the explicit form when a program needs the unchanged daemon contract
output:

```text
orchestrate '(Explicit (Canonical (Observe Lanes)))'
```

Both forms lower to the same ordinary Signal request. `Canonical` emits the
`signal-orchestrate` Dotos reply exactly; only the CLI-side `Human`
presentation converts elapsed `DurationNanos` values. An explicit human form,
`(Explicit (Human (Observe Lanes)))`, is equivalent to shorthand.

It is not Persona's central mind database. Work graph state, thoughts,
relations, and policy truth belong in `mind`; this crate owns collaborative
orchestration machinery in `orchestrate.sema`.
