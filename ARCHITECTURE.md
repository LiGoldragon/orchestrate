# persona-orchestrate - architecture

*Persona orchestration machinery: role claims, activity, lane/run
coordination, scope acquisition, scheduling, escalation, and the
daemon boundary that replaces the transitional workspace lock helper.*

> Status: the repo, ordinary contract, owner contract, sema-backed
> claim/activity store, dynamic role registry, raw role-creation path,
> local repository-index refresh, daemon socket runtime, and thin CLI
> client exist. The CLI has no direct-store path. Lock-file projection
> from daemon state and GitHub/ghq-backed report-repository creation
> are still missing.
> `tools/orchestrate` remains the live workspace helper until the
> daemon projects compatibility lock files and is supervised as a
> workspace service.

## 0 - TL;DR

`persona-orchestrate` owns orchestration machinery. `persona-mind`
owns state: work graph, thoughts, memories, relations, durable policy
truth, and channel-grant authority decisions. Orchestrate owns the
mechanics that make work run: claims, handoffs, activity, agent-run
lifecycle, spawn plans, scope acquisition, executor capacity,
scheduling, escalation, and the lane registry.

The current implemented slice is the usable triad skeleton: ordinary
`signal-persona-orchestrate` request/reply surface, owner-only
`owner-signal-persona-orchestrate`, a daemon that owns the
`persona-orchestrate.redb` sema store, and a thin
`persona-orchestrate` CLI that sends Signal frames to the daemon
sockets.

```mermaid
flowchart TB
    mind["persona-mind<br/>state + policy truth"]
    cli["persona-orchestrate CLI<br/>one NOTA request"]
    daemon["persona-orchestrate daemon"]
    ordinary["signal-persona-orchestrate<br/>ordinary peer surface"]
    owner["owner-signal-persona-orchestrate<br/>owner-only surface"]
    store["persona-orchestrate.redb<br/>sema-engine"]
    router["persona-router"]
    harness["persona-harness"]
    locks["orchestrate/*.lock<br/>temporary projection"]

    cli -- "ordinary/owner Signal frames" --> daemon
    daemon -- "ordinary surface" --> ordinary
    mind --> owner
    owner --> daemon
    daemon --> store
    daemon --> locks
    daemon -- "owner-signal-persona-router" --> router
    daemon -- "owner-signal-persona-harness" --> harness
```

## 1 - Component Surface

This runtime repo contains:

- a library crate, `persona_orchestrate`, that consumes
  `signal-persona-orchestrate` and dispatches typed
  `OrchestrateRequest` values;
- sema-backed `claims`, `roles`, `repositories`, `activities`, and
  `activity_next_slot` tables;
- claim, release, handoff, role-observation, activity-submission,
  and activity-query handlers;
- owner-request handlers for role creation, role retirement, and
  local repository-index refresh;
- a daemon binary that accepts one NOTA config argument, binds ordinary
  and owner Unix sockets, decodes Signal frames, dispatches to the
  service, and writes Signal replies;
- a thin CLI client that accepts one NOTA request argument, encodes it
  as a Signal frame, and connects only to the `persona-orchestrate`
  daemon sockets.

The full component surface is:

```text
persona-orchestrate/
  src/lib.rs
  src/main.rs
  bootstrap-policy.nota
signal-persona-orchestrate/
owner-signal-persona-orchestrate/
```

The contract crates carry wire vocabulary only. This repo owns the
runtime, actor tree, socket binding, lock-file projection, and
`persona-orchestrate.redb`.

## 2 - Authority Chain

`persona-mind` owns `persona-orchestrate` through
`owner-signal-persona-orchestrate`. Orchestrate then owns the runtime
execution edges it controls:

| Link | Contract | Direction |
|---|---|---|
| `persona-mind -> persona-orchestrate` | `owner-signal-persona-orchestrate` | mind orders orchestration machinery |
| `persona-orchestrate -> persona-router` | `owner-signal-persona-router` | orchestrate orders channel grants and retractions |
| `persona-orchestrate -> persona-harness` | `owner-signal-persona-harness` | orchestrate orders agent-run lifecycle transitions |

Observation flows back through `Subscribe` surfaces. Authority moves
down through `Mutate` and `Retract`; current state moves up through
subscriptions. No orchestration actor polls another component for
state that component can push.

## 3 - Ordinary Wire Surface

`signal-persona-orchestrate` is the peer-callable surface. It carries
requests peers and the CLI can make without owner authority:

- `RoleClaim` / `RoleRelease` / `RoleHandoff`
- `RoleObservation`
- `ActivitySubmission` / `ActivityQuery`
- destination additions: activity, claim, and lane-registry
  subscriptions

The current ordinary contract uses `RoleIdentifier` for dynamic role
identity. `RoleName` remains as a compatibility alias only; role
creation is data in the runtime registry, not a contract enum edit.

## 4 - Owner Wire Surface

`owner-signal-persona-orchestrate` is the owner-only surface. The
implemented MVP carries:

- `CreateRoleOrder`
- `RetireRoleOrder`
- `RefreshRepositoryIndexOrder`

Destination additions include agent-run orders, scope acquisition
orders, scheduling/supervision policy, escalation orders, and owner
subscriptions for snapshots, agent lifecycle, executor capacity, and
scope events.

Owner-only operations are inexpressible on the ordinary contract.
The daemon binds a separate socket and actor for this surface.

## 5 - State And Ownership

Durable state lives in one `persona-orchestrate.redb` opened through
`sema-engine`. No other component opens that database directly.

Policy tables change only through owner-signal `Mutate` or `Retract`
after first-start bootstrap:

| Table | Purpose |
|---|---|
| `roles` | dynamic role registry, harness kind, report repository path, report lane path |
| `repositories` | refreshed local checkout index and workspace link metadata |
| `lane_registry` | registered lanes, assistant-of relation, beads label, metadata |
| `scheduling_policy` | capacity caps, priorities, backpressure rules |
| `supervision_policies` | restart, drain, and escalation policy |

Working tables are produced by operation:

| Table | Purpose | Status |
|---|---|---|
| `claims` | active role claims | implemented |
| `claim_archive` | released or replaced claims | missing |
| `activities` | store-stamped activity log | implemented |
| `activity_next_slot` | next activity slot | implemented |
| `agent_runs` | agent-run lifecycle records | missing |
| `spawn_plans` | planned executor allocations | missing |
| `agent_executors` | registered execution capacity | missing |
| `scope_acquisitions` | scope request/adjudication flow | missing |
| `channel_grants` | channel rights ordered through router | missing |
| `escalation_state` | blocked work and user-decision state | missing |

The first-start policy seed is `bootstrap-policy.nota`. Once policy
has bootstrapped into sema state, owner-signal is the mutation path.

## 6 - Lock-File Projection

`tools/orchestrate` writes `orchestrate/*.lock` today. That is the
transitional surface. In the component shape, the daemon owns typed
claim state and projects lock files as compatibility output for
human and cross-harness visibility.

The projection is downstream of accepted state mutation. Lock files
are never the source of truth once the daemon is live.

## 7 - Constraints

- The CLI accepts exactly one NOTA request and talks to exactly one
  Signal peer: the `persona-orchestrate` daemon.
- The CLI never opens `persona-orchestrate.redb`, sema-engine, or the
  in-process `OrchestrateService`; all state mutation and reads cross
  the daemon boundary.
- The daemon's external traffic is Signal frames only.
- The daemon has one typed actor per Signal contract socket.
- The ordinary socket accepts ordinary frames; the owner socket
  accepts owner frames; each rejects the other's vocabulary.
- The runtime store is `persona-orchestrate.redb`.
- Activity timestamps and slots are minted by the store, never by the
  caller.
- Claim conflicts reject overlapping path scopes across different
  lanes.
- Task scopes overlap only by exact task token.
- Handoff requires the source lane to hold the exact scope being
  handed off.
- Claiming a directory claims every path below it; there is no
  directory-minus-file handoff shape.
- Lane registry changes are owner-authority operations, not contract
  enum additions.
- Role creation records a typed harness kind beside the role
  identifier; harness assignment is not hidden in the role string.
- Role creation creates a report-repository path and report-lane path
  before inserting the role record.
- Repository refresh reads local checkouts from the configured Git
  index root and creates workspace `repos/` links.
- Lock files are projections of typed state, not durable authority.
- BEADS is never an owned claim scope.

## 8 - Invariants

- Mind owns state; orchestrate owns machinery.
- The lane registry is data, not a closed role enum.
- Owner authority enters through `owner-signal-persona-orchestrate`;
  ordinary peers cannot compile owner-only orders.
- Push subscriptions carry current state and deltas; polling is not an
  orchestration mechanism.
- The component can be used in raw form before every downstream
  integration is wired, but the raw form still follows the triad.

## Code Map

```text
src/lib.rs        public library surface and re-exports
src/error.rs      crate error enum
src/configuration.rs
                  daemon NOTA config record
src/daemon.rs     ordinary/owner socket listeners and frame dispatch
src/location.rs   redb store path wrapper
src/layout.rs     workspace/git-index path policy
src/tables.rs     sema-backed claim/activity/role/repository tables
src/claim.rs      claim, release, handoff, and observation handlers
src/activity.rs   activity submission and query handlers
src/role.rs       owner role creation and retirement handlers
src/repository.rs local repository-index refresh handler
src/service.rs    ordinary and owner request dispatch
src/main.rs       daemon binary, one NOTA config argument
src/bin/persona-orchestrate.rs
                  thin CLI, one NOTA request argument, Signal to daemon only
tests/ledger.rs   sema-backed claim/activity/role/repository witnesses
tests/architecture.rs
                  CLI boundary source-scan witnesses
tests/daemon_cli.rs
                  production daemon + production CLI socket witnesses
tests/smoke.rs    legacy claim-state smoke test
```

## See Also

- `../signal-persona-orchestrate/ARCHITECTURE.md` - ordinary wire
  contract.
- `../persona/ARCHITECTURE.md` - Persona component topology.
- `../persona-mind/ARCHITECTURE.md` - mind state boundary.
- `/home/li/primary/orchestrate/ARCHITECTURE.md` - workspace helper
  today and component destination.
- `/home/li/primary/skills/component-triad.md` - daemon + CLI +
  ordinary/owner contract invariants.
