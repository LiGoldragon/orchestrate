# Orchestrate

`meta-orchestrate` owns durable lane, role, and declared-worktree state.
`orchestrate` owns ordinary claim, activity, observation, agent, topic, and
worktree-conclusion requests. Each client accepts one Dotos request and prints
one typed reply.

`RequestWorktree` is intentionally state-only and returns
`WorktreeRequestRejected(RepositoryNotFound)`; it never creates a checkout,
bookmark, directory, or VCS state. To record an existing caller-owned worktree
fact, use meta `RegisterWorktree` with the full typed record. `ArchiveWorktree`
and `ConcludeWorktree` change durable status only.

For a fresh lane, register with meta and then claim a path or task through the
ordinary client:

```sh
meta-orchestrate "(Register ((Example example ([Example Operator] Structural) [example work]) Fresh))"
orchestrate "(Claim (example [(Task example-work)] [coordinate example]))"
orchestrate "(Release example)"
meta-orchestrate "(Unregister (Example example [example complete]))"
```

Read the reply record, not only the exit status: accepted and refused valid
Dotos requests can both exit successfully. `Observe` and `Query` are pure Sema
store projections. Human time output uses the shared typed
`relative-age-display` presentation; use `(Explicit (Canonical ...))` when a
program requires canonical contract values.
