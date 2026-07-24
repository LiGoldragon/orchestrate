# Orchestrate

Use `meta-orchestrate` for lane `Register` and `Unregister`; use
`orchestrate` for `RequestWorktree`, `Claim`, `Submit`, `Release`, and
`ConcludeWorktree`. Each command takes exactly one shell-quoted NOTA
expression.

## Cold start

Use this structural-owner flow unchanged for a disposable proof. It makes a
new identity on every run; do not copy a fixed lane name. `tag` is decimal,
`session` and `worktree` are bare PascalCase atoms, and `lane` is the
lowercase-hyphen form of the role vector `[Cold Start $tag]`.

```sh
tag="$(date -u +%s%N)$$"
session="ColdStart${tag}"
lane="cold-start-${tag}"
worktree="ColdStart${tag}"
reason='cold start probe'

meta-orchestrate "(Register ((${session} ${lane} ([Cold Start ${tag}] Structural) [${reason}]) Fresh))"
orchestrate "(RequestWorktree (orchestrate ${worktree} ${lane} [${reason}]))"
```

`Register` is nested exactly as
`(Register ((SESSION LANE ([ROLE TOKENS] AUTHORITY) [DETAILS]) MODE))`.
`[ROLE TOKENS]` and `[DETAILS]` are nonempty bracketed vectors. Use `Fresh`
for a new lane; use `Recovery` only for the exact handed-over assignment.
`Structural` is the authority for the lane that owns this worktree and its
paths. `Support` is the only other authority and is for a helper, not this
owner flow.

The second reply is `WorktreeScaffolded`. Copy its absolute worktree-path
field into `worktree_path`, then use a unique disposable path beneath it. Do
not create this path: it is a harmless claim-only probe.

```sh
worktree_path='/absolute/path/from/WorktreeScaffolded'
probe_path="${worktree_path}/.orchestrate-cold-start-${tag}"

orchestrate "(Claim (${lane} [(Path ${probe_path})] [${reason}]))"
orchestrate "(Submit (${lane} (Path ${probe_path}) [cold start stamp]))"
```

`Claim` requires a lane, a bracketed vector of scopes, and a reason; this
scope is exactly `(Path ABSOLUTE_PATH)`. `Submit` is the activity stamp: its
required fields are the lane, one scope, and a reason. There is no `Stamp`
request name.

Success reply heads, in order, are `LaneRegistered`, `WorktreeScaffolded`,
`ClaimAcceptance`, and `ActivityAcknowledgment`. Any other reply is not a
successful cold start.

## Closeout

After a disposable probe, or after committing and pushing an edit, release
claims before closing the worktree and lane:

```sh
orchestrate "(Release ${lane})"
orchestrate "(ConcludeWorktree (${lane} Rejected))"
meta-orchestrate "(Unregister (${session} ${lane} [cold start complete]))"
```

`Rejected` pushes `discard/<worktree>` and tears the worktree down without
landing it. Do not use `Merged` unless automatic landing without a review gate
is intended. Expected reply heads are `ReleaseAcknowledgment`,
`WorktreeConcluded`, and `LaneUnregistered`.
