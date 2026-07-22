# orchestrate

```sh
meta-orchestrate '(Register ((DocumentationSession documentation-operator ([Documentation Operator] Structural) [correct operational documentation]) Fresh))'
orchestrate '(Claim (documentation-operator [(Path /absolute/path/to/readme.md)] [correct operational documentation]))'
orchestrate '(Submit (documentation-operator (Path /absolute/path/to/readme.md) [checked command syntax]))'
orchestrate '(Release documentation-operator)'
meta-orchestrate '(Unregister (DocumentationSession documentation-operator [documentation complete]))'
```

`meta-orchestrate` performs meta-policy lane registration and closeout:
`Register` and `Unregister`. `orchestrate` performs ordinary work: `Claim`,
`Submit`, `Release`, `RequestWorktree`, and `ConcludeWorktree`. Pass exactly
one quoted NOTA expression; these are not flag commands.

## Lane shape

The registration payload is exactly:

```text
(Register ((Session Lane ([Role Tokens] Authority) [details]) Fresh))
```

The inner `(Session Lane ([Role Tokens] Authority) [details])` is the
`LaneAssignment`; the outer pair adds `Fresh` (or `Recovery`). Choose a
PascalCase session, a nonempty role vector, a lane identifier, and a brief
reason. The normal structural flow is the first block.

Derive the base lane identifier by lowercasing and hyphenating its role vector:
`[Documentation Operator]` becomes `documentation-operator`. Give a second
same-role lane an ordinal prefix, such as `second-documentation-operator`. A
support helper conventionally uses `documentation-operator-assistant`. The
daemon stores the supplied identifier; it does not derive or validate this
convention, and a live identifier cannot be reused.

Choose `Structural` for the lane that owns the change or worktree. Use `Support`
for a helper lane; it uses the same ordinary claim and closeout operations:

```sh
meta-orchestrate '(Register ((DocumentationSession documentation-operator-assistant ([Documentation Operator] Support) [assist documentation owner]) Fresh))'
orchestrate '(Claim (documentation-operator-assistant [(Path /absolute/path/to/readme.md)] [assist documentation owner]))'
orchestrate '(Release documentation-operator-assistant)'
meta-orchestrate '(Unregister (DocumentationSession documentation-operator-assistant [assistance complete]))'
```

## Worktrees

After registering a structural lane, request its worktree, claim exact paths in
the returned absolute path, then release before conclusion:

```sh
orchestrate '(RequestWorktree (orchestrate DocumentationCorrection documentation-operator [correct operational documentation]))'
orchestrate '(Release documentation-operator)'
# choose one conclusion:
orchestrate '(ConcludeWorktree (documentation-operator Merged))'
orchestrate '(ConcludeWorktree (documentation-operator Rejected))'
```

`Merged` auto-lands work with no review gate. `Rejected` pushes
`discard/<branch>` before teardown. If that push is refused, the daemon leaves
the worktree intact; do not remove it by hand—repair the push path and retry.

## Recovery and replies

Use `Recovery` only for the exact handed-over assignment:

```sh
meta-orchestrate '(Register ((DocumentationSession documentation-operator ([Documentation Operator] Structural) [resume exact handover]) Recovery))'
```

A live lane returns `LaneAlreadyRegistered` with `RecoveryInherited`; a
released or absent lane returns `LaneRegistered`. `Fresh` against a live lane
returns `FreshConflict`. A malformed expression or a request sent to the wrong
binary is rejected by that client; `ClaimRejection`, `PartialApplied`, and
`WorktreeRequestRejected` are daemon replies. Read the reply and do not force
state files or lock projections.

The deployed PATH wrappers currently send `orchestrate` to
`$XDG_RUNTIME_DIR/orchestrate/orchestrate.sock` and `meta-orchestrate` to
`$XDG_RUNTIME_DIR/orchestrate/orchestrate-owner.sock` (with the usual
`/run/user/$(id -u)` fallback). They overwrite
`PERSONA_ORCHESTRATE_SOCKET` and `PERSONA_ORCHESTRATE_META_SOCKET` before
starting the client, so neither variable is a deployed-client isolation switch.
These are ordinary live daemon lifecycle operations; use a uniquely named lane
when rehearsing one.
