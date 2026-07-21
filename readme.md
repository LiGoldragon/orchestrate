# orchestrate

Use Orchestrate to register a session lane, claim exact paths, record work, and
close the lane. `orchestrate` accepts ordinary requests; `meta-orchestrate`
accepts lane-policy requests. Each command takes exactly one NOTA expression.

## Normal lane

Choose a PascalCase session, a role vector whose last token is the base
discipline, and a lane identifier. Derive the identifier by lowercasing and
hyphenating the role tokens: `[Documentation Operator]` becomes
`documentation-operator`. A `Support` lane adds `-assistant`; a second lane
with the same role uses an ordinal prefix such as
`second-documentation-operator`.

The daemon stores the identifier supplied in `LaneAssignment`; it does not
derive or validate that convention. Do not reuse a live lane identifier.

```sh
meta-orchestrate '(Register ((DocumentationSession documentation-operator ([Documentation Operator] Structural) [correct operational documentation]) Fresh))'
```

Claim each exact path before writing it. Use absolute paths.

```sh
orchestrate '(Claim (documentation-operator [(Path /absolute/path/to/readme.md)] [correct operational documentation]))'
```

Record a completed step with `Submit`; the daemon mints the activity stamp.

```sh
orchestrate '(Submit (documentation-operator (Path /absolute/path/to/readme.md) [checked command syntax]))'
```

Observe, release claims, then end the registration.

```sh
orchestrate '(Observe Lanes)'
orchestrate '(Release documentation-operator)'
meta-orchestrate '(Unregister (DocumentationSession documentation-operator [documentation complete]))'
```

`Release` clears claims but leaves the lane active. `Unregister` releases the
lane and clears any remaining claims.

## Managed worktree

Register the lane first. Ask the daemon to create the worktree; use the path in
its reply, then claim exact paths there before writing.

```sh
orchestrate '(RequestWorktree (orchestrate DocumentationCorrection documentation-correction [correct operational documentation]))'
```

After release, conclude the one worktree owned by the lane.

```sh
orchestrate '(ConcludeWorktree (documentation-correction Merged))'
```

`Merged` auto-lands the work on `main` and removes the worktree; the tool has
no review gate. To preserve rejected work under `discard/<branch>` before
removal, use:

```sh
orchestrate '(ConcludeWorktree (documentation-correction Rejected))'
```

## Boundaries and errors

- `Structural` and `Support` are lane authority metadata; both use the same
  claim grammar. Use `Support` only for support work.
- `Fresh` on a live identifier returns `LaneAlreadyRegistered` with
  `FreshConflict`. Choose a distinct ordinal lane instead of taking it over.
- Use `Recovery` only for the exact handed-over lane assignment:

  ```sh
  meta-orchestrate '(Register ((DocumentationSession documentation-operator ([Documentation Operator] Structural) [resume exact handover]) Recovery))'
  ```

  A live lane returns `RecoveryInherited`; a released lane is registered again.
  Recovery keys on the supplied lane identifier, so it is not an authorization
  mechanism.
- Do not edit daemon state or projected lock files. The daemon owns
  `orchestrate.sema` and lock projection.
- `invalid ordinary/meta orchestrate ... NOTA` means the client rejected the
  expression or wrong contract tier before contacting the daemon. Correct the
  one NOTA argument; flag-style commands are not supported.
- `ClaimRejection`, `PartialApplied`, and `WorktreeRequestRejected` are daemon
  replies. Read their reason, observe state, and do not force the operation.
  A lane-selected `ConcludeWorktree` with no matching row can currently surface
  as an engine-rejected transport error; it has not changed state.
