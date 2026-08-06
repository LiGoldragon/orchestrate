# Non-Ideal Agents Registry

Known non-idealities in this repository. Ordinary rules live in `AGENTS.md`;
the intended component shape lives in `ARCHITECTURE.md`.

## Store-format stamping is not atomic with family registration

A store open is logically “open engine, then register all row families,” but
`sema-engine` persists its storage-layout stamp before the later family
registrations have all succeeded. A family-identity failure can therefore leave
the store stamped forward although startup did not complete.

Orchestrate admits only its current family identities and fails closed on any
mismatch. The remaining atomicity belongs in `sema-engine`: it must defer or
roll back the layout stamp until family registration succeeds, or accept the
family descriptors as part of open.

## Worktree conclusion does not carry the durable worktree identity

`ConcludeWorktree` selects by owning lane, while a durable worktree is
identified by `(repository, branch)`. If multiple active rows name one lane,
Orchestrate refuses before any state change rather than choosing one row.

The producer contract should make the ambiguous state inexpressible by carrying
an exact `WorktreeIdentity { repository, branch }` in the conclusion request.
Until that contract lands, the current fail-closed lookup is mandatory.

## Ambiguous conclusion has only the general partial-application reply

The service detects the ambiguous worktree selection precisely, but the current
ordinary reply vocabulary has no dedicated ambiguity refusal. The daemon
therefore returns a committed `PartialApplied` with no successful leg. This is
safe but less expressive than the underlying decision.

The same exact-worktree producer change should add a typed conclusion refusal,
then this runtime should return it directly.

## Upgrade execution errors close without a typed refusal

The ordinary and meta tiers always answer a decoded request with a contract
reply. The version-handover tier can still return an engine error before it
writes a response because the shared handover contract has no general engine
refusal.

The refusal belongs in `signal-version-handover`; Orchestrate should consume it
once the shared contract can express the failure.
