# Non-Ideal Agents Registry

Known non-idealities in this repository. Ordinary rules live in `AGENTS.md`;
the intended component shape lives in `ARCHITECTURE.md`.

## Store-format stamping is not atomic with family registration

A store open is logically “open engine, then register all row families,” but
`sema-engine` persists its storage-layout stamp before the later family
registrations have all succeeded. A family-identity failure can therefore leave
the store stamped forward although startup did not complete.

Orchestrate Nexus admits only its current family identities and fails closed on any
mismatch. The remaining atomicity belongs in `sema-engine`: it must defer or
roll back the layout stamp until family registration succeeds, or accept the
family descriptors as part of open.
