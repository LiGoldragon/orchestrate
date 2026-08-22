# Orchestrate

Orchestrate durably registers named native Datom path locks. A registration is
metadata only: it neither acquires an operating-system lock nor creates,
changes, or removes a filesystem path.

The ordinary `orchestrate` CLI is the textual boundary. It accepts one native
Datom path-lock record, sends a binary Signal request to `orchestrate-daemon`,
and prints the matching native Datom registered or rejected reply. The daemon
requires explicit paths for its Sema store and ordinary, meta, and upgrade
sockets; it has no production-path or environment fallback.

Active lock names are unique. Paths are normalized and validated by Datom; an
exact, ancestor, or descendant overlap with an active lock returns the typed
`PathOverlap` rejection. Registration is one atomic Sema assertion.
