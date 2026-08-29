# orchestrate skill

Work here when the change concerns Orchestrate Nexus Lock acquisition,
Release-by-ID, `Observe.Locks` observation, configuration, or either client.

Rules for work here:

- Keep the ordinary and meta policy clients separate: `orchestrate`
  accepts the generated `signal-orchestrate` Request Datomic root and `meta-orchestrate` accepts
  the generated `meta-signal-orchestrate` Request Datomic root. Do not add tier auto-routing back to
  either client.
- This component owns **its own** `sema-engine` database file
  (`orchestrate.sema`). Orchestrate Nexus serializes request handling through
  its one service mutex. There is no shared cross-component database.
