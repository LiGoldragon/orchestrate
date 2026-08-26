# orchestrate skill

Work here when the change concerns Orchestrate Nexus PathLock registration,
release, configuration, or either client.

Rules for work here:

- Keep the ordinary and meta policy clients separate: `orchestrate`
  accepts `signal-orchestrate` Datom and `meta-orchestrate` accepts
  `meta-signal-orchestrate` Datom. Do not add tier auto-routing back to
  either client.
- This component owns **its own** `sema-engine` database file
  (`orchestrate.sema`). Orchestrate Nexus serializes request handling through
  its one service mutex. There is no shared cross-component database.
