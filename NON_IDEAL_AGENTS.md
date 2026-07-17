# Non-Ideal Agents Registry

Known non-idealities in this repository: sanctioned workarounds and deferred
proper fixes. Ordinary rules live in `AGENTS.md`; the ideal shape lives in
`ARCHITECTURE.md`. This file is debt with a named future fix target.

## Worktree tests are not sandbox-hermetic

- Symptom: the pure Nix `test` flake check (`.#checks.<system>.test`) fails on
  `tests/worktree.rs`. Those tests shell out to `jj` (e.g. `jj config set
  --repo user.name`), which needs a writable config home; the Nix build sandbox
  has none, so `jj` fails with `Cannot access /homeless-shelter/.config/jj …
  Permission denied`. The rest of the suite (including `tests/ledger.rs`) passes
  in-sandbox.
- Current status: no workaround in place. The aggregate `test` check is red for
  this reason only; the tests pass under `cargo test` in an impure environment.
- Proper fix: `tests/worktree.rs` exercises real filesystem + `jj` process
  effects, so per the testing skill it belongs as a named **stateful** test
  output run outside the pure sandbox, not inside the hermetic `test` flake
  check — either move it to a stateful runner or give the `jj` invocations a
  writable, sandbox-provided config home. This is a test-harness reshape owned
  by the worktree-lifecycle surface, deferred from the lane-registration lane
  that recorded it.
