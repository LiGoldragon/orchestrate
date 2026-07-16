# orchestrate — non-ideal agent operations

This file is the operational mirror of `AGENTS.md` for accepted, temporary
non-idealities in the orchestrate tree. The workarounds here are known and
sanctioned: honor them without stalling, and route the proper fix to a bigger
feature or a psyche design decision rather than force-fixing out of an unrelated
lane. When you discover a new non-ideality that is not yours to fix now, append
it here; keep ordinary rules in `AGENTS.md` and the ideal shape in
`ARCHITECTURE.md`.

## nota is pinned surgically; do not run a full cargo update

- **`Cargo.lock` freezes `nota` at an old revision on purpose.** Both `orchestrate`
  and `signal-orchestrate` lock `nota` at git `ce7c564de0a0518eaa1938d55dccc460a67cadb4`
  (`Cargo.lock` only — the manifests carry `branch = "main"` with no `rev`). A full
  `cargo update` advances `nota` to `main` HEAD, which retires
  `Delimiter::PipeParenthesis` / `Delimiter::PipeBrace` and reshuffles delimiters
  in a next-gen grammar migration this tree has not adopted, so the build churns.
  Update only the intended crates surgically; do not regenerate the whole lock.
- **Proper fix:** adopt retired-pipe / next-gen `nota` across the orchestrate tree
  (grammar, parser, and codec call sites), then unpin and take `nota` HEAD. This
  is a deliberate migration feature, not an in-lane change.
- Witnessed 2026-07-16: `nota` HEAD was 13 commits ahead of the pin, including
  `nota: remove retired structural pipe forms from grammar, parser, and codec`.
