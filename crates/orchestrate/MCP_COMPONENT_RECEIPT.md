# MCP component POC receipt

Commit `5ac0cbb72a5e19857c3bfb4488da1fafa178e278` added a bounded component
selector and `orchestrate-mcp-component` CLI proof surface. It is **not an MCP
server**: it does not implement JSON-RPC, `tools/list`, `tools/call`, or MCP
stdio framing.

The completed leg accepts one component label in its CLI surface. `Orchestrate`
delegates `Observe.Locks` to the existing `orchestrate` CLI, which sends the
typed `Signal<Query>` over `ORCHESTRATE_SOCKET` and renders the typed
`Response`. `Message`, `Persona`, `Psyche`, and `Flow` are explicitly
unavailable. It does not replace send-subflow or claim that the unavailable
backends exist.

Completed validation for that commit:

- `cargo test -p orchestrate --test mcp_component_fixture`: 2 passed, 0 failed,
  0 filtered.
- `cargo check -p orchestrate --bins`: passed.

The focused Nix check did not run: evaluation timed out while obtaining
`/nix/store/77dbgds155bbz3vd3qywq1sii07i5ljs-source` from the configured cache
and then cache.nixos.org. No derivation path or Nix build result was produced.
