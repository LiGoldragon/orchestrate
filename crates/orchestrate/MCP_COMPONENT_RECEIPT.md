# MCP component POC receipt

Commit `5ac0cbb72a5e19857c3bfb4488da1fafa178e278` added a bounded component
selector and its initial CLI proof surface. That commit was not an MCP server.

The subsequent source change makes `orchestrate-mcp-component` a JSON-RPC 2.0
MCP stdio server. It implements `initialize`, `tools/list`, and `tools/call`
for one tool, `orchestrate_component`. Its input schema requires exactly one
string property, `component`, with the five declared labels. Malformed JSON
returns a JSON-RPC parse error. Bad component argument shapes and unavailable
backends are tool results with `isError: true`.

The completed leg accepts one component label in its CLI surface. `Orchestrate`
delegates `Observe.Locks` to the existing `orchestrate` CLI, which sends the
typed `Signal<Query>` over `ORCHESTRATE_SOCKET` and renders the typed
`Response`. `Message`, `Persona`, `Psyche`, and `Flow` are explicitly
unavailable. It does not replace send-subflow or claim that the unavailable
backends exist.

Completed validation for the CLI POC and MCP server:

- `cargo test -p orchestrate --test mcp_component_fixture`: 4 passed, 0 failed,
  0 filtered.
- `cargo check -p orchestrate --bins`: passed.

The focused Nix check did not run: evaluation timed out while obtaining
`/nix/store/77dbgds155bbz3vd3qywq1sii07i5ljs-source` from the configured cache
and then cache.nixos.org. No derivation path or Nix build result was produced.
