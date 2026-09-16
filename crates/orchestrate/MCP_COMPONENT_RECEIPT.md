# MCP component POC receipt

Commit `5ac0cbb72a5e19857c3bfb4488da1fafa178e278` added a bounded component
selector and its initial CLI proof surface. That commit was not an MCP server.

The subsequent source change makes `orchestrate-mcp-component` a JSON-RPC 2.0
MCP stdio server. It implements `initialize`, `tools/list`, and `tools/call`
for one tool, `orchestrate_component`. Its input schema requires exactly one
string property, `component`, with the five declared labels. Malformed JSON
returns a JSON-RPC parse error. Bad component argument shapes and unavailable
backends are tool results with `isError: true`.

The actual MCP server source revision is
`1916956b29416796d000dc4343f8b41c7903b1b2`.

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

The focused Nix check initially waited for
`/nix/store/77dbgds155bbz3vd3qywq1sii07i5ljs-source`. After that exact source
materialized from cache.nixos.org, evaluation produced
`/nix/store/aknbax0ynbz70ikp07xjb38bfvlfy8lc-orchestrate-test-0.35.0.drv` and
the remote `--max-jobs 0` build completed. Its valid output is
`/nix/store/6ld576drb5a099h2izm976dr03hz39ab-orchestrate-test-0.35.0`.
