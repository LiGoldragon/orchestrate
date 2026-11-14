{
  description = "Orchestrate Nexus — durable Lock state.";
  inputs.base.url = "github:LiGoldragon/build-base";
  outputs = { self, base }:
    base.lib.workspaceOutputs {
      cargoNix = ./Cargo.nix;
      memberChecks = { check, ... }:
        {
          orchestrate-nexus = check "orchestrate-nexus" [ "default" ];
          orchestrate = check "orchestrate" [ "default" ];
          orchestrate-meta = check "orchestrate-meta" [ "default" ];
        };
      extraChecks = { check, ... }:
        {
          datom-free-nexus = check "orchestrate-nexus" [ "default" ];
          live-nexus = check "orchestrate-nexus" [ "default" ];
          ordinary-lock-contract = check "orchestrate-nexus" [ "default" ];
          configuration-authority = check "orchestrate-nexus" [ "default" ];
          peer-authority = check "orchestrate-nexus" [ "default" ];
          socket-claim = check "orchestrate-nexus" [ "default" ];
          carried-store = check "orchestrate-nexus" [ "default" ];
          relocation = check "orchestrate-nexus" [ "default" ];
          store-generations = check "orchestrate-nexus" [ "default" ];
          stopping = check "orchestrate-nexus" [ "default" ];
          ordinary-client = check "orchestrate" [ "default" ];
          meta-client = check "orchestrate-meta" [ "default" ];
          test-doc = check "orchestrate-nexus" [ "default" ];
        };
    };
}
