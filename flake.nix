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
      extraChecks = { pkgs, toolchain, workspace, check, doc, ... }:
        let
          nexus = workspace.workspaceMembers.orchestrate-nexus.build;
          clients = [ workspace.workspaceMembers.orchestrate.build
                      workspace.workspaceMembers.orchestrate-meta.build ];
          nexusClosure = pkgs.closureInfo { rootPaths = [ nexus ]; };
          clientClosure = pkgs.closureInfo { rootPaths = clients; };
        in
        {
          datom-free-nexus = pkgs.runCommand "datom-free-nexus" { } ''
            forbidden="datom-codec protos";
            for crate in $forbidden; do
              if grep -q "$crate" ${nexusClosure}/store-paths; then
                echo "orchestrate-nexus links $crate" >&2
                exit 1
              fi
            done
            for crate in $forbidden; do
              if ! grep -q "$crate" ${clientClosure}/store-paths; then
                echo "clients no longer link $crate" >&2
                exit 1
              fi
            done
            mkdir -p $out
          '';
          fmt = pkgs.runCommand "orchestrate-fmt" { nativeBuildInputs = [ toolchain ]; } ''
            find ${./.} -type f -name '*.rs' -print0 | xargs -0 rustfmt --edition 2024 --check
            mkdir -p $out
          '';
          doc = doc "orchestrate-nexus" [ "default" ] [ "-D" "warnings" ];
          test-doc = doc "orchestrate-nexus" [ "default" ] [ "-D" "warnings" ];
        };
    };
}
