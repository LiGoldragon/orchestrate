{
  description = "Orchestrate Nexus — durable Lock state.";

  inputs.base.url = "github:LiGoldragon/build-base";

  outputs =
    { self, base }:
    base.lib.workspaceOutputs {
      cargoNix = ./Cargo.nix;
      extraChecks =
        {
          pkgs,
          toolchain,
          binaries,
          check,
          doc,
          ...
        }:
        let
          features = [ "default" ];
          nexus = binaries "orchestrate-nexus" features;
          clients = map (name: binaries name features) [
            "orchestrate"
            "orchestrate-meta"
          ];
          nexusTests = (check "orchestrate-nexus" features).tests;
          linkedCrates =
            root:
            builtins.genericClosure {
              startSet = [
                {
                  key = root.drvPath;
                  crate = root;
                }
              ];
              operator =
                node:
                map (crate: {
                  key = crate.drvPath;
                  inherit crate;
                }) node.crate.dependencies;
            };
          linkedNames = root: map (node: node.crate.crateName) (linkedCrates root);
          forbidden = [
            "datom-codec"
            "protos"
          ];
          isPlain = root: builtins.all (name: !(builtins.elem name (linkedNames root))) forbidden;
          isTextual = root: builtins.all (name: builtins.elem name (linkedNames root)) forbidden;
          nexusClosure = pkgs.closureInfo { rootPaths = [ nexus ]; };
          documentation = doc "orchestrate-nexus" features [
            "-D"
            "warnings"
          ];
        in
        assert pkgs.lib.assertMsg (
          isPlain nexus && isPlain nexusTests
        ) "The Nexus and its test build must link neither datom-codec nor protos";
        assert pkgs.lib.assertMsg (
          nexus.crateFeatures == nexusTests.crateFeatures
        ) "The Nexus check must exercise the packaged feature graph";
        assert pkgs.lib.assertMsg (builtins.all isTextual clients)
          "Both clients must link datom-codec and protos";
        {
          datom-free-nexus = pkgs.runCommand "datom-free-nexus" { } ''
            test -x ${nexus}/bin/orchestrate-nexus
            test ! -e ${nexus}/bin/orchestrate
            test ! -e ${nexus}/bin/orchestrate-meta
            ${pkgs.lib.concatMapStrings (client: ''
              grep -aq datom_codec ${client}/bin/${client.crateName}
            '') clients}
            for crate in ${pkgs.lib.escapeShellArgs forbidden}; do
              if grep -F "$crate" ${nexusClosure}/store-paths; then
                echo "orchestrate-nexus runtime closure contains $crate" >&2
                exit 1
              fi
            done
            mkdir -p $out
            cp ${nexusClosure}/store-paths $out/nexus-store-paths
          '';
          fmt = pkgs.runCommand "orchestrate-fmt" { nativeBuildInputs = [ toolchain ]; } ''
            find ${./.} -type f -name '*.rs' -print0 | xargs -0 rustfmt --edition 2024 --check
            mkdir -p $out
          '';
          doc = documentation;
          test-doc = documentation;
        };
    };
}
