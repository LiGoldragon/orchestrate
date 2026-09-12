{
  description = "Orchestrate Nexus — durable Lock state.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-build = {
      url = "github:LiGoldragon/rust-build";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-build,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        rust = rust-build.lib.${system}.fromToolchainFile pkgs {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        inherit (rust) craneLib toolchain;
        ethosFilter = path: type: type == "regular" && pkgs.lib.hasSuffix ".ethos" path;
        src = rust.cleanSource {
          root = ./.; extraFilters = [ ethosFilter ];
        };
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
        commonArgs = {
          inherit src version;
          pname = "orchestrate";
          strictDeps = true;
        };

        # The Nexus and the Datom clients are built as two separate Cargo
        # resolutions, never as one `--workspace` build.
        #
        # Cargo unifies features across the members of a single build. The
        # clients enable the contract crates' `datom` feature; a `--workspace`
        # build therefore compiles those contracts with Datom on and links
        # `datom-codec` and `protos` into `orchestrate-nexus` as well — a
        # manifest that is clean and an artefact that is not. Resolving the
        # Nexus alone is what makes the Datom-free Nexus a fact about the
        # binary. The `datom-free-nexus` check witnesses it on exactly this
        # resolution.
        nexusArgs = commonArgs // {
          pname = "orchestrate-nexus";
          cargoExtraArgs = "--package orchestrate-nexus";
        };
        clientArgs = commonArgs // {
          pname = "orchestrate-clients";
          cargoExtraArgs = "--package orchestrate --package orchestrate-meta";
        };
        workspaceArgs = commonArgs // { cargoExtraArgs = "--workspace"; };

        nexusArtifacts = craneLib.buildDepsOnly nexusArgs;
        clientArtifacts = craneLib.buildDepsOnly clientArgs;
        workspaceArtifacts = craneLib.buildDepsOnly workspaceArgs;

        nexusPackage = craneLib.buildPackage (
          nexusArgs // { cargoArtifacts = nexusArtifacts; }
        );
        clientPackage = craneLib.buildPackage (
          clientArgs // { cargoArtifacts = clientArtifacts; }
        );
      in
      {
        packages.nexus = nexusPackage;
        packages.clients = clientPackage;
        packages.default = pkgs.symlinkJoin {
          name = "orchestrate-${version}";
          paths = [ nexusPackage clientPackage ];
          meta.mainProgram = "orchestrate";
        };
        checks = {
          build = craneLib.cargoBuild (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoExtraArgs = "--workspace --all-targets";
            }
          );
          test = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "--workspace --all-targets";
            }
          );
          live-nexus = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "-p orchestrate-nexus --test live_nexus";
            }
          );
          ordinary-lock-contract = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "-p orchestrate-nexus --test ordinary_lock_contract";
            }
          );
          configuration-authority = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "-p orchestrate-nexus --test configuration_authority";
            }
          );
          # The peer check's refusing branch. The named-owner witness runs
          # anywhere; the second-user witness runs where the host grants a
          # subordinate uid range, and says so when it cannot — a build
          # sandbox has none to grant.
          peer-authority = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              # --nocapture so that a host without a subordinate uid range
              # says so in the build log, rather than passing in silence.
              cargoTestExtraArgs = "-p orchestrate-nexus --test second_user_peer -- --nocapture";
            }
          );
          ordinary-client = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "-p orchestrate --test client";
            }
          );
          meta-client = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "-p orchestrate-meta --test client";
            }
          );
          test-doc = craneLib.cargoTest (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoTestExtraArgs = "--doc";
            }
          );
          doc = craneLib.cargoDoc (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoDocExtraArgs = "--workspace --no-deps";
              RUSTDOCFLAGS = "-D warnings";
            }
          );
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (
            commonArgs
            // {
              cargoArtifacts = workspaceArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
            }
          );

          # The Datom-free Nexus, witnessed on the configuration the Nexus
          # package is actually built with rather than on a manifest reading.
          # Both the Nexus resolution and the client resolution are printed,
          # so the check also shows that the clients do carry Datom — a run
          # where neither carries it would mean the command stopped resolving
          # anything.
          datom-free-nexus = craneLib.mkCargoDerivation (
            nexusArgs
            // {
              cargoArtifacts = nexusArtifacts;
              pnameSuffix = "-datom-free-nexus";
              buildPhaseCargoCommand = ''
                nexus_tree=$(cargo tree --package orchestrate-nexus --edges normal --locked --offline)
                client_tree=$(cargo tree --package orchestrate --package orchestrate-meta \
                  --edges normal --locked --offline)
                echo "$nexus_tree"
                for forbidden in datom-codec protos; do
                  if grep -q "$forbidden v" <<< "$nexus_tree"; then
                    echo "orchestrate-nexus links $forbidden as built" >&2
                    exit 1
                  fi
                  if ! grep -q "$forbidden v" <<< "$client_tree"; then
                    echo "the clients no longer link $forbidden; this check has stopped resolving" >&2
                    exit 1
                  fi
                done
                echo "orchestrate-nexus links neither datom-codec nor protos as built"
              '';
              installPhaseCommand = "mkdir -p $out";
            }
          );
        };
        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate";
        };
        apps.nexus = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate-nexus";
        };
        apps.meta = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate-meta";
        };
        devShells.default = pkgs.mkShell {
          name = "orchestrate";
          packages = [
            pkgs.pkg-config
            toolchain
          ];
        };
      }
    );
}
