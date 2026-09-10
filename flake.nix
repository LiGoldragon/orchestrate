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
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        packageArgs = commonArgs // { cargoExtraArgs = "--workspace"; };
        cargoArtifacts = craneLib.buildDepsOnly packageArgs;
      in
      {
        packages.default = craneLib.buildPackage (
          packageArgs
          // {
            inherit cargoArtifacts;
            meta.mainProgram = "orchestrate";
          }
        );
        checks = {
          build = craneLib.cargoBuild (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--workspace --all-targets";
            }
          );
          test = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--workspace --all-targets";
            }
          );
          live-nexus = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "-p orchestrate-nexus --test live_nexus";
            }
          );
          ordinary-lock-contract = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "-p orchestrate-nexus --test ordinary_lock_contract";
            }
          );
          ordinary-client = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "-p orchestrate --test client";
            }
          );
          meta-client = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "-p orchestrate-meta --test client";
            }
          );
          test-doc = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--doc";
            }
          );
          doc = craneLib.cargoDoc (
            commonArgs
            // {
              inherit cargoArtifacts;
              RUSTDOCFLAGS = "-D warnings";
            }
          );
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
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
