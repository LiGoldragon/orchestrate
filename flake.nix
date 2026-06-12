{
  description = "orchestrate — Persona orchestration machinery daemon and client.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            let
              pathString = toString path;
              schemaRoot = "${toString ./.}/schema";
            in
            craneLib.filterCargoSources path type
            || pathString == schemaRoot
            || pkgs.lib.hasPrefix "${schemaRoot}/" pathString;
          name = "source";
        };
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        packageArgs = commonArgs // {
          cargoExtraArgs = "--features nota-text";
        };
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
              cargoExtraArgs = "--features nota-text --all-targets";
            }
          );
          test = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
          test-dynamic-role-creation = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test ledger dynamic_role_creation_creates_report_lane_and_lock_identity";
            }
          );
          test-repository-refresh = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test ledger repository_refresh_indexes_local_checkouts_and_workspace_links";
            }
          );
          test-cli-boundary = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test architecture";
            }
          );
          test-daemon-cli = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--features nota-text --test daemon_cli";
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
              cargoClippyExtraArgs = "--features nota-text --all-targets -- -D warnings";
            }
          );
        };
        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate";
        };
        apps.daemon = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate-daemon";
        };
        apps.meta = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "meta-orchestrate";
        };
        devShells.default = pkgs.mkShell {
          name = "orchestrate";
          packages = [
            pkgs.jujutsu
            pkgs.pkg-config
            toolchain
          ];
        };
      }
    );
}
