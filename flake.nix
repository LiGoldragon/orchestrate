{
  description = "orchestrate — durable PathLock Nexus.";

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
        src = rust.cleanSource {
          root = ./.;
        };
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        packageArgs = commonArgs;
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
              cargoExtraArgs = "--all-targets";
            }
          );
          test = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
          live-nexus = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--test live_nexus";
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
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
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
            pkgs.pkg-config
            toolchain
          ];
        };
      }
    );
}
