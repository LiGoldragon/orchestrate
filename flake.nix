{
  description = "orchestrate — durable native Datom path-lock registration.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-build = {
      url = "github:LiGoldragon/rust-build";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-build }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        rust = rust-build.lib.${system}.fromToolchainFile pkgs {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        inherit (rust) craneLib toolchain;
        src = rust.cleanSource { root = ./.; };
        commonArgs = { inherit src; strictDeps = true; };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      in {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          meta.mainProgram = "orchestrate";
        });
        checks = {
          build = craneLib.cargoBuild (commonArgs // {
            inherit cargoArtifacts;
            cargoExtraArgs = "--all-targets";
          });
          test = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
          test-path-lock-registry = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test path_lock_registry";
          });
          stateful-path-lock-scenario = pkgs.runCommand "orchestrate-path-lock-scenario" {
            nativeBuildInputs = [ pkgs.bash pkgs.inotify-tools ];
          } ''
            ${pkgs.bash}/bin/bash ${./checks/path-lock-scenario.sh} \
              ${self.packages.${system}.default}/bin/orchestrate-daemon \
              ${self.packages.${system}.default}/bin/orchestrate
            touch $out
          '';
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };
        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate";
        };
        apps.daemon = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
          name = "orchestrate-daemon";
        };
        devShells.default = pkgs.mkShell {
          name = "orchestrate";
          packages = [ pkgs.pkg-config toolchain ];
        };
      });
}
