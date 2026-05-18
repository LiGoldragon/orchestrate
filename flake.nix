{
  description = "Typed orchestration state for Persona agents.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system nixpkgs.legacyPackages.${system});
    in
    {
      packages = forSystems (
        system: pkgs:
        {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "persona-orchestrate";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            meta.mainProgram = "persona-orchestrate-daemon";
          };
        }
      );

      checks = forSystems (
        system: pkgs:
        {
          default = self.packages.${system}.default;
        }
      );

      apps = forSystems (
        system: pkgs:
        {
          default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/persona-orchestrate-daemon";
          };
        }
      );

      devShells = forSystems (
        system: pkgs:
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.clippy
              pkgs.rust-analyzer
              pkgs.rustc
              pkgs.rustfmt
            ];
          };
        }
      );

      formatter = forSystems (system: pkgs: pkgs.nixfmt);
    };
}
