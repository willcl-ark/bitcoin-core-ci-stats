{
  description = "bitcoin core ci fetcher";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/25.05";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = {
    nixpkgs,
    flake-utils,
    rust-overlay,
    ...
  }:
    flake-utils.lib.eachDefaultSystem
    (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {inherit system overlays;};
      in
        with pkgs; {
          formatter = alejandra;

          packages.default = callPackage ./. {
            rustPlatform = makeRustPlatform {
              cargo = rust-bin.stable.latest.default;
              rustc = rust-bin.stable.latest.default;
            };
          };

          devShells.default = mkShell {
            stdenv = gcc14Stdenv;
            nativeBuildInputs = [];
            buildInputs = [
              rust-bin.stable.latest.default
            ];
          };
        }
    );
}
