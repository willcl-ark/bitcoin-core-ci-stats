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

          packages.default = let
            rustPlatform = makeRustPlatform {
              cargo = rust-bin.stable.latest.default;
              rustc = rust-bin.stable.latest.default;
            };
          in rustPlatform.buildRustPackage rec {
            pname = "fetch-tasks-github";
            version = "0.1.0";

            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            nativeBuildInputs = [
              pkg-config
            ];

            buildInputs = [
              openssl
            ];

            # Skip tests during build as they may require network access
            doCheck = false;

            meta = with lib; {
              description = "Bitcoin Core CI fetcher for GitHub Actions data";
              license = licenses.mit;
              maintainers = [ ];
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
