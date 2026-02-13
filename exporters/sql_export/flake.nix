{
  description = "SQLExport";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          # Tools needed at build-time (like pkg-config)
          nativeBuildInputs = with pkgs; [ 
            pkg-config 
          ];

          buildInputs = with pkgs; [
            python311
            python311Packages.pip
            # Ensure you use the Nix-provided poetry
            poetry            
            # Needed to build C extensions
            gcc
            # For pyodbc
            unixODBC
            # For mysqlclient
            libmysqlclient
          ];
        };
      });
}
