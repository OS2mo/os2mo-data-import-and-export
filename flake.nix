# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  };

  outputs = {nixpkgs, ...}: let
    forAllSystems = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed;
  in {
    # `nix fmt`
    formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.alejandra);

    # `nix develop`
    devShells = forAllSystems (system: let
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      default = pkgs.mkShell {
        packages = [
          pkgs.python311
          pkgs.poetry

          # dependency of: pyodbc
          pkgs.unixodbc

          # dependency of: gssapi and pykerberos
          pkgs.krb5

          # dependency of: mysqlclient
          pkgs.mariadb-connector-c
        ];

        # nix does not install krb5 headers in /usr/include or /usr/lib, so we
        # instead have to explicitly provide gssapi's wheel the location in the
        # nix store
        GSSAPI_COMPILER_ARGS = "-I${pkgs.krb5.dev}/include -DHAS_GSSAPI_EXT_H";
        GSSAPI_LINKER_ARGS = "-L${pkgs.krb5.lib}/lib -lgssapi_krb5 -lkrb5 -lk5crypto -lcom_err";

        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
          # runtime dependency of: pyodbc
          pkgs.unixodbc

          # runtime dependency of: numpy and pandas
          pkgs.stdenv.cc.cc.lib
        ];

        shellHook = ''
          poetry env use ${pkgs.python311}/bin/python3.11
          eval $(poetry env activate)
          poetry install --no-root
        '';
      };
    });
  };
}
