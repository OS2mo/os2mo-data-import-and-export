# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
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

          # pyodbc
          pkgs.unixODBC

          # pymssql
          pkgs.freetds

          # requests-kerberos
          pkgs.krb5

          # mysqlclient
          pkgs.mariadb-connector-c

          # lxml
          pkgs.libxml2
          pkgs.libxml2.dev
          pkgs.libxslt
          pkgs.libxslt.dev

          # psycopg2-binary (prebuilt, but kept for safety)
          pkgs.libpq
          pkgs.libpq.pg_config

          # Convenience utilities
          pkgs.jq
        ];

        # python-gssapi (dependency of requests-kerberos) uses krb5-config to
        # detect headers, but nixpkgs' krb5-config reports the lib output as
        # prefix, so gssapi_ext.h is never found and the build fails with
        # implicit-declaration errors. Pass the flags explicitly instead.
        GSSAPI_COMPILER_ARGS = "-I${pkgs.krb5.dev}/include -DHAS_GSSAPI_EXT_H";
        GSSAPI_LINKER_ARGS = "-L${pkgs.krb5.out}/lib -lgssapi_krb5 -lkrb5 -lk5crypto -lcom_err";

        # Runtime library lookup for the pyodbc manylinux wheel (libodbc) and
        # C extension wheels linking libstdc++ (pyodbc, numpy).
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
          pkgs.unixODBC
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
