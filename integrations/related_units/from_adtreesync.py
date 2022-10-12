import sys
from csv import DictReader

import click


@click.command()
@click.argument("from-file", type=click.File())
@click.argument("to-file", type=click.File())
def cli(from_file, to_file):
    """Tool to create an input file for `import_related_units`.

    This program takes two semicolon-separated CSV files as input, and writes its output
    file to `stdout`. The output is intended to be imported into MO using
    `import_related_units.py`.

    The first input file `from-file` contains columns `from_uuid` and `to_dn`.
    It is produced by hand based on an Excel file delivered by the customer.

    The second input file `to-file` is written by invoking
    `python -m integrations.adtreesync.main`, and is usually called `adtreesync.csv`.
    It contains the columns `UUID` and `DistinguishedName`.

    This program finds the "from" and "to" UUIDs by "joining" the two input files on
    the columns `to_dn`/`DistinguishedName`.

    Any non-matching lines in the first input file are written to `stderr`.
    Any lines where the "from" and "to" UUID are identical are also written to `stderr`.
    """

    from_map = {
        row["to_dn"].replace(r"\\,", r"\,"): row["from_uuid"]
        for row in DictReader(from_file, delimiter=";")
    }

    to_map = {
        row["DistinguishedName"]: row["UUID"]
        for row in DictReader(to_file, delimiter=";")
    }

    for dn, to_uuid in to_map.items():
        from_uuid = from_map.get(dn)
        if from_uuid:
            if from_uuid == to_uuid:
                print("'to' and 'from' UUIDs must differ", file=sys.stderr)
            else:
                print(f"{from_uuid} {to_uuid}", file=sys.stdout)
        else:
            print(f"no match on {dn}", file=sys.stderr)


if __name__ == "__main__":
    cli()
