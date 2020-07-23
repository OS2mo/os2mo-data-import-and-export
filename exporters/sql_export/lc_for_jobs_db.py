""" This program makes an actual state sqlite-database for use by
jobs run under job-runner as opposed to the actual state used
by customers. It is meant to be run just after the nightly imports
to be used as a speed up in comparison with hitting MO's rest interface.
"""
import click
import json
import logging
import pathlib


from exporters.sql_export.sql_export import SqlExport


LOG_LEVEL = logging.DEBUG
LOG_FILE = "lc-for-jobs.log"

logger = logging.getLogger("lc-for-jobs")


@click.group()
def cli():
    # Solely used for command grouping
    pass


@cli.command()
@click.option("--resolve-dar/--no-resolve-dar", default=False)
@click.option("--historic/--no-historic", default=False)
def sql_export(resolve_dar, historic):

    # Load settings file
    cfg_file = pathlib.Path.cwd() / "settings" / "settings.json"
    if not cfg_file.is_file():
        raise Exception("No setting file")
    org_settings = json.loads(cfg_file.read_text())

    # Override settings
    settings = {
        "exporters.actual_state.type": "SQLite",
        "exporters.actual_state_historic.type": "SQLite",
        "exporters.actual_state.db_name": org_settings[
            "lc-for-jobs.actual.db_name"
        ],
        "exporters.actual_state_historic.db_name": org_settings.get(
            "lc-for-jobs.historic.db_name", ""
        ),
        "exporters.actual_state.manager_responsibility_class": org_settings[
            "exporters.actual_state.manager_responsibility_class"
        ]
    }

    # Generate sql export

    if historic and not org_settings["lc-for-jobs.historic.db_name"]:
        raise ValueError("'lc-for-jobs.historic.db_name' not present in settings file")

    sql_export = SqlExport(
        force_sqlite=True, historic=historic, settings=settings
    )
    sql_export.perform_export(resolve_dar=resolve_dar, use_pickle=False)


if __name__ == "__main__":
    for name in logging.root.manager.loggerDict:
        if name in ("lc-for-jobs", "LoraCache", "SqlExport"):
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )
    cli()
