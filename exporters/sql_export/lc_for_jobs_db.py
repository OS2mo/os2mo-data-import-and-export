""" This program makes an actual state sqlite-database for use by
jobs run under job-runner as opposed to the actual state used
by customers. It is meant to be run just after the nightly imports
to be used as a speed up in comparison with hitting MO's rest interface.
"""
import logging

import click
from ra_utils.load_settings import load_settings
from sqlalchemy import create_engine

from exporters.sql_export.sql_export import SqlExport

LOG_LEVEL = logging.DEBUG
LOG_FILE = "lc-for-jobs.log"

logger = logging.getLogger("lc-for-jobs")


def get_engine(dbpath=None):
    if dbpath is None:
        settings = load_settings()
        dbpath = settings.get("lc-for-jobs.actual_db_name", "ActualState")

    dbpath = str(dbpath)
    if dbpath != ":memory:":
        dbpath += ".db"
    db_string = "sqlite:///{}".format(dbpath)
    return create_engine(db_string)


@click.group()
def cli():
    # Solely used for command grouping
    pass


@cli.command()
@click.option("--resolve-dar/--no-resolve-dar", default=True)
def sql_export(resolve_dar):

    # Load settings file
    org_settings = load_settings()

    # Override settings
    settings = {
        "exporters.actual_state.type": "SQLite",
        "exporters.actual_state_historic.type": "SQLite",
        "exporters.actual_state.db_name": org_settings.get(
            "lc-for-jobs.actual_db_name", "ActualState"
        ),
        "exporters.actual_state.manager_responsibility_class": org_settings[
            "exporters.actual_state.manager_responsibility_class"
        ],
    }

    sql_export = SqlExport(force_sqlite=True, historic=False, settings=settings)
    sql_export.perform_export(resolve_dar=resolve_dar)


if __name__ == "__main__":
    for name in logging.root.manager.loggerDict:  # type: ignore
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
