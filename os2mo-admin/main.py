# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Utility CLI for OS2mo via GraphQL."""
import logging
from typing import Any

import structlog

import click

from handlers.purge_addresses import purge_addresses
from handlers.purge_itusers import purge_itusers
from handlers.refresher import refresher


@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("--mora-base", envvar="MORA_BASE", default="http://localhost:5000")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option(
    "--client-secret",
    envvar="CLIENT_SECRET",
    default="603f1c82-d012-4d04-9382-dbe659c533fb",
)
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option(
    "--auth-server", envvar="AUTH_SERVER", default="http://localhost:5000/auth"
)
@click.option(
    "--graphql-version", type=click.INT, envvar="GRAPHQL_VERSION", default="20"
)
@click.pass_context
def cli(
    ctx: Any,
    verbose: int,
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    graphql_version: int,
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["VERBOSITY"] = verbose
    verbosity_to_loglevel = {
        0: logging.WARN,
        1: logging.WARN,
        2: logging.INFO,
        3: logging.DEBUG,
    }
    log_level = verbosity_to_loglevel[verbose]
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
    )

    ctx.obj["GRAPHQL_PARAMS"] = {
        "url": f"{mora_base}/graphql/v{graphql_version}",
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_realm": auth_realm,
        "auth_server": auth_server,
    }


cli.add_command(purge_addresses)
cli.add_command(purge_itusers)
cli.add_command(refresher)


if __name__ == "__main__":
    cli()
