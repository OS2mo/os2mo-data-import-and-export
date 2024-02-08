# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
"""Utility CLI for OS2mo via GraphQL."""
import json
import logging
from typing import Any
from textwrap import dedent

import structlog

import click
from gql import gql
from raclients.graph.client import GraphQLClient
from more_itertools import one


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


class JSONParamType(click.ParamType):
    name = "json"

    def convert(self, value: Any, _1, _2) -> dict[Any, Any]:
        return json.loads(value)


JSON = JSONParamType()


VALID_COLLECTIONS = [
    "address",
    "association",
    "class",
    "employee",
    "engagement",
    "facet",
    "itsystem",
    "ituser",
    "kle",
    "leave",
    "manager",
    "org_unit",
    "owner",
    "related_unit",
    "role",
]


@cli.command()
@click.pass_context
@click.option(
    "--limit",
    type=click.INT,
    default=100,
    help="Number entities to update per refresh.",
)
@click.option(
    "--print-uuids",
    type=click.BOOL,
    is_flag=True,
    default=False,
    help="Whether to print UUIDs as they get deleted.",
)
@click.option(
    "--address-type",
    required=True,
    help="The address type to purge.",
)
def purge_addresses(
    ctx: Any,
    limit: int,
    print_uuids: bool,
    address_type: str,
) -> None:
    """Script to purge/delete an address-type and all related addreses.

    NOTE: This is *not* a bulk termination script, it is for deletion.
    """
    verbosity = ctx.obj["VERBOSITY"]

    address_delete_query = """
    mutation DeleteAddress($uuid: UUID!) {
      address_delete(uuid: $uuid) {
        uuid
      }
    }
    """

    class_delete_query = """
    mutation DeleteClass($uuid: UUID!) {
      class_delete(uuid: $uuid) {
        uuid
      }
    }
    """

    class_query = """
    query ClassFetch($address_type_user_key: String!) {
      classes(filter: {user_keys: [$address_type_user_key]}) {
        objects {
          uuid
        }
      }
    }
    """

    query = """
    query AddressFetch(
        $limit: int!,
        $cursor: Cursor,
        $address_type_uuid: UUID!,
    ) {
      addresses(
        filter: {address_type: {uuids: [$address_type_uuid]}}
        limit: $limit
        cursor: $cursor
      ) {
        objects {
          uuid
        }
        page_info {
          next_cursor
        }
      }
    }
    """
    with GraphQLClient(
        **ctx.obj["GRAPHQL_PARAMS"],
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        gql_query = gql(query)
        gql_class_query = gql(class_query)
        gql_address_delete_query = gql(address_delete_query)
        gql_class_delete_query = gql(class_delete_query)

        class_result = session.execute(
            gql_class_query, variable_values={"address_type_user_key": address_type}
        )
        class_uuid = one(class_result["classes"]["objects"])["uuid"]

        params = {
            "limit": limit,
            "cursor": None,
            "address_type_uuid": class_uuid,
        }
        if verbosity > 0:
            click.echo(params)

        cursor = object()
        counter = 0
        while cursor is not None:
            result = session.execute(gql_query, variable_values=params)
            cursor = result["addresses"]["page_info"]["next_cursor"]
            objects = result["addresses"]["objects"]
            for obj in objects:
                uuid = obj["uuid"]
                delete_result = session.execute(
                    gql_address_delete_query, variable_values={"uuid": uuid}
                )
                deleted_uuid = delete_result["address_delete"]["uuid"]
                assert deleted_uuid == uuid
                if print_uuids:
                    click.echo(f"address: {deleted_uuid}")
            params["cursor"] = cursor
            counter += limit
            if verbosity > 0:
                click.echo(counter)

        class_delete_result = session.execute(
            gql_class_delete_query, variable_values={"uuid": class_uuid}
        )
        deleted_class_uuid = class_delete_result["class_delete"]["uuid"]
        assert deleted_class_uuid == class_uuid
        if print_uuids:
            click.echo(f"class: {deleted_class_uuid}")


@cli.command()
@click.pass_context
@click.option(
    "--limit",
    type=click.INT,
    default=100,
    help="Number entities to update per refresh.",
)
@click.option(
    "--filter", type=JSON, help="JSON filter to apply to the refresh mutator."
)
@click.option(
    "--collection",
    type=click.Choice(VALID_COLLECTIONS),
    required=True,
    help="Name of the collection to refresh.",
)
@click.option(
    "--print-uuids",
    type=click.BOOL,
    is_flag=True,
    default=False,
    help="Whether to print UUIDs as they get refreshed.",
)
@click.option("--queue", type=click.STRING, help="The specific queue to target.")
def refresher(
    ctx: Any,
    limit: int,
    filter: dict[Any, Any],
    collection: str,
    print_uuids: bool,
    queue: str,
) -> None:
    verbosity = ctx.obj["VERBOSITY"]

    filter_type = f"{collection.capitalize()}Filter"
    mutator_name = f"{collection}_refresh"

    query = dedent(
        f"""
        mutation RefreshMutation(
          $limit: int!,
          $cursor: Cursor,
          $include_uuid: Boolean!
          $queue: String,
          $filter: {filter_type}
        ) {{
        {mutator_name}(
            limit: $limit
            queue: $queue
            cursor: $cursor
            filter: $filter
          ) {{
            objects @include(if: $include_uuid)
            page_info {{
              next_cursor
            }}
          }}
        }}
    """
    )
    if verbosity > 0:
        click.echo(query)
    params = {
        "limit": limit,
        "cursor": None,
        "include_uuid": print_uuids,
        "queue": queue,
        "filter": filter,
    }
    if verbosity > 0:
        click.echo(params)
    with GraphQLClient(
        **ctx.obj["GRAPHQL_PARAMS"],
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        gql_query = gql(query)
        cursor = object()
        counter = 0
        while cursor is not None:
            result = session.execute(gql_query, variable_values=params)
            cursor = result[mutator_name]["page_info"]["next_cursor"]
            if print_uuids:
                uuids = result[mutator_name]["objects"]
                for uuid in uuids:
                    click.echo(uuid)
            params["cursor"] = cursor
            counter += limit
            if verbosity > 0:
                click.echo(counter)


if __name__ == "__main__":
    cli()
