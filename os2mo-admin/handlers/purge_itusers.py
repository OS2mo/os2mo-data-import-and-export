# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any

import click
from gql import gql
from raclients.graph.client import GraphQLClient
from more_itertools import one


@click.command()
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
    "--itsystem",
    required=True,
    help="The itsystem to purge.",
)
def purge_itusers(
    ctx: Any,
    limit: int,
    print_uuids: bool,
    itsystem: str,
) -> None:
    """Script to purge/delete an itsystem and all related itusers.

    NOTE: This is *not* a bulk termination script, it is for deletion.
    """
    verbosity = ctx.obj["VERBOSITY"]

    ituser_delete_query = """
    mutation DeleteITUsers($uuid: UUID!) {
      ituser_delete(uuid: $uuid) {
        uuid
      }
    }
    """

    itsystem_delete_query = """
    mutation DeleteITSystem($uuid: UUID!) {
      itsystem_delete(uuid: $uuid) {
        uuid
      }
    }
    """

    itsystem_query = """
    query ITSystemFetch($itsystem_user_key: String!) {
      itsystems(filter: {user_keys: [$itsystem_user_key]}) {
        objects {
          uuid
        }
      }
    }
    """

    query = """
    query ITUserFetch(
        $limit: int!,
        $cursor: Cursor,
        $itsystem_uuid: UUID!,
    ) {
      itusers(
        filter: {itsystem: {uuids: [$itsystem_uuid]}}
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
        gql_itsystem_query = gql(itsystem_query)
        gql_ituser_delete_query = gql(ituser_delete_query)
        gql_itsystem_delete_query = gql(itsystem_delete_query)

        itsystem_result = session.execute(
            gql_itsystem_query, variable_values={"itsystem_user_key": itsystem}
        )
        itsystem_uuid = one(itsystem_result["itsystems"]["objects"])["uuid"]

        params = {
            "limit": limit,
            "cursor": None,
            "itsystem_uuid": itsystem_uuid,
        }
        if verbosity > 0:
            click.echo(params)

        cursor = object()
        counter = 0
        while cursor is not None:
            result = session.execute(gql_query, variable_values=params)
            cursor = result["itusers"]["page_info"]["next_cursor"]
            objects = result["itusers"]["objects"]
            for obj in objects:
                uuid = obj["uuid"]
                delete_result = session.execute(
                    gql_ituser_delete_query, variable_values={"uuid": uuid}
                )
                deleted_uuid = delete_result["ituser_delete"]["uuid"]
                assert deleted_uuid == uuid
                if print_uuids:
                    click.echo(f"ituser: {deleted_uuid}")
            params["cursor"] = cursor
            counter += limit
            if verbosity > 0:
                click.echo(counter)

        itsystem_delete_result = session.execute(
            gql_itsystem_delete_query, variable_values={"uuid": itsystem_uuid}
        )
        deleted_itsystem_uuid = itsystem_delete_result["itsystem_delete"]["uuid"]
        assert deleted_itsystem_uuid == itsystem_uuid
        if print_uuids:
            click.echo(f"itsystem: {deleted_itsystem_uuid}")
