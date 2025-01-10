# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from typing import Any

import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
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
