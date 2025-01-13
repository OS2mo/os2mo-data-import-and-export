# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from textwrap import dedent
from typing import Any

import click
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql


class JSONParamType(click.ParamType):
    name = "json"

    def convert(self, value: Any, _1, _2) -> dict[Any, Any]:  # type: ignore
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


@click.command()
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
            cursor = result[mutator_name]["page_info"]["next_cursor"]  # type: ignore
            if print_uuids:
                uuids = result[mutator_name]["objects"]  # type: ignore
                for uuid in uuids:
                    click.echo(uuid)
            params["cursor"] = cursor  # type: ignore
            counter += limit
            if verbosity > 0:
                click.echo(counter)
