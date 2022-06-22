import json
from typing import List
from uuid import UUID

import click
import pydantic
import requests
from gql import gql
from more_itertools import one
from raclients.graph.client import GraphQLClient


class Title(pydantic.BaseModel):
    uuid: UUID
    name: str = pydantic.Field(alias="user_key")


class Titles(pydantic.BaseModel):
    titles: List[Title]


def read_engagement_job_function(session):
    query = gql(
        """query MyQuery {
            facets {
                user_key
                uuid
                classes {
                    user_key
                    uuid
                    }
                }
            }
        """
    )
    r = session.execute(query)
    facets = r["facets"]
    # Get engagement_job_function
    eng_types = one(
        filter(lambda f: f["user_key"] == "engagement_job_function", facets)
    )

    # load into model
    titles = Titles(titles=eng_types["classes"])
    # Dump model to json and load back to convert uuids to str and "user_key" to "name"
    titles = json.loads(titles.json())
    return titles["titles"]


def check_update_titles(
    url: str, api_key: UUID, titles: Titles, dry_run: bool = False
) -> None:
    """Checks titles in rollekataloget and updates if changes are found"""
    res = requests.get(
        url,
        headers={"ApiKey": str(api_key)},
        verify=False,
    )
    res.raise_for_status()
    current_titles = res.json()

    if dry_run:
        if current_titles == titles:
            click.echo("Current titles in rollekataloget dosn't match OS2MO")
            click.echo(f"{current_titles=}")
            click.echo(f"{titles=}")
        else:
            click.echo("Current titles in rollekataloget matches OS2MO")

        return None

    if current_titles == titles:
        return None

    post = requests.post(
        url,
        json=titles,
        headers={"ApiKey": str(api_key)},
        verify=False,
    )
    post.raise_for_status()


def export_titles(
    mora_base,
    client_id,
    client_secret,
    auth_realm,
    auth_server,
    rollekatalog_url,
    rollekatalog_api_key,
    dry_run,
):
    with GraphQLClient(
        url=f"{mora_base}/graphql",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    ) as session:
        engagement_job_function = read_engagement_job_function(session)

    titles_url = rollekatalog_url.replace("organisation/v3", "title")
    check_update_titles(
        url=titles_url,
        api_key=rollekatalog_api_key,
        titles=engagement_job_function,
        dry_run=dry_run,
    )
