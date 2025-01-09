import json
from typing import List
from uuid import UUID

import click
import pydantic
import requests
from gql import gql
from raclients.graph.client import GraphQLClient


class Title(pydantic.BaseModel):
    uuid: UUID
    name: str = pydantic.Field(alias="user_key")


class Titles(pydantic.BaseModel):
    titles: List[Title]


def read_engagement_job_function(session):
    query = gql(
        """
      query MyQuery {
        classes(filter: {facet: {user_keys: "engagement_job_function" }}) {
          objects {
            current {
              uuid
              user_key
            }
          }
        }
      }
    """
    )
    r = session.execute(query)
    eng_types = [obj["current"] for obj in r["classes"]["objects"]]
    titles = Titles(titles=eng_types)
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
            click.echo("Current titles in rollekataloget matches OS2MO")
        else:
            click.echo("Current titles in rollekataloget doesn't match OS2MO")
            click.echo(f"{current_titles=}")
            click.echo(f"{titles=}")

        return None

    if current_titles == titles:
        click.echo("No changes to titles - not posting titles to OS2Rollekataloget.")
        return None

    post = requests.post(
        url,
        json=titles,
        headers={"ApiKey": str(api_key)},
        verify=False,
    )
    post.raise_for_status()


def export_titles(
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
    rollekatalog_url: str,
    rollekatalog_api_key: UUID,
    dry_run: bool,
) -> None:
    with GraphQLClient(
        url=f"{mora_base}/graphql/v22",
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
