"""Script to fixup issues relating to engagement end-dates around 9999-12-30."""
import json
from datetime import date
from datetime import datetime
from operator import itemgetter
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
from uuid import UUID

import click
from exporters.utils.apply import apply
from integrations.ad_integration.utils import AttrDict
from more_itertools import one
from os2mo_helpers.mora_helpers import MoraHelper
from pydantic import AnyHttpUrl
from tqdm import tqdm


def find_bad_engagements(mora_base: AnyHttpUrl) -> Iterator[Tuple[UUID, List[UUID]]]:
    """Find users with engagements that ends after 9999-01-01."""

    def enrich_user_uuid(user_uuid: UUID) -> Tuple[UUID, List[UUID]]:
        """Enrich each user_uuid with engagements that end after 9999-01-01."""
        # Fetch all engagements for the user
        mo_engagements: List[Dict] = helper.read_user_engagement(
            user=str(user_uuid), only_primary=False, read_all=True, skip_past=True
        )
        # Extract uuid and end-date, filter out infinity end-dates.
        mo_engagement_tuples_str: Iterator[Tuple[UUID, str]] = map(
            lambda mo_engagement: (
                mo_engagement["uuid"],
                mo_engagement["validity"]["to"],
            ),
            mo_engagements,
        )
        mo_engagement_tuples_str = filter(
            apply(lambda mo_uuid, end_date: end_date is not None),
            mo_engagement_tuples_str,
        )
        # Convert end-date to datetime.date, and filter out dates before 9999-01-01
        mo_engagement_tuples: Iterator[Tuple[UUID, date]] = map(
            apply(
                lambda mo_uuid, end_date: (
                    mo_uuid,
                    datetime.strptime(end_date, "%Y-%m-%d").date(),
                )
            ),
            mo_engagement_tuples_str,
        )
        mo_engagement_tuples = filter(
            apply(lambda mo_uuid, end_date: end_date >= date(9999, 1, 1)),
            mo_engagement_tuples,
        )
        # Extract and convert resulting engagement uuids
        mo_engagement_uuid_strings: Iterator[str] = map(
            itemgetter(0), mo_engagement_tuples
        )
        mo_engagement_uuids: Iterator[UUID] = map(UUID, mo_engagement_uuid_strings)
        return user_uuid, list(mo_engagement_uuids)

    helper = MoraHelper(hostname=mora_base, use_cache=False)
    # Read all users and map to just their UUIDs
    users: Iterator[Dict] = tqdm(helper.read_all_users())
    user_uuid_strings: Iterator[str] = map(itemgetter("uuid"), users)
    user_uuids: Iterator[UUID] = map(UUID, user_uuid_strings)
    # Enrich each user_uuid with a list of uuids from engagements that has a bad end-date, filter empty lists
    user_tuples: Iterator[Tuple[UUID, List[UUID]]] = map(enrich_user_uuid, user_uuids)
    user_tuples = filter(
        apply(lambda user_uuid, engagement_uuids: bool(engagement_uuids)), user_tuples
    )
    return user_tuples


def fixup_single_user(
    mora_base: AnyHttpUrl,
    person_uuid: UUID,
    engagement_uuid: UUID,
    dry_run: bool = False,
) -> Tuple[Dict[str, Any], Any]:
    """Fixup the end-date of a single engagement for a single user."""
    helper = MoraHelper(hostname=mora_base, use_cache=False)
    # Fetch all present engagements for the user
    engagements: Iterator[Dict[str, Any]] = helper._mo_lookup(
        person_uuid,
        "e/{}/details/engagement",
        validity="present",
        only_primary=False,
        use_cache=False,
        calculate_primary=False,
    )
    # Find the engagement we are looking for in the list
    engagements = filter(
        lambda engagement: engagement["uuid"] == str(engagement_uuid), engagements
    )
    engagement: Dict[str, Any] = one(engagements)

    # Construct data-part of our payload using current data.
    uuid_keys = [
        "engagement_type",
        "job_function",
        "org_unit",
        "person",
        "primary",
    ]
    direct_keys = ["extension_" + str(i) for i in range(1, 11)] + [
        "fraction",
        "is_primary",
        "user_key",
        "uuid",
    ]
    data: Dict[str, Any] = {}
    data.update({key: {"uuid": engagement[key]["uuid"]} for key in uuid_keys})
    data.update({key: engagement[key] for key in direct_keys})
    data.update(
        {
            "validity": {
                "from": engagement["validity"]["from"],
                "to": None,
            }
        }
    )

    # Construct entire payload
    payload: Dict[str, Any] = {
        "type": "engagement",
        "uuid": str(engagement_uuid),
        "data": data,
        "person": {
            "uuid": str(
                person_uuid,
            )
        },
    }
    if dry_run:
        return payload, AttrDict({"status_code": 200, "text": "Dry-run"})
    response = helper._mo_post("details/edit", payload)
    return payload, response


@click.group()
def cli():
    pass


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--output-json", is_flag=True, default=False)
def find(mora_base: AnyHttpUrl, output_json: bool):
    """Find engagements / users with the issue."""
    all_users = find_bad_engagements(mora_base)
    if output_json:
        all_users_dict = dict(all_users)
        print(json.dumps(all_users_dict, indent=4))
    else:
        for user_uuid, engagement_uuids in all_users:
            print(user_uuid, engagement_uuids)


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--person-uuid", required=True, type=click.UUID)
@click.option("--engagement-uuid", required=True, type=click.UUID)
def fixup_user(mora_base: AnyHttpUrl, person_uuid: UUID, engagement_uuid: UUID):
    """Fixup a single engagement end-date for a single user."""
    payload, response = fixup_single_user(mora_base, person_uuid, engagement_uuid)
    print(json.dumps(payload, indent=4))
    print(response.status_code)
    print(response.text)


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--dry-run", is_flag=True, default=False)
def fixup_all(mora_base: AnyHttpUrl, dry_run: bool):
    """Find and fixup all users with the issue."""
    all_users = find_bad_engagements(mora_base)
    for user_uuid, engagement_uuids in all_users:
        print(user_uuid, engagement_uuids)
        for engagement_uuid in engagement_uuids:
            try:
                payload, response = fixup_single_user(
                    mora_base, user_uuid, engagement_uuid, dry_run=dry_run
                )
            except ValueError:
                print("Unable to fixup", engagement_uuid)
                continue
            if response.status_code != 200:
                print(response.text)
            # print(json.dumps(payload, indent=4))


if __name__ == "__main__":
    cli()
