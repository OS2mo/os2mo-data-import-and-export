import json
from uuid import UUID
from typing import Iterator, List, Tuple, Any
from operator import itemgetter
from datetime import date
from datetime import datetime

import click
from os2mo_helpers.mora_helpers import MoraHelper
from tqdm import tqdm
from pydantic import AnyHttpUrl
from more_itertools import one

from exporters.utils.apply import apply
from integrations.ad_integration.utils import AttrDict


def find_users(mora_base: AnyHttpUrl) -> Iterator[Tuple[UUID, List[UUID]]]:
    helper = MoraHelper(hostname=mora_base, use_cache=False)

    def find_problematic(user_uuid: UUID) -> Tuple[UUID, List[UUID]]:
        mo_engagements = helper.read_user_engagement(
            user=str(user_uuid),
            only_primary=True,
            read_all=True,
            skip_past=True
        )
        mo_engagements = map(
            lambda mo_engagement: (mo_engagement['uuid'], mo_engagement['validity']['to']),
            mo_engagements
        )
        mo_engagements = filter(
            apply(lambda mo_uuid, end_date: end_date is not None),
            mo_engagements
        )
        mo_engagements = map(
            apply(lambda mo_uuid, end_date: (mo_uuid, datetime.strptime(end_date, "%Y-%m-%d").date())),
            mo_engagements
        )
        mo_engagements = filter(
            apply(lambda mo_uuid, end_date: end_date >= date(9999, 1, 1)),
            mo_engagements
        )
        mo_engagements = map(itemgetter(0), mo_engagements)
        mo_engagements = map(UUID, mo_engagements)
        return user_uuid, list(mo_engagements)

    all_users = helper.read_all_users()
    all_users = tqdm(all_users)
    all_users = map(itemgetter('uuid'), all_users)
    all_users = map(UUID, all_users)
    all_users = map(find_problematic, all_users)
    all_users = filter(
        apply(lambda user_uuid, engagement_uuids: bool(engagement_uuids)),
        all_users
    )
    return all_users


def fixup_user(mora_base: AnyHttpUrl, person_uuid: UUID, engagement_uuid: UUID, dry_run: bool = False) -> Tuple[str, Any]:
    helper = MoraHelper(hostname=mora_base, use_cache=False)
    # Fetch all present engagements for the user
    engagements = helper._mo_lookup(
        person_uuid,
        "e/{}/details/engagement",
        validity='present',
        only_primary=False,
        use_cache=False,
        calculate_primary=False,
    )
    # Find the engagement we are looking for in the list
    engagements = filter(
        lambda engagement: engagement['uuid'] == str(engagement_uuid),
        engagements
    )
    engagement = one(engagements)

    # Prepare our fix payload
    data = {}
    data.update({
        key: {
            "uuid": engagement[key]["uuid"],
        } for key in ["engagement_type", "job_function", "org_unit", "person", "primary"]
    })
    data.update({
        key: engagement[key] for key in ["extension_" + str(i) for i in range(1, 11)]
    })
    data.update({
        key: engagement[key] for key in ["fraction", "is_primary", "user_key", "uuid"]
    })
    data.update({
        "validity": {
            "from": engagement["validity"]["from"],
            "to": None,
        }
    })
    # Fire the payload
    payload = {
        'type': 'engagement',
        'uuid': str(engagement_uuid),
        'data': data,
        'person': {
            'uuid': str(person_uuid,)
        }
    }
    if dry_run:
        return payload, AttrDict({'status_code': 200, 'text': "Dry-run"})
    response = helper._mo_post("details/edit", payload)
    return payload, response


@click.group()
def cli():
    pass


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--json", is_flag=True, default=False)
def find(mora_base: AnyHttpUrl, json: bool):
    all_users = find_users(mora_base)
    if json:
        all_users = dict(all_users)
        print(json.dumps(all_users, indent=4))
    else:
        for user_uuid, engagement_uuids in all_users:
            print(user_uuid, engagement_uuids)


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--person-uuid", required=True, type=click.UUID)
@click.option("--engagement-uuid", required=True, type=click.UUID)
def fixup(mora_base: AnyHttpUrl, person_uuid: UUID, engagement_uuid: UUID):
    payload, response = fixup_user(mora_base, person_uuid, engagement_uuid)
    print(json.dumps(payload, indent=4))
    print(response.status_code)
    print(response.text)


@cli.command()
@click.option("--mora-base", default="http://localhost:5000")
@click.option("--dry-run", is_flag=True, default=False)
def rework(mora_base: AnyHttpUrl, dry_run: bool):
    all_users = find_users(mora_base)
    for user_uuid, engagement_uuids in all_users:
        print(user_uuid, engagement_uuids)
        for engagement_uuid in engagement_uuids:
            try:
                payload, response = fixup_user(mora_base, user_uuid, engagement_uuid, dry_run=dry_run)
            except ValueError:
                print("Unable to fixup", engagement_uuid)
                continue
            if response.status_code != 200:
                print(response.text)
            # print(json.dumps(payload, indent=4))


if __name__ == '__main__':
    cli()
