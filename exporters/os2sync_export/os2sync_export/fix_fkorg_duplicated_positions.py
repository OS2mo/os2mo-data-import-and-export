import logging
from collections import Counter
from operator import itemgetter
from uuid import UUID

import click
from os2sync_export.config import get_os2sync_settings
from os2sync_export.os2sync import get_hierarchy
from os2sync_export.os2sync import get_os2sync_session
from os2sync_export.os2sync import trigger_hierarchy
from os2sync_export.os2synccli import update_single_user


logger = logging.getLogger(__name__)


def doofus():
    # Stub returning test data while I wait for access to Ballerup DEV or TEST
    return [
        {
            "Uuid": "aae73669-8ed9-4a52-9c99-7fcec8131c1f",
            "UserId": "NN",
            "Name": "Name Nameson",
            "Email": "NN@nn.dk",
            "Telephone": "whatever",
            "Positions": [
                {"Uuid": "319b0db8-2a93-4a2c-9f84-b650187ba67b", "Name": "Ingen"},
                {"Uuid": "319b0db8-2a93-4a2c-9f84-b650187ba67b", "Name": "Ingen"},
            ],
        }
    ]


def get_hierarchy_users(settings, client):
    return doofus()  # TODO: remove!
    request_uuid = trigger_hierarchy(client, os2sync_api_url=settings.os2sync_api_url)
    existing_os2sync_org_units, existing_os2sync_users = get_hierarchy(
        client,
        os2sync_api_url=settings.os2sync_api_url,
        request_uuid=request_uuid,
    )
    return existing_os2sync_users


def get_user_uuids_to_fix(hierarchy_users: list[dict]) -> list[UUID]:
    return [
        UUID(item["Uuid"])
        for item in hierarchy_users
        if len(item.get("Positions", [])) > 0  # item must have more than one position
        and any(
            # If the same position UUID occurs more than once in the list "Positions",
            # this item counts as a "hit" that needs to be fixed.
            count > 1
            for count in Counter(map(itemgetter("Uuid"), item["Positions"])).values()
        )
    ]


def remove_from_fkorg(settings, client, user_uuids: list[UUID]) -> list[UUID]:
    return user_uuids  # TODO: remove!

    result = []

    for user_uuid in user_uuids:
        url = f"{settings.os2sync_api_url}/user/{user_uuid}"
        try:
            client.delete(url)
        except Exception:
            logger.exception("could not remove user %r (url=%r)", user_uuid, url)
        else:
            result.append(user_uuid)

    return result


def redo_export(settings, user_uuids: list[UUID], dry_run: bool = True):
    result = []

    for user_uuid in user_uuids:
        sts_users = update_single_user(user_uuid, settings, dry_run)
        result.append((user_uuid, sts_users))

    return result


def _fix_fkorg_duplicated_positions() -> None:
    settings = get_os2sync_settings()
    settings.start_logging_based_on_settings()
    client = get_os2sync_session()

    hierarchy_users = get_hierarchy_users(settings, client)
    user_uuids_to_fix = get_user_uuids_to_fix(hierarchy_users)

    removed_from_fkorg = remove_from_fkorg(settings, client, user_uuids_to_fix)
    # remove_from_os2sync(removed_from_fkorg)
    redo_export(settings, removed_from_fkorg)


@click.command()
# @click.option(
#     "--uuid",
#     type=click.UUID,
#     required=True,
#     help="UUID of the MO user to delete in FK org.",
# )
def fix_fkorg_duplicated_positions() -> None:
    _fix_fkorg_duplicated_positions()


if __name__ == "__main__":
    fix_fkorg_duplicated_positions()
