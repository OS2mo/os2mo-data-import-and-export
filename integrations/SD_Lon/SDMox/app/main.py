#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Triggerkoden i dette modul har to funktioner:
# 1) At oprette/rette/flytte en afdeling i SD, inden det sker i OS2mo
# 2) At forhinde oprettelse/flytning/rettelse i OS2mo, hvis det ikke
# lykkedes i SD
#
# Adressernes rækkefølge har betydning.
#     Der skal findes en postadresse inden man opretter et Pnummer,
#     ellers går tilbagemeldingen fra SD tilsyneladende i ged.
#     Der er indført et check for det i sd_mox.py

from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

import requests
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import RedirectResponse
from os2mo_helpers.mora_helpers import MoraHelper
from pydantic import BaseModel, BaseSettings, Field

tags_metadata = [
    {
        "name": "Meta",
        "description": "Various meta endpoints",
    },
    {
        "name": "API",
        "description": "Direct API for end-users.",
    },
    {
        "name": "Trigger API",
        "description": "Trigger API for mo-triggers.",
        "externalDocs": {
            "description": "OS2MO Trigger docs",
            "url": "https://os2mo.readthedocs.io/en/development/api/triggers.html",
        },
    },
]
app = FastAPI(openapi_tags=tags_metadata)

from config import get_settings
from sd_mox import sdMox

# Called for side-effect
get_settings()


def get_mora_helper():
    return MoraHelper(hostname=get_settings().mora_url, use_cache=False)


def should_mox_run(mo_ou):
    """Determine whether sdmox should trigger code for this organizational unit.

    This is determined by whether the UUID of the unit is in the
    triggered_uuids settings variable.
    """
    # TODO: Consider a parent iterator
    while mo_ou and mo_ou["uuid"]:
        if UUID(mo_ou["uuid"]) in get_settings().triggered_uuids:
            return True
        mo_ou = mo_ou["parent"]
    return False


class Validity(BaseModel):
    time_from: datetime = Field(None, alias="from")
    time_to: datetime = Field(None, alias="to")


class OUParent(BaseModel):
    uuid: UUID


class OUData(BaseModel):
    validity: Validity
    name: Optional[str] = None
    parent: Optional[OUParent] = None


class OUObject(BaseModel):
    data: OUData


class EventType(int, Enum):
    ON_BEFORE, ON_AFTER = range(2)


class RequestType(int, Enum):
    CREATE, EDIT, TERMINATE = range(3)


class MOTriggerPayload(BaseModel):
    """MO trigger payload.

    See: https://os2mo.readthedocs.io/en/development/api/triggers.html#the-trigger-function for details.
    """

    event_type: EventType
    request: OUObject
    request_type: RequestType
    role_type: str
    uuid: UUID


class DetailError(BaseModel):
    """Default Error model."""

    class Config:
        schema_extra = {
            "example": {
                "detail": "string",
            }
        }

    detail: str


def _verify_ou_ok(uuid: UUID, at: datetime, mora_helper):
    try:
        # TODO: AIOHTTP MoraHelpers?
        mo_ou = mora_helper.read_ou(uuid, at=at)
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not establish a connection to MO",
        )

    if "error_key" in mo_ou and mo_ou["error_key"] == "E_ORG_UNIT_NOT_FOUND":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The requested organizational unit was not found in MO",
        )

    if not should_mox_run(mo_ou):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The requested organizational unit is outside the configured allow list",
        )


_verify_ou_ok.responses = {
    status.HTTP_502_BAD_GATEWAY: {
        "model": DetailError,
        "description": "Bad Gateway: Returned when unable to establish a connection to MO.",
    },
    status.HTTP_404_NOT_FOUND: {
        "model": DetailError,
        "description": "Not Found: Returned when the requested organizational unit cannot be found in MO.",
    },
    status.HTTP_403_FORBIDDEN: {
        "model": DetailError,
        "description": "Forbidden: Returned when the requested organizational unit is outside the configured allow list.",
    },
}


def verify_ou_ok(uuid: UUID, at: datetime, mora_helper=Depends(get_mora_helper)):
    _verify_ou_ok(uuid, at, mora_helper)


verify_ou_ok.responses = _verify_ou_ok.responses


def verify_ou_ok_trigger(
    payload: MOTriggerPayload, mora_helper=Depends(get_mora_helper)
):
    uuid = payload.uuid
    at = payload.request.data.validity.time_from
    _verify_ou_ok(uuid, at, mora_helper)


verify_ou_ok_trigger.responses = _verify_ou_ok.responses


def _ou_edit_name(ou_uuid: UUID, new_name: str, at: datetime):
    if new_name is None:
        raise ValueError("NO")

    mox = sdMox.create(from_date=at)
    mox.amqp_connect()
    print("Changing name")
    mox.rename_unit(ou_uuid, new_name, at=from_date, dry_run=True)


def _ou_edit_parent(ou_uuid: UUID, new_parent: UUID, at: datetime):
    if new_parent is None:
        raise ValueError("NO")

    print("Changing parent")
    mox = sdMox.create(from_date=at)
    mox.amqp_connect()
    mox.move_unit(ou_uuid, new_parent, at=from_date, dry_run=True)


@app.get("/", response_class=RedirectResponse, tags=["Meta"])
async def root() -> RedirectResponse:
    """Redirect to /docs."""
    return RedirectResponse(url="/docs")


@app.patch(
    "/ou/{uuid}/edit/name",
    responses=verify_ou_ok.responses,
    dependencies=[Depends(verify_ou_ok)],
    tags=["API"],
)
def ou_edit_name(uuid: UUID, new_name: str, at: datetime):
    """Rename an organizational unit."""
    _ou_edit_name(uuid, new_name, at)


@app.patch(
    "/ou/{uuid}/edit/parent",
    responses=verify_ou_ok.responses,
    dependencies=[Depends(verify_ou_ok)],
    tags=["API"],
)
def ou_edit_parent(uuid: UUID, new_parent: UUID, at: datetime):
    """Move an organizational unit."""
    _ou_edit_parent(uuid, new_parent, at)


@app.get(
    "/triggers",
    tags=["Trigger API"],
)
def triggers():
    """List triggers to be registered."""
    return {
        "ORG_UNIT": {
            "CREATE": {
                "ON_BEFORE": [
                    "/triggers/ou/create",
                ]
            },
            "EDIT": {
                "ON_BEFORE": [
                    "/triggers/ou/edit",
                ]
            },
        },
        "ADDRESS": {
            "CREATE": {
                "ON_BEFORE": [
                    "/triggers/address/create",
                ]
            },
            "EDIT": {
                "ON_BEFORE": [
                    "/triggers/address/edit",
                ]
            },
        },
    }


@app.post("/triggers/ou/create", tags=["Trigger API"])
def ou_create():
#    """An ou is about to be created"""
#    mora_org = sdmox_config.get("ORG_UUID")
#    if mora_org is None:
#        mora_org = sdmox_config.setdefault(
#            "ORG_UUID", mo_request("o").json()[0]["uuid"]
#        )
#
#    if (
#        # we will never create at top level
#        not data["request"]["parent"]
#        or mora_org == data["request"]["parent"]["uuid"]
#    ):
#        return
#
#    parent = mo_request("ou/" + data["request"]["parent"]["uuid"]).json()
#
#    if not is_sd_triggered(parent):
#        return
#
#    # try to create a unit in sd
#    from_date = datetime.datetime.strptime(
#        data["request"]["validity"]["from"], "%Y-%m-%d"
#    )
#    mox = sd_mox.sdMox.create(from_date=from_date)
#
#    payload = mox.payload_create(data["uuid"], data["request"], parent)
#    mox.create_unit(test_run=False, **payload)
#
#    if not data["request"].get("details", []):
#        # check unit here
#        mox.check_unit(operation="import", **payload)
#    else:
#        # check unit after editing addresses
#        address_before_create(data, unit_given=True)
    return {"ou": "create"}


@app.patch(
    "/triggers/ou/edit",
    responses=verify_ou_ok_trigger.responses,
    dependencies=[Depends(verify_ou_ok_trigger)],
    tags=["Trigger API"],
)
def trggers_ou_edit(payload: MOTriggerPayload):
    """Rename or move an organizational unit from a MO Trigger payload."""
    uuid = payload.uuid
    at = payload.request.data.validity.time_from

    new_name = payload.request.data.name
    if new_name:
        _ou_edit_name(uuid, new_name, at)

    new_parent_obj = payload.request.data.parent
    if new_parent_obj:
        new_parent_uuid = new_parent_obj.uuid
        _ou_edit_parent(uuid, new_parent_uuid, at)


@app.post("/triggers/address/create", tags=["Trigger API"])
def address_create():
#    """Addresses are about to be created
#    if unit is also new, it is given as a whole
#    """
#
#    # whole department changes validity - including addresses
#    from_date = data["request"]["validity"]["from"]
#
#    if unit_given:
#
#        # a new unit has been created
#        ou = data["uuid"]
#        unit = data["request"]
#        addresses = unit["details"]
#
#    else:
#
#        # a new address is being added to an existing unit
#        ou = data.get("org_unit_uuid")
#        if not ou:
#            return
#        try:
#            unit = mo_request("ou/" + ou, at=from_date).json()
#            if not is_sd_triggered(unit):
#                return
#        except requests.exceptions.HTTPError as e:
#            if e.response.status_code == 404:
#                return  # new unit - checked elsewhere
#            raise
#
#        previous_addresses = mo_request(
#            "ou/" + ou + "/details/address", at=from_date
#        ).json()
#
#        # the new address is prepended to addresses and
#        # thereby given higher priority in sd_mox.py
#        # see 'grouped_addresses'
#        addresses = [data["request"]] + previous_addresses
#
#    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
#    mox = sd_mox.sdMox.create(from_date=from_date)
#
#    payload = mox.payload_edit(ou, unit, addresses)
#    mox.edit_unit(test_run=False, **payload)
#    mox.check_unit(operation="ret", **payload)
    return {"address": "create"}


@app.patch("/triggers/address/edit", tags=["Trigger API"])
def address_edit(uuid: UUID):
#    """An address is about to be changed"""
#    ou = data.get("org_unit_uuid")
#    if not ou:
#        return
#    from_date = data["request"]["data"]["validity"]["from"]
#    unit = mo_request("ou/" + ou, at=from_date).json()
#    if not is_sd_triggered(unit):
#        return
#
#    # the changed address is prepended to addresses and
#    # thereby given higher priority in sd_mox.py
#    # see 'grouped_addresses'
#    addresses = [data["request"]["data"]] + mo_request(
#        "ou/" + ou + "/details/address", at=from_date
#    ).json()
#    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
#    mox = sd_mox.sdMox.create(from_date=from_date)
#
#    # doing a read department here will give the non-unique error
#    # here - where we still have access to the mo-error reporting
#    code_errors = mox._validate_unit_code(unit["user_key"], can_exist=True)
#    if code_errors:
#        raise sd_mox.SdMoxError(", ".join(code_errors))
#
#    payload = mox.payload_edit(ou, unit, addresses)
#    mox.edit_unit(test_run=False, **payload)
#    mox.check_unit(operation="ret", **payload)
    return {"address": "edit"}
