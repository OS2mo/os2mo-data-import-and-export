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

from datetime import date
from functools import partial
from typing import Dict, List, Optional
from uuid import UUID

import requests
from config import get_settings
from fastapi import Depends, FastAPI, HTTPException, Path, Query, status
from fastapi.responses import RedirectResponse
from os2mo_helpers.mora_helpers import MoraHelper
from os2mo_http_trigger_protocol import (EventType, MOTriggerPayload,
                                         MOTriggerRegister, RequestType)
from pydantic import BaseModel, BaseSettings
from sd_mox import SDMox
from util import first_of_month, get_mora_helper

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
app = FastAPI(
    title="SDMox",
    description="API to make changes in SD.",
    version="0.0.1",
    openapi_tags=tags_metadata,
)
# Called for side-effect
get_settings()


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


class DetailError(BaseModel):
    """Default Error model."""

    class Config:
        schema_extra = {
            "example": {
                "detail": "string explaining the error",
            }
        }

    detail: str


def get_date(
    date: Optional[date] = Query(
        None,
        description=(
            "Effective start date for change."
            + "<br/>"
            + "Must be the first day of a month."
            + "<br/>"
            "If omitted it will default to the first of the current month."
        ),
    )
):
    if date:
        return date
    return first_of_month()


def _verify_ou_ok(uuid: UUID, at: date, mora_helper):
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
        "description": (
            "Bad Gateway Error"
            + "<br/><br/>"
            + "Returned when unable to establish a connection to MO."
        ),
    },
    status.HTTP_404_NOT_FOUND: {
        "model": DetailError,
        "description": (
            "Not Found Error"
            + "<br/><br/>"
            + "Returned when the requested organizational unit cannot be found in MO."
        ),
    },
    status.HTTP_403_FORBIDDEN: {
        "model": DetailError,
        "description": (
            "Forbidden Error"
            + "<br/><br/>"
            + "Returned when the requested organizational unit is outside the configured allow list."
        ),
    },
}


def verify_ou_ok(
    uuid: UUID,
    at: date = Depends(get_date),
    mora_helper=Depends(partial(get_mora_helper, None)),
):
    _verify_ou_ok(uuid, at, mora_helper)


verify_ou_ok.responses = _verify_ou_ok.responses


def verify_ou_ok_trigger(
    payload: MOTriggerPayload, mora_helper=Depends(partial(get_mora_helper, None))
):
    uuid = payload.uuid
    data = payload.request.data

    at = data["validity"]["from"]
    _verify_ou_ok(uuid, at, mora_helper)


verify_ou_ok_trigger.responses = _verify_ou_ok.responses


async def _ou_edit_name(ou_uuid: UUID, new_name: str, at: date):
    if new_name is None:
        raise ValueError("NO")

    print("Changing name")
    mox = SDMox(from_date=at)
    mox.amqp_connect()
    await mox.rename_unit(ou_uuid, new_name, at=at, dry_run=True)


async def _ou_edit_parent(ou_uuid: UUID, new_parent: UUID, at: date):
    if new_parent is None:
        raise ValueError("NO")

    print("Changing parent")
    mox = SDMox(from_date=at)
    mox.amqp_connect()
    await mox.move_unit(ou_uuid, new_parent, at=at, dry_run=True)


@app.get(
    "/", response_class=RedirectResponse, tags=["Meta"], summary="Redirect to /docs"
)
async def root() -> RedirectResponse:
    """Redirect to /docs."""
    return RedirectResponse(url="/docs")


@app.patch(
    "/ou/{uuid}/edit/name",
    responses=verify_ou_ok.responses,
    dependencies=[Depends(verify_ou_ok)],
    tags=["API"],
    summary="Rename an organizational unit.",
)
async def ou_edit_name(
    uuid: UUID = Path(..., description="UUID of the organizational unit to rename."),
    new_name: str = Query(..., description="The name we wish to change to."),
    at: date = Depends(get_date),
):
    """Rename an organizational unit."""
    # TODO: Document using Query() instead?
    # See: https://github.com/tiangolo/fastapi/issues/1007
    await _ou_edit_name(uuid, new_name, at)
    return {"status": "OK"}


@app.patch(
    "/ou/{uuid}/edit/parent",
    responses=verify_ou_ok.responses,
    dependencies=[Depends(verify_ou_ok)],
    tags=["API"],
    summary="Move an organizational unit.",
)
async def ou_edit_parent(
    uuid: UUID = Path(..., description="UUID of the organizational unit to move."),
    new_parent: UUID = Query(..., description="The parent unit we wish to move under."),
    at: date = Depends(get_date),
):
    """Move an organizational unit."""
    await _ou_edit_parent(uuid, new_parent, at)
    return {"status": "OK"}


@app.get(
    "/triggers",
    tags=["Trigger API"],
    summary="List triggers to be registered.",
    response_model=List[MOTriggerRegister],
    response_description=(
        "Successful Response" + "<br/>" + "List of triggers to register."
    ),
)
def triggers():
    """List triggers to be registered."""
    return [
        {
            "event_type": EventType.ON_BEFORE,
            "request_type": RequestType.CREATE,
            "role_type": "org_unit",
            "url": "/triggers/ou/create",
        },
        {
            "event_type": EventType.ON_BEFORE,
            "request_type": RequestType.EDIT,
            "role_type": "org_unit",
            "url": "/triggers/ou/edit",
        },
        {
            "event_type": EventType.ON_BEFORE,
            "request_type": RequestType.CREATE,
            "role_type": "address",
            "url": "/triggers/address/create",
        },
        {
            "event_type": EventType.ON_BEFORE,
            "request_type": RequestType.EDIT,
            "role_type": "address",
            "url": "/triggers/address/edit",
        },
    ]


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


@app.post(
    "/triggers/ou/edit",
    responses=verify_ou_ok_trigger.responses,
    dependencies=[Depends(verify_ou_ok_trigger)],
    tags=["Trigger API"],
    summary="Rename or move an organizational unit.",
)
async def trggers_ou_edit(payload: MOTriggerPayload):
    """Rename or move an organizational unit."""
    uuid = payload.uuid
    data = payload.request.data

    at = data["validity"]["from"]

    new_name = data["name"]
    if new_name:
        await _ou_edit_name(uuid, new_name, at)

    new_parent_obj = data["parent"]
    if new_parent_obj:
        new_parent_uuid = new_parent_obj.uuid
        await _ou_edit_parent(uuid, new_parent_uuid, at)
    return {"status": "OK"}


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


@app.post("/triggers/address/edit", tags=["Trigger API"])
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
