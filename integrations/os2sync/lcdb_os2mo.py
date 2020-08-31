#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import requests
import logging
from integrations.os2sync import config
from integrations.os2sync import os2mo
from exporters.sql_export.lc_for_jobs_db import get_engine
from exporters.sql_export.sql_table_defs import (
    Adresse,
    Bruger,
    Engagement,
    Enhed,
    KLE,
    ItForbindelse,
)
from sqlalchemy import or_, and_
from sqlalchemy.orm import sessionmaker


settings = config.settings
logger = logging.getLogger(config.loggername)

def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()

def has_kle():
    # nothing in db if no kle
    return True


scope_to_scope={
    "E-mail": "EMAIL",
    "Telefon": "PHONE",
    "DAR": "DAR",
    "EAN": "EAN",
    "P-nummer": "PNUMBER",
    "Text": "TEXT",
    "Ansvarlig": "ANSVARLIG",
    "Udførende": "UDFOERENDE",
    "Indsigt": "INDSIGT",
}

def get_sts_user(session, uuid, allowed_unitids):
    base = session.query(Bruger).filter(Bruger.uuid == uuid).one()

    sts_user = {
        "Uuid": uuid,
        "UserId": uuid,
        "Positions": [],
        "Person": {"Name": base.fornavn + " " + base.efternavn , "Cpr": base.cpr},
    }
    if not settings["OS2SYNC_XFER_CPR"]:
        sts_user["Person"]["Cpr"] = None

    addresses = []
    for lc_address in session.query(Adresse).filter(Adresse.bruger_uuid == uuid).all():
        address = {
            "address_type":{
                "uuid": lc_address.adressetype_uuid,
                "scope": scope_to_scope[lc_address.adressetype_scope]},
            "name": lc_address.værdi,
            "value": lc_address.dar_uuid,
            "uuid": lc_address.uuid # not used currently
        }
        addresses.append(address)
    os2mo.addresses_to_user(sts_user, addresses)

    engagements=[]
    for lc_engagement in session.query(Engagement
    ).filter(Engagement.bruger_uuid == uuid).all():
        engagements.append({
            "org_unit": {"uuid": lc_engagement.enhed_uuid},
            "job_function":{"name":lc_engagement.stillingsbetegnelse_titel}
        })

    os2mo.engagements_to_user(sts_user, engagements, allowed_unitids)

    return sts_user


top_per_unit = {}
def get_top_unit(session, lc_enhed):
    top_unit = top_per_unit.get(lc_enhed.uuid)
    if top_unit:
        return top_unit
    branch=[lc_enhed.uuid]

    # walk as far up as necessary
    while lc_enhed.forældreenhed_uuid is not None:
        uuid = lc_enhed.forældreenhed_uuid
        top_unit = top_per_unit.get(uuid)
        if top_unit:
            break
        branch.append(uuid)
        lc_enhed = session.query(Enhed).filter(Enhed.uuid == uuid).one()
        top_unit = uuid # last one effective

    # register top unit for all encountered
    for buuid in branch:
        top_per_unit[buuid] = top_unit
    return top_unit


def get_sts_orgunit(session, uuid):
    base = session.query(Enhed).filter(Enhed.uuid == uuid).one()

    top_unit = get_top_unit(session, base)

    if top_unit != settings["OS2MO_TOP_UNIT_UUID"]:
        # not part of right tree
        return None

    sts_org_unit = {"ItSystemUuids": [], "Name": base.navn, "Uuid": uuid}

    if base.forældreenhed_uuid is not None:
        sts_org_unit["ParentOrgUnitUuid"] = base.forældreenhed_uuid

    itconnections = session.query(ItForbindelse).filter(ItForbindelse.enhed_uuid == uuid).all()
    os2mo.itsystems_to_orgunit(
        sts_org_unit,
        [{"itsystem":{"uuid": itf.it_system_uuid}} for itf in itconnections]
    )

    addresses = []
    for lc_address in session.query(Adresse).filter(Adresse.enhed_uuid == uuid).all():
        address = {
            "address_type":{
                "uuid": lc_address.adressetype_uuid,
                "scope": scope_to_scope[lc_address.adressetype_scope]},
            "name": lc_address.værdi,
            "value": lc_address.dar_uuid,
            "uuid": lc_address.uuid # not used currently
        }
        addresses.append(address)
    os2mo.addresses_to_orgunit( sts_org_unit, addresses)

    mokles={}
    lc_kles = session.query(KLE).filter(KLE.enhed_uuid == uuid).all()
    for lc_kle in lc_kles:
        mokles.setdefault(lc_kle.uuid, {
            "uuid": lc_kle.uuid,
            "kle_aspect":[]
        })
        mokles[lc_kle.uuid]["kle_aspect"].append({"scope": scope_to_scope[lc_kle.kle_aspekt_titel]})
    os2mo.kle_to_orgunit(sts_org_unit, mokles.values())
    os2mo.strip_truncate_and_warn(sts_org_unit, sts_org_unit)

    return sts_org_unit

