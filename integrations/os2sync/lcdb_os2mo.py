#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from typing import Optional

from more_itertools import flatten
from more_itertools import only
from sqlalchemy.orm import sessionmaker

from constants import AD_it_system
from exporters.sql_export.lc_for_jobs_db import get_engine  # noqa
from exporters.sql_export.sql_table_defs import Adresse
from exporters.sql_export.sql_table_defs import Bruger
from exporters.sql_export.sql_table_defs import Engagement
from exporters.sql_export.sql_table_defs import Enhed
from exporters.sql_export.sql_table_defs import ItForbindelse
from exporters.sql_export.sql_table_defs import ItSystem
from exporters.sql_export.sql_table_defs import KLE
from exporters.sql_export.sql_table_defs import Leder
from integrations.os2sync.config import get_os2sync_settings
from integrations.os2sync import config
from integrations.os2sync import os2mo
from integrations.os2sync.templates import Person
from integrations.os2sync.templates import User
from uuid import UUID

logger = logging.getLogger(config.loggername)


def get_session(engine):
    return sessionmaker(bind=engine, autoflush=False)()


def has_kle():
    # nothing in db if no kle
    return True


scope_to_scope = {
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


def try_get_ad_user_key(session, uuid: str) -> Optional[str]:
    ad_system_user_names = (
        session.query(ItForbindelse.brugernavn)
        .join(ItSystem, ItForbindelse.it_system_uuid == ItSystem.uuid)
        .filter(ItSystem.navn == AD_it_system, ItForbindelse.bruger_uuid == uuid)
        .all()
    )
    ad_system_user_names = list(flatten(ad_system_user_names))

    if len(ad_system_user_names) != 1:
        return
    return ad_system_user_names[0]


def to_mo_employee(employee):
    """Convert `Bruger` row `employee` to something which resembles a MO
    employee JSON response.

    This is done so we can pass a suitable template context to
    `os2sync.templates.Person` even when running with `OS2SYNC_USE_LC_DB=True`.
    """

    def or_none(val):
        return val or None

    def to_name(*parts):
        return or_none(" ".join(part for part in parts if part))

    return dict(
        # Name
        name=to_name(employee.fornavn, employee.efternavn),
        givenname=or_none(employee.fornavn),
        surname=or_none(employee.efternavn),
        # Nickname
        nickname=to_name(employee.kaldenavn_fornavn, employee.kaldenavn_efternavn),
        nickname_givenname=or_none(employee.kaldenavn_fornavn),
        nickname_surname=or_none(employee.kaldenavn_efternavn),
        # Other fields
        cpr_no=or_none(employee.cpr),
        user_key=or_none(employee.bvn),
        uuid=or_none(employee.uuid),
    )


def get_sts_user(session, uuid, allowed_unitids, settings):
    employee = session.query(Bruger).filter(Bruger.uuid == uuid).one()
    user = User(
        dict(
            uuid=uuid,
            candidate_user_id=try_get_ad_user_key(session, uuid),
            person=Person(to_mo_employee(employee), settings=settings),
        ),
        settings=settings,
    )

    sts_user = user.to_json()

    addresses = []
    for lc_address in session.query(Adresse).filter(Adresse.bruger_uuid == uuid).all():
        address = {
            "address_type": {
                "uuid": lc_address.adressetype_uuid,
                "scope": scope_to_scope[lc_address.adressetype_scope],
            },
            "name": lc_address.værdi,
            "value": lc_address.dar_uuid,
            "uuid": lc_address.uuid,  # not used currently
        }
        addresses.append(address)
    os2mo.addresses_to_user(sts_user, addresses, phone_scope_classes=settings.os2sync_phone_scope_classes, email_scope_classes=settings.os2sync_email_scope_classes)

    engagements = []
    for lc_engagement in (
        session.query(Engagement).filter(Engagement.bruger_uuid == uuid).all()
    ):
        engagements.append(
            {
                "uuid": lc_engagement.uuid,
                "org_unit": {"uuid": lc_engagement.enhed_uuid},
                "job_function": {"name": lc_engagement.stillingsbetegnelse_titel},
                "is_primary": lc_engagement.primær_boolean,
            }
        )

    os2mo.engagements_to_user(sts_user, engagements, allowed_unitids)

    return sts_user


top_per_unit = {}


def get_top_unit(session, lc_enhed):
    """
    return the top unit for a unit
    """
    top_unit = top_per_unit.get(lc_enhed.uuid)
    if top_unit:
        return top_unit
    branch = [lc_enhed.uuid]

    # walk as far up as necessary
    while lc_enhed.forældreenhed_uuid is not None:
        uuid = lc_enhed.forældreenhed_uuid
        top_unit = top_per_unit.get(uuid)
        if top_unit:
            break
        branch.append(uuid)
        lc_enhed = session.query(Enhed).filter(Enhed.uuid == uuid).one()
        top_unit = uuid  # last one effective

    # register top unit for all encountered
    for buuid in branch:
        top_per_unit[buuid] = top_unit
    return top_unit


def is_ignored(unit, settings):
    """Determine if unit should be left out of transfer

    Example:
        >>> from unittest.mock import Mock
        >>> unit=Mock(enhedsniveau_uuid="1", enhedstype_uuid="2")
        >>> settings={
        ... "ignored_unit_levels": ["10","2"],
        ... "ignored_unit_types":['6','7']}
        >>> is_ignored(unit, settings)
        False
        >>> unit.enhedstype_uuid="6"
        >>> is_ignored(unit, settings)
        True
        >>> unit.enhedstype_uuid="2"
        >>> is_ignored(unit, settings)
        False
        >>> unit.enhedsniveau_uuid="2"
        >>> is_ignored(unit, settings)
        True

    Args:
        unit: The organization unit to enrich with kle information.
        settings: a dictionary

    Returns:
        Boolean
    """

    return (
        unit.enhedstype_uuid in settings.os2sync_ignored_unit_types or
        unit.enhedsniveau_uuid in settings.os2sync_ignored_unit_levels)


def get_sts_orgunit(session, uuid, settings):
    base = session.query(Enhed).filter(Enhed.uuid == uuid).one()

    if is_ignored(base, settings):
        logger.info(
            "Ignoring %s (%s, %s)",
            base.uuid,
            base.enhedsniveau_titel,
            base.enhedstype_titel,
        )
        return None

    top_unit = get_top_unit(session, base)
    if not top_unit or (UUID(top_unit) != settings.os2sync_top_unit_uuid):
        logger.debug(f"ignoring unit {uuid=}, as it is not a unit below {settings.os2sync_top_unit_uuid=}")
        return None

    sts_org_unit = {"ItSystemUuids": [], "Name": base.navn, "Uuid": uuid}

    if base.forældreenhed_uuid is not None:
        sts_org_unit["ParentOrgUnitUuid"] = base.forældreenhed_uuid

    itconnections = (
        session.query(ItForbindelse).filter(ItForbindelse.enhed_uuid == uuid).all()
    )
    os2mo.itsystems_to_orgunit(
        sts_org_unit,
        [{"itsystem": {"uuid": itf.it_system_uuid}} for itf in itconnections],
    )

    addresses = []
    for lc_address in session.query(Adresse).filter(Adresse.enhed_uuid == uuid).all():
        address = {
            "address_type": {
                "uuid": lc_address.adressetype_uuid,
                "user_key": lc_address.adressetype_bvn,
                "scope": scope_to_scope[lc_address.adressetype_scope],
            },
            "name": lc_address.værdi,
            "value": lc_address.dar_uuid,
            "uuid": lc_address.uuid,  # not used currently
        }
        addresses.append(address)
    os2mo.addresses_to_orgunit(sts_org_unit, addresses)

    if settings.os2sync_sync_managers:
        lc_manager = session.query(Leder).filter(Leder.enhed_uuid == uuid).all()
        manager_uuid = only(lc_manager.bruger_uuid)
        sts_org_unit.update({'managerUuid': manager_uuid})

    mokles = {}
    lc_kles = session.query(KLE).filter(KLE.enhed_uuid == uuid).all()
    for lc_kle in lc_kles:
        mokles[lc_kle.uuid] = {
            "kle_number": {"uuid": lc_kle.kle_nummer_uuid},
        }
    os2mo.kle_to_orgunit(sts_org_unit, mokles.values(), use_contact_for_tasks=settings.os2sync_use_contact_for_tasks)
    truncate_length = max(36, settings.os2sync_truncate_length)

    os2mo.strip_truncate_and_warn(sts_org_unit, sts_org_unit, length=truncate_length)

    return sts_org_unit
