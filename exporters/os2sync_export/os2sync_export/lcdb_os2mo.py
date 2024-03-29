#
# Copyright (c) 2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from more_itertools import flatten
from more_itertools import only
from os2sync_export import os2mo
from os2sync_export.config import Settings
from os2sync_export.os2sync_models import OrgUnit
from os2sync_export.templates import Person
from os2sync_export.templates import User
from sqlalchemy.orm import sessionmaker

from exporters.sql_export.lc_for_jobs_db import get_engine  # noqa
from exporters.sql_export.sql_table_defs import WAdresse as Adresse
from exporters.sql_export.sql_table_defs import WBruger as Bruger
from exporters.sql_export.sql_table_defs import WEngagement as Engagement
from exporters.sql_export.sql_table_defs import WEnhed as Enhed
from exporters.sql_export.sql_table_defs import WItForbindelse as ItForbindelse
from exporters.sql_export.sql_table_defs import WItSystem as ItSystem
from exporters.sql_export.sql_table_defs import WKLE as KLE
from exporters.sql_export.sql_table_defs import WLeder as Leder

logger = logging.getLogger(__name__)


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
    "Url": "URL",
}


def try_get_it_user_key(
    session, uuid: str, user_key_it_system_name: str
) -> Optional[str]:
    it_system_user_names = (
        session.query(ItForbindelse.brugernavn)
        .join(ItSystem, ItForbindelse.it_system_uuid == ItSystem.uuid)
        .filter(
            ItSystem.navn == user_key_it_system_name, ItForbindelse.bruger_uuid == uuid
        )
        .all()
    )
    it_system_user_names = list(flatten(it_system_user_names))

    if len(it_system_user_names) != 1:
        return None
    return it_system_user_names[0]


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


def lookup_unit_it_connections(session, uuid):
    it_connections = (
        session.query(ItForbindelse).filter(ItForbindelse.enhed_uuid == uuid).all()
    )
    return [
        {"itsystem": {"uuid": itf.it_system_uuid, "name": itf.it_system_name}}
        for itf in it_connections
    ]


def lookup_user_it_connections(session, uuid):
    it_connections = (
        session.query(ItForbindelse).filter(ItForbindelse.bruger_uuid == uuid).all()
    )
    return [
        {"itsystem": {"uuid": itf.it_system_uuid, "name": itf.it_system_name}}
        for itf in it_connections
    ]


def overwrite_user_uuids(session, sts_user: Dict, os2sync_uuid_from_it_systems: List):
    uuid = sts_user["Uuid"]
    it = lookup_user_it_connections(session, uuid)
    sts_user["Uuid"] = os2mo.get_fk_org_uuid(it, uuid, os2sync_uuid_from_it_systems)
    for p in sts_user["Positions"]:
        unit_uuid = p["OrgUnitUuid"]
        it = lookup_unit_it_connections(session, unit_uuid)
        p["OrgUnitUuid"] = os2mo.get_fk_org_uuid(
            it, unit_uuid, os2sync_uuid_from_it_systems
        )


def get_sts_user_raw(
    session,
    uuid,
    settings: Settings,
    fk_org_uuid=None,
    user_key=None,
    engagement_uuid=None,
):
    employee = session.query(Bruger).filter(Bruger.uuid == uuid).one()
    if user_key is None:
        user_key = try_get_it_user_key(
            session,
            uuid,
            user_key_it_system_name=settings.os2sync_user_key_it_system_name,
        )
    user = User(
        dict(
            uuid=uuid,
            candidate_user_id=user_key,
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
    os2mo.addresses_to_user(
        sts_user,
        addresses,
        phone_scope_classes=settings.os2sync_phone_scope_classes,
        landline_scope_classes=settings.os2sync_landline_scope_classes,
        email_scope_classes=settings.os2sync_email_scope_classes,
    )

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

    allowed_unitids = os2mo.org_unit_uuids(
        root=settings.os2sync_top_unit_uuid,
        hierarchy_uuids=os2mo.get_org_unit_hierarchy(
            settings.os2sync_filter_hierarchy_names
        ),
    )
    os2mo.engagements_to_user(sts_user, engagements, allowed_unitids)
    if not sts_user["Positions"]:
        return None
    if settings.os2sync_uuid_from_it_systems:
        overwrite_user_uuids(session, sts_user, settings.os2sync_uuid_from_it_systems)

    return sts_user


def is_ignored(unit, settings: Settings):
    """Determine if unit should be left out of transfer

    Args:
        unit: The organization unit to enrich with kle information.
        settings: a dictionary

    Returns:
        Boolean
    """
    unittype_uuid = UUID(unit.enhedstype_uuid) if unit.enhedstype_uuid else None
    unitlevel_uuid = UUID(unit.enhedsniveau_uuid) if unit.enhedsniveau_uuid else None
    return (
        unittype_uuid in settings.os2sync_ignored_unit_types
        or unitlevel_uuid in settings.os2sync_ignored_unit_levels
    )


def overwrite_unit_uuids(
    session, sts_org_unit: Dict, os2sync_uuid_from_it_systems: List
):
    # Overwrite UUIDs with values from it-account
    uuid = sts_org_unit["Uuid"]
    it = lookup_unit_it_connections(session, uuid)

    sts_org_unit["Uuid"] = os2mo.get_fk_org_uuid(it, uuid, os2sync_uuid_from_it_systems)
    # Also check if parent unit has a UUID from an it-account
    parent_uuid = sts_org_unit.get("ParentOrgUnitUuid")
    if parent_uuid:
        it = lookup_unit_it_connections(session, parent_uuid)

        sts_org_unit["ParentOrgUnitUuid"] = os2mo.get_fk_org_uuid(
            it, parent_uuid, os2sync_uuid_from_it_systems
        )


def get_sts_orgunit(session, uuid, settings: Settings) -> Optional[OrgUnit]:
    base = session.query(Enhed).filter(Enhed.uuid == uuid).one()

    if is_ignored(base, settings):
        logger.info(
            "Ignoring %s (%s, %s)",
            base.uuid,
            base.enhedsniveau_titel,
            base.enhedstype_titel,
        )
        return None

    sts_org_unit = {"ItSystems": [], "Name": base.navn, "Uuid": uuid}

    # TODO: check that only one parent_uuid is None
    sts_org_unit["ParentOrgUnitUuid"] = base.forældreenhed_uuid

    itconnections = (
        session.query(ItForbindelse).filter(ItForbindelse.enhed_uuid == uuid).all()
    )
    os2mo.itsystems_to_orgunit(
        sts_org_unit,
        [
            {"itsystem": {"uuid": itf.it_system_uuid, "name": itf.it_system_name}}
            for itf in itconnections
        ],
        uuid_from_it_systems=settings.os2sync_uuid_from_it_systems,
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
        manager = only(lc_manager)
        if manager:
            sts_org_unit.update({"ManagerUuid": manager.bruger_uuid})

    if settings.os2sync_enable_kle:
        mokles = {}
        lc_kles = session.query(KLE).filter(KLE.enhed_uuid == uuid).all()
        for lc_kle in lc_kles:
            mokles[lc_kle.uuid] = {
                "kle_number": {"uuid": lc_kle.kle_nummer_uuid},
            }
        os2mo.kle_to_orgunit(
            sts_org_unit,
            list(mokles.values()),
            use_contact_for_tasks=settings.os2sync_use_contact_for_tasks,
        )

    if settings.os2sync_uuid_from_it_systems:
        overwrite_unit_uuids(
            session, sts_org_unit, settings.os2sync_uuid_from_it_systems
        )

    truncate_length = max(36, settings.os2sync_truncate_length)

    os2mo.strip_truncate_and_warn(sts_org_unit, sts_org_unit, length=truncate_length)

    return OrgUnit(**sts_org_unit)
