import copy
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
from functools import partial
from integrations.dar_helper import dar_helper
from itertools import chain
from operator import itemgetter
from typing import Iterable
from typing import List
from typing import Optional

import pydantic
from aiohttp import ClientSession
from aiohttp import TCPConnector
from more_itertools import bucket
from more_itertools import flatten
from more_itertools import partition
from pydantic import Field
from pydantic import validator

import config
import payloads as mo_payloads
import util
import uuids
from util import generate_uuid
from util import parse_filenames
from util import read_csv


class OrgUnit(pydantic.BaseModel):
    OrgUUID: str
    BVN: str = Field(alias="BrugervendtNøgle")
    StartDato: date
    SlutDato: date
    MagID: str
    OrgEnhedsNavn: Optional[str]
    OrgEnhedsTypeUUID: Optional[str]
    ParentUUID: str
    PostAdresse: Optional[str]
    LOSID: str
    CVR: str
    EAN: str
    PNr: str = Field(alias="P-Nr")
    SENr: str = Field(alias="SE-Nr")
    IntDebitorNr: str = Field(alias="IntDebitor-Nr")
    LinjeOrg: bool = Field(alias="Med-i-LinjeOrg")

    @validator("StartDato", "SlutDato", pre=True)
    def validate_date(cls, v, values):
        formats = ["%d-%m-%Y", "%Y-%m-%d %H:%M:%S.%f"]
        for format in formats:
            try:
                parsed = datetime.strptime(v, format).date().isoformat()
                return parsed
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date '{v}'")


class OrgUnitImporter:
    def __init__(self):
        self.dar_cache = {}

    @staticmethod
    def _convert_validities(from_date: date, to_date: date):
        from_time, to_time = from_date.isoformat(), to_date.isoformat()
        return from_time, to_time if to_time != "9999-12-31" else None

    def generate_unit_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        parent_uuid = orgunit.ParentUUID
        if parent_uuid.lower() == orgunit.OrgUUID.lower():
            parent_uuid = uuids.ORG_UUID

        return mo_payloads.create_org_unit(
            uuid=orgunit.OrgUUID,
            user_key=orgunit.BVN,
            name=orgunit.OrgEnhedsNavn,
            parent_uuid=parent_uuid,
            org_unit_hierarchy=uuids.LINJE_ORG_HIERARCHY if orgunit.LinjeOrg else None,
            org_unit_type_uuid=orgunit.OrgEnhedsTypeUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_los_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "los")
        assert orgunit.LOSID
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.LOSID,
            address_type_uuid=uuids.UNIT_LOS,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_cvr_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "cvr")
        assert orgunit.CVR
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.CVR,
            address_type_uuid=uuids.UNIT_CVR,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_ean_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "ean")
        assert orgunit.EAN
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.EAN,
            address_type_uuid=uuids.UNIT_EAN,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_pnr_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "pnr")
        assert orgunit.PNr
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.PNr,
            address_type_uuid=uuids.UNIT_PNR,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_senr_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "senr")
        assert orgunit.SENr
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.SENr,
            address_type_uuid=uuids.UNIT_SENR,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_debitornr_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "debitornr")
        assert orgunit.IntDebitorNr
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.IntDebitorNr,
            address_type_uuid=uuids.UNIT_DEBITORNR,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_postaddr_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "postaddr")
        assert orgunit.PostAdresse
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=self.dar_cache[orgunit.PostAdresse],
            address_type_uuid=uuids.UNIT_POSTADDR,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_mag_id_payload(self, orgunit: OrgUnit):
        from_date, to_date = self._convert_validities(
            orgunit.StartDato, orgunit.SlutDato
        )
        obj_uuid = generate_uuid(orgunit.OrgUUID + "mag_id")
        assert orgunit.MagID
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.MagID,
            address_type_uuid=uuids.UNIT_MAG_ID,
            org_unit_uuid=orgunit.OrgUUID,
            from_date=from_date,
            to_date=to_date,
        )

    @staticmethod
    def consolidate_payloads(payload_list: List[dict]):
        """
        Given a list of sorted payloads, collapse identical payloads that are
        immediately consecutive, i.e. payloads with the same fields,
        where the end date of one corresponds to the start date of the next

        The function assumes that input does not contain overlapping timespans
        """
        if not payload_list:
            return []

        def payloads_identical(p1, p2):
            p1_copy, p2_copy = copy.deepcopy(p1), copy.deepcopy(p2)
            del p1_copy["validity"]
            del p2_copy["validity"]
            return p1_copy == p2_copy

        consolidated_payloads = []
        current_payload = payload_list[0]
        for payload in payload_list[1:]:
            # We add one day here, as dates do not overlap.
            # e.g. two consecutive chunks will have
            # an end date of 10/1/2020 and start date of 11/1/2020
            current_to_date = datetime.strptime(
                current_payload["validity"]["to"], "%Y-%m-%d"
            ) + timedelta(days=1)
            new_from_date = datetime.strptime(payload["validity"]["from"], "%Y-%m-%d")

            if (
                payloads_identical(current_payload, payload)
                and current_to_date == new_from_date
            ):
                current_payload["validity"]["to"] = payload["validity"]["to"]
            else:
                consolidated_payloads.append(current_payload)
                current_payload = payload
        consolidated_payloads.append(current_payload)
        return consolidated_payloads

    def write_failed_addresses(self, addresses: List[str], filename: str):
        """Write failed addresses to an external file"""
        settings = config.get_config()
        export_path = os.path.join(settings.queries_dir, f"failed_addr_{filename}")
        with open(export_path, "w") as f:
            print(f"Writing failed addresses to {export_path}")
            f.writelines("\n".join(addresses))

    async def handle_addresses(self, org_units, filename):
        addresses = map(lambda orgunit: orgunit.PostAdresse, org_units)
        addresses = set(filter(None, addresses))

        if len(addresses) == 0:
            return

        address_lookups = await dar_helper.dar_datavask_multiple(addresses)

        # Split into two lists where lookup succeeded and failed
        success, failure = partition(lambda x: x[1] is None, address_lookups)

        success = {x[0]: x[1] for x in success}
        print(f"{len(list(success))} addresses found")
        self.dar_cache.update(success)

        failed_addresses = set(map(itemgetter(0), failure))
        print(f"{len(failed_addresses)} addresses could not be found")
        if len(failed_addresses) > 0:
            self.write_failed_addresses(failed_addresses, filename)

    def create_unit_payloads(self, org_units) -> Iterable[dict]:
        return map(self.generate_unit_payload, org_units)

    async def create_detail_payloads(self, org_units) -> Iterable[dict]:
        """
        Generate all relevant detail payloads based on the org unit objects
        All rows are run through different sets of generators and filters
        """

        generators = [
            (self.generate_los_payload, lambda orgunit: orgunit.LOSID),
            (self.generate_cvr_payload, lambda orgunit: orgunit.CVR),
            (self.generate_ean_payload, lambda orgunit: orgunit.EAN),
            (self.generate_pnr_payload, lambda orgunit: orgunit.PNr),
            (self.generate_senr_payload, lambda orgunit: orgunit.SENr),
            (self.generate_debitornr_payload, lambda orgunit: orgunit.IntDebitorNr),
            (self.generate_mag_id_payload, lambda orgunit: orgunit.MagID),
            (
                self.generate_postaddr_payload,
                lambda orgunit: orgunit.PostAdresse
                and orgunit.PostAdresse in self.dar_cache,
            ),
        ]
        payloads = []
        for generator, filter_fn in generators:
            payloads.append(list(map(generator, filter(filter_fn, org_units))))
        return flatten(payloads)

    async def handle_initial(self, filename):
        """
        Handles reading the special 'initial' file

        The file contains org unit data, as well as data on the associated details

        The initial org unit file contains historic data, so a minimal set of
        create/edit payloads are created accordingly
        """
        org_units = read_csv(filename, OrgUnit)

        await self.handle_addresses(org_units, filename)

        unit_payloads = self.create_unit_payloads(org_units)
        detail_payloads = await self.create_detail_payloads(org_units)
        payloads = list(unit_payloads) + list(detail_payloads)

        # Bucket all payloads referring to the same object
        uuid_buckets = bucket(payloads, key=lambda payload: payload["uuid"])
        sorted_buckets = map(
            lambda uuid_key: sorted(
                uuid_buckets[uuid_key], key=lambda x: x["validity"]["from"]
            ),
            uuid_buckets,
        )
        consolidated_buckets = list(map(self.consolidate_payloads, sorted_buckets))

        heads = [payload[0] for payload in consolidated_buckets]
        tails = list(flatten([payload[1:] for payload in consolidated_buckets]))

        edit_payloads = map(mo_payloads.convert_create_to_edit, tails)

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.create_details(session, heads)
            await util.edit_details(session, edit_payloads)

    async def handle_create(self, filename):
        """
        Handle creating new org units and details
        We are guaranteed to only have one row per org unit
        """
        org_units = read_csv(filename, OrgUnit)

        await self.handle_addresses(org_units, filename)

        org_unit_payloads = self.create_unit_payloads(org_units)
        detail_payloads = await self.create_detail_payloads(org_units)

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.create_details(
                session, chain(org_unit_payloads, detail_payloads)
            )

    async def handle_edit(self, filename: str, filedate: datetime):
        """
        Handle changes to existing org units and details
        We are guaranteed to only have one row per org unit

        New details on an existing org unit will show up in this file, rather than the
        'nye' file. So we have to potentially perform inserts of new data.

        As a row contains information about the org unit as well as its details,
        we do not know what has been changed. However, all information is managed
        by the external system so we can safely reimport the "same" data, as opposed to
        trying to compare the existing objects in OS2mo
        """
        org_units = read_csv(filename, OrgUnit)
        org_unit_payloads = self.create_unit_payloads(org_units)
        detail_payloads = await self.create_detail_payloads(org_units)

        orgfunk_uuids = set(await util.lookup_organisationfunktion())
        detail_creates, detail_edits = partition(
            lambda payload: payload["uuid"] in orgfunk_uuids, detail_payloads
        )
        converter = partial(
            mo_payloads.convert_create_to_edit, from_date=filedate.date().isoformat()
        )
        edits = map(converter, chain(org_unit_payloads, detail_edits))

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.create_details(session, detail_creates)
            await util.edit_details(session, edits)

    async def run(self, last_import: datetime):
        """
        Reads org unit files newer than last_import
        and performs inserts/updates as needed
        """
        print("Starting org unit import")
        ftp = util.get_ftp_connector()
        filenames = ftp.nlst()

        initials = parse_filenames(
            filenames, prefix="Org_inital", last_import=last_import
        )
        creates = parse_filenames(filenames, prefix="Org_nye", last_import=last_import)
        edits = parse_filenames(filenames, prefix="Org_ret", last_import=last_import)

        for filename, _ in initials:
            await self.handle_initial(filename)

        for filename, _ in creates:
            await self.handle_create(filename)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        print("Org unit import done")
