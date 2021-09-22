import ftplib
import uuid
from csv import DictWriter
from datetime import date
from datetime import datetime
from datetime import timedelta
from functools import partial
from io import StringIO
from itertools import chain
from itertools import starmap
from itertools import zip_longest
from operator import attrgetter
from operator import itemgetter
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional

import los_files
import payloads as mo_payloads
import pydantic
import util
import uuids
from more_itertools import bucket
from more_itertools import first
from more_itertools import flatten
from more_itertools import last
from more_itertools import partition
from more_itertools import split_when
from more_itertools import unzip
from pydantic import Field
from pydantic import validator
from ra_utils.generate_uuid import uuid_generator

from integrations.dar_helper import dar_helper


class OrgUnitBase(pydantic.BaseModel):
    start_date: date = Field(alias="StartDato")
    end_date: date = Field(alias="SlutDato")

    @validator("start_date", "end_date", pre=True)
    def validate_date(cls, v, values):
        if isinstance(v, date):
            return v

        formats = ["%d-%m-%Y", "%Y-%m-%d %H:%M:%S.%f"]
        for format in formats:
            try:
                parsed = datetime.strptime(v, format).date().isoformat()
                return parsed
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date '{v}'")


class OrgUnit(OrgUnitBase):
    org_uuid: uuid.UUID = Field(alias="OrgUUID")
    bvn: str = Field(alias="BrugervendtNøgle")
    org_unit_name: Optional[str] = Field(alias="OrgEnhedsNavn")
    org_unit_type_uuid: Optional[uuid.UUID] = Field(alias="OrgEnhedsTypeUUID")
    parent_uuid: uuid.UUID = Field(alias="ParentUUID")
    is_in_line_org: bool = Field(default=False, alias="Med-i-LinjeOrg")
    los_id: Optional[str] = Field(alias="LOSID")
    cvr: Optional[str] = Field(alias="CVR")
    ean: Optional[str] = Field(alias="EAN")
    p_number: Optional[str] = Field(alias="P-Nr")
    se_number: Optional[str] = Field(alias="SE-Nr")
    int_debitor_number: Optional[str] = Field(alias="IntDebitor-Nr")
    mag_id: Optional[str] = Field(alias="MagID")
    post_address: Optional[str] = Field(default="", alias="PostAdresse")


class FailedDARLookup(pydantic.BaseModel):
    org_uuid: uuid.UUID
    post_address: str


class OrgUnitImporter:
    def __init__(self):
        self.dar_cache = {}
        self.uuid_generator = uuid_generator("AAK")

    def generate_unit_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        parent_uuid = orgunit.parent_uuid
        if parent_uuid == orgunit.org_uuid:
            parent_uuid = uuids.ORG_UUID

        return mo_payloads.create_org_unit(
            uuid=orgunit.org_uuid,
            user_key=orgunit.bvn,
            name=orgunit.org_unit_name,
            parent_uuid=parent_uuid,
            org_unit_hierarchy=uuids.LINJE_ORG_HIERARCHY
            if orgunit.is_in_line_org
            else None,
            org_unit_type_uuid=orgunit.org_unit_type_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_los_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "los")
        assert orgunit.los_id
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.los_id,
            address_type_uuid=uuids.UNIT_LOS,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_cvr_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "cvr")
        assert orgunit.cvr
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.cvr,
            address_type_uuid=uuids.UNIT_CVR,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_ean_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "ean")
        assert orgunit.ean
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.ean,
            address_type_uuid=uuids.UNIT_EAN,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_pnr_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "pnr")
        assert orgunit.p_number
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.p_number,
            address_type_uuid=uuids.UNIT_PNR,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_senr_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "senr")
        assert orgunit.se_number
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.se_number,
            address_type_uuid=uuids.UNIT_SENR,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_debitornr_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "debitornr")
        assert orgunit.int_debitor_number
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.int_debitor_number,
            address_type_uuid=uuids.UNIT_DEBITORNR,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_postaddr_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "postaddr")
        assert orgunit.post_address
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=self.dar_cache[orgunit.post_address],
            address_type_uuid=uuids.UNIT_POSTADDR,
            org_unit_uuid=orgunit.org_uuid,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_mag_id_payload(self, orgunit: OrgUnit):
        from_date, to_date = util.convert_validities(
            orgunit.start_date, orgunit.end_date
        )
        obj_uuid = self.uuid_generator(str(orgunit.org_uuid) + "mag_id")
        assert orgunit.mag_id
        return mo_payloads.create_address(
            uuid=obj_uuid,
            value=orgunit.mag_id,
            address_type_uuid=uuids.UNIT_MAG_ID,
            org_unit_uuid=orgunit.org_uuid,
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

        def payloads_identical(p1, p2):
            check_keys = (p1.keys() | p2.keys()) - {"validity"}
            return all(p1.get(k) == p2.get(k) for k in check_keys)

        def is_not_consecutive(p1, p2):
            p1_to = datetime.strptime(p1["validity"]["to"], "%Y-%m-%d")
            p2_from = datetime.strptime(p2["validity"]["from"], "%Y-%m-%d")
            return not (
                payloads_identical(p1, p2) and p1_to + timedelta(days=1) == p2_from
            )

        def merge_objects(group):
            start, end = first(group), last(group)
            start["validity"]["to"] = end["validity"]["to"]
            return start

        if len(payload_list) < 2:
            return payload_list
        consecutive_groups = split_when(payload_list, is_not_consecutive)
        return list(map(merge_objects, consecutive_groups))

    def write_failed_addresses(
        self,
        failed: List[FailedDARLookup],
        filename: str,
        delimiter: str = "#",
    ):
        """Write failed addresses to an external file"""
        output_filename = f"failed_addr_{filename}"  # `filename` is `xxx.csv`

        # Write CSV to output buffer
        output = StringIO()
        writer = DictWriter(output, ["org_uuid", "post_address"], delimiter=delimiter)
        writer.writeheader()
        writer.writerows(f.dict() for f in failed)

        # Write CSV file to FTP folder
        ftp_fileset = los_files.FTPFileSet()
        try:
            result = ftp_fileset.write_file(
                output_filename, output, folder="DARfejlliste"
            )
        except ftplib.Error as exc:
            print("Could not write %r to FTP, error = %r" % (output_filename, str(exc)))
        else:
            print("Wrote %r to FTP, result = %r" % (output_filename, result))

        # Write CSV file to MO queries folder
        fs_fileset = los_files.FSFileSet()
        try:
            result = fs_fileset.write_file(output_filename, output)
        except IOError as exc:
            print(
                "Could not write %r to queries folder, error = %r"
                % (output_filename, str(exc))
            )
        else:
            print("Wrote %r to queries folder, result = %r" % (output_filename, result))

    async def handle_addresses(self, org_units, filename):
        addresses = map(attrgetter("post_address"), org_units)
        addresses = set(filter(None.__ne__, addresses))

        if len(addresses) == 0:
            return

        address_lookups = await dar_helper.dar_datavask_multiple(addresses)

        # Split into two lists where lookup succeeded and failed
        success, failure = partition(lambda x: x[1] is None, address_lookups)

        success = dict(success)
        print(f"{len(success)} addresses found")
        self.dar_cache.update(success)

        failed_addresses = set(map(itemgetter(0), failure))
        print(f"{len(failed_addresses)} addresses could not be found")
        if failed_addresses:
            # Join `failed_addresses` with `org_units` on `post_address` to get
            # `org_uuid` for each failed address lookup.
            joined: List[FailedDARLookup] = [
                FailedDARLookup(
                    org_uuid=org_unit.org_uuid,
                    post_address=org_unit.post_address,
                )
                for org_unit in org_units
                if org_unit.post_address in failed_addresses
            ]
            self.write_failed_addresses(joined, filename)

    def create_unit_payloads(self, org_units) -> Iterable[dict]:
        return map(self.generate_unit_payload, org_units)

    async def create_detail_payloads(self, org_units) -> Iterable[dict]:
        """
        Generate all relevant detail payloads based on the org unit objects
        All rows are run through different sets of generators and filters
        """

        generators = [
            (self.generate_los_payload, lambda orgunit: orgunit.los_id),
            (self.generate_cvr_payload, lambda orgunit: orgunit.cvr),
            (self.generate_ean_payload, lambda orgunit: orgunit.ean),
            (self.generate_pnr_payload, lambda orgunit: orgunit.p_number),
            (self.generate_senr_payload, lambda orgunit: orgunit.se_number),
            (
                self.generate_debitornr_payload,
                lambda orgunit: orgunit.int_debitor_number,
            ),
            (self.generate_mag_id_payload, lambda orgunit: orgunit.mag_id),
            (
                self.generate_postaddr_payload,
                lambda orgunit: orgunit.post_address
                and orgunit.post_address in self.dar_cache,
            ),
        ]

        def run_generator(generator, filter_fn) -> Iterator:
            return map(generator, filter(filter_fn, org_units))

        return flatten(chain(starmap(run_generator, generators)))

    async def handle_initial(self, filename):
        """
        Handles reading the special 'initial' file

        The file contains org unit data, as well as data on the associated details

        The initial org unit file contains historic data, so a minimal set of
        create/edit payloads are created accordingly
        """
        org_units = los_files.read_csv(filename, OrgUnit)

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

        split_lists = map(lambda x: (x[0], x[1:]), consolidated_buckets)
        heads, tails = unzip(split_lists)

        # OS2mo reads an object before performing an edit to it, so we need to ensure
        # that we don't perform multiple edits to an object in parallel, which could
        # cause one edit to be overwritten by another
        # We create layers containing at most one edit request for each org unit UUID,
        # and execute the layers sequentially, while allowing the importer to submit the
        # individual requests in a layer in parallel
        edit_payloads = map(partial(map, mo_payloads.convert_create_to_edit), tails)
        edit_layers = zip_longest(*edit_payloads)
        edit_layers_filtered = map(partial(filter, None.__ne__), edit_layers)

        async with util.get_client_session() as session:
            await util.create_details(session, heads)
            for edit_layer in edit_layers_filtered:
                await util.edit_details(session, edit_layer)

    async def handle_create(self, filename):
        """
        Handle creating new org units and details
        We are guaranteed to only have one row per org unit
        """
        org_units = los_files.read_csv(filename, OrgUnit)

        await self.handle_addresses(org_units, filename)

        org_unit_payloads = self.create_unit_payloads(org_units)
        detail_payloads = await self.create_detail_payloads(org_units)

        async with util.get_client_session() as session:
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
        org_units = los_files.read_csv(filename, OrgUnit)
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

        async with util.get_client_session() as session:
            await util.create_details(session, detail_creates)
            await util.edit_details(session, edits)

    async def run(self, last_import: datetime):
        """
        Reads org unit files newer than last_import
        and performs inserts/updates as needed
        """
        print("Starting org unit import")
        filenames = los_files.get_fileset_implementation().get_import_filenames()

        initials = los_files.parse_filenames(
            filenames, prefix="Org_inital", last_import=last_import
        )
        creates = los_files.parse_filenames(
            filenames, prefix="Org_nye", last_import=last_import
        )
        edits = los_files.parse_filenames(
            filenames, prefix="Org_ret", last_import=last_import
        )

        for filename, _ in initials:
            await self.handle_initial(filename)

        for filename, _ in creates:
            await self.handle_create(filename)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        print("Org unit import done")
