from datetime import datetime
from functools import partial
from itertools import chain

import pydantic
from aiohttp import ClientSession
from aiohttp import TCPConnector
from more_itertools import bucket
from more_itertools import first
from more_itertools import partition
from pydantic import Field

import payloads as mo_payloads
import util
import uuids


class Person(pydantic.BaseModel):
    UUID: str  # UUID for engagement
    BVN: str = Field(alias="BrugervendtNøgle")
    CPR: str
    CPRNavn: str
    StartTid: datetime
    SlutTid: datetime
    Email: str
    Primary: str = Field(alias="PrimærAnsættelse")
    OrgenhedUUID: str
    EngagementTypeUUID: str
    AZID: str
    ADITSystemUUID: str
    ADkonto: str
    ADkontoUUID: str
    StillingsBetegnelseUUID: str


class PersonTermination(pydantic.BaseModel):
    UUID: str  # UUID for engagement
    CPR: str
    CPRNavn: str


class PersonImporter:
    @staticmethod
    def _convert_validities(from_time: datetime, to_time: datetime):
        from_time_str, to_time_str = (
            from_time.date().isoformat(),
            to_time.date().isoformat(),
        )
        return from_time_str, to_time_str if to_time_str != "9999-12-31" else None

    def generate_employee_email_payload(self, person: Person):
        from_date, to_date = self._convert_validities(person.StartTid, person.SlutTid)
        assert person.Email
        return mo_payloads.create_address(
            uuid=util.generate_uuid(person.CPR + "email"),
            value=person.Email,
            from_date=from_date,
            to_date=to_date,
            person_uuid=util.generate_uuid(person.CPR),
            address_type_uuid=uuids.PERSON_EMAIL,
        )

    def generate_employee_az_id_payload(self, person: Person):
        from_date, to_date = self._convert_validities(person.StartTid, person.SlutTid)
        assert person.AZID
        return mo_payloads.create_it_rel(
            uuid=util.generate_uuid(person.CPR + "azid"),
            user_key=person.AZID,
            person_uuid=util.generate_uuid(person.CPR),
            itsystem_uuid=uuids.AZID_SYSTEM,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_employee_ad_account_payload(self, person: Person):
        from_date, to_date = self._convert_validities(person.StartTid, person.SlutTid)
        assert person.ADkonto
        return mo_payloads.create_it_rel(
            uuid=person.ADkontoUUID,
            user_key=person.ADkonto,
            person_uuid=util.generate_uuid(person.CPR),
            itsystem_uuid=person.ADITSystemUUID,
            from_date=from_date,
            to_date=to_date,
        )

    def generate_engagement_payload(self, person: Person):
        from_date, to_date = self._convert_validities(person.StartTid, person.SlutTid)

        primary_uuids = {"Nej": uuids.NOT_PRIMARY, "Ja": uuids.PRIMARY}
        return mo_payloads.create_engagement(
            uuid=person.UUID,
            org_unit_uuid=person.OrgenhedUUID,
            person_uuid=util.generate_uuid(person.CPR),
            job_function_uuid=person.StillingsBetegnelseUUID,
            engagement_type_uuid=person.EngagementTypeUUID,
            primary_uuid=primary_uuids[person.Primary],
            user_key=person.BVN,
            from_date=from_date,
            to_date=to_date,
        )

    @staticmethod
    def generate_employee_payload(person: Person):
        # Same person info in every row
        return mo_payloads.create_employee(
            name=person.CPRNavn, cpr_no=person.CPR, uuid=util.generate_uuid(person.CPR)
        )

    @staticmethod
    def generate_engagement_termination_payload(person: Person, to_date: datetime):
        return {
            "type": "engagement",
            "uuid": person.UUID,
            "validity": {"to": to_date.date().isoformat()},
        }

    def create_employee_payloads(self, persons):
        cpr_buckets = bucket(persons, key=lambda x: x.CPR)

        # Every person row contains the same info, so we just pick one
        unique_persons = map(lambda key: first(cpr_buckets[key]), cpr_buckets)
        return map(self.generate_employee_payload, unique_persons)

    def create_detail_payloads(self, persons):
        engagement_payloads = map(self.generate_engagement_payload, persons)
        email_payloads = map(
            self.generate_employee_email_payload,
            filter(lambda person: person.Email, persons),
        )
        az_id_payloads = map(
            self.generate_employee_az_id_payload,
            filter(lambda person: person.AZID, persons),
        )
        ad_account_payloads = map(
            self.generate_employee_ad_account_payload,
            filter(lambda person: person.ADkonto, persons),
        )
        return chain(
            engagement_payloads,
            email_payloads,
            az_id_payloads,
            ad_account_payloads,
        )

    async def handle_create(self, filename: str, filedate: datetime):
        """
        Handle creating new persons and details
        We are guaranteed to only have one row per person
        """
        persons = util.read_csv(filename, Person)
        employee_payloads = self.create_employee_payloads(persons)
        detail_payloads = self.create_detail_payloads(persons)

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.create_details(
                session, chain(employee_payloads, detail_payloads)
            )

    async def handle_edit(self, filename: str, filedate: datetime):
        """
        Handle changes to existing persons and details
        We are guaranteed to only have one row per person

        New details on an existing person will show up in this file, rather than the
        'nye' file. So we have to potentially perform inserts of new data.

        As a row contains information about the person as well as its details,
        we do not know what has been changed. However, all information is managed
        by the external system so we can safely reimport the "same" data, as opposed to
        trying to compare the existing objects in OS2mo
        """
        persons = util.read_csv(filename, Person)
        employee_payloads = self.create_employee_payloads(persons)
        detail_payloads = self.create_detail_payloads(persons)

        orgfunk_uuids = set(await util.lookup_organisationfunktion())
        detail_creates, detail_edits = partition(
            lambda payload: payload["uuid"] in orgfunk_uuids, detail_payloads
        )
        converter = partial(
            mo_payloads.convert_create_to_edit, from_date=filedate.date().isoformat()
        )
        edits = map(converter, chain(employee_payloads, detail_edits))

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.create_details(session, detail_creates)
            await util.edit_details(session, edits)

    async def handle_terminate(self, filename: str, filedate: datetime):
        """
        Handle termination of engagements. We are guaranteed one row per engagement.
        """
        persons = util.read_csv(filename, PersonTermination)
        termination_fn = partial(
            self.generate_engagement_termination_payload, to_date=filedate
        )
        payloads = map(termination_fn, persons)

        connector = TCPConnector()
        async with ClientSession(connector=connector) as session:
            await util.terminate_details(session, payloads)

    async def run(self, last_import: datetime):
        print("Starting person import")
        ftp = util.get_ftp_connector()
        filenames = ftp.nlst()

        creates = util.parse_filenames(
            filenames, prefix="Pers_nye", last_import=last_import
        )
        edits = util.parse_filenames(
            filenames, prefix="Pers_ret", last_import=last_import
        )
        terminates = util.parse_filenames(
            filenames, prefix="Pers_luk", last_import=last_import
        )

        for filename, filedate in creates:
            await self.handle_create(filename, filedate)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        for filename, filedate in terminates:
            await self.handle_terminate(filename, filedate)

        print("Person import done")
