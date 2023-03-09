import logging
import uuid
from datetime import datetime
from functools import partial
from itertools import chain
from typing import Optional

import config
import los_files
import payloads as mo_payloads
import pydantic
import util
import uuids
from more_itertools import bucket
from more_itertools import first
from more_itertools import partition
from pydantic import Field
from ra_utils.generate_uuid import uuid_generator


logger = logging.getLogger(__name__)


class Person(pydantic.BaseModel):
    person_uuid: uuid.UUID = Field(alias="UUID")
    bvn: str = Field(alias="BrugervendtNøgle")
    cpr: str = Field(alias="CPR")
    name: str = Field(alias="CPRNavn")
    start_time: datetime = Field(alias="StartTid")
    end_time: datetime = Field(alias="SlutTid")
    email: Optional[str] = Field(alias="Email")
    primary: str = Field(alias="PrimærAnsættelse")
    org_unit_uuid: uuid.UUID = Field(alias="OrgenhedUUID")
    engagement_type_uuid: uuid.UUID = Field(alias="EngagementTypeUUID")
    az_id: Optional[str] = Field(alias="AZID")
    # These 3 fields are present but no longer read/used by the LOS importer
    ad_it_system_uuid: Optional[uuid.UUID] = Field(alias="ADITSystemUUID")
    ad_account: Optional[str] = Field(alias="ADkonto")
    ad_acccount_uuid: Optional[uuid.UUID] = Field(alias="ADkontoUUID")
    # This field is used when creating engagements
    job_function_uuid: uuid.UUID = Field(alias="StillingsBetegnelseUUID")


class PersonTermination(pydantic.BaseModel):
    engagement_uuid: uuid.UUID = Field(alias="UUID")
    cpr: str = Field(alias="CPR")
    name: str = Field(alias="CPRNavn")


class PersonImporter:
    def __init__(self):
        self.uuid_generator = uuid_generator("AAK")
        self.settings = config.get_config()
        # Get all existing CPRs in MO
        self._cpr_map = util.build_cpr_map()

    def _get_person_mo_uuid(self, person: Person) -> uuid.UUID:
        if person.cpr in self._cpr_map:
            # Re-use existing person UUID
            return self._cpr_map[person.cpr]
        else:
            # Create a new person UUID based on the CPR
            return self.uuid_generator(person.cpr)

    def generate_employee_payload(self, person: Person):
        return mo_payloads.create_employee(
            name=person.name,
            cpr_no=person.cpr,
            uuid=self._get_person_mo_uuid(person),
        )

    def generate_employee_email_payload(self, person: Person, filedate: str):
        # For creation of Employee address/IT details we always
        # use the parsed file-date as from-date
        # The start_time field is still used for creating engagements
        _, to_date = util.convert_validities(
            person.start_time.date(), person.end_time.date()
        )
        return mo_payloads.create_address(
            person_uuid=self._get_person_mo_uuid(person),
            uuid=self.uuid_generator(person.cpr + "email"),
            value=person.email,
            from_date=filedate,
            to_date=to_date,
            address_type_uuid=uuids.PERSON_EMAIL,
        )

    def generate_employee_az_id_payload(self, person: Person, filedate: str):
        # For creation of Employee address/IT details we always
        # use the parsed file-date as from-date
        # The start_time field is still used for creating engagements
        _, to_date = util.convert_validities(
            person.start_time.date(), person.end_time.date()
        )
        return mo_payloads.create_it_rel(
            person_uuid=self._get_person_mo_uuid(person),
            uuid=self.uuid_generator(person.cpr + "azid"),
            user_key=person.az_id,
            itsystem_uuid=self.settings.azid_it_system_uuid,
            from_date=filedate,
            to_date=to_date,
        )

    def generate_engagement_payload(self, person: Person):
        from_date, to_date = util.convert_validities(
            person.start_time.date(), person.end_time.date()
        )
        primary_uuids = {"Nej": uuids.NOT_PRIMARY, "Ja": uuids.PRIMARY}
        return mo_payloads.create_engagement(
            person_uuid=self._get_person_mo_uuid(person),
            uuid=person.person_uuid,
            org_unit_uuid=person.org_unit_uuid,
            job_function_uuid=person.job_function_uuid,
            engagement_type_uuid=person.engagement_type_uuid,
            primary_uuid=primary_uuids[person.primary],
            user_key=person.bvn,
            from_date=from_date,
            to_date=to_date,
        )

    @staticmethod
    def generate_engagement_termination_payload(
        termination: PersonTermination, to_date: datetime
    ):
        return mo_payloads.terminate_detail(
            "engagement", termination.engagement_uuid, to_date
        )

    def create_employee_payloads(self, persons):
        # Every person row contains the same info, so we just pick one
        cpr_buckets = bucket(persons, key=lambda x: x.cpr)
        unique_persons = map(lambda key: first(cpr_buckets[key]), cpr_buckets)
        return map(self.generate_employee_payload, unique_persons)

    def create_detail_payloads(self, persons, filedate: str):
        engagement_payloads = map(self.generate_engagement_payload, persons)
        email_payloads = map(
            partial(self.generate_employee_email_payload, filedate=filedate),
            filter(lambda person: person.email, persons),
        )
        az_id_payloads = map(
            partial(self.generate_employee_az_id_payload, filedate=filedate),
            filter(lambda person: person.az_id, persons),
        )
        return chain(
            engagement_payloads,
            email_payloads,
            az_id_payloads,
        )

    async def handle_create(self, filename: str, filedate: datetime):
        """
        Handle creating new persons and details
        We are guaranteed to only have one row per person
        """
        persons = los_files.read_csv(filename, Person)
        employee_payloads = self.create_employee_payloads(persons)
        detail_payloads = self.create_detail_payloads(
            persons, filedate.date().isoformat()
        )

        async with util.get_client_session() as session:
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
        persons = los_files.read_csv(filename, Person)
        employee_payloads = self.create_employee_payloads(persons)
        detail_payloads = self.create_detail_payloads(
            persons, filedate.date().isoformat()
        )

        orgfunk_uuids = set(await util.lookup_organisationfunktion())
        detail_creates, detail_edits = partition(
            lambda payload: payload["uuid"] in orgfunk_uuids, detail_payloads
        )
        converter = partial(
            mo_payloads.convert_create_to_edit, from_date=filedate.date().isoformat()
        )
        edits = map(converter, chain(employee_payloads, detail_edits))

        async with util.get_client_session() as session:
            await util.create_details(session, detail_creates)
            await util.edit_details(session, edits)

    async def handle_terminate(self, filename: str, filedate: datetime):
        """
        Handle termination of engagements. We are guaranteed one row per engagement.
        """
        persons = los_files.read_csv(filename, PersonTermination)
        termination_fn = partial(
            self.generate_engagement_termination_payload, to_date=filedate
        )
        payloads = map(termination_fn, persons)

        async with util.get_client_session() as session:
            await util.terminate_details(session, payloads)

    async def run(self, last_import: datetime):
        logger.info("Starting person import")
        filenames = los_files.get_fileset_implementation().get_import_filenames()

        creates = los_files.parse_filenames(
            filenames, prefix="Pers_nye", last_import=last_import
        )
        edits = los_files.parse_filenames(
            filenames, prefix="Pers_ret", last_import=last_import
        )
        terminates = los_files.parse_filenames(
            filenames, prefix="Pers_luk", last_import=last_import
        )

        for filename, filedate in creates:
            await self.handle_create(filename, filedate)

        for filename, filedate in edits:
            await self.handle_edit(filename, filedate)

        for filename, filedate in terminates:
            await self.handle_terminate(filename, filedate)

        logger.info("Person import done")
