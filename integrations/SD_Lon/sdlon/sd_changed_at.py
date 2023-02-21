import datetime
import logging
import pathlib
import sqlite3
import sys
from functools import lru_cache
from itertools import tee
from operator import itemgetter
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import OrderedDict
from typing import Set
from typing import Tuple
from typing import Union
from uuid import UUID
from uuid import uuid4

import click
import requests
import sentry_sdk
from fastapi.encoders import jsonable_encoder
from integrations import cpr_mapper
from integrations.ad_integration import ad_reader
from integrations.calculate_primary.common import NoPrimaryFound
from integrations.calculate_primary.sd import SDPrimaryEngagementUpdater
from integrations.rundb.db_overview import DBOverview
from more_itertools import last
from more_itertools import one
from more_itertools import partition
from os2mo_helpers.mora_helpers import MoraHelper
from ramodels.mo import Employee
from ramodels.mo._shared import OrganisationRef
from tqdm import tqdm

from . import sd_payloads
from .config import ChangedAtSettings
from .config import get_changed_at_settings
from .date_utils import date_to_datetime
from .date_utils import gen_date_intervals
from .date_utils import sd_to_mo_termination_date
from .engagement import create_engagement
from .engagement import engagement_components
from .engagement import (
    is_employment_id_and_no_salary_minimum_consistent,
)
from .engagement import update_existing_engagement
from .fix_departments import FixDepartments
from .models import JobFunction
from .models import SDBasePerson
from .sd_common import calc_employment_id
from .sd_common import EmploymentStatus
from .sd_common import ensure_list
from .sd_common import mora_assert
from .sd_common import primary_types
from .sd_common import read_employment_at
from .sd_common import sd_lookup
from .sd_common import skip_fictional_users
from .skip import cpr_env_filter
from .sync_job_id import JobIdSync


DUMMY_CPR = "0000000000"

logger = logging.getLogger("sdChangedAt")


# TODO: SHOULD WE IMPLEMENT PREDICTABLE ENGAGEMENT UUIDS ALSO IN THIS CODE?!?


class ChangeAtSD:
    def __init__(
        self,
        settings: ChangedAtSettings,
        from_date: datetime.datetime,
        to_date: Optional[datetime.datetime] = None,
    ):
        self.settings = settings

        job_function_type = self.settings.sd_job_function
        if job_function_type == JobFunction.job_position_identifier:
            logger.info("Read settings. JobPositionIdentifier for job_functions")
            self.use_jpi = True
        elif job_function_type == JobFunction.employment_name:
            logger.info("Read settings. Do not update job_functions")
            self.use_jpi = False

        self.employee_forced_uuids = (
            self._read_forced_uuids() if self.settings.sd_read_forced_uuids else dict()
        )
        self.department_fixer = self._get_fix_departments()
        self.helper = self._get_mora_helper(self.settings.mora_base)
        self.job_sync = self._get_job_sync(self.settings)

        # List of job_functions that should be ignored.
        self.skip_job_functions = self.settings.sd_skip_job_functions
        self.use_ad = self.settings.sd_use_ad_integration

        # See https://os2web.atlassian.net/browse/MO-245 for more details
        # about no_salary_minimum
        self.no_salary_minimum = self.settings.sd_no_salary_minimum_id

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        self.updater = (
            self._get_primary_engagement_updater()
            if self.settings.sd_update_primary_engagement
            else None
        )
        self.from_date = from_date
        self.to_date = to_date

        # Cache of mo engagements
        self.mo_engagements_cache: Dict[str, list] = {}

        self.primary_types = self._get_primary_types(self.helper)

        logger.info("Read job_functions")
        facet_info = self.helper.read_classes_in_facet("engagement_job_function")
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        # Map from user-key to uuid if jpi, name to uuid otherwise
        job_function_mapper = cast(
            Callable[[Any], Tuple[str, str]], itemgetter("name", "uuid")
        )
        if self.use_jpi:
            job_function_mapper = cast(
                Callable[[Any], Tuple[str, str]], itemgetter("user_key", "uuid")
            )
        self.job_functions: Dict[str, str] = dict(
            map(job_function_mapper, job_functions)
        )

        logger.info("Read engagement types")
        # The Opus diff-import contains a slightly more abstrac def to do this
        engagement_types = self.helper.read_classes_in_facet("engagement_type")
        self.engagement_type_facet = engagement_types[1]
        engagement_type_mapper = cast(
            Callable[[Any], Tuple[str, str]], itemgetter("user_key", "uuid")
        )
        self.engagement_types: Dict[str, str] = dict(
            map(engagement_type_mapper, engagement_types[0])
        )

        # SD supports only one type of leave
        self.leave_uuid = self.helper.ensure_class_in_facet("leave_type", "Orlov")

        facet_info = self.helper.read_classes_in_facet("association_type")
        self.association_uuid = facet_info[0][0]["uuid"]

    def _get_primary_types(self, mora_helper: MoraHelper):
        return primary_types(mora_helper)

    def _get_primary_engagement_updater(self) -> SDPrimaryEngagementUpdater:
        return SDPrimaryEngagementUpdater()

    def _get_fix_departments(self) -> FixDepartments:
        return FixDepartments(self.settings)

    def _get_mora_helper(self, mora_base) -> MoraHelper:
        return MoraHelper(hostname=mora_base, use_cache=False)

    def _get_job_sync(self, settings: ChangedAtSettings) -> JobIdSync:
        return JobIdSync(settings)

    def _read_forced_uuids(self):
        cpr_map = pathlib.Path(self.settings.cpr_uuid_map_path)
        if not cpr_map.is_file():
            message = f"Did not find cpr mapping: {cpr_map}"
            logger.error(message)
            raise Exception(message)

        logger.info("Found cpr mapping")
        employee_forced_uuids = cpr_mapper.employee_mapper(str(cpr_map))
        return employee_forced_uuids

    @lru_cache(maxsize=None)
    def _get_ad_reader(self):
        if self.use_ad:
            logger.info("AD integration in use")
            return ad_reader.ADParameterReader()
        logger.info("AD integration not in use")
        return None

    def _fetch_ad_information(self, cpr) -> Union[Tuple[None, None], Tuple[str, str]]:
        ad_reader = self._get_ad_reader()
        if ad_reader is None:
            return None, None

        ad_info = ad_reader.read_user(cpr=cpr)
        object_guid = ad_info.get("ObjectGuid", None)
        sam_account_name = ad_info.get("SamAccountName", None)
        return sam_account_name, object_guid

    @lru_cache(maxsize=None)
    def _fetch_ad_it_system_uuid(self):
        if not self.use_ad:
            raise ValueError("_fetch_ad_it_system_uuid called without AD enabled")
        it_systems = self.helper.read_it_systems()
        return one(
            map(
                itemgetter("uuid"),
                filter(lambda system: system["name"] == "Active Directory", it_systems),
            )
        )

    @lru_cache(maxsize=None)
    def read_employment_changed(
        self,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        employment_identifier: Optional[str] = None,
        in_cpr: Optional[str] = None,
    ):
        from_date = from_date or self.from_date
        to_date = to_date or self.to_date

        params = {
            "ActivationDate": from_date.strftime("%d.%m.%Y"),
            "ActivationTime": from_date.strftime("%H:%M"),
            "DepartmentIndicator": "true",
            "EmploymentStatusIndicator": "true",
            "ProfessionIndicator": "true",
            "WorkingTimeIndicator": "true",
            "UUIDIndicator": "true",
            "StatusPassiveIndicator": "true",
            "SalaryAgreementIndicator": "false",
            "SalaryCodeGroupIndicator": "false",
        }
        if employment_identifier:
            params.update(
                {
                    "EmploymentIdentifier": employment_identifier,
                }
            )
        if in_cpr:
            params.update(
                {
                    "PersonCivilRegistrationIdentifier": in_cpr,
                }
            )

        if to_date is not None:
            url = "GetEmploymentChangedAtDate20111201"
            params.update(
                {
                    "DeactivationDate": to_date.strftime("%d.%m.%Y"),
                    "DeactivationTime": to_date.strftime("%H:%M"),
                    "StatusActiveIndicator": "true",
                    "StatusPassiveIndicator": "true",
                    "FutureInformationIndicator": "true",
                }
            )
        else:
            url = "GetEmploymentChanged20111201"
            params.update(
                {
                    "DeactivationDate": "31.12.9999",
                }
            )
        response = sd_lookup(url, settings=self.settings, params=params)

        employment_response = ensure_list(response.get("Person", []))

        return employment_response

    def get_sd_persons_changed(
        self, from_date: datetime.datetime, to_date: Optional[datetime.datetime] = None
    ) -> List[OrderedDict[str, Any]]:
        """
        Get list of SD Løn persons that have changed between `from_date`
        and `to_date`

        Returns:
            List of SD Løn persons changed between the two dates
        """

        params = {
            "ActivationDate": from_date.strftime("%d.%m.%Y"),
            "ActivationTime": from_date.strftime("%H:%M"),
            "DeactivationDate": "31.12.9999",
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "true",
            "ContactInformationIndicator": "false",
            "PostalAddressIndicator": "false"
            # TODO: Er der kunder, som vil udlæse adresse-information?
        }
        if to_date:
            params["DeactivationDate"] = to_date.strftime("%d.%m.%Y")
            params["DeactivationTime"] = to_date.strftime("%H:%M")

        url = "GetPersonChangedAtDate20111201"
        response = sd_lookup(url, settings=self.settings, params=params)
        persons_changed = ensure_list(response.get("Person", []))
        return persons_changed

    def get_sd_person(self, cpr: str) -> List[OrderedDict[str, Any]]:
        """
        Get a single person from SD Løn at `self.from_date`

        Args:
            cpr: the cpr number of the person

        Returns:
            A list containing the person (or an empty list if no person
            is found)
        """

        params = {
            "EffectiveDate": self.from_date.strftime("%d.%m.%Y"),
            "PersonCivilRegistrationIdentifier": cpr,
            "StatusActiveIndicator": "True",
            "StatusPassiveIndicator": "false",
            "ContactInformationIndicator": "false",
            "PostalAddressIndicator": "false",
        }
        url = "GetPerson20111201"
        response = sd_lookup(url, settings=self.settings, params=params)
        person = ensure_list(response.get("Person", []))
        return person

    def update_changed_persons(
        self, in_cpr: Optional[str] = None, dry_run: bool = False
    ) -> None:
        """Update and insert (upsert) changed persons.

        Args:
            in_cpr: Optional CPR number of a specific person to upsert instead of
                    using SDs GetPersonChangedAtDate endpoint.

        Note:
            This method does not create employments at all, as this responsibility is
            handled by the update_employment method instead.
        """

        def extract_cpr_and_name(person: Dict[str, Any]) -> SDBasePerson:
            """
            Get CPR, given name (firstname) and surname (lastname) of
            SD Løn person.

            Args:
                person: the person from SD Løn

            Returns:
                Basic SD person with cpr and names
            """

            return SDBasePerson(
                cpr=person["PersonCivilRegistrationIdentifier"],
                given_name=person.get("PersonGivenName"),
                surname=person.get("PersonSurnameName"),
            )

        def fetch_mo_person(person: SDBasePerson) -> Dict[str, Any]:
            mo_person = self.helper.read_user(
                user_cpr=person.cpr, org_uuid=self.org_uuid
            )
            return mo_person

        def upsert_employee(
            uuid: str, given_name: Optional[str], sur_name: Optional[str], cpr: str
        ) -> str:
            model = Employee(
                uuid=uuid,
                user_key=uuid,
                givenname=given_name,
                surname=sur_name,
                cpr_no=cpr,
                org=OrganisationRef(uuid=self.org_uuid),
            )
            payload = jsonable_encoder(model.dict(by_alias=True, exclude_none=True))
            if dry_run:
                print("Dry-run: upsert_employee", payload)
                return "invalid-uuid"
            response = self.helper._mo_post("e/create", payload)
            assert response.status_code == 201
            return_uuid = response.json()
            logger.info(
                "Created or updated employee {} {} with uuid {}".format(
                    given_name, sur_name, return_uuid
                )
            )
            return return_uuid

        def create_itsystem_connection(sam_account_name: str, user_uuid: str):
            payload = sd_payloads.connect_it_system_to_user(
                sam_account_name, self._fetch_ad_it_system_uuid(), user_uuid
            )
            if dry_run:
                print("Dry-run: create_itsystem_connection", payload)
                return
            logger.debug("Connect it-system: {}".format(payload))
            response = self.helper._mo_post("details/create", payload)
            assert response.status_code == 201
            logger.info("Added AD account info to {}".format(user_uuid))

        # Fetch a list of persons to update
        if in_cpr is not None:
            all_sd_persons_changed = self.get_sd_person(in_cpr)
        else:
            logger.info("Update all persons")
            all_sd_persons_changed = self.get_sd_persons_changed(
                self.from_date, self.to_date
            )

        logger.info(f"Number of changed persons: {len(all_sd_persons_changed)}")
        all_sd_persons_changed = tqdm(all_sd_persons_changed, desc="update persons")
        real_sd_persons_changed = filter(skip_fictional_users, all_sd_persons_changed)

        # Filter employees based on the sd_cprs list
        sd_cpr_filtered_persons = filter(cpr_env_filter, real_sd_persons_changed)

        sd_persons_changed = map(extract_cpr_and_name, sd_cpr_filtered_persons)

        sd_persons_iter1, sd_persons_iter2 = tee(sd_persons_changed)
        mo_persons_iter = map(fetch_mo_person, sd_persons_iter2)

        person_pairs = zip(sd_persons_iter1, mo_persons_iter)
        has_mo_person = itemgetter(1)

        new_pairs, current_pairs = partition(has_mo_person, person_pairs)

        # Update the names of the persons already in MO
        for sd_person, mo_person in current_pairs:
            given_name = sd_person.given_name or mo_person.get("givenname", "")
            surname = sd_person.surname or mo_person.get("surname", "")
            sd_name = f"{sd_person.given_name} {sd_person.surname}"
            if mo_person["name"] == sd_name:
                continue
            uuid = mo_person["uuid"]

            upsert_employee(str(uuid), given_name, surname, sd_person.cpr)

        # Create new SD persons in MO
        for sd_person, _ in new_pairs:
            given_name = sd_person.given_name or ""
            surname = sd_person.surname or ""

            # Use previously created UUID (if such exists) for MO person
            # to be created
            forced_uuid = self.employee_forced_uuids.get(sd_person.cpr)
            sam_account_name, object_guid = self._fetch_ad_information(sd_person.cpr)

            uuid = None
            if forced_uuid:
                uuid = forced_uuid
                logger.info("Employee in force list: {}".format(uuid))
            elif object_guid:
                uuid = object_guid
                logger.debug("Using ObjectGuid as MO UUID: {}".format(uuid))
            else:
                uuid = uuid4()
                logger.debug(
                    "User not in MO, UUID list or AD, assigning UUID: {}".format(uuid)
                )

            return_uuid = upsert_employee(str(uuid), given_name, surname, sd_person.cpr)

            if sam_account_name:
                # Create an IT system for the person If the person is found in the AD
                create_itsystem_connection(sam_account_name, return_uuid)

    def _compare_dates(self, first_date, second_date, expected_diff=1):
        """
        Return true if the amount of days between second and first is smaller
        than  expected_diff.
        """
        first = datetime.datetime.strptime(first_date, "%Y-%m-%d")
        second = datetime.datetime.strptime(second_date, "%Y-%m-%d")
        delta = second - first
        # compare = first + datetime.timedelta(days=expected_diff)
        compare = abs(delta.days) <= expected_diff
        logger.debug(
            "Compare. First: {}, second: {}, expected: {}, compare: {}".format(
                first, second, expected_diff, compare
            )
        )
        return compare

    def _validity(self, engagement_info, original_end=None, cut=False):
        """
        Extract a validity object from the supplied SD information.
        If the validity extends outside the current engagment, the
        change is either refused (by returning None) or cut to the
        length of the current engagment.
        :param engagement_info: The SD object to extract from.
        :param orginal_end: The engagment end to compare with.
        :param cut: If True the returned validity will cut to fit
        rather than rejeted, if the validity is too long.
        :return: A validity dict suitable for a MO payload. None if
        the change is rejected.
        """
        from_date = engagement_info["ActivationDate"]
        to_date = engagement_info["DeactivationDate"]

        if original_end is not None:
            edit_from = datetime.datetime.strptime(from_date, "%Y-%m-%d")
            edit_end = datetime.datetime.strptime(to_date, "%Y-%m-%d")
            eng_end = datetime.datetime.strptime(original_end, "%Y-%m-%d")
            if edit_from >= eng_end:
                logger.info("This edit starts after the end of the engagement")
                return None

            if edit_end > eng_end:
                if cut:
                    to_date = datetime.datetime.strftime(eng_end, "%Y-%m-%d")
                else:
                    logger.info("This edit would have extended outside engagement")
                    return None

        if to_date == "9999-12-31":
            to_date = None
        validity = {"from": from_date, "to": to_date}

        return validity

    def _refresh_mo_engagements(self, person_uuid):
        self.mo_engagements_cache.pop(person_uuid, None)

    def _fetch_mo_engagements(self, person_uuid):
        if person_uuid in self.mo_engagements_cache:
            return self.mo_engagements_cache[person_uuid]

        mo_engagements = self.helper.read_user_engagement(
            person_uuid, read_all=True, only_primary=True, use_cache=False
        )
        self.mo_engagements_cache[person_uuid] = mo_engagements
        return mo_engagements

    def _find_engagement(self, job_id, person_uuid):
        try:
            user_key = str(int(job_id)).zfill(5)
        except ValueError:  # We will end here, if int(job_id) fails
            user_key = job_id

        logger.debug(
            "Find engagement, from date: {}, user_key: {}".format(
                self.from_date, user_key
            )
        )

        mo_engagements = self._fetch_mo_engagements(person_uuid)

        relevant_engagements = filter(
            lambda mo_eng: mo_eng["user_key"] == user_key, mo_engagements
        )
        relevant_engagement = last(relevant_engagements, None)

        if relevant_engagement is None:
            msg = "Fruitlessly searched for {} in {}".format(job_id, mo_engagements)
            logger.info(msg)
        return relevant_engagement

    def _create_class(self, payload):
        """Create a new class using the provided class payload.

        Args:
            payload: A class created using sd_payloads.* via lora_klasse

        Returns:
            uuid of the newly created class.
        """
        response = requests.post(
            url=self.settings.mox_base + "/klassifikation/klasse", json=payload
        )
        assert response.status_code == 201
        return response.json()["uuid"]

    def _create_engagement_type(self, engagement_type_ref, job_position):
        # Could not fetch, attempt to create it
        logger.warning(
            "Missing engagement_type: {} (now creating)".format(engagement_type_ref)
        )
        payload = sd_payloads.engagement_type(
            engagement_type_ref, job_position, self.org_uuid, self.engagement_type_facet
        )
        engagement_type_uuid = self._create_class(payload)
        self.engagement_types[engagement_type_ref] = engagement_type_uuid

        self.job_sync.sync_from_sd(job_position, refresh=True)

        return engagement_type_uuid

    def _create_professions(self, job_function, job_position):
        # Could not fetch, attempt to create it
        logger.warning("Missing profession: {} (now creating)".format(job_function))
        payload = sd_payloads.profession(
            job_function, self.org_uuid, self.job_function_facet
        )
        job_uuid = self._create_class(payload)
        self.job_functions[job_function] = job_uuid

        self.job_sync.sync_from_sd(job_position, refresh=True)

        return job_uuid

    def _fetch_engagement_type(self, job_position):
        """Fetch an engagement type UUID, create if missing.

        Args:
            engagement_type_ref: String of the expected engagement_type name

        Returns:
            uuid of the engagement type or None if it could not be created.
        """
        # Attempt to fetch the engagement type
        engagement_type_ref = "engagement_type" + job_position
        engagement_type_uuid = self.engagement_types.get(engagement_type_ref)
        if engagement_type_uuid:
            return engagement_type_uuid
        return self._create_engagement_type(engagement_type_ref, job_position)

    def _fetch_professions(self, job_function, job_position):
        """Fetch an job function UUID, create if missing.

        This function does not depend on self.use_jpi, as the argument is just a
        string. If self.use_jpi is true, the string will be the SD
        JobPositionIdentifier, otherwise it will be the actual job name.

        Args:
            emp_name: Overloaded job identifier string / employment name.

        Returns:
            uuid of the job function or None if it could not be created.
        """
        # Add new profssions to LoRa
        job_uuid = self.job_functions.get(job_function)
        if job_uuid:
            return job_uuid
        return self._create_professions(job_function, job_position)

    def create_leave(self, status, job_id, person_uuid: str):
        """Create a leave for a user"""
        logger.info("Create leave, job_id: {}, status: {}".format(job_id, status))
        # TODO: This code potentially creates duplicated leaves.

        # Notice, the expected and desired behaviour for leaves is for the engagement
        # to continue during the leave. It turns out this is actually what happens
        # because a leave is apparently always accompanied by a worktime-update that
        # forces an edit to the engagement that will extend it to span the
        # leave. If this ever turns out not to hold, add a dummy-edit to the
        # engagement here.
        mo_eng = self._find_engagement(job_id, person_uuid)
        payload = sd_payloads.create_leave(
            mo_eng, person_uuid, str(self.leave_uuid), job_id, self._validity(status)
        )

        response = self.helper._mo_post("details/create", payload)
        assert response.status_code == 201

    def create_association(self, department, person_uuid, job_id, validity):
        """Create a association for a user"""
        logger.info("Consider to create an association")
        associations = self.helper.read_user_association(
            person_uuid, read_all=True, only_primary=True
        )
        logger.debug("Associations read from MO: {}".format(associations))
        hit = False
        for association in associations:
            if (
                association["validity"] == validity
                and association["org_unit"]["uuid"] == department
            ):
                hit = True
        if not hit:
            logger.info("Association needs to be created")
            payload = sd_payloads.create_association(
                department, person_uuid, self.association_uuid, job_id, validity
            )
            response = self.helper._mo_post("details/create", payload)
            assert response.status_code == 201
        else:
            logger.info("No new Association is needed")

    def apply_NY_logic(self, org_unit, job_id, validity, person_uuid) -> str:
        msg = "Apply NY logic for job: {}, unit: {}, validity: {}"
        logger.debug(msg.format(job_id, org_unit, validity))
        too_deep = self.settings.sd_import_too_deep
        # Move users and make associations according to NY logic
        today = datetime.datetime.today()
        ou_info = self.helper.read_ou(org_unit, use_cache=False)
        if "status" in ou_info:
            # This unit does not exist, read its state in the not-too
            # distant future.
            fix_date = today + datetime.timedelta(weeks=80)
            self.department_fixer.fix_department(org_unit, fix_date)
            ou_info = self.helper.read_ou(org_unit, use_cache=False)
            if "status" in ou_info:
                # TODO: This code should be removed once the 80-week magic number
                #       is eliminated, right now it serves to ensure solely that
                #       the SD integration does not crash due to the bad code.
                #       The solution will then be to fix all the issues using SDTool.
                logger.warning("Unable to apply NY-logic", org_unit)
                return org_unit

        if ou_info["org_unit_level"]["user_key"] in too_deep:
            self.create_association(org_unit, person_uuid, job_id, validity)

        # logger.debug('OU info is currently: {}'.format(ou_info))
        while ou_info["org_unit_level"]["user_key"] in too_deep:
            ou_info = ou_info["parent"]
            logger.debug("Parent unit: {}".format(ou_info))
        org_unit = ou_info["uuid"]

        return org_unit

    def create_new_engagement(self, engagement, status, cpr, person_uuid):
        """
        Create a new engagement
        AD integration handled in check for primary engagement.
        """

        # beware - name engagement_info used for engagement in engagement_components
        user_key, engagement_info = engagement_components(engagement)
        if not engagement_info["departments"] or not engagement_info["professions"]:
            # I am looking into the possibility that creating AND finishing
            # an engagement in the past gives the problem that the engagement
            # is reported to this function without the components needed to create
            # the engagement in os2mo

            # to fix the problem we get the information for the employment at the
            # activation date

            # use a local engagement copy so we don't spill into the rest of the program
            engagement = dict(engagement)

            activation_date_info = read_employment_at(
                employment_id=engagement["EmploymentIdentifier"],
                settings=self.settings,
                effective_date=datetime.datetime.strptime(
                    status["ActivationDate"], "%Y-%m-%d"
                ).date(),
            )

            # at least check the cpr

            if cpr != activation_date_info["PersonCivilRegistrationIdentifier"]:
                logger.error(
                    "wrong cpr for position %r at date %r",
                    engagement["EmploymentIdentifier"],
                    status["ActivationDate"],
                )
                raise ValueError("unexpected cpr, see log")

            activation_date_engagement = activation_date_info["Employment"]
            _, activation_date_engagement_info = engagement_components(
                activation_date_engagement
            )

            # fill out the missing values
            if not engagement_info["departments"]:
                engagement_info["departments"] = activation_date_engagement_info[
                    "departments"
                ]

            if not engagement_info["professions"]:
                engagement_info["professions"] = activation_date_engagement_info[
                    "professions"
                ]

        job_position = engagement_info["professions"][0]["JobPositionIdentifier"]

        if job_position in self.skip_job_functions:
            logger.info("Skipping {} due to job_pos_id".format(engagement))
            return None

        validity = self._validity(status)
        also_edit = False
        if (
            len(engagement_info["professions"]) > 1
            or len(engagement_info["working_time"]) > 1
            or len(engagement_info["departments"]) > 1
        ):
            also_edit = True
        logger.debug("Create new engagement: also_edit: {}".format(also_edit))

        try:
            org_unit = engagement_info["departments"][0]["DepartmentUUIDIdentifier"]
            logger.info("Org unit for new engagement: {}".format(org_unit))
            org_unit = self.apply_NY_logic(org_unit, user_key, validity, person_uuid)
        except IndexError:
            msg = "No unit for engagement {}".format(user_key)
            logger.error(msg)
            raise Exception(msg)

        try:
            emp_name = engagement_info["professions"][0]["EmploymentName"]
        except (KeyError, IndexError):
            emp_name = "Ukendt"

        job_function = emp_name
        if self.use_jpi:
            job_function = job_position

        primary = self.primary_types["non_primary"]
        if status["EmploymentStatusCode"] == "0":
            primary = self.primary_types["no_salary"]

        engagement_type = self.determine_engagement_type(engagement, job_position)
        if engagement_type is None:
            return False

        extension_field = self.settings.sd_employment_field
        extension = {}
        if extension_field is not None:
            extension = {extension_field: emp_name}

        job_function_uuid = self._fetch_professions(job_function, job_position)

        payload = sd_payloads.create_engagement(
            org_unit=org_unit,
            person_uuid=person_uuid,
            job_function=job_function_uuid,
            engagement_type=engagement_type,
            primary=primary,
            user_key=user_key,
            engagement_info=engagement_info,
            validity=validity,
            **extension,
        )
        response = self.helper._mo_post("details/create", payload)
        assert response.status_code == 201

        self._refresh_mo_engagements(person_uuid)
        logger.info("Engagement {} created".format(user_key))

        if also_edit:
            # This will take of the extra entries
            self.edit_engagement(engagement, person_uuid)

        return True

    def _terminate_engagement(
        self,
        user_key: str,
        person_uuid: str,  # TODO: change type to UUID
        from_date: str,  # TODO: Introduce MO date version
        to_date: Optional[str] = None,
    ) -> bool:
        """
        Terminate an employment (engagement) in MO. Since this function calls
        MO, the parameters are adapted to accommodate MO instead of SD Løn,
        i.e. SD dates should be converted to MO dates before the function is
        invoked.

        Args:
            user_key: SD Løn employment ID. Used as BVN (user_key) in MO.
            person_uuid: The employee UUID in MO.
            from_date: The MO "from" date (to be set in virkning).
            to_date: The MO "to" date (to be set in virkning).

        Returns:
            `True` if the termination in MO was successful and `False`
            otherwise
        """
        mo_engagement = self._find_engagement(user_key, person_uuid)

        if not mo_engagement:
            logger.warning(f"Terminating non-existing job: {user_key}!")
            return False

        validity = {"from": from_date, "to": to_date}

        # TODO: use/create termination object from RA Models
        payload = {
            "type": "engagement",
            "uuid": mo_engagement["uuid"],
            "validity": validity,
        }

        logger.debug("Terminate payload: {}".format(payload))
        response = self.helper._mo_post("details/terminate", payload)
        logger.debug("Terminate response: {}".format(response.text))
        mora_assert(response)

        self._refresh_mo_engagements(person_uuid)

        return True

    def edit_engagement_department(self, engagement, mo_eng, person_uuid):
        job_id, engagement_info = engagement_components(engagement)
        for department in engagement_info["departments"]:
            logger.info("Change department of engagement {}:".format(job_id))
            logger.debug("Department object: {}".format(department))

            validity = self._validity(department, mo_eng["validity"]["to"], cut=True)
            if validity is None:
                continue

            logger.debug("Validity of this department change: {}".format(validity))
            org_unit = department["DepartmentUUIDIdentifier"]
            if org_unit is None:
                logger.warning(
                    "DepartmentUUIDIdentifier was None, attempting GetDepartment"
                )
                # This code should not be necessary, but SD returns bad data.
                # Sometimes the UUID is missing, even if it can be looked up?
                url = "GetDepartment20111201"
                params = {
                    "ActivationDate": self.from_date.strftime("%d.%m.%Y"),
                    "DeactivationDate": self.from_date.strftime("%d.%m.%Y"),
                    "DepartmentNameIndicator": "true",
                    "UUIDIndicator": "true",
                    "DepartmentIdentifier": department["DepartmentIdentifier"],
                }
                response = sd_lookup(url, settings=self.settings, params=params)
                logger.warning("GetDepartment returned: {}".format(response))
                org_unit = response["Department"]["DepartmentUUIDIdentifier"]
                if org_unit is None:
                    logger.fatal("DepartmentUUIDIdentifier was None inside failover.")
                    sys.exit(1)

            associations = self.helper.read_user_association(person_uuid, read_all=True)
            logger.debug("User associations: {}".format(associations))
            current_association = None
            # TODO: This is a filter + next (only?)
            for association in associations:
                if association["user_key"] == job_id:
                    current_association = association["uuid"]

            if current_association:
                logger.debug("We need to move {}".format(current_association))
                data = {"org_unit": {"uuid": org_unit}, "validity": validity}
                payload = sd_payloads.association(data, current_association)
                logger.debug("Association edit payload: {}".format(payload))
                response = self.helper._mo_post("details/edit", payload)
                mora_assert(response)

            org_unit = self.apply_NY_logic(org_unit, job_id, validity, person_uuid)

            logger.debug("New org unit for edited engagement: {}".format(org_unit))
            data = {"org_unit": {"uuid": org_unit}, "validity": validity}
            payload = sd_payloads.engagement(data, mo_eng)
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

    def determine_engagement_type(self, engagement, job_position):
        split = self.settings.sd_monthly_hourly_divide
        employment_id = calc_employment_id(engagement)
        if employment_id["value"] < split:
            return self.engagement_types.get("månedsløn")
        # XXX: Is the first condition not implied by not hitting the above case?
        if (split - 1) < employment_id["value"] < 999999:
            return self.engagement_types.get("timeløn")
        # This happens if EmploymentID is not a number
        # XXX: Why are we checking against 999999 instead of checking the type?
        # Once we get here, we know that it is a no-salary employee

        # We should not create engagements (or engagement_types) for engagements
        # with too low of a job_position id compared to no_salary_minimum_id.
        if (
            self.no_salary_minimum is not None
            and int(job_position) < self.no_salary_minimum
        ):
            message = "No salary employee, with too low job_position id"
            logger.warning(message)
            return None

        # We need a special engagement type for the engagement.
        # We will try to fetch and try to create it if we cannot find it.
        logger.info("Non-nummeric id. Job pos id: {}".format(job_position))
        return self._fetch_engagement_type(job_position)

    def edit_engagement_type(self, engagement, mo_eng):
        job_id, engagement_info = engagement_components(engagement)
        for profession_info in engagement_info["professions"]:
            logger.info("Change engagement type of engagement {}".format(job_id))
            job_position = profession_info["JobPositionIdentifier"]

            validity = self._validity(
                profession_info, mo_eng["validity"]["to"], cut=True
            )
            if validity is None:
                continue

            engagement_type = self.determine_engagement_type(engagement, job_position)
            if engagement_type is None:
                continue
            data = {"engagement_type": {"uuid": engagement_type}, "validity": validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug("Update engagement type payload: {}".format(payload))
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

    def edit_engagement_profession(self, engagement, mo_eng):
        job_id, engagement_info = engagement_components(engagement)
        for profession_info in engagement_info["professions"]:
            logger.info("Change profession of engagement {}".format(job_id))
            job_position = profession_info["JobPositionIdentifier"]

            # The variability handling introduced in the following lines
            # (based on the value of job_position) is not optimal, i.e.
            # a parametric if-switch is used, where a strategy pattern would
            # be more appropriate. However, due to all the hard couplings in
            # the code, a strategy pattern is not feasible for now. Let's
            # leave it as is until the whole SD code base is rewritten

            if not is_employment_id_and_no_salary_minimum_consistent(
                engagement, self.no_salary_minimum
            ):
                sd_from_date = profession_info["ActivationDate"]
                sd_to_date = profession_info["DeactivationDate"]
                self._terminate_engagement(
                    mo_eng["user_key"],
                    mo_eng["person"]["uuid"],
                    sd_from_date,
                    sd_to_mo_termination_date(sd_to_date),
                )
            else:
                emp_name = profession_info.get("EmploymentName", job_position)
                validity = self._validity(
                    profession_info, mo_eng["validity"]["to"], cut=True
                )
                if validity is None:
                    continue

                job_function = emp_name
                if self.use_jpi:
                    job_function = job_position
                logger.debug("Employment name: {}".format(job_function))

                ext_field = self.settings.sd_employment_field
                extention = {}
                if ext_field is not None:
                    extention = {ext_field: emp_name}

                job_function_uuid = self._fetch_professions(job_function, job_position)

                data = {
                    "job_function": {"uuid": job_function_uuid},
                    "validity": validity,
                }
                data.update(extention)
                payload = sd_payloads.engagement(data, mo_eng)
                logger.debug("Update profession payload: {}".format(payload))

                response = self.helper._mo_post("details/edit", payload)
                mora_assert(response)

    def edit_engagement_worktime(self, engagement, mo_eng):
        job_id, engagement_info = engagement_components(engagement)
        for worktime_info in engagement_info["working_time"]:
            logger.info("Change working time of engagement {}".format(job_id))

            validity = self._validity(worktime_info, mo_eng["validity"]["to"], cut=True)
            if validity is None:
                continue

            working_time = float(worktime_info["OccupationRate"])
            data = {"fraction": int(working_time * 1000000), "validity": validity}
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug("Change worktime, payload: {}".format(payload))
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

    def _set_non_primary(self, status, mo_eng):
        logger.debug("Setting non-primary for: {}".format(mo_eng["uuid"]))

        validity = self._validity(status)
        logger.debug("Validity for edit: {}".format(validity))

        data = {
            "primary": {"uuid": self.primary_types["non_primary"]},
            "validity": validity,
        }
        payload = sd_payloads.engagement(data, mo_eng)
        logger.debug("Setting non-primary payload: {}".format(payload))

        response = self.helper._mo_post("details/edit", payload)
        mora_assert(response)

    def edit_engagement(self, engagement, person_uuid):
        """
        Edit an engagement
        """
        employment_id, engagement_info = engagement_components(engagement)
        mo_eng = self._find_engagement(employment_id, person_uuid)

        employment_consistent = is_employment_id_and_no_salary_minimum_consistent(
            engagement, self.no_salary_minimum
        )

        if mo_eng is None:
            if employment_consistent:
                create_engagement(self, employment_id, person_uuid)
            return

        update_existing_engagement(self, mo_eng, engagement, person_uuid)

    def _handle_employment_status_changes(
        self, cpr: str, sd_employment: OrderedDict, person_uuid: str
    ) -> bool:
        """
        Update MO with SD employment changes.

        Args:
            cpr: The CPR number of the person.
            sd_employment: The SD employment (see example below).
            person_uuid: The UUID of the MO employee.

        Returns:
            `True` if an employment CRUD operation has been executed and
            `False` otherwise.

        Examples:
            The sd_employment could for example look like this:
                ```python
                OrderedDict([
                    ('EmploymentIdentifier', '12345'),
                    ('EmploymentDate', '2020-11-10'),
                    ('EmploymentDepartment', OrderedDict([
                        ('@changedAtDate', '2020-11-10'),
                        ('ActivationDate', '2020-11-10'),
                        ('ActivationTime', '06:00'),
                        ('DeactivationDate', '9999-12-31'),
                        ('DepartmentIdentifier', 'department_id'),
                        ('DepartmentUUIDIdentifier', 'department_uuid')
                    ])),
                    ('Profession', OrderedDict([
                        ('@changedAtDate', '2020-11-10'),
                        ('ActivationDate', '2020-11-10'),
                        ('ActivationTime', '06:00'),
                        ('DeactivationDate', '9999-12-31'),
                        ('JobPositionIdentifier', '1'),
                        ('EmploymentName', 'chief'),
                        ('AppointmentCode', '0')
                    ])),
                    ('EmploymentStatus', [
                        OrderedDict([
                            ('@changedAtDate', '2020-11-10'),
                            ('ActivationDate', '2020-11-10'),
                            ('ActivationTime', '06:00'),
                            ('DeactivationDate', '2021-02-09'),
                            ('EmploymentStatusCode', '1')
                        ]),
                        OrderedDict([
                            ('@changedAtDate', '2020-11-10'),
                            ('ActivationDate', '2021-02-10'),
                            ('ActivationTime', '06:00'),
                            ('DeactivationDate', '9999-12-31'),
                            ('EmploymentStatusCode', '8')
                        ])
                    ])
                ])
                ```
        """

        skip = False
        # The EmploymentStatusCode can take a number of magical values.
        # that must be handled separately.
        employment_id, eng = engagement_components(sd_employment)
        for status in eng["status_list"]:
            logger.info("Status is: {}".format(status))
            code = status["EmploymentStatusCode"]
            code = EmploymentStatus(code)

            if code == EmploymentStatus.AnsatUdenLoen:
                logger.info(
                    "Status 0. Cpr: {}, job: {}".format(
                        f"{cpr[:6]}-xxxx", employment_id
                    )
                )
                mo_eng = self._find_engagement(employment_id, person_uuid)
                if mo_eng:
                    logger.info("Status 0, edit eng {}".format(mo_eng["uuid"]))

                    self._set_non_primary(status, mo_eng)

                    self.edit_engagement(sd_employment, person_uuid)
                else:
                    logger.info("Status 0, create new engagement")
                    if is_employment_id_and_no_salary_minimum_consistent(
                        sd_employment, self.no_salary_minimum
                    ):
                        self.create_new_engagement(
                            sd_employment, status, cpr, person_uuid
                        )
                skip = True
            elif code == EmploymentStatus.AnsatMedLoen:
                logger.info("Setting {} to status 1".format(employment_id))
                mo_eng = self._find_engagement(employment_id, person_uuid)
                if mo_eng:
                    logger.info("Status 1, edit eng. {}".format(mo_eng["uuid"]))

                    self._set_non_primary(status, mo_eng)
                    self._refresh_mo_engagements(person_uuid)

                    self.edit_engagement(sd_employment, person_uuid)
                else:
                    logger.info("Status 1: Create new engagement")
                    logger.debug(sd_employment)
                    if is_employment_id_and_no_salary_minimum_consistent(
                        sd_employment, self.no_salary_minimum
                    ):
                        self.create_new_engagement(
                            sd_employment, status, cpr, person_uuid
                        )
                skip = True
            elif code == EmploymentStatus.Orlov:
                mo_eng = self._find_engagement(employment_id, person_uuid)
                if not mo_eng:
                    logger.info("Leave for non existent eng., create one")
                    if is_employment_id_and_no_salary_minimum_consistent(
                        sd_employment, self.no_salary_minimum
                    ):
                        self.create_new_engagement(
                            sd_employment, status, cpr, person_uuid
                        )
                logger.info("Create a leave for {} ".format(cpr))
                self.create_leave(status, employment_id, person_uuid)
            elif code in EmploymentStatus.let_go():
                sd_from_date = status["ActivationDate"]
                sd_to_date = status["DeactivationDate"]
                logger.info(
                    "Terminate {}, job_id {} ".format(f"{cpr[:6]}-xxxx", employment_id)
                )
                success = self._terminate_engagement(
                    user_key=employment_id,
                    person_uuid=person_uuid,
                    from_date=sd_from_date,
                    to_date=sd_to_mo_termination_date(sd_to_date),
                )
                if not success:
                    logger.error("Problem with job-id: {}".format(employment_id))
                    skip = True
            elif code == EmploymentStatus.Slettet:

                # TODO: rename user_key to something unique in MO when employee
                # is terminated.
                #
                # The reason for this is that SD Løn sometimes reuses the same
                # job_id for *different* persons, e.g. if a person with
                # job_id=12345 is set to "Slettet" then a different newly
                # employed SD person can get the SAME job_id!
                #
                # In MO we therefore have to do the following. When a MO person
                # is terminated, we have to make sure the the user_key (BVN) of
                # that user is changed to some unique, e.g. "old user_key +
                # UUID (or date)". In that way we can avoid user_key conflicts
                # between different employees.
                #
                # Note that an SD person can jump from any status to "Slettet"

                for mo_eng in self._fetch_mo_engagements(person_uuid):
                    if mo_eng["user_key"] == employment_id:
                        sd_from_date = status["ActivationDate"]
                        logger.info("Status S: Terminate {}".format(employment_id))
                        self._terminate_engagement(
                            user_key=employment_id,
                            person_uuid=person_uuid,
                            from_date=sd_from_date,
                        )
                skip = True
        return skip

    def _update_user_employments(
        self, cpr: str, sd_employments, person_uuid: str
    ) -> None:
        for sd_employment in sd_employments:
            job_id, eng = engagement_components(sd_employment)
            logger.info("Update Job id: {}".format(job_id))
            logger.debug("SD Engagement: {}".format(sd_employment))
            # If status is present, we have a potential creation
            if eng["status_list"] and self._handle_employment_status_changes(
                cpr, sd_employment, person_uuid
            ):
                continue
            self.edit_engagement(sd_employment, person_uuid)

    def update_all_employments(
        self, in_cpr: Optional[str] = None, dry_run: bool = False
    ) -> None:
        if in_cpr is not None:
            employments_changed = self.read_employment_changed(in_cpr=in_cpr)
        else:
            logger.info("Update all employments")
            employments_changed = self.read_employment_changed()

        logger.info("Update a total of {} employments".format(len(employments_changed)))

        employments_changed = tqdm(employments_changed, desc="update employments")
        employments_changed = filter(skip_fictional_users, employments_changed)

        # Filter employees based on the sd_cprs list
        employments_changed = filter(cpr_env_filter, employments_changed)

        recalculate_users: Set[UUID] = set()

        for employment in employments_changed:
            cpr = employment["PersonCivilRegistrationIdentifier"]
            sd_employments = ensure_list(employment["Employment"])

            logger.info("---------------------")
            logger.info("We are now updating {}".format(f"{cpr[:6]}-xxxx"))
            logger.debug("From date: {}".format(self.from_date))
            logger.debug("To date: {}".format(self.to_date))
            logger.debug("Employment: {}".format(employment))

            mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            # Person not in MO, but they should be
            if not mo_person:
                logger.warning("This person should be in MO, but is not")
                try:
                    self.update_changed_persons(in_cpr=cpr, dry_run=dry_run)
                    mo_person = self.helper.read_user(
                        user_cpr=cpr, org_uuid=self.org_uuid
                    )
                except Exception as exp:
                    logger.error("Unable to find person in MO, SD error: " + str(exp))
                    continue

            if not mo_person:
                logger.warning("MO person not set!!")
                continue
            person_uuid = mo_person["uuid"]

            self._refresh_mo_engagements(person_uuid)
            if dry_run:
                print("Dry-run: update_user_employments", sd_employments, person_uuid)
            else:
                self._update_user_employments(cpr, sd_employments, person_uuid)
            # Re-calculate primary after all updates for user has been performed.
            recalculate_users.add(person_uuid)

        if self.updater is None:
            return

        logger.info("Beginning recalculation of all users...")
        for user_uuid in recalculate_users:
            if dry_run:
                print("Dry-run: recalculate_user", user_uuid)
                continue

            try:
                self.updater.recalculate_user(user_uuid)
            except NoPrimaryFound:
                logger.warning("Could not find primary for: {}".format(user_uuid))


def _local_db_insert(path_to_run_db, insert_tuple):
    conn = sqlite3.connect(
        path_to_run_db,
        detect_types=sqlite3.PARSE_DECLTYPES,
    )
    c = conn.cursor()
    query = "INSERT INTO runs (from_date, to_date, status) VALUES (?, ?, ?)"
    final_tuple = (
        insert_tuple[0],
        insert_tuple[1],
        insert_tuple[2].format(datetime.datetime.now()),
    )
    c.execute(query, final_tuple)
    conn.commit()
    conn.close()


def initialize_changed_at(from_date, run_db, force=False):
    if not run_db.is_file():
        raise Exception("RunDB not created, use 'db_overview.py' to create it")

    settings = get_changed_at_settings()
    _local_db_insert(
        settings.sd_import_run_db, (from_date, from_date, "Running since {}")
    )

    logger.info("Start initial ChangedAt")
    sd_updater = ChangeAtSD(settings, from_date)
    sd_updater.update_changed_persons()
    sd_updater.update_all_employments()
    logger.info("Ended initial ChangedAt")

    _local_db_insert(
        settings.sd_import_run_db, (from_date, from_date, "Initial import: {}")
    )


def get_from_date(run_db, force: bool = False) -> datetime.datetime:
    db_overview = DBOverview(run_db)
    # To date from last entries, becomes from_date for current entry
    from_date, status = cast(
        Tuple[datetime.datetime, str], db_overview._read_last_line("to_date", "status")
    )
    if "Running" in status:
        if force:
            db_overview.delete_last_row()
            from_date, status = cast(
                Tuple[datetime.datetime, str],
                db_overview._read_last_line("to_date", "status"),
            )
        else:
            logging.error("Previous ChangedAt run did not return!")
            raise click.ClickException("Previous ChangedAt run did not return!")
    return from_date


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--init",
    is_flag=True,
    type=click.BOOL,
    default=False,
    help="Initialize a new rundb",
)
@click.option(
    "--force",
    is_flag=True,
    type=click.BOOL,
    default=False,
    help="Ignore previously unfinished runs",
)
@click.option(
    "--from-date",
    type=click.DateTime(),
    help="Global import from-date, only used if init is True",
)
def changed_at_cli(init: bool, force: bool, from_date: datetime.datetime):
    """Tool to delta synchronize with MO with SD."""
    changed_at(init, force, from_date)


def changed_at(init: bool, force: bool, from_date: Optional[datetime.datetime] = None):
    """Tool to delta synchronize with MO with SD."""
    settings = get_changed_at_settings()
    settings.start_logging_based_on_settings()

    run_db = settings.sd_import_run_db

    if settings.sentry_dsn:
        sentry_sdk.init(dsn=settings.sentry_dsn)

    logger.info("***************")
    logger.info("Program started")

    if init:
        if not from_date:
            from_date = date_to_datetime(settings.sd_global_from_date)
        run_db_path = pathlib.Path(run_db)

        initialize_changed_at(from_date, run_db_path, force=True)
        exit()

    from_date = get_from_date(run_db, force=force)
    to_date = datetime.datetime.now()
    dates = gen_date_intervals(from_date, to_date)
    for from_date, to_date in dates:
        logger.info("Importing {} to {}".format(from_date, to_date))
        _local_db_insert(
            settings.sd_import_run_db, (from_date, to_date, "Running since {}")
        )

        logger.info("Start ChangedAt module")
        sd_updater = ChangeAtSD(settings, from_date, to_date)  # type: ignore

        logger.info("Update changed persons")
        sd_updater.update_changed_persons()

        logger.info("Update all employments")
        sd_updater.update_all_employments()

        _local_db_insert(
            settings.sd_import_run_db, (from_date, to_date, "Update finished: {}")
        )

        logger.info("Program stopped.")


@cli.command()
@click.option(
    "--cpr",
    required=True,
    type=click.STRING,
    help="CPR number of the person to import",
)
@click.option(
    "--from-date",
    type=click.DateTime(),
    help="Global import from-date",
)
@click.option(
    "--dry-run", is_flag=True, default=False, help="Dry-run making no actual changes."
)
def import_single_user(cpr: str, from_date: datetime.datetime, dry_run: bool):
    """Import a single user into MO."""

    settings = get_changed_at_settings()
    if not from_date:
        from_date = date_to_datetime(settings.sd_global_from_date)

    sd_updater = ChangeAtSD(settings, from_date, None)
    sd_updater.update_changed_persons(cpr, dry_run=dry_run)
    sd_updater.update_all_employments(cpr, dry_run=dry_run)


if __name__ == "__main__":
    cli()
