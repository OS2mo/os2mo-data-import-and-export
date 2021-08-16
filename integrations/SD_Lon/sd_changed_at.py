import datetime
import logging
import pathlib
import sqlite3
import sys
from functools import lru_cache
from operator import itemgetter
from typing import Tuple

import click
import pandas as pd
import requests
from more_itertools import last
from more_itertools import pairwise
from more_itertools import only
from os2mo_helpers.mora_helpers import MoraHelper
from tqdm import tqdm

from integrations import cpr_mapper
from integrations.ad_integration import ad_reader
from integrations.calculate_primary.common import LOGGER_NAME
from integrations.calculate_primary.common import NoPrimaryFound
from integrations.calculate_primary.sd import SDPrimaryEngagementUpdater
from integrations.SD_Lon import exceptions
from integrations.SD_Lon import sd_payloads
from integrations.SD_Lon.db_overview import DBOverview
from integrations.SD_Lon.fix_departments import FixDepartments
from integrations.SD_Lon.sd_common import calc_employment_id
from integrations.SD_Lon.sd_common import EmploymentStatus
from integrations.SD_Lon.sd_common import LetGo
from integrations.SD_Lon.sd_common import load_settings
from integrations.SD_Lon.sd_common import mora_assert
from integrations.SD_Lon.sd_common import primary_types
from integrations.SD_Lon.sd_common import sd_lookup
from integrations.SD_Lon.sync_job_id import JobIdSync

# from integrations.SD_Lon.sd_common import generate_uuid


LOG_LEVEL = logging.DEBUG
LOG_FILE = "mo_integrations.log"

logger = logging.getLogger("sdChangedAt")


def ensure_list(element):
    if not isinstance(element, list):
        return [element]
    return element


def setup_logging():
    detail_logging = (
        "sdCommon",
        "sdChangedAt",
        LOGGER_NAME,
        "fixDepartments",
        "sdSyncJobId",
    )
    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )


# TODO: SHOULD WE IMPLEMENT PREDICTABLE ENGAGEMENT UUIDS ALSO IN THIS CODE?!?


class ChangeAtSD:
    def __init__(self, from_date, to_date=None, settings=None):
        self.settings = settings or load_settings()

        if self.settings["integrations.SD_Lon.job_function"] == "JobPositionIdentifier":
            logger.info("Read settings. JobPositionIdentifier for job_functions")
            self.use_jpi = True
        elif self.settings["integrations.SD_Lon.job_function"] == "EmploymentName":
            logger.info("Read settings. Do not update job_functions")
            self.use_jpi = False
        else:
            raise exceptions.JobfunctionSettingsIsWrongException()

        self.employee_forced_uuids = self._read_forced_uuids()
        self.department_fixer = FixDepartments()
        self.helper = self._get_mora_helper(self.settings["mora.base"])
        self.job_sync = self._get_job_sync(self.settings)

        # List of job_functions that should be ignored.
        self.skip_job_functions = self.settings.get("skip_job_functions", [])
        self.use_ad = self.settings.get("integrations.SD_Lon.use_ad_integration", True)

        try:
            self.org_uuid = self.helper.read_organisation()
        except requests.exceptions.RequestException as e:
            logger.error(e)
            print(e)
            exit()

        self.updater = SDPrimaryEngagementUpdater()
        self.from_date = from_date
        self.to_date = to_date

        self.mo_person = None  # Updated continously with the person currently
        self.mo_engagement = None  # being processed.

        self.primary_types = primary_types(self.helper)

        logger.info("Read job_functions")
        facet_info = self.helper.read_classes_in_facet("engagement_job_function")
        job_functions = facet_info[0]
        self.job_function_facet = facet_info[1]
        # Map from user-key to uuid if jpi, name to uuid otherwise
        job_function_mapper = itemgetter("name", "uuid")
        if self.use_jpi:
            job_function_mapper = itemgetter("user_key", "uuid")
        self.job_functions = dict(map(job_function_mapper, job_functions))

        logger.info("Read engagement types")
        # The Opus diff-import contains a slightly more abstrac def to do this
        engagement_types = self.helper.read_classes_in_facet("engagement_type")
        self.engagement_type_facet = engagement_types[1]
        self.engagement_types = dict(
            map(itemgetter("user_key", "uuid"), engagement_types[0])
        )

        logger.info("Read leave types")
        facet_info = self.helper.read_classes_in_facet("leave_type")
        self.leave_uuid = facet_info[0][0]["uuid"]
        facet_info = self.helper.read_classes_in_facet("association_type")
        self.association_uuid = facet_info[0][0]["uuid"]

    def _get_mora_helper(self, mora_base):
        return MoraHelper(hostname=mora_base, use_cache=False)

    def _get_job_sync(self, settings):
        return JobIdSync(settings)

    def _read_forced_uuids(self):
        cpr_map = pathlib.Path.cwd() / "settings" / "cpr_uuid_map.csv"
        if not cpr_map.is_file():
            logger.error("Did not find cpr mapping")
            raise Exception("Did not find cpr mapping")

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

    def _fetch_ad_information(self, cpr) -> Tuple[str, str]:
        ad_reader = self._get_ad_reader()
        if ad_reader is None:
            return None, None

        ad_info = ad_reader.read_user(cpr=cpr)
        object_guid = ad_info.get("ObjectGuid", None)
        sam_account_name = ad_info.get("SamAccountName", None)
        return sam_account_name, object_guid

    @lru_cache(maxsize=None)
    def _fetch_ad_it_system_uuid(self):
        if self.use_ad:
            raise ValueError("_fetch_ad_it_system_uuid called without AD enabled")
        it_systems = self.helper.read_it_systems()
        return one(map(itemgetter('uuid'), filter(
            lambda system: system["name"] == "Active Directory",
            it_systems
        )))

    @lru_cache(maxsize=None)
    def read_employment_changed(
        self, from_date=None, to_date=None, employment_identifier=None
    ):
        from_date = from_date or self.from_date
        to_date = to_date or self.to_date

        params = {
            "ActivationDate": from_date.strftime("%d.%m.%Y"),
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

        if to_date is not None:
            url = "GetEmploymentChangedAtDate20111201"
            params.update(
                {
                    "DeactivationDate": to_date.strftime("%d.%m.%Y"),
                    "StatusActiveIndicator": "true",
                    "StatusPassiveIndicator": "true",
                }
            )
        else:
            url = "GetEmploymentChanged20111201"
            params.update(
                {
                    "DeactivationDate": "31.12.9999",
                }
            )
        response = sd_lookup(url, params)

        employment_response = ensure_list(response.get("Person", []))

        return employment_response

    def read_person_changed(self):
        deactivate_date = "31.12.9999"
        if self.to_date:
            deactivate_date = self.to_date.strftime("%d.%m.%Y")
        params = {
            "ActivationDate": self.from_date.strftime("%d.%m.%Y"),
            "DeactivationDate": deactivate_date,
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "true",
            "ContactInformationIndicator": "false",
            "PostalAddressIndicator": "false"
            # TODO: Er der kunder, som vil udlæse adresse-information?
        }
        url = "GetPersonChangedAtDate20111201"
        response = sd_lookup(url, params=params)
        person_changed = ensure_list(response.get("Person", []))
        return person_changed

    def read_person(self, cpr):
        params = {
            "EffectiveDate": self.from_date.strftime("%d.%m.%Y"),
            "PersonCivilRegistrationIdentifier": cpr,
            "StatusActiveIndicator": "True",
            "StatusPassiveIndicator": "false",
            "ContactInformationIndicator": "false",
            "PostalAddressIndicator": "false",
        }
        url = "GetPerson20111201"
        response = sd_lookup(url, params=params)
        person = ensure_list(response.get("Person", []))
        return person

    def update_changed_persons(self, cpr=None):
        # Ansættelser håndteres af update_employment, så vi tjekker for ændringer i
        # navn og opdaterer disse poster. Nye personer oprettes.
        if cpr is not None:
            person_changed = self.read_person(cpr)
        else:
            person_changed = self.read_person_changed()

        logger.info("Number of changed persons: {}".format(len(person_changed)))

        for person in tqdm(person_changed, desc="update persons"):
            cpr = person["PersonCivilRegistrationIdentifier"]
            logger.debug("Updating: {}".format(cpr))
            if cpr[-4:] == "0000":
                logger.warning("Skipping fictional user: {}".format(cpr))
                continue

            old_values = mo_person = self.helper.read_user(
                user_cpr=cpr, org_uuid=self.org_uuid
            )
            old_values = old_values or {}

            given_name = person.get("PersonGivenName", old_values.get("givenname", ""))
            sur_name = person.get("PersonSurnameName", old_values.get("surname", ""))
            sd_name = " ".join([given_name, sur_name])
            if mo_person and mo_person["name"] == sd_name:
                continue

            sam_account_name, object_guid = self._fetch_ad_information(cpr)

            uuid = None
            if mo_person:
                uuid = mo_person["uuid"]
            else:
                uuid = self.employee_forced_uuids.get(cpr)
                logger.info("Employee in force list: {} {}".format(cpr, uuid))
                if uuid is None:
                    if object_guid:
                        uuid = ad_info["ObjectGuid"]
                        msg = "Using ObjectGuid as MO UUID: {}"
                        logger.debug(msg.format(uuid))
                    else:
                        msg = "{} not in MO, UUID list or AD, assign random uuid"
                        logger.debug(msg.format(cpr))

            payload = {
                "givenname": given_name,
                "surname": sur_name,
                "cpr_no": cpr,
                "org": {"uuid": self.org_uuid},
            }

            if uuid:
                payload["uuid"] = uuid

            return_uuid = self.helper._mo_post("e/create", payload).json()
            logger.info(
                "Created or updated employee {} with uuid {}".format(
                    sd_name, return_uuid
                )
            )

            if (not mo_person) and sam_account_name:
                payload = sd_payloads.connect_it_system_to_user(
                    sam_account_name, self._fetch_ad_it_system_uuid(), return_uuid
                )
                logger.debug("Connect it-system: {}".format(payload))
                response = self.helper._mo_post("details/create", payload)
                assert response.status_code == 201
                logger.info("Added AD account info to {}".format(cpr))

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

    def _find_engagement(self, job_id):
        try:
            user_key = str(int(job_id)).zfill(5)
        except ValueError:  # We will end here, if int(job_id) fails
            user_key = job_id

        logger.debug(
            "Find engagement, from date: {}, user_key: {}".format(
                self.from_date, user_key
            )
        )

        relevant_engagements = filter(
            lambda mo_eng: mo_eng["user_key"] == user_key, self.mo_engagement
        )
        relevant_engagement = last(relevant_engagements, None)

        if relevant_engagement is None:
            msg = "Fruitlessly searched for {} in {}".format(job_id, self.mo_engagement)
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
            url=self.settings["mox.base"] + "/klassifikation/klasse", json=payload
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

    def engagement_components(self, engagement_info):
        job_id = engagement_info["EmploymentIdentifier"]

        components = {}
        status_list = ensure_list(engagement_info.get("EmploymentStatus", []))
        components["status_list"] = status_list

        professions = ensure_list(engagement_info.get("Profession", []))
        components["professions"] = professions

        departments = ensure_list(engagement_info.get("EmploymentDepartment", []))
        components["departments"] = departments

        working_time = ensure_list(engagement_info.get("WorkingTime", []))
        components["working_time"] = working_time

        # Employment date is not used for anyting
        # components['employment_date'] = engagement_info.get('EmploymentDate')
        return job_id, components

    def create_leave(self, status, job_id):
        """Create a leave for a user"""
        logger.info("Create leave, job_id: {}, status: {}".format(job_id, status))
        # TODO: This code potentially creates duplicated leaves.

        # Notice, the expected and desired behaviour for leaves is for the engagement
        # to continue during the leave. It turns out this is actually what happens
        # because a leave is apparently always accompanied by a worktime-update that
        # forces an edit to the engagement that will extend it to span the
        # leave. If this ever turns out not to hold, add a dummy-edit to the
        # engagement here.
        mo_eng = self._find_engagement(job_id)
        payload = sd_payloads.create_leave(
            mo_eng, self.mo_person, self.leave_uuid, job_id, self._validity(status)
        )

        response = self.helper._mo_post("details/create", payload)
        assert response.status_code == 201

    def create_association(self, department, person, job_id, validity):
        """Create a association for a user"""
        logger.info("Consider to create an association")
        associations = self.helper.read_user_association(
            person["uuid"], read_all=True, only_primary=True
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
                department, person, self.association_uuid, job_id, validity
            )
            response = self.helper._mo_post("details/create", payload)
            assert response.status_code == 201
        else:
            logger.info("No new Association is needed")

    def apply_NY_logic(self, org_unit, job_id, validity):
        msg = "Apply NY logic for job: {}, unit: {}, validity: {}"
        logger.debug(msg.format(job_id, org_unit, validity))
        too_deep = self.settings["integrations.SD_Lon.import.too_deep"]
        # Move users and make associations according to NY logic
        today = datetime.datetime.today()
        ou_info = self.helper.read_ou(org_unit, use_cache=False)
        if "status" in ou_info:
            # This unit does not exist, read its state in the not-too
            # distant future.
            fix_date = today + datetime.timedelta(weeks=80)
            self.department_fixer.fix_or_create_branch(org_unit, fix_date)
            ou_info = self.helper.read_ou(org_unit, use_cache=False)

        if ou_info["org_unit_level"]["user_key"] in too_deep:
            self.create_association(org_unit, self.mo_person, job_id, validity)

        # logger.debug('OU info is currently: {}'.format(ou_info))
        while ou_info["org_unit_level"]["user_key"] in too_deep:
            ou_info = ou_info["parent"]
            logger.debug("Parent unit: {}".format(ou_info))
        org_unit = ou_info["uuid"]

        return org_unit

    def read_employment_at(self, EmploymentIdentifier, EffectiveDate):
        url = "GetEmployment20111201"
        params = {
            "EmploymentIdentifier": EmploymentIdentifier,
            "EffectiveDate": EffectiveDate.strftime("%d.%m.%Y"),
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "true",
            "DepartmentIndicator": "true",
            "EmploymentStatusIndicator": "true",
            "ProfessionIndicator": "true",
            "WorkingTimeIndicator": "true",
            "UUIDIndicator": "true",
            "SalaryAgreementIndicator": "false",
            "SalaryCodeGroupIndicator": "false",
        }
        response = sd_lookup(url, params=params)
        return response["Person"]

    def create_new_engagement(self, engagement, status, cpr):
        """
        Create a new engagement
        AD integration handled in check for primary engagement.
        """
        # beware - name engagement_info used for engagement in engagement_components
        user_key, engagement_info = self.engagement_components(engagement)
        if not engagement_info["departments"] or not engagement_info["professions"]:

            # I am looking into the possibility that creating AND finishing
            # an engagement in the past gives the problem that the engagement
            # is reported to this function without the components needed to create
            # the engagement in os2mo

            # to fix the problem we get the information for the employment at the
            # activation date

            # use a local engagement copy so we don't spill into the rest of the program
            engagement = dict(engagement)

            activation_date_info = self.read_employment_at(
                engagement["EmploymentIdentifier"],
                datetime.datetime.strptime(status["ActivationDate"], "%Y-%m-%d").date(),
            )

            # at least check the cpr

            if cpr != activation_date_info["PersonCivilRegistrationIdentifier"]:
                logger.error(
                    "wrong cpr %r for position %r at date %r",
                    activation_date_info["PersonCivilRegistrationIdentifier"],
                    engagement["EmploymentIdentifier"],
                    status["ActivationDate"],
                )
                raise ValueError("unexpected cpr, see log")

            activation_date_engagement = activation_date_info["Employment"]
            _, activation_date_engagement_info = self.engagement_components(
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
            org_unit = self.apply_NY_logic(org_unit, user_key, validity)
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

        extension_field = self.settings.get("integrations.SD_Lon.employment_field")
        extension = {}
        if extension_field is not None:
            extension = {extension_field: emp_name}

        job_function_uuid = self._fetch_professions(job_function, job_position)

        payload = sd_payloads.create_engagement(
            org_unit=org_unit,
            mo_person=self.mo_person,
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

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person["uuid"], read_all=True, only_primary=True, use_cache=False
        )
        logger.info("Engagement {} created".format(user_key))

        if also_edit:
            # This will take of the extra entries
            self.edit_engagement(engagement)

        return True

    def _terminate_engagement(self, from_date, job_id):
        mo_engagement = self._find_engagement(job_id)

        if not mo_engagement:
            logger.warning("Terminating non-existing job: {}!".format(job_id))
            return False

        # In MO, the termination date is the last day of work,
        # in SD it is the first day of non-work.
        date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
        terminate_datetime = date - datetime.timedelta(days=1)
        terminate_date = terminate_datetime.strftime("%Y-%m-%d")

        payload = {
            "type": "engagement",
            "uuid": mo_engagement["uuid"],
            "validity": {"to": terminate_date},
        }

        logger.debug("Terminate payload: {}".format(payload))
        response = self.helper._mo_post("details/terminate", payload)
        logger.debug("Terminate response: {}".format(response.text))
        mora_assert(response)

        self.mo_engagement = self.helper.read_user_engagement(
            self.mo_person["uuid"], read_all=True, only_primary=True, use_cache=False
        )

        return True

    def _edit_engagement_department(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
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
                response = sd_lookup(url, params)
                logger.warning("GetDepartment returned: {}".format(response))
                org_unit = response["Department"]["DepartmentUUIDIdentifier"]
                if org_unit is None:
                    logger.fatal("DepartmentUUIDIdentifier was None inside failover.")
                    sys.exit(1)

            associations = self.helper.read_user_association(
                self.mo_person["uuid"], read_all=True
            )
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

            org_unit = self.apply_NY_logic(org_unit, job_id, validity)

            logger.debug("New org unit for edited engagement: {}".format(org_unit))
            data = {"org_unit": {"uuid": org_unit}, "validity": validity}
            payload = sd_payloads.engagement(data, mo_eng)
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

    def determine_engagement_type(self, engagement, job_position):
        split = self.settings["integrations.SD_Lon.monthly_hourly_divide"]
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
        no_salary_minimum = self.settings.get(
            "integrations.SD_Lon.no_salary_minimum_id", None
        )
        if no_salary_minimum is not None and int(job_position) < no_salary_minimum:
            message = "No salary employee, with too low job_position id"
            logger.warning(message)
            return None

        # We need a special engagement type for the engagement.
        # We will try to fetch and try to create it if we cannot find it.
        logger.info("Non-nummeric id. Job pos id: {}".format(job_position))
        return self._fetch_engagement_type(job_position)

    def _edit_engagement_type(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
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

    def _edit_engagement_profession(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
        for profession_info in engagement_info["professions"]:
            logger.info("Change profession of engagement {}".format(job_id))
            job_position = profession_info["JobPositionIdentifier"]
            emp_name = profession_info["JobPositionIdentifier"]
            if "EmploymentName" in profession_info:
                emp_name = profession_info["EmploymentName"]
            validity = self._validity(
                profession_info, mo_eng["validity"]["to"], cut=True
            )
            if validity is None:
                continue

            job_function = emp_name
            if self.use_jpi:
                job_function = job_position
            logger.debug("Employment name: {}".format(job_function))

            ext_field = self.settings.get("integrations.SD_Lon.employment_field")
            extention = {}
            if ext_field is not None:
                extention = {ext_field: emp_name}

            job_function_uuid = self._fetch_professions(job_function, job_position)

            data = {"job_function": {"uuid": job_function_uuid}, "validity": validity}
            data.update(extention)
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug("Update profession payload: {}".format(payload))
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

    def _edit_engagement_worktime(self, engagement, mo_eng):
        job_id, engagement_info = self.engagement_components(engagement)
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

    def edit_engagement(self, engagement, validity=None, status0=False):
        """
        Edit an engagement
        """
        job_id, engagement_info = self.engagement_components(engagement)

        mo_eng = self._find_engagement(job_id)
        if not mo_eng:
            # Should have been created at an earlier status-code
            logger.error("Engagement {} has never existed!".format(job_id))
            return

        validity = validity or mo_eng["validity"]

        if status0:
            logger.info("Setting {} to status0".format(job_id))
            data = {
                "primary": {"uuid": self.primary_types["non_primary"]},
                "validity": validity,
            }
            payload = sd_payloads.engagement(data, mo_eng)
            logger.debug("Status0 payload: {}".format(payload))
            response = self.helper._mo_post("details/edit", payload)
            mora_assert(response)

        self._edit_engagement_department(engagement, mo_eng)
        self._edit_engagement_profession(engagement, mo_eng)
        self._edit_engagement_type(engagement, mo_eng)
        self._edit_engagement_worktime(engagement, mo_eng)

    def _handle_status_changes(self, cpr, engagement):
        skip = False
        # The EmploymentStatusCode can take a number of magical values.
        # that must be handled seperately.
        job_id, eng = self.engagement_components(engagement)
        for status in eng["status_list"]:
            logger.info("Status is: {}".format(status))
            code = status["EmploymentStatusCode"]

            if code not in ("0", "1", "3", "7", "8", "9", "S"):
                logger.error("Unknown status code {}!".format(status))
                raise ValueError("Unknown status code")

            if status["EmploymentStatusCode"] == "0":
                logger.info("Status 0. Cpr: {}, job: {}".format(cpr, job_id))
                mo_eng = self._find_engagement(job_id)
                if mo_eng:
                    logger.info("Status 0, edit eng {}".format(mo_eng["uuid"]))
                    self.edit_engagement(engagement, status0=True)
                else:
                    logger.info("Status 0, create new engagement")
                    self.create_new_engagement(engagement, status, cpr)
                skip = True

            if status["EmploymentStatusCode"] == "1":
                logger.info("Setting {} to status 1".format(job_id))
                mo_eng = self._find_engagement(job_id)
                if mo_eng:
                    logger.info("Status 1, edit eng. {}".format(mo_eng["uuid"]))

                    validity = self._validity(status)
                    logger.debug("Validity for edit: {}".format(validity))
                    data = {
                        "validity": validity,
                        "primary": {"uuid": self.primary_types["non_primary"]},
                    }
                    payload = sd_payloads.engagement(data, mo_eng)
                    logger.debug("Edit status 1, payload: {}".format(payload))
                    response = self.helper._mo_post("details/edit", payload)
                    mora_assert(response)
                    self.mo_engagement = self.helper.read_user_engagement(
                        self.mo_person["uuid"],
                        read_all=True,
                        only_primary=True,
                        use_cache=False,
                    )
                    self.edit_engagement(engagement, validity)
                else:
                    logger.info("Status 1: Create new engagement")
                    self.create_new_engagement(engagement, status, cpr)
                skip = True

            if status["EmploymentStatusCode"] == "3":
                mo_eng = self._find_engagement(job_id)
                if not mo_eng:
                    logger.info("Leave for non existent eng., create one")
                    self.create_new_engagement(engagement, status, cpr)
                logger.info("Create a leave for {} ".format(cpr))
                self.create_leave(status, job_id)

            if status["EmploymentStatusCode"] in ("7", "8"):
                from_date = status["ActivationDate"]
                logger.info("Terminate {}, job_id {} ".format(cpr, job_id))
                success = self._terminate_engagement(from_date, job_id)
                if not success:
                    logger.error("Problem with job-id: {}".format(job_id))
                    skip = True

            if status["EmploymentStatusCode"] in ("S", "9"):
                for mo_eng in self.mo_engagement:
                    if mo_eng["user_key"] == job_id:
                        logger.info("Status S, 9: Terminate {}".format(job_id))
                        self._terminate_engagement(status["ActivationDate"], job_id)
                skip = True
        return skip

    def _update_user_employments(self, cpr, sd_engagement):
        for engagement in sd_engagement:
            job_id, eng = self.engagement_components(engagement)
            logger.info("Update Job id: {}".format(job_id))
            logger.debug("SD Engagement: {}".format(engagement))
            # If status is present, we have a potential creation
            if eng["status_list"] and self._handle_status_changes(cpr, engagement):
                continue
            self.edit_engagement(engagement)

    def update_all_employments(self):
        logger.info("Update all employments:")
        employments_changed = self.read_employment_changed()
        logger.info("Update a total of {} employments".format(len(employments_changed)))

        def skip_fictional_users(employment):
            cpr = employment["PersonCivilRegistrationIdentifier"]
            if cpr[-4:] == "0000":
                logger.warning("Skipping fictional user: {}".format(cpr))
                return False
            return True

        def skip_initial_deleted(employment_info):
            emp_status = employment_info["EmploymentStatus"]
            if isinstance(emp_status, list):
                code = emp_status[0]["EmploymentStatusCode"]
            else:
                code = emp_status["EmploymentStatusCode"]
            code = EmploymentStatus(code)
            if code in LetGo:
                # NOTE: I think we should still import Migreret and Ophørt,
                #       as you might change from that to Ansat later.
                logger.warning("Employment deleted or ended before initial import.")
                return False
            return True

        employments_changed = tqdm(employments_changed, desc="update employments")
        employments_changed = filter(skip_fictional_users, employments_changed)

        for employment in employments_changed:
            cpr = employment["PersonCivilRegistrationIdentifier"]
            sd_engagement = ensure_list(employment["Employment"])

            logger.info("---------------------")
            logger.info("We are now updating {}".format(cpr))
            logger.debug("From date: {}".format(self.from_date))
            logger.debug("To date: {}".format(self.to_date))
            logger.debug("Employment: {}".format(employment))

            self.mo_person = self.helper.read_user(user_cpr=cpr, org_uuid=self.org_uuid)
            if not self.mo_person:
                sd_engagement = filter(skip_initial_deleted, sd_engagement)
                for employment_info in sd_engagement:
                    logger.warning("This person should be in MO, but is not")
                    try:
                        self.update_changed_persons(cpr=cpr)
                        self.mo_person = self.helper.read_user(
                            user_cpr=cpr, org_uuid=self.org_uuid
                        )
                    except Exception as exp:
                        logger.error(
                            "Unable to find person in MO, SD error: " + str(exp)
                        )
            else:  # if self.mo_person:
                self.mo_engagement = self.helper.read_user_engagement(
                    self.mo_person["uuid"],
                    read_all=True,
                    only_primary=True,
                    use_cache=False,
                )
                self._update_user_employments(cpr, sd_engagement)

                # Re-calculate primary after all updates for user has been performed.
                try:
                    self.updater.recalculate_primary(self.mo_person["uuid"])
                except NoPrimaryFound:
                    logger.warning(
                        "Could not find primary for: " + str(self.mo_person["uuid"])
                    )


def _local_db_insert(insert_tuple):
    settings = load_settings()
    conn = sqlite3.connect(
        settings["integrations.SD_Lon.import.run_db"],
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
        logger.error("Local base not correctly initialized")
        if not force:
            raise Exception("Local base not correctly initialized")
        logger.info("Force is true, create new db")
        conn = sqlite3.connect(str(run_db))
        c = conn.cursor()
        c.execute(
            """
          CREATE TABLE runs (id INTEGER PRIMARY KEY,
            from_date timestamp, to_date timestamp, status text)
        """
        )
        conn.commit()
        conn.close()

    _local_db_insert((from_date, from_date, "Running since {}"))

    logger.info("Start initial ChangedAt")
    sd_updater = ChangeAtSD(from_date)
    sd_updater.update_changed_persons()
    sd_updater.update_all_employments()
    logger.info("Ended initial ChangedAt")

    _local_db_insert((from_date, from_date, "Initial import: {}"))


def gen_date_pairs(from_date: datetime.datetime, one_day: bool = False):
    def generate_date_tuples(from_date, to_date):
        date_range = pd.date_range(from_date, to_date)
        mapped_dates = map(lambda date: date.to_pydatetime(), date_range)
        return pairwise(mapped_dates)

    to_date = datetime.date.today()
    if from_date.date() >= to_date:
        return iter(())
    if one_day:
        to_date = from_date + datetime.timedelta(days=1)
    dates = generate_date_tuples(from_date, to_date)
    return dates


@click.command()
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
    "--one-day",
    is_flag=True,
    type=click.BOOL,
    default=False,
    help="Only import changes for the next missing day",
)
def changed_at(init, force, one_day):
    """Tool to delta synchronize with MO with SD."""
    setup_logging()

    settings = load_settings()
    run_db = settings["integrations.SD_Lon.import.run_db"]

    logger.info("***************")
    logger.info("Program started")

    if init:
        run_db = pathlib.Path(run_db)

        from_date = datetime.datetime.strptime(
            settings["integrations.SD_Lon.global_from_date"], "%Y-%m-%d"
        )
        initialize_changed_at(from_date, run_db, force=True)
        exit()

    db_overview = DBOverview()
    # To date from last entries, becomes from_date for current entry
    from_date, status = db_overview.read_last_line("to_date", "status")

    if "Running" in status:
        if force:
            db_overview.delete_last_row()
            from_date, status = db_overview.read_last_line("to_date", "status")
        else:
            logging.error("Previous ChangedAt run did not return!")
            raise click.ClickException("Previous ChangedAt run did not return!")

    dates = gen_date_pairs(from_date)

    for from_date, to_date in dates:
        logger.info("Importing {} to {}".format(from_date, to_date))
        _local_db_insert((from_date, to_date, "Running since {}"))

        logger.info("Start ChangedAt module")
        sd_updater = ChangeAtSD(from_date, to_date)

        logger.info("Update changed persons")
        sd_updater.update_changed_persons()

        logger.info("Update all employments")
        sd_updater.update_all_employments()

        _local_db_insert((from_date, to_date, "Update finished: {}"))

    logger.info("Program stopped.")


if __name__ == "__main__":
    changed_at()
