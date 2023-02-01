#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import datetime
import logging
import uuid
from operator import itemgetter
from typing import Any
from typing import Dict
from typing import Optional

import click
from anytree import Node
from integrations import dawa_helper
from integrations.ad_integration import ad_reader
from os2mo_data_import import ImportHelper
from os2mo_helpers.mora_helpers import MoraHelper

from .fix_departments import FixDepartments
from .config import get_changed_at_settings
from .config import get_importer_settings
from .config import ImporterSettings
from .date_utils import format_date, date_to_datetime
from .date_utils import get_employment_dates
from .date_utils import parse_date
from .models import JobFunction
from .sd_common import calc_employment_id
from .sd_common import EmploymentStatus
from .sd_common import ensure_list
from .sd_common import generate_uuid
from .sd_common import read_employment_at
from .sd_common import sd_lookup
from .sd_common import skip_fictional_users


LOG_LEVEL = logging.DEBUG
LOG_FILE = "mo_initial_import.log"

HISTORIC = "historic"

logger = logging.getLogger("sdImport")


class SdImport:
    # XXX: This does really expensive calls against SD in __init__, caution!

    def __init__(
        self,
        importer,
        settings: ImporterSettings,
        ad_info=None,
        org_only=False,
        org_id_prefix=None,
        employee_mapping=None,
    ):
        employee_mapping = employee_mapping or {}

        self.settings = settings

        self.base_url = "https://service.sd.dk/sdws/"
        self.address_errors: Dict[str, Dict] = {}

        self.importer = importer

        self.org_name = self.settings.municipality_name
        self.sd_institution_uuid = self._get_institution()

        self.importer.add_organisation(
            identifier=self.org_name,
            user_key=self.org_name,
            municipality_code=self.settings.municipality_code,
            uuid=str(self.sd_institution_uuid),
        )

        self.org_id_prefix = org_id_prefix

        self.import_date = self.settings.sd_global_from_date.strftime("%d.%m.%Y")
        # List of job_functions that should be ignored.
        self.skip_job_functions = self.settings.sd_skip_employment_types

        # Whether to create SD-medarbejder associations
        self.create_associations = self.settings.sd_importer_create_associations

        # Whether to import email addresses for organisations
        self.create_email_addresses = self.settings.sd_importer_create_email_addresses

        # Whether to use <Employment><EmploymentDate> as engagement start date instead
        # of <Employment><EmploymentStatus><ActivationDate>.
        self.employment_date_as_engagement_start_date = (
            self.settings.sd_importer_employment_date_as_engagement_start_date
        )

        self.historic_org_unit_uuid = str(uuid.uuid4())

        # CPR indexed dictionary of AD users
        self.ad_people: Dict[str, Dict] = {}
        self.employee_forced_uuids = employee_mapping
        self.ad_reader = None
        if isinstance(ad_info, ad_reader.ADParameterReader):
            self.ad_reader = ad_info
            self.importer.new_itsystem(identifier="AD", system_name="Active Directory")
            self.ad_reader.cache_all()

        self.nodes: Dict[str, Dict] = {}  # Will be populated when org-tree is created

        self.org_only = org_only
        if not org_only:
            self.add_people()

        self.info = self._read_department_info()

        self._add_classes()

    def _add_classes(self):
        # Format is facet -> list of entries
        # Entries are tuples on the format '(klasse_id, klasse, scope)'
        classes = {
            "manager_type": [
                ("leder_type", "Leder"),
            ],
            "org_unit_type": [
                ("Enhed", "Enhed"),
            ],
            "org_unit_address_type": [
                ("Pnummer", "Pnummer", "PNUMBER"),
                ("AddressMailUnit", "Postadresse", "DAR"),
                ("AdresseReturUnit", "Returadresse", "DAR"),
                ("AdresseHenvendelseUnit", "Henvendelsessted", "DAR"),
                ("PhoneUnit", "Telefon", "PHONE"),
                ("EmailUnit", "Email", "EMAIL"),
            ],
            "employee_address_type": [
                ("AdressePostEmployee", "Postadresse", "DAR"),
                ("PhoneEmployee", "Telefon", "PHONE"),
                ("MobilePhoneEmployee", "Mobiltelefon", "PHONE"),
                ("LocationEmployee", "Lokation", "TEXT"),
                ("EmailEmployee", "Email", "EMAIL"),
            ],
            "leave_type": [
                ("Orlov", "Orlov"),
            ],
            "engagement_job_function": [(HISTORIC, "Ukendt")],
            "engagement_type": [
                ("månedsløn", "Medarbejder (månedsløn)"),
                ("timeløn", "Medarbejder (timeløn)"),
                ("historisk", "Historisk ansættelse"),
            ],
            "primary_type": [
                ("Ansat", "Ansat", "3000"),
                ("status0", "Ansat - Ikke i løn", "1000"),
                ("non-primary", "Ikke-primær ansættelse", "0"),
                ("explicitly-primary", "Manuelt primær ansættelse", "5000"),
            ],
            "association_type": [
                ("SD-medarbejder", "SD-medarbejder"),
            ],
            "visibility": [
                ("Ekstern", "Må vises eksternt", "PUBLIC"),
                ("Intern", "Må vises internt", "INTERNAL"),
                ("Hemmelig", "Hemmelig", "SECRET"),
            ],
            "manager_level": [
                # XXX: Why 1040, 1035 and 1030?
                ("manager_1040", "Leder"),
                ("manager_1035", "Chef"),
                ("manager_1030", "Direktør"),
            ],
            "responsibility": [
                ("Lederansvar", "Lederansvar"),
            ],
        }

        for facet, klasses in classes.items():
            for klass in klasses:
                if len(klass) == 3:
                    klasse_id, klasse, scope = klass
                else:
                    klasse_id, klasse = klass
                    scope = "TEXT"
                self._add_klasse(klasse_id, klasse, facet, scope)

    def _update_ad_map(self, cpr):
        logger.debug("_update_ad_map called")
        self.ad_people[cpr] = {}
        if self.ad_reader:
            response = self.ad_reader.read_user(cpr=cpr, cache_only=True)
            if response:
                self.ad_people[cpr] = response

    def _read_department_info(self):
        """Load all department details and store for later user."""
        department_info = {}

        params = {
            "ActivationDate": self.import_date,
            "DeactivationDate": self.import_date,
            "ContactInformationIndicator": "true",
            "DepartmentNameIndicator": "true",
            "PostalAddressIndicator": "true",
            "ProductionUnitIndicator": "true",
            "UUIDIndicator": "true",
            "EmploymentDepartmentIndicator": "false",
        }
        departments = sd_lookup(
            "GetDepartment20111201", settings=self.settings, params=params
        )

        for department in departments["Department"]:
            uuid = department["DepartmentUUIDIdentifier"]
            if self.org_id_prefix:
                uuid = generate_uuid(uuid, self.org_id_prefix, self.org_name)

            department_info[uuid] = department
            unit_level = department["DepartmentLevelIdentifier"]
            if not self.importer.check_if_exists("klasse", unit_level):
                self._add_klasse(unit_level, unit_level, "org_unit_level", scope="TEXT")
        return department_info

    def _add_klasse(self, klasse_id, klasse, facet, scope="TEXT"):
        if isinstance(klasse_id, str):
            klasse_id = klasse_id.replace("&", "_")
        if not self.importer.check_if_exists("klasse", klasse_id):
            klasse_uuid = generate_uuid(klasse_id, self.org_id_prefix, self.org_name)
            self.importer.add_klasse(
                identifier=klasse_id,
                uuid=klasse_uuid,
                facet_type_ref=facet,
                user_key=str(klasse_id),
                scope=scope,
                title=klasse,
            )
        return klasse_id

    def _check_subtree(self, department, sub_tree):
        """Check if a department is member of given sub-tree"""
        while "DepartmentReference" in department:
            department = department["DepartmentReference"]
            dep_uuid = department["DepartmentUUIDIdentifier"]
            if self.org_id_prefix:
                dep_uuid = generate_uuid(dep_uuid, self.org_id_prefix)
            if dep_uuid == sub_tree:
                return True
        return False

    def _add_sd_department(
        self, department, contains_subunits=False, sub_tree=None, super_unit=None
    ):
        """
        Add add a deparment to MO. If the unit has parents, these will
        also be added
        :param department: The SD-department, including parent units.
        :param contains_subunits: True if the unit has sub-units.
        """
        ou_level = department["DepartmentLevelIdentifier"]
        if not self.org_id_prefix:
            unit_id = department["DepartmentUUIDIdentifier"]
            user_key = department["DepartmentIdentifier"]
        else:
            unit_id = generate_uuid(
                department["DepartmentUUIDIdentifier"], self.org_id_prefix
            )
            user_key = self.org_id_prefix + "_" + department["DepartmentIdentifier"]

        # parent_uuid = None
        parent_uuid = super_unit

        # If contain_subunits is true, this sub tree is a valid member
        import_unit = contains_subunits
        if "DepartmentReference" in department:
            if self._check_subtree(department, sub_tree):
                import_unit = True

            parent_uuid = department["DepartmentReference"]["DepartmentUUIDIdentifier"]
            if self.org_id_prefix:
                parent_uuid = self._generate_uuid(parent_uuid, self.org_id_prefix)
        else:
            import_unit = unit_id == sub_tree

        if not import_unit and sub_tree is not None:
            return

        info = self.info[unit_id]
        assert info["DepartmentLevelIdentifier"] == ou_level
        logger.debug("Add unit: {}".format(unit_id))
        if (
            (not contains_subunits)
            and (parent_uuid is super_unit)
            and self.importer.check_if_exists("organisation_unit", "OrphanUnits")
        ):
            parent_uuid = "OrphanUnits"

        date_from = info["ActivationDate"]
        # No units have termination dates: date_to is None
        if self.importer.check_if_exists("organisation_unit", unit_id):
            return
        else:
            self.importer.add_organisation_unit(
                identifier=unit_id,
                name=info["DepartmentName"],
                user_key=user_key,
                org_unit_level_ref=ou_level,
                type_ref="Enhed",
                date_from=date_from,
                uuid=unit_id,
                date_to=None,
                parent_ref=parent_uuid,
            )

        def import_email_addresses(info: Dict[str, Any]) -> None:
            if "ContactInformation" not in info:
                return
            if "EmailAddressIdentifier" not in info["ContactInformation"]:
                return

            emails = info["ContactInformation"]["EmailAddressIdentifier"]
            # filter empty entities
            emails = filter(lambda email: email.find("Empty") == -1, emails)
            for email in emails:
                self.importer.add_address_type(
                    organisation_unit=unit_id,
                    type_ref="EmailUnit",
                    value=email,
                    date_from=date_from,
                )

        def import_pnumber(info: Dict[str, Any]) -> None:
            if "ProductionUnitIdentifier" not in info:
                return
            self.importer.add_address_type(
                organisation_unit=unit_id,
                type_ref="Pnummer",
                value=info["ProductionUnitIdentifier"],
                date_from=date_from,
            )

        def import_postal_address(info: Dict[str, Any]) -> None:
            if "PostalAddress" not in info:
                return
            postal_address = info["PostalAddress"]

            if "StandardAddressIdentifier" not in postal_address:
                return
            address_string = postal_address["StandardAddressIdentifier"]

            if "PostalCode" not in postal_address:
                return
            zip_code = postal_address["PostalCode"]

            logger.debug("Look in Dawa: {}".format(address_string))
            dar_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
            logger.debug("DAR: {}".format(dar_uuid))

            if dar_uuid is None:
                logger.error("Unable to lookup address: {}".format(address_string))
                self.address_errors[unit_id] = info
                return

            self.importer.add_address_type(
                organisation_unit=unit_id,
                type_ref="AddressMailUnit",
                value=dar_uuid,
                date_from=date_from,
            )

        if self.create_email_addresses:
            import_email_addresses(info)
        import_pnumber(info)
        import_postal_address(info)

        # Recursively create parents (higher level OUs)
        # Note: These do not have their own entry in SD
        if "DepartmentReference" in department:
            self._add_sd_department(
                department["DepartmentReference"],
                contains_subunits=True,
                sub_tree=sub_tree,
                super_unit=super_unit,
            )

    def _get_institution(self) -> uuid.UUID:
        params = {
            "InstitutionIdentifier": self.settings.sd_institution_identifier,
            "UUIDIndicator": "true",
        }
        r = sd_lookup("GetInstitution20111201", settings=self.settings, params=params)
        return uuid.UUID(r["Region"]["Institution"]["InstitutionUUIDIdentifier"])

    def _create_org_tree_structure(self):
        nodes = {}
        all_ous = self.importer.export("organisation_unit")
        new_ous = []
        for key, ou in all_ous.items():
            parent = ou.parent_ref
            if parent is None:
                uuid = key
                niveau = ou.org_unit_level_ref
                nodes[uuid] = Node(niveau, uuid=uuid)
            else:
                new_ous.append(ou)

        while len(new_ous) > 0:
            logger.info("Number of new ous: {}".format(len(new_ous)))
            print("Number of new ous: {}".format(len(new_ous)))
            all_ous = new_ous
            new_ous = []
            for ou in all_ous:
                parent = ou.parent_ref
                if parent in nodes.keys():
                    uuid = ou.uuid
                    niveau = ou.org_unit_level_ref
                    nodes[uuid] = Node(niveau, parent=nodes[parent], uuid=uuid)
                else:
                    new_ous.append(ou)
        return nodes

    def add_people(self):
        """Load all person details and store for later user"""
        params = {
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "false",
            "ContactInformationIndicator": "false",
            "PostalAddressIndicator": "false",
            "EffectiveDate": self.import_date,
        }
        active_people = sd_lookup(
            "GetPerson20111201", settings=self.settings, params=params
        )
        if not isinstance(active_people["Person"], list):
            active_people["Person"] = [active_people["Person"]]

        params["StatusActiveIndicator"] = False
        params["StatusPassiveIndicator"] = True
        passive_people = sd_lookup(
            "GetPerson20111201", settings=self.settings, params=params
        )
        if not isinstance(passive_people["Person"], list):
            passive_people["Person"] = [passive_people["Person"]]

        cprs = set(
            map(
                itemgetter("PersonCivilRegistrationIdentifier"), active_people["Person"]
            )
        )
        # Collect all people, prefering their active variants
        # TODO: Consider doing this with a dict keyed by cpr number
        people = active_people["Person"]
        for person in passive_people["Person"]:
            cpr = person["PersonCivilRegistrationIdentifier"]
            if cpr not in cprs:
                people.append(person)

        people = filter(skip_fictional_users, people)

        # TODO: Almost identitcal code exists in sd_changed_at's update_changed_persons
        for person in people:
            cpr = person["PersonCivilRegistrationIdentifier"]
            self._update_ad_map(cpr)

            given_name = person.get("PersonGivenName", "")
            sur_name = person.get("PersonSurnameName", "")

            uuid = self.employee_forced_uuids.get(cpr)
            logger.info(f"Employee in force list: {uuid}")
            if uuid is None and "ObjectGuid" in self.ad_people[cpr]:
                uuid = self.ad_people[cpr]["ObjectGuid"]

            # Name is placeholder for initals, do not know which field to extract
            if "Name" in self.ad_people[cpr]:
                user_key = self.ad_people[cpr]["Name"]
            else:
                user_key = "{} {}".format(given_name, sur_name)

            self.importer.add_employee(
                name=(given_name, sur_name),
                identifier=cpr,
                cpr_no=cpr,
                user_key=user_key,
                uuid=uuid,
            )

            if "SamAccountName" in self.ad_people[cpr]:
                self.importer.join_itsystem(
                    employee=cpr,
                    user_key=self.ad_people[cpr]["SamAccountName"],
                    itsystem_ref="AD",
                    date_from="1930-01-01",
                )

    def create_ou_tree(self, create_orphan_container, sub_tree=None, super_unit=None):
        """Read all department levels from SD."""
        # TODO: Currently we can only read a top sub-tree
        if create_orphan_container:
            self._add_klasse("Orphan", "Virtuel Enhed", "org_unit_type")
            self.importer.add_organisation_unit(
                identifier="OrphanUnits",
                uuid="11111111-0000-0000-0000-111111111111",
                name="Forældreløse enheder",
                user_key="OrphanUnits",
                type_ref="Orphan",
                date_from="1930-01-01",
                date_to=None,
                parent_ref=None,
            )

        # Create historic dummy org unit
        historic_org_unit_date_to = format_date(
            date_to_datetime(self.settings.sd_global_from_date)
            - datetime.timedelta(days=1)
        )
        self._add_klasse(HISTORIC, "Historisk Enhed", "org_unit_type")
        self.importer.add_organisation_unit(
            identifier=self.historic_org_unit_uuid,
            uuid=self.historic_org_unit_uuid,
            name="Tidligere ansættelser",
            user_key=HISTORIC,
            type_ref=HISTORIC,
            date_from="1930-01-01",
            date_to=historic_org_unit_date_to,
            parent_ref=None,
        )

        params = {
            "ActivationDate": self.import_date,
            "DeactivationDate": self.import_date,
            "UUIDIndicator": "true",
        }
        organisation = sd_lookup(
            "GetOrganization20111201", settings=self.settings, params=params
        )
        departments = organisation["Organization"]["DepartmentReference"]

        for department in departments:
            self._add_sd_department(
                department, sub_tree=sub_tree, super_unit=super_unit
            )
        self.nodes = self._create_org_tree_structure()

    def create_employees(self):
        logger.info("Create employees")

        effective_date = datetime.datetime.strptime(self.import_date, "%d.%m.%Y").date()

        logger.info("Get active people from SD...")
        active_people = ensure_list(
            read_employment_at(
                effective_date,
                settings=self.settings,
                status_passive_indicator=False,
            )
        )

        logger.info("Get passive people from SD...")
        passive_people = ensure_list(
            read_employment_at(
                effective_date,
                settings=self.settings,
                status_active_indicator=False,
                status_passive_indicator=True,
            )
        )

        # TODO: condense the two calls above into one and then create the variables
        # active_people and passive_people by using the partition function from
        # more_itertools

        logger.info("Create employees from active SD employees...")
        self._create_employees(active_people)

        logger.info("Create employees from passive SD employees...")
        self._create_employees(passive_people, skip_manager=True)

    def _create_employees(self, persons, skip_manager=False):
        people = filter(skip_fictional_users, persons)
        for person in people:
            self.create_employee(person, skip_manager=skip_manager)

    def create_employee(self, person, skip_manager=False):
        logger.debug(79 * "-")
        logger.debug("Person object to create: {}".format(person))

        cpr = person["PersonCivilRegistrationIdentifier"]
        employments = ensure_list(person["Employment"])

        for employment in employments:
            status = EmploymentStatus(
                employment["EmploymentStatus"]["EmploymentStatusCode"]
            )

            # Job_position_id: Klassificeret liste over stillingstyper.
            # job_name: Fritekstfelt med stillingsbetegnelser.
            job_position_id = employment["Profession"]["JobPositionIdentifier"]
            if job_position_id in self.skip_job_functions:
                continue
            job_name = employment["Profession"].get("EmploymentName", job_position_id)

            occupation_rate = float(employment["WorkingTime"]["OccupationRate"])

            employment_id = calc_employment_id(employment)

            # TODO: Identital code to this exists in sd_changed_at
            split = self.settings.sd_monthly_hourly_divide
            if employment_id["value"] < split:
                engagement_type_ref = "månedsløn"
            elif (split - 1) < employment_id["value"] < 999999:
                engagement_type_ref = "timeløn"
            else:  # This happens if EmploymentID is not a number
                engagement_type_ref = "engagement_type" + job_position_id
                self._add_klasse(
                    engagement_type_ref, job_position_id, "engagement_type"
                )
                logger.info("Non-numeric id. Job pos id: {}".format(job_position_id))

            primary_type_ref = "non-primary"
            # If status 0, uncondtionally override
            if status == EmploymentStatus.AnsatUdenLoen:
                primary_type_ref = "status0"

            job_function_type = self.settings.sd_job_function
            if job_function_type == JobFunction.employment_name:
                job_func_ref = self._add_klasse(
                    job_name, job_name, "engagement_job_function"
                )
            elif job_function_type == JobFunction.job_position_identifier:
                job_func_ref = self._add_klasse(
                    job_position_id, job_position_id, "engagement_job_function"
                )

            emp_dep = employment["EmploymentDepartment"]
            unit = emp_dep["DepartmentUUIDIdentifier"]

            date_from_engagement, date_to = get_employment_dates(employment)
            # Use org unit start date if engagement starts *before*
            # the org unit start date
            org_unit_date_from = parse_date(
                self.importer.organisation_units[unit].date_from
            )
            date_from = max(date_from_engagement, org_unit_date_from)

            date_from_str = format_date(date_from)
            date_to_str = format_date(date_to)
            if date_to == datetime.datetime(9999, 12, 31, 0, 0):
                date_to_str = None

            sd_employment_id = employment_id["id"]
            logger.info(
                f"Validty for {sd_employment_id}: from: {date_from_str}, "
                f"to: {date_to_str}"
            )

            if date_to <= date_from:
                logger.warning(f"Skip creating employment for id: {sd_employment_id}")
                continue

            original_unit = unit
            # Remove this to remove any sign of the employee from the
            # lowest levels of the org
            if self.create_associations:
                self.importer.add_association(
                    employee=cpr,
                    user_key=employment_id["id"],
                    organisation_unit=original_unit,
                    association_type_ref="SD-medarbejder",
                    date_from=date_from_str,
                    date_to=date_to_str,
                )

            # Employees are not allowed to be in these units (allthough
            # we do make an association). We must instead find the lowest
            # higher level to put she or he.
            too_deep = self.settings.sd_import_too_deep
            while self.nodes[unit].name in too_deep:
                unit = self.nodes[unit].parent.uuid

            ext_field = self.settings.sd_employment_field
            extention = {}
            if ext_field is not None:
                extention[ext_field] = job_name

            # Generate UUID for engagement here since we need this UUID when/if
            # creating a leave below
            engagement_uuid = str(uuid.uuid4())

            self.importer.add_engagement(
                employee=cpr,
                uuid=engagement_uuid,
                user_key=employment_id["id"],
                organisation_unit=unit,
                job_function_ref=job_func_ref,
                fraction=int(occupation_rate * 1000000),
                primary_ref=primary_type_ref,
                engagement_type_ref=engagement_type_ref,
                date_from=date_from_str,
                date_to=date_to_str,
                **extention,
            )

            # Add historic dummy engagement if the start date of the engagement
            # is older than the start date of the corresponding org unit
            # (see https://redmine.magenta-aps.dk/issues/51898)
            if date_from_engagement < org_unit_date_from:
                dummy_eng_date_to = org_unit_date_from - datetime.timedelta(days=1)
                dummy_eng_date_to_str = format_date(dummy_eng_date_to)
                self.importer.add_engagement(
                    employee=cpr,
                    organisation_unit=self.historic_org_unit_uuid,
                    job_function_ref=HISTORIC,
                    engagement_type_ref="historisk",
                    date_from=format_date(date_from_engagement),
                    date_to=dummy_eng_date_to_str,
                )

            if status == EmploymentStatus.Orlov:
                self.importer.add_leave(
                    employee=cpr,
                    engagement_uuid=engagement_uuid,
                    leave_type_ref="Orlov",
                    date_from=date_from_str,
                    date_to=date_to_str,
                )

            # These job functions will normally (but necessarily)
            #  correlate to a manager position
            if job_position_id in ["1040", "1035", "1030"]:
                self.importer.add_manager(
                    employee=cpr,
                    organisation_unit=unit,
                    manager_level_ref="manager_" + job_position_id,
                    address_uuid=None,  # Manager address is not used
                    manager_type_ref="leder_type",
                    responsibility_list=["Lederansvar"],
                    date_from=date_from_str,
                    date_to=employment["Profession"]["DeactivationDate"],
                )


@click.group()
def cli():
    for name in logging.root.manager.loggerDict:
        if name in (
            "sdImport",
            "sdCommon",
            "AdReader",
            "moImporterMoraTypes",
            "moImporterMoxTypes",
            "moImporterUtilities",
            "moImporterHelpers",
        ):
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )


@cli.command()
@click.option(
    "--org-only",
    is_flag=True,
    default=False,
    type=click.BOOL,
    help="Only import organisation structure",
)
@click.option(
    "--mora-base",
    required=False,
    help="URL for OS2mo.",
)
@click.option(
    "--mox-base",
    required=False,
    help="URL for LoRa.",
)
def full_import_cli(org_only: bool, mora_base: Optional[str], mox_base: Optional[str]):
    """Tool to do an initial full import."""
    full_import(org_only, mora_base, mox_base)


def full_import(
    org_only: bool = False,
    mora_base: Optional[str] = None,
    mox_base: Optional[str] = None,
):
    """Tool to do an initial full import."""
    overrides = {}
    if mora_base:
        overrides["mora_base"] = mora_base
    if mox_base:
        overrides["mox_base"] = mox_base

    settings = get_importer_settings(**overrides)

    # Check connection to MO before we fire requests against SD
    mh = MoraHelper(settings.mora_base)
    if not mh.check_connection():
        raise click.ClickException("No MO reply, aborting.")

    importer = ImportHelper(
        create_defaults=True,
        mox_base=settings.mox_base,
        mora_base=settings.mora_base,
        seperate_names=True,
    )
    sd = SdImport(importer, settings=settings, org_only=org_only, ad_info=None)

    sd.create_ou_tree(create_orphan_container=False, sub_tree=None, super_unit=None)
    if not org_only:
        sd.create_employees()

    importer.import_all()
    print("IMPORT DONE")


if __name__ == "__main__":
    cli()
