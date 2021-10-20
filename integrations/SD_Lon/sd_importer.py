#!/usr/bin/env python3
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import datetime
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import click
from anytree import Node
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_setting
from ra_utils.load_settings import load_settings

from integrations import dawa_helper
from integrations.ad_integration import ad_reader
from integrations.SD_Lon.sd_common import calc_employment_id
from integrations.SD_Lon.sd_common import EmploymentStatus
from integrations.SD_Lon.sd_common import generate_uuid
from integrations.SD_Lon.sd_common import sd_lookup
from os2mo_data_import import ImportHelper


LOG_LEVEL = logging.DEBUG
LOG_FILE = "mo_initial_import.log"

logger = logging.getLogger("sdImport")


def get_import_date(settings):
    import_date_from = datetime.datetime.strptime(
        settings["integrations.SD_Lon.global_from_date"], "%Y-%m-%d"
    )
    import_date = import_date_from.strftime("%d.%m.%Y")
    return import_date


class SdImport(object):
    # XXX: This does really expensive calls against SD in __init__, caution!

    def __init__(
        self,
        importer,
        ad_info=None,
        org_only=False,
        org_id_prefix=None,
        manager_rows=None,
        super_unit=None,
        employee_mapping=None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        manager_rows = manager_rows or []
        employee_mapping = employee_mapping or {}

        self.settings = settings or load_settings()

        self.base_url = "https://service.sd.dk/sdws/"
        # List of CPR numbers for users with multiple employments
        self.double_employment: List[str] = []
        self.address_errors: Dict[str, Dict] = {}
        self.manager_rows = manager_rows

        self.importer = importer

        self.org_name = self.settings["municipality.name"]

        self.importer.add_organisation(
            identifier=self.org_name,
            user_key=self.org_name,
            municipality_code=self.settings["municipality.code"],
        )

        self.org_id_prefix = org_id_prefix

        self.import_date = get_import_date(self.settings)
        # List of job_functions that should be ignored.
        self.skip_job_functions = self.settings.get(
            "integrations.SD_Lon.skip_employment_types", []
        )
        # Whether to create SD-medarbejder associations
        self.create_associations = self.settings.get(
            "integrations.SD_Lon.sd_importer.create_associations", True
        )
        # Whether to import email addresses for organisations
        self.create_email_addresses = self.settings.get(
            "integrations.SD_Lon.sd_importer.create_email_addresses", True
        )

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

        self._add_classes(manager_rows)

    def _add_classes(self, manager_rows):
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
            "engagement_type": [
                ("månedsløn", "Medarbejder (månedsløn)"),
                ("timeløn", "Medarbejder (timeløn)"),
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

        # If specified manager_rows override default one
        if self.manager_rows:

            def transform_row(row):
                resp = row.get("ansvar")
                return resp, resp

            classes["responsibilty"] = list(map(transform_row, self.manager_rows))

        for facet, klasses in classes.items():
            for klass in klasses:
                if len(klass) == 3:
                    klasse_id, klasse, scope = klass
                else:
                    klasse_id, klasse = klass
                    scope = "TEXT"
                self._add_klasse(klasse_id, klasse, facet, scope)

    def _update_ad_map(self, cpr):
        logger.debug("Update cpr{}".format(cpr))
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
        departments = sd_lookup("GetDepartment20111201", params)

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
        in_sub_tree = False
        while "DepartmentReference" in department:
            department = department["DepartmentReference"]
            dep_uuid = department["DepartmentUUIDIdentifier"]
            if self.org_id_prefix:
                dep_uuid = generate_uuid(dep_uuid, self.org_id_prefix)
            if dep_uuid == sub_tree:
                in_sub_tree = True
        return in_sub_tree

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

            for row in self.manager_rows:
                if row["afdeling"].upper() == user_key.upper():
                    row["uuid"] = unit_id

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
        active_people = sd_lookup("GetPerson20111201", params)
        if not isinstance(active_people["Person"], list):
            active_people["Person"] = [active_people["Person"]]

        params["StatusActiveIndicator"] = False
        params["StatusPassiveIndicator"] = True
        passive_people = sd_lookup("GetPerson20111201", params)
        if not isinstance(passive_people["Person"], list):
            passive_people["Person"] = [passive_people["Person"]]

        people = active_people["Person"]

        cprs = []
        for person in active_people["Person"]:
            cprs.append(person["PersonCivilRegistrationIdentifier"])
        for person in passive_people["Person"]:
            if not person["PersonCivilRegistrationIdentifier"] in cprs:
                people.append(person)

        for person in people:
            cpr = person["PersonCivilRegistrationIdentifier"]
            logger.info("Importing {}".format(cpr))
            if cpr[-4:] == "0000":
                logger.warning("Skipping fictional user: {}".format(cpr))
                continue

            self._update_ad_map(cpr)

            given_name = person.get("PersonGivenName", "")
            sur_name = person.get("PersonSurnameName", "")

            uuid = self.employee_forced_uuids.get(cpr)
            logger.info("Employee in force list: {} {}".format(cpr, uuid))
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
        params = {
            "ActivationDate": self.import_date,
            "DeactivationDate": self.import_date,
            "UUIDIndicator": "true",
        }
        organisation = sd_lookup("GetOrganization20111201", params)
        departments = organisation["Organization"]["DepartmentReference"]

        for department in departments:
            self._add_sd_department(
                department, sub_tree=sub_tree, super_unit=super_unit
            )
        self.nodes = self._create_org_tree_structure()

    def create_employees(self):
        params = {
            "StatusActiveIndicator": "true",
            "StatusPassiveIndicator": "false",
            "DepartmentIndicator": "true",
            "EmploymentStatusIndicator": "true",
            "ProfessionIndicator": "true",
            "WorkingTimeIndicator": "true",
            "UUIDIndicator": "true",
            "SalaryAgreementIndicator": "false",
            "SalaryCodeGroupIndicator": "false",
            "EffectiveDate": self.import_date,
        }
        logger.info("Create employees")
        active_people = sd_lookup("GetEmployment20111201", params)
        if not isinstance(active_people["Person"], list):
            active_people["Person"] = [active_people["Person"]]

        params["StatusActiveIndicator"] = False
        params["StatusPassiveIndicator"] = True
        passive_people = sd_lookup("GetEmployment20111201", params)
        if not isinstance(passive_people["Person"], list):
            passive_people["Person"] = [passive_people["Person"]]

        self._create_employees(active_people)
        self._create_employees(passive_people, skip_manager=True)

    def _create_employees(self, persons, skip_manager=False):
        for person in persons["Person"]:
            self.create_employee(person, skip_manager=skip_manager)

    def create_employee(self, person, skip_manager=False):
        logger.debug("Person object to create: {}".format(person))
        cpr = person["PersonCivilRegistrationIdentifier"]
        if cpr[-4:] == "0000":
            logger.warning("Skipping fictional user: {}".format(cpr))
            return

        employments = person["Employment"]
        if not isinstance(employments, list):
            employments = [employments]

        max_rate = -1
        min_id = 999999
        for employment in employments:
            job_position_id = employment["Profession"]["JobPositionIdentifier"]
            if job_position_id in self.skip_job_functions:
                logger.info("Skipping {} due to job_pos_id".format(employment))
                continue

            status = EmploymentStatus(
                employment["EmploymentStatus"]["EmploymentStatusCode"]
            )
            if status == EmploymentStatus.Orlov:
                # Orlov
                pass
            if status in EmploymentStatus.let_go():
                # Fratrådt eller pensioneret.
                continue

            employment_id = calc_employment_id(employment)
            occupation_rate = float(employment["WorkingTime"]["OccupationRate"])

            if occupation_rate == max_rate:
                if employment_id["value"] < min_id:
                    min_id = employment_id["value"]
            if occupation_rate > max_rate:
                max_rate = occupation_rate
                min_id = employment_id["value"]

        exactly_one_primary = False
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

            split = self.settings["integrations.SD_Lon.monthly_hourly_divide"]
            if employment_id["value"] < split:
                engagement_type_ref = "månedsløn"
            elif (split - 1) < employment_id["value"] < 999999:
                engagement_type_ref = "timeløn"
            else:  # This happens if EmploymentID is not a number
                engagement_type_ref = "engagement_type" + job_position_id
                self._add_klasse(
                    engagement_type_ref, job_position_id, "engagement_type"
                )
                logger.info("Non-nummeric id. Job pos id: {}".format(job_position_id))

            if occupation_rate == max_rate and employment_id["value"] == min_id:
                assert exactly_one_primary is False, "More than one primary found"
                primary_type_ref = "Ansat"
                exactly_one_primary = True
            else:
                primary_type_ref = "non-primary"

            if status == EmploymentStatus.AnsatUdenLoen:
                # If status 0, uncondtionally override
                primary_type_ref = "status0"

            job_function_type = self.settings["integrations.SD_Lon.job_function"]
            if job_function_type == "EmploymentName":
                job_func_ref = self._add_klasse(
                    job_name, job_name, "engagement_job_function"
                )
            elif job_function_type == "JobPositionIdentifier":
                job_func_ref = self._add_klasse(
                    job_position_id, job_position_id, "engagement_job_function"
                )
            else:
                raise Exception("integrations.SD_Lon.job_function is wrong")

            emp_dep = employment["EmploymentDepartment"]
            unit = emp_dep["DepartmentUUIDIdentifier"]

            if status in EmploymentStatus.let_go():
                date_from = datetime.datetime.strptime(
                    employment["EmploymentDate"], "%Y-%m-%d"
                )
                date_to = datetime.datetime.strptime(
                    employment["EmploymentStatus"]["ActivationDate"], "%Y-%m-%d"
                )
                date_to = date_to - datetime.timedelta(days=1)
            else:
                date_from = datetime.datetime.strptime(
                    employment["EmploymentStatus"]["ActivationDate"], "%Y-%m-%d"
                )
                date_to = datetime.datetime.strptime(
                    employment["EmploymentStatus"]["DeactivationDate"], "%Y-%m-%d"
                )

            if date_from > date_to:
                date_from = date_to

            date_from = datetime.datetime.strftime(date_from, "%Y-%m-%d")
            if date_to == datetime.datetime(9999, 12, 31, 0, 0):
                date_to = None
            else:
                date_to = datetime.datetime.strftime(date_to, "%Y-%m-%d")

            logger.info(
                "Validty for {}: from: {}, to: {}".format(
                    employment_id["id"], date_from, date_to
                )
            )

            # Employees are not allowed to be in these units (allthough
            # we do make an association). We must instead find the lowest
            # higher level to put she or he.
            too_deep = self.settings["integrations.SD_Lon.import.too_deep"]
            original_unit = unit
            while self.nodes[unit].name in too_deep:
                unit = self.nodes[unit].parent.uuid
            try:
                # In a distant future, an employment id will be re-used and
                # then a more refined version of this code will be needed.
                # engagement_uuid = generate_uuid(employment_id['id'],
                #                                 self.org_id_prefix,
                #                                 self.org_name)

                ext_field = self.settings.get("integrations.SD_Lon.employment_field")
                if ext_field is not None:
                    extention = {ext_field: job_name}
                else:
                    extention = {}

                self.importer.add_engagement(
                    employee=cpr,
                    # uuid=engagement_uuid,
                    user_key=employment_id["id"],
                    organisation_unit=unit,
                    job_function_ref=job_func_ref,
                    fraction=int(occupation_rate * 1000000),
                    primary_ref=primary_type_ref,
                    engagement_type_ref=engagement_type_ref,
                    date_from=date_from,
                    date_to=date_to,
                    **extention,
                )
                # Remove this to remove any sign of the employee from the
                # lowest levels of the org
                if self.create_associations:
                    self.importer.add_association(
                        employee=cpr,
                        user_key=employment_id["id"],
                        organisation_unit=original_unit,
                        association_type_ref="SD-medarbejder",
                        date_from=date_from,
                        date_to=date_to,
                    )
                if status == EmploymentStatus.Orlov:
                    self.importer.add_leave(
                        employee=cpr,
                        leave_type_ref="Orlov",
                        date_from=date_from,
                        date_to=date_to,
                    )
            except AssertionError:
                self.double_employment.append(cpr)

            # If we do not have a list of managers, we take the manager,
            # information from the job_function_code.
            if not self.manager_rows:
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
                        date_from=date_from,
                        date_to=date_to,
                    )

        if self.manager_rows and (not skip_manager):
            for row in self.manager_rows:
                if row["cpr"] == cpr:
                    if "uuid" not in row:
                        logger.warning("NO UNIT: {}".format(row["afdeling"]))
                        continue
                    if job_position_id in ["1040", "1035", "1030"]:
                        manager_level = "manager_" + job_position_id
                    else:
                        manager_level = "manager_1040"

                    logger.info("Manager {} to {}".format(cpr, row["afdeling"]))

                    self.importer.add_manager(
                        employee=cpr,
                        organisation_unit=row["uuid"],
                        manager_level_ref=manager_level,
                        manager_type_ref="leder_type",
                        responsibility_list=[row["ansvar"]],
                        date_from="1930-01-01",
                        date_to=None,
                    )

        # This assertment really should hold...
        # assert(exactly_one_primary is True)
        if exactly_one_primary is not True:
            pass
            # print()
            # print('More than one primary: {}'.format(employments))


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
    default=load_setting("mora.base", "http://localhost:5000"),
    help="URL for OS2mo.",
)
@click.option(
    "--mox-base",
    default=load_setting("mox.base", "http://localhost:8080"),
    help="URL for LoRa.",
)
def full_import(org_only: bool, mora_base: str, mox_base: str):
    """Tool to do an initial full import."""
    # Check connection to MO before we fire requests against SD
    mh = MoraHelper(mora_base)
    if not mh.check_connection():
        raise click.ClickException("No MO reply, aborting.")

    importer = ImportHelper(
        create_defaults=True,
        mox_base=mox_base,
        mora_base=mora_base,
        store_integration_data=False,
        seperate_names=True,
    )
    sd = SdImport(importer, org_only=org_only, ad_info=None, manager_rows=None)

    sd.create_ou_tree(create_orphan_container=False, sub_tree=None, super_unit=None)
    if not org_only:
        sd.create_employees()

    importer.import_all()


if __name__ == "__main__":
    cli()
