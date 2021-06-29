from copy import deepcopy
from csv import DictReader
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import UUID

from more_itertools import flatten
from pydantic import BaseModel, Field

from os2mo_data_import.Clients.LoRa.model import Klasse
from os2mo_data_import.Clients.MO.model import Address, Employee, Engagement, \
    EngagementAssociation, OrgUnit
from os2mo_data_import.Clients.MO.model_parts.interface import MoObj
from os2mo_data_import.util import generate_uuid


class OrstedBusinessError(Exception):
    pass


class UUIDGenerator:
    def __init__(self, uuid_gen_seed: str = ""):
        self.__seed = uuid_gen_seed
        self.__cache: Dict[str, str] = {}  # Dict[user_key: uuid]

    def get_all(self) -> Dict[str, str]:
        return deepcopy(self.__cache)

    def get_uuid(self, key: str) -> str:
        """
        Generate uuid if missing
        :param key:
        :return:
        """
        if key in self.__cache:
            return self.__cache[key]

        uuid = generate_uuid(self.__seed + key)
        self.__cache[key] = uuid
        return uuid


class AddressKlasses(BaseModel):
    EMAIL: Klasse
    PHONE: Klasse
    MOBILE: Klasse
    HR_COUNTRY_CODE: Klasse
    CONSULTANT_EMAIL: Klasse
    HR_TDB_MOBILE_PHONE: Klasse
    HR_TDB_FIXED_PHONE: Klasse
    HR_LOCATION_ID_and_OFFICE: Klasse

    COUNTRY_OF_RESIDENCE: Klasse

    @classmethod
    def from_simplified_fields(
        cls,
        organisation_uuid: UUID,
        engagement_address_type_uuid: UUID,
        employee_address_type_uuid: UUID,
    ):
        gen_klass = partial(gen_single_klass, organisation_uuid=organisation_uuid)
        return AddressKlasses(
            EMAIL=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="EMAIL",
                title="EMAIL",
            ),
            PHONE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="PHONE",
                title="PHONE",
            ),
            MOBILE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="PHONE",
                title="MOBILE",
            ),
            HR_COUNTRY_CODE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="TEXT",
                title="HR_COUNTRY_CODE",
            ),
            CONSULTANT_EMAIL=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="EMAIL",
                title="CONSULTANT_EMAIL",
            ),
            HR_TDB_MOBILE_PHONE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="PHONE",
                title="HR_TDB_MOBILE_PHONE",
            ),
            HR_TDB_FIXED_PHONE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="EMAIL",
                title="EMAIL",
            ),
            HR_LOCATION_ID_and_OFFICE=gen_klass(
                facet_uuid=engagement_address_type_uuid,
                scope="MULTIFIELD_TEXT",
                title="HR_LOCATION_ID_and_OFFICE",
            ),
            COUNTRY_OF_RESIDENCE=gen_klass(
                facet_uuid=employee_address_type_uuid,
                scope="TEXT",
                title="COUNTRY_OF_RESIDENCE",
            ),
        )


def gen_single_klass(
    facet_uuid: UUID, organisation_uuid: UUID, title: str, scope: Optional[str] = None
) -> Klasse:
    return Klasse.from_simplified_fields(
        facet_uuid=facet_uuid,
        uuid=generate_uuid("klasse" + str(facet_uuid) + title),
        user_key=title,
        title=title,
        scope=scope,
        organisation_uuid=organisation_uuid,
    )


class RawEmdData(BaseModel):
    Initials: str = Field(alias="Initials")
    First_Name: str = Field(alias="First Name")
    Last_Name: str = Field(alias="Last Name")
    Title: str = Field(alias="Title")
    Email: str = Field(alias="Email")
    Phone: str = Field(alias="Phone")
    Mobile: str = Field(alias="Mobile")
    Office: str = Field(alias="Office")
    Employment_Date: str = Field(alias="Employment Date")
    Departure_Date: str = Field(alias="Departure Date")
    Hr_Employee_No: str = Field(alias="Hr Employee No")
    Hr_Department_No: str = Field(alias="Hr Department No")
    Phonebook_Relevant: str = Field(alias="Phonebook Relevant")
    Hr_Employee_Type: str = Field(alias="Hr Employee Type")
    Hr_Location_Id: str = Field(alias="Hr Location Id")
    Hr_Country_Code: str = Field(alias="Hr Country Code")
    Hr_Cost_Center: str = Field(alias="Hr Cost Center")
    Hr_Hirakey_Relevant: str = Field(alias="Hr Hirakey Relevant")
    Hr_Active_Status: str = Field(alias="Hr Active Status")
    Hr_Legal_Company: str = Field(alias="Hr Legal Company")
    Consultant_Email: str = Field(alias="Consultant Email")
    Hr_Tdb_Mobile_Phone: str = Field(alias="Hr Tdb Mobile Phone")
    Hr_Tdb_Fixed_Phone: str = Field(alias="Hr Tdb Fixed Phone")
    Hr_Consultant_Type: str = Field(alias="Hr Consultant Type")
    Hr_Company_Code: str = Field(alias="Hr Company Code")
    Ee_Subgroup: str = Field(alias="Ee Subgroup")
    Country_Of_Residence: str = Field(alias="Country Of Residence")
    Seniority: str = Field(alias="Seniority")
    Career_Level: str = Field(alias="Career Level")
    Position_Level: str = Field(alias="Position Level")
    Career_Track: str = Field(alias="Career Track")
    Sender_Cost_Center: str = Field(alias="Sender Cost Center")

    def __business_validate(self):
        if self.Hr_Employee_Type and self.Hr_Consultant_Type:
            raise OrstedBusinessError(
                f"mutually exclusive: {self.Hr_Employee_Type}, "
                f"{self.Hr_Consultant_Type}"
            )
        if self.Office and not self.Hr_Location_Id:
            raise OrstedBusinessError(
                f"need location of office: {self.Office}, {self.Hr_Location_Id}"
            )
        if bool(self.Hr_Legal_Company) != bool(self.Hr_Company_Code):
            raise OrstedBusinessError(
                f"need both or neither: {self.Hr_Legal_Company}, {self.Hr_Company_Code}"
            )
        try:
            datetime.strptime(self.Employment_Date, "%d-%b-%y").strftime("%Y-%m-%d")
        except Exception:
            raise OrstedBusinessError(
                f"invalid employment_date format: {self.Employment_Date}"
            )

    def __generate_employee(self) -> Employee:
        name = f"{self.First_Name} {self.Last_Name}"
        person_uuid = generate_uuid(name)
        return Employee(uuid=person_uuid, name=name, seniority=self.Seniority)

    def __get_from_to_dates(self) -> Tuple[str, Optional[str]]:
        from_date = datetime.strptime(self.Employment_Date, "%d-%b-%y").strftime(
            "%Y-%m-%d"
        )
        to_date = None
        if self.Departure_Date:
            to_date = datetime.strptime(self.Departure_Date, "%d-%b-%y")
            # seems like Orsted data is weird, quickfix: roll ahead one centry
            if to_date == datetime(year=1999, month=12, day=31):
                to_date = datetime(year=2099, month=12, day=31)
            to_date = to_date.strftime("%Y-%m-%d")
        return from_date, to_date

    def __generate_engagement(
        self,
        *,
        person_uuid: UUID,
        job_function_uuid_generator: UUIDGenerator,
        engagement_type_uuid_generator: UUIDGenerator,
        org_unit_uuids: Dict[str, str],
        primary_uuid: UUID,
    ) -> Engagement:
        engagement_type = (
            self.Hr_Employee_Type
            if self.Hr_Employee_Type
            else self.Hr_Consultant_Type
        )

        job_function_uuid = job_function_uuid_generator.get_uuid(self.Title)
        engagement_type_uuid = engagement_type_uuid_generator.get_uuid(
            engagement_type
        )

        engagement_seed = "unique_engagement_salt_" + self.Initials
        engagement_uuid = generate_uuid(engagement_seed)

        from_date, to_date = self.__get_from_to_dates()

        return Engagement.from_simplified_fields(
            uuid=engagement_uuid,
            org_unit_uuid=org_unit_uuids[str(self.Hr_Department_No)],
            person_uuid=person_uuid,
            job_function_uuid=job_function_uuid,
            engagement_type_uuid=engagement_type_uuid,
            from_date=from_date,
            to_date=to_date,
            primary_uuid=primary_uuid,
            user_key=self.Initials,
            extension_1=self.Hr_Employee_No,
            extension_2=self.Hr_Hirakey_Relevant,
            extension_3=self.Hr_Active_Status,
            extension_4=self.Ee_Subgroup,
            extension_5=self.Career_Level,
            extension_6=self.Position_Level,
            extension_7=self.Career_Track,
        )

    def __generate_addresses(
        self,
        visible_uuid,
        not_visible_uuid,
        engagement_uuid,
        address_klasses: AddressKlasses,
        from_date,
        to_date,
        person_uuid,
        org_uuid: UUID,
    ) -> List[Address]:
        visibility_uuid = (
            visible_uuid if self.Phonebook_Relevant else not_visible_uuid
        )

        address_candidates = [
            (self.Email, None, address_klasses.EMAIL),
            (self.Phone, None, address_klasses.PHONE),
            (self.Mobile, None, address_klasses.MOBILE),
            (self.Hr_Country_Code, None, address_klasses.HR_COUNTRY_CODE),
            (self.Hr_Tdb_Fixed_Phone, None, address_klasses.HR_TDB_FIXED_PHONE),
            (self.Hr_Tdb_Mobile_Phone, None, address_klasses.HR_TDB_MOBILE_PHONE),
            (self.Consultant_Email, None, address_klasses.CONSULTANT_EMAIL),
            (
                self.Hr_Location_Id,
                self.Office,
                address_klasses.HR_LOCATION_ID_and_OFFICE,
            ),
        ]

        def to_address(
            value: str, value2: Optional[str], address_type_klasse: Klasse
        ) -> Optional[Address]:
            if not value:
                return
            if address_type_klasse.attributes.properties[0].scope == "PHONE":
                try:
                    int(value)
                except Exception:
                    return None
            return Address.from_simplified_fields(
                uuid=generate_uuid(
                    str(engagement_uuid) + str(address_type_klasse.uuid) + value + str(value2)
                ),
                value=value,
                value2=value2,
                address_type_uuid=address_type_klasse.uuid,
                engagement_uuid=engagement_uuid,
                from_date=from_date,
                to_date=to_date,
                visibility_uuid=visibility_uuid,
                org_uuid=org_uuid,
            )

        addresses = list(
            filter(
                lambda tmp: tmp is not None,
                [
                    to_address(value, value2, address_type_klasse)
                    for value, value2, address_type_klasse in address_candidates
                ],
            )
        )
        if self.Country_Of_Residence:
            addresses.append(
                Address.from_simplified_fields(
                    uuid=generate_uuid(
                        str(person_uuid)
                        + str(address_klasses.COUNTRY_OF_RESIDENCE.uuid)
                        + self.Country_Of_Residence
                    ),
                    value=self.Country_Of_Residence,
                    value2=None,
                    address_type_uuid=address_klasses.COUNTRY_OF_RESIDENCE.uuid,
                    person_uuid=person_uuid,
                    from_date=from_date,
                    to_date=to_date,
                    visibility_uuid=visibility_uuid,
                    org_uuid=org_uuid,
                )
            )

        return addresses

    def __generate_org_units(
        self,
        org_unit_type_uuid,
        org_unit_level_uuid,
        financial_org_unit_uuid,
        legal_org_unit_uuid,
    ) -> Tuple[Optional[OrgUnit], Optional[OrgUnit], Optional[OrgUnit]]:
        cost_center = None
        if self.Hr_Cost_Center:
            cost_center = OrgUnit.from_simplified_fields(
                uuid=generate_uuid("unique_org_unit_seed" + self.Hr_Cost_Center),
                user_key=self.Hr_Cost_Center,
                name=self.Hr_Cost_Center,
                parent_uuid=financial_org_unit_uuid,
                org_unit_hierarchy_uuid=None,
                org_unit_type_uuid=org_unit_type_uuid,
                org_unit_level_uuid=org_unit_level_uuid,
            )

        sender_cost_center = None
        if self.Sender_Cost_Center:
            sender_cost_center = OrgUnit.from_simplified_fields(
                uuid=generate_uuid("unique_org_unit_seed" + self.Sender_Cost_Center),
                user_key=self.Sender_Cost_Center,
                name=self.Sender_Cost_Center,
                parent_uuid=financial_org_unit_uuid,
                org_unit_hierarchy_uuid=None,
                org_unit_type_uuid=org_unit_type_uuid,
                org_unit_level_uuid=org_unit_level_uuid,
            )

        legal_company = None
        if self.Hr_Legal_Company:
            legal_company = OrgUnit.from_simplified_fields(
                uuid=generate_uuid("unique_org_unit_seed" + self.Hr_Legal_Company),
                user_key=self.Hr_Company_Code,
                name=self.Hr_Legal_Company,
                parent_uuid=legal_org_unit_uuid,
                org_unit_hierarchy_uuid=None,
                org_unit_type_uuid=org_unit_type_uuid,
                org_unit_level_uuid=org_unit_level_uuid,
            )
        return cost_center, sender_cost_center, legal_company

    @staticmethod
    def __generate_engagement_association(
        org_unit_uuids: Iterable[UUID],
        engagement_uuid: UUID,
        association_type_uuid: UUID,
        from_date: str,
        to_date: Optional[str],
    ) -> List[EngagementAssociation]:
        return [
            EngagementAssociation.from_simplified_fields(
                uuid=generate_uuid(
                    "engagement_association"
                    + str(engagement_uuid)
                    + str(org_unit_uuid)
                    + str(association_type_uuid)
                ),
                org_unit_uuid=org_unit_uuid,
                engagement_uuid=engagement_uuid,
                engagement_association_type_uuid=association_type_uuid,
                from_date=from_date,
                to_date=to_date,
            )
            for org_unit_uuid in org_unit_uuids
        ]

    def to_mo_objs(
        self,
        org_unit_uuids: Dict[str, str],
        primary_uuid: UUID,
        engagement_type_uuid_generator: UUIDGenerator,
        job_function_uuid_generator: UUIDGenerator,
        visible_uuid: UUID,
        not_visible_uuid: UUID,
        address_klasses: AddressKlasses,
        org_unit_type_uuid: UUID,
        financial_org_unit_uuid: UUID,
        legal_org_unit_uuid: UUID,
        engagement_association_type_uuid: UUID,
        org_unit_level_uuid: UUID,
        org_uuid: UUID,
    ):
        """
        :param org_unit_uuids: Hr_Department_No: uuid
        :param primary_uuid: Precreated Klasse -- I guess NOT primary for now?
        :param engagement_type_uuid_generator: Generate on-the-fly
        :param job_function_uuid_generator: Generate on-the-fly
        :param visible_uuid: Precreated Klasse
        :param not_visible_uuid: Precreated Klasse
        :param address_klasses: Obj containing precreated Address-related Klass'es
        :param org_unit_type_uuid: Precreated Klasse
        :param financial_org_unit_uuid: Precreated OU
        :param legal_org_unit_uuid: Precreated OU
        :param org_unit_level_uuid: Precreated Klasse
        :param engagement_association_type_uuid: Precreated Klasse
        :param org_uuid:
        :return:
        """

        try:
            self.__business_validate()
        except OrstedBusinessError:
            # traceback.print_exc()
            return

        employee = self.__generate_employee()

        engagement = self.__generate_engagement(
            person_uuid=employee.uuid,
            job_function_uuid_generator=job_function_uuid_generator,
            engagement_type_uuid_generator=engagement_type_uuid_generator,
            org_unit_uuids=org_unit_uuids,
            primary_uuid=primary_uuid,
        )

        addresses = self.__generate_addresses(
            visible_uuid=visible_uuid,
            not_visible_uuid=not_visible_uuid,
            engagement_uuid=engagement.uuid,
            address_klasses=address_klasses,
            from_date=engagement.validity.from_date,
            to_date=engagement.validity.to_date,
            person_uuid=employee.uuid,
            org_uuid=org_uuid,
        )

        org_units = list(
            filter(
                lambda ou: ou is not None,
                self.__generate_org_units(
                    org_unit_type_uuid=org_unit_type_uuid,
                    financial_org_unit_uuid=financial_org_unit_uuid,
                    legal_org_unit_uuid=legal_org_unit_uuid,
                    org_unit_level_uuid=org_unit_level_uuid,
                ),
            )
        )
        engagement_associations = self.__generate_engagement_association(
            org_unit_uuids=[ou.uuid for ou in org_units],
            engagement_uuid=engagement.uuid,
            association_type_uuid=engagement_association_type_uuid,
            from_date=engagement.validity.from_date,
            to_date=engagement.validity.to_date,
        )

        # order matters!
        return employee, org_units, engagement, engagement_associations, addresses


def read_emd(
    path: Path,
    org_unit_uuids: Dict[str, str],
    primary_uuid: UUID,
    engagement_type_uuid_generator: UUIDGenerator,
    job_function_uuid_generator: UUIDGenerator,
    visible_uuid: UUID,
    not_visible_uuid: UUID,
    address_klasses: AddressKlasses,
    org_unit_type_uuid: UUID,
    financial_org_unit_uuid: UUID,
    legal_org_unit_uuid: UUID,
    engagement_association_type_uuid: UUID,
    org_unit_level_uuid: UUID,
    org_uuid: UUID,
) -> Tuple[Iterable[Iterable[MoObj]], ...]:
    with path.open("r") as file:
        raw_emd_datas: Iterable[RawEmdData] = map(
            RawEmdData.parse_obj, DictReader(file, delimiter=";")
        )
        rows = [
            row.to_mo_objs(
                org_unit_uuids=org_unit_uuids,
                primary_uuid=primary_uuid,
                engagement_type_uuid_generator=engagement_type_uuid_generator,
                job_function_uuid_generator=job_function_uuid_generator,
                visible_uuid=visible_uuid,
                not_visible_uuid=not_visible_uuid,
                address_klasses=address_klasses,
                org_unit_type_uuid=org_unit_type_uuid,
                financial_org_unit_uuid=financial_org_unit_uuid,
                legal_org_unit_uuid=legal_org_unit_uuid,
                engagement_association_type_uuid=engagement_association_type_uuid,
                org_unit_level_uuid=org_unit_level_uuid,
                org_uuid=org_uuid,
            )
            for row in raw_emd_datas
        ]
        rows = filter(lambda objs: objs is not None, rows)
        # convert from rows to columns
        (
            employees,
            org_unitss,
            engagements,
            engagement_associationss,
            addressess,
        ) = zip(*rows)
        flat_ous = list(set(flatten(org_unitss)))  # get unique
        return (  # FIX THIS
            [[emp] for emp in employees],  # employees one at a time
            [[ou] for ou in flat_ous],  # one at a time for some reason
            [[eng] for eng in engagements],  # one at a time for some reason
            [[ea] for ea in flatten(engagement_associationss)],
            [[addr] for addr in flatten(addressess)]
            # one at a time for some reason
        )


if __name__ == "__main__":

    with Path("/home/mw/gir/gir_from_emd.csv").open("r") as f:
        raw_emd_dat = map(RawEmdData.parse_obj, DictReader(f, delimiter=";"))
        for x in raw_emd_dat:
            print(x)
            exit()
