from typing import List, Literal, Optional

from pydantic import Field

from os2mo_data_import.MoClient.model_parts.interface import MoObj
from os2mo_data_import.MoClient.model_parts.lora import (
    Authority,
    EffectiveTime,
    FacetAttributes,
    FacetProperties,
    FacetRef,
    FacetRelations,
    FacetStates,
    KlasseAttributes,
    KlasseProperties,
    KlasseRelations,
    KlasseStates,
    OrganisationAttributes,
    OrganisationProperties,
    OrganisationRelations,
    OrganisationStates,
    OrganisationValidState,
    Published,
    Responsible,
)
from os2mo_data_import.MoClient.model_parts.mo import (
    AddressType,
    EngagementAssociationType,
    EngagementRef,
    EngagementType,
    JobFunction,
    ManagerLevel,
    ManagerType,
    OrganisationRef,
    OrgUnitHierarchy,
    OrgUnitLevel,
    OrgUnitRef,
    OrgUnitType,
    Parent,
    Person,
    Primary,
    Responsibility,
    Validity,
    Visibility,
)


class OrgUnit(MoObj):
    type: Literal["org_unit"] = "org_unit"
    uuid: str
    user_key: str
    validity: Validity
    name: str
    parent: Optional[Parent] = None
    org_unit_hierarchy: Optional[OrgUnitHierarchy] = None
    org_unit_type: OrgUnitType
    org_unit_level: OrgUnitLevel

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        user_key: str,
        name: str,
        org_unit_type_uuid: str,
        org_unit_level_uuid: str,
        parent_uuid: Optional[str] = None,
        org_unit_hierarchy_uuid: Optional[str] = None,
        from_date: str = "1930-01-01",
        to_date: Optional[str] = None,
    ) -> "OrgUnit":
        parent = None
        if parent_uuid:
            parent = Parent(uuid=parent_uuid)

        org_unit_hierarchy = None
        if org_unit_hierarchy_uuid:
            org_unit_hierarchy = OrgUnitHierarchy(uuid=org_unit_hierarchy_uuid)

        return cls(
            uuid=uuid,
            user_key=user_key,
            validity=Validity(from_date=from_date, to_date=to_date),
            name=name,
            parent=parent,
            org_unit_hierarchy=org_unit_hierarchy,
            org_unit_type=OrgUnitType(uuid=org_unit_type_uuid),
            org_unit_level=OrgUnitLevel(uuid=org_unit_level_uuid),
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Employee(MoObj):
    type: str = "employee"
    uuid: str
    name: str
    cpr_no: Optional[str] = None
    seniority: Optional[str] = None

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Engagement(MoObj):
    type: str = "engagement"
    uuid: str
    org_unit: OrgUnitRef
    person: Person
    job_function: JobFunction
    engagement_type: EngagementType
    validity: Validity
    primary: Primary
    user_key: str
    extension_1: str = ""
    extension_2: str = ""
    extension_3: str = ""
    extension_4: str = ""
    extension_5: str = ""
    extension_6: str = ""
    extension_7: str = ""
    extension_8: str = ""
    extension_9: str = ""
    extension_10: str = ""

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        org_unit_uuid: str,
        person_uuid: str,
        job_function_uuid: str,
        engagement_type_uuid: str,
        from_date: Optional[str],
        to_date: Optional[str],
        primary_uuid: str,
        user_key: str,
        extension_1: str = "",
        extension_2: str = "",
        extension_3: str = "",
        extension_4: str = "",
        extension_5: str = "",
        extension_6: str = "",
        extension_7: str = "",
        extension_8: str = "",
        extension_9: str = "",
        extension_10: str = "",
    ):
        return cls(
            uuid=uuid,
            org_unit=OrgUnitRef(uuid=org_unit_uuid),
            person=Person(uuid=person_uuid),
            job_function=JobFunction(uuid=job_function_uuid),
            engagement_type=EngagementType(uuid=engagement_type_uuid),
            validity=Validity(from_date=from_date, to_date=to_date),
            primary=Primary(uuid=primary_uuid),
            user_key=user_key,
            extension_1=extension_1,
            extension_2=extension_2,
            extension_3=extension_3,
            extension_4=extension_4,
            extension_5=extension_5,
            extension_6=extension_6,
            extension_7=extension_7,
            extension_8=extension_8,
            extension_9=extension_9,
            extension_10=extension_10,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Address(MoObj):
    type = "address"
    uuid: str
    value: str
    value2: Optional[str]
    address_type: AddressType
    org: OrganisationRef
    person: Optional[Person] = None
    org_unit: Optional[OrgUnitRef] = None
    engagement: Optional[EngagementRef] = None
    validity: Validity
    visibility: Optional[Visibility] = None

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        value: str,
        value2: Optional[str],
        address_type_uuid: str,
        org_uuid: str,
        from_date: str,
        to_date: Optional[str] = None,
        person_uuid: Optional[str] = None,
        org_unit_uuid: Optional[str] = None,
        engagement_uuid: Optional[str] = None,
        visibility_uuid: Optional[str] = None,
    ):
        address_type = AddressType(uuid=address_type_uuid)
        org = OrganisationRef(uuid=org_uuid)
        validity = Validity(from_date=from_date, to_date=to_date)
        person = None
        if person_uuid:
            person = Person(uuid=person_uuid)
        org_unit = None
        if org_unit_uuid:
            org_unit = OrgUnitRef(uuid=org_unit_uuid)
        engagement = None
        if engagement_uuid:
            engagement = EngagementRef(uuid=engagement_uuid)
        visibility = None
        if visibility_uuid:
            visibility = Visibility(uuid=visibility_uuid)
        return cls(
            uuid=uuid,
            value=value,
            value2=value2,
            address_type=address_type,
            org=org,
            person=person,
            org_unit=org_unit,
            engagement=engagement,
            visibility=visibility,
            validity=validity,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Manager(MoObj):
    type: str = "manager"
    uuid: str
    # user_key: str
    org_unit: OrgUnitRef
    person: Person
    responsibility: List[Responsibility]
    manager_level: ManagerLevel
    manager_type: ManagerType
    validity: Validity

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        # user_key: str,
        org_unit_uuid: str,
        person_uuid: str,
        responsibility_uuid: str,
        manager_level_uuid: str,
        manager_type_uuid: str,
        from_date: str = "1930-01-01",
        to_date: Optional[str] = None,
    ):
        person = Person(uuid=person_uuid)
        org_unit = OrgUnitRef(uuid=org_unit_uuid)
        responsibility = [Responsibility(uuid=responsibility_uuid)]
        manager_level = ManagerLevel(uuid=manager_level_uuid)
        manager_type = ManagerType(uuid=manager_type_uuid)
        validity = Validity(from_date=from_date, to_date=to_date)

        return cls(
            uuid=uuid,
            # user_key=        # user_key,
            org_unit=org_unit,
            person=person,
            responsibility=responsibility,
            manager_level=manager_level,
            manager_type=manager_type,
            validity=validity,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class EngagementAssociation(MoObj):
    type: str = "engagement_association"
    uuid: str
    # user_key: str
    org_unit: OrgUnitRef
    engagement: EngagementRef
    engagement_association_type: EngagementAssociationType
    validity: Validity

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        # user_key: str,
        org_unit_uuid: str,
        engagement_uuid: str,
        engagement_association_type_uuid: str,
        from_date: str = "1930-01-01",
        to_date: Optional[str] = None,
    ):
        validity = Validity(from_date=from_date, to_date=to_date)
        org_unit = OrgUnitRef(uuid=org_unit_uuid)
        engagement = EngagementRef(uuid=engagement_uuid)
        engagement_association_type = EngagementAssociationType(
            uuid=engagement_association_type_uuid
        )
        return cls(
            uuid=uuid,
            org_unit=org_unit,
            engagement=engagement,
            engagement_association_type=engagement_association_type,
            validity=validity,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Organisation(MoObj):
    attributes: OrganisationAttributes = Field(alias="attributter")
    states: OrganisationStates = Field(alias="tilstande")
    relations: Optional[OrganisationRelations] = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included,
    # and has to be excluded manually when converting to json
    uuid: Optional[str] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        name: str,
        user_key: str,  # often == name,
        municipality_code: Optional[int] = None,
        date_from: str = "1930-01-01",
        date_to: str = "infinity",
    ):
        effective_time = EffectiveTime(from_date=date_from, to_date=date_to)
        attributes = OrganisationAttributes(
            properties=[
                OrganisationProperties(
                    user_key=user_key, name=name, effective_time=effective_time
                )
            ]
        )
        states = OrganisationStates(
            valid_state=[OrganisationValidState(effective_time=effective_time)]
        )

        relations = None
        if municipality_code:
            relations = OrganisationRelations(
                authority=[
                    Authority(
                        urn=f"urn:dk:kommune:{municipality_code}",
                        effective_time=effective_time,
                    )
                ]
            )

        return cls(
            attributes=attributes,
            states=states,
            relations=relations,
            uuid=uuid,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Facet(MoObj):
    attributes: FacetAttributes = Field(alias="attributter")
    states: FacetStates = Field(alias="tilstande")
    relations: FacetRelations = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included, and has to be excluded when converted to json
    uuid: Optional[str] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: str,
        user_key: str,
        organisation_uuid: str,
        date_from: str = "1930-01-01",
        date_to: str = "infinity",
    ):
        effective_time = EffectiveTime(from_date=date_from, to_date=date_to)
        attributes = FacetAttributes(
            properties=[
                FacetProperties(user_key=user_key, effective_time=effective_time)
            ]
        )
        states = FacetStates(
            published_state=[Published(effective_time=effective_time)]
        )

        relations = FacetRelations(
            responsible=[
                Responsible(
                    uuid=organisation_uuid,
                    effective_time=effective_time,
                )
            ]
        )
        return cls(
            attributes=attributes,
            states=states,
            relations=relations,
            uuid=uuid,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid


class Klasse(MoObj):
    attributes: KlasseAttributes = Field(alias="attributter")
    states: KlasseStates = Field(alias="tilstande")
    relations: KlasseRelations = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included, and has to be excluded when converted to json
    uuid: Optional[str] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        facet_uuid: str,  # uuid
        uuid: str,
        user_key: str,  # rarely used
        scope: Optional[str],
        organisation_uuid: str,
        title: str,
        date_from: str = "1930-01-01",
        date_to: str = "infinity",
    ):
        effective_time = EffectiveTime(from_date=date_from, to_date=date_to)
        attributes = KlasseAttributes(
            properties=[
                KlasseProperties(
                    user_key=user_key,
                    title=title,
                    scope=scope,
                    effective_time=effective_time,
                )
            ]
        )
        states = KlasseStates(
            published_state=[Published(effective_time=effective_time)]
        )

        relations = KlasseRelations(
            responsible=[
                Responsible(
                    uuid=organisation_uuid,
                    effective_time=effective_time,
                )
            ],
            facet=[
                FacetRef(
                    uuid=facet_uuid,
                    effective_time=effective_time,
                )
            ],
        )
        return cls(
            attributes=attributes,
            states=states,
            relations=relations,
            uuid=uuid,
        )

    def get_uuid(self) -> Optional[str]:
        return self.uuid
