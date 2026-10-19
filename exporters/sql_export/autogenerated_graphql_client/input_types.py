# This file has been modified by the UnsetInputTypesPlugin
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from pydantic import Field

from .base_model import UNSET, BaseModel, UnsetType
from .enums import (
    AccessLogModel,
    Collection,
    FileStore,
    GraphQLVersion,
    Mutator,
    OwnerInferencePriority,
    PolicySelectorKind,
)


class AccessLogFilter(BaseModel):
    ids: Optional[List[UUID]] = None
    uuids: Optional[List[UUID]] = None
    actors: Optional[List[UUID]] = None
    models: Optional[List[AccessLogModel]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ActorFilter(BaseModel):
    uuids: Optional[List[UUID]] = None


class AddressCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    org_unit: Optional[UUID] = None
    person: Optional[UUID] = None
    employee: Optional[UUID] = None
    engagement: Optional[UUID] = None
    ituser: Optional[UUID] = None
    visibility: Optional[UUID] = None
    validity: "RAValidityInput"
    user_key: Optional[str] = None
    value: str
    address_type: UUID


class AddressFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AddressRegistrationFilter"] = None
    address_type: Optional["ClassFilter"] = None
    address_types: Optional[List[UUID]] = None
    address_type_user_keys: Optional[List[str]] = None
    engagement: Optional["EngagementFilter"] = None
    engagements: Optional[List[UUID]] = None
    ituser: Optional["ITUserFilter"] = None
    visibility: Optional["ClassFilter"] = None


class AddressRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class AddressTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class AddressUpdateInput(BaseModel):
    uuid: UUID
    org_unit: Optional[UUID] = None
    person: Optional[UUID] = None
    employee: Optional[UUID] = None
    engagement: Optional[UUID] = None
    ituser: Optional[UUID] = None
    visibility: Optional[UUID] = None
    validity: "RAValidityInput"
    user_key: Optional[str] = None
    value: Optional[str] = None
    address_type: Optional[UUID] = None


class AssociationCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    validity: "RAValidityInput"
    person: Optional[UUID] = None
    employee: Optional[UUID] = None
    substitute: Optional[UUID] = None
    trade_union: Optional[UUID] = None
    org_unit: UUID
    association_type: UUID


class AssociationFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AssociationRegistrationFilter"] = None
    association_type: Optional["ClassFilter"] = None
    association_types: Optional[List[UUID]] = None
    association_type_user_keys: Optional[List[str]] = None
    it_association: Optional[bool] = None


class AssociationRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class AssociationTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class AssociationUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    validity: "RAValidityInput"
    person: Optional[UUID] = None
    employee: Optional[UUID] = None
    substitute: Optional[UUID] = None
    trade_union: Optional[UUID] = None
    org_unit: Optional[UUID] = None
    association_type: Optional[UUID] = None


class ClassCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    name: str
    user_key: str
    facet_uuid: UUID
    scope: Optional[str] = None
    published: str = "Publiceret"
    parent_uuid: Optional[UUID] = None
    example: Optional[str] = None
    owner: Optional[UUID] = None
    validity: "ValidityInput"
    it_system_uuid: Optional[UUID] = None
    description: Optional[str] = None


class ClassFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ClassRegistrationFilter"] = None
    name: Optional[List[str]] = None
    facet: Optional["FacetFilter"] = None
    facets: Optional[List[UUID]] = None
    facet_user_keys: Optional[List[str]] = None
    parent: Optional["ClassFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None
    it_system: Optional["ITSystemFilter"] = None
    owner: Optional["ClassOwnerFilter"] = None
    scope: Optional[List[str]] = None


class ClassOwnerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["OrganisationUnitRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    names: Optional[List[str]] | UnsetType = UNSET
    parent: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    parents: Optional[List[UUID]] | UnsetType = UNSET
    child: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    hierarchy: Optional["ClassFilter"] = None
    hierarchies: Optional[List[UUID]] = None
    subtree: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    descendant: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    ancestor: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    engagement: Optional["EngagementFilter"] = None
    owner: Optional["OwnerFilter"] = None
    include_none: bool = False


class ClassRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ClassTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class ClassUpdateInput(BaseModel):
    uuid: UUID
    name: str
    user_key: str
    facet_uuid: UUID
    scope: Optional[str] = None
    published: str = "Publiceret"
    parent_uuid: Optional[UUID] = None
    example: Optional[str] = None
    owner: Optional[UUID] = None
    validity: "ValidityInput"
    it_system_uuid: Optional[UUID] = None
    description: Optional[str] = None


class DescendantParentBoundOrganisationUnitFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["OrganisationUnitRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    names: Optional[List[str]] | UnsetType = UNSET
    child: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    hierarchy: Optional["ClassFilter"] = None
    hierarchies: Optional[List[UUID]] = None
    ancestor: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    engagement: Optional["EngagementFilter"] = None
    owner: Optional["OwnerFilter"] = None


class EmployeeBoundAddressFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AddressRegistrationFilter"] = None
    address_type: Optional["ClassFilter"] = None
    address_types: Optional[List[UUID]] = None
    address_type_user_keys: Optional[List[str]] = None
    engagement: Optional["EngagementFilter"] = None
    engagements: Optional[List[UUID]] = None
    ituser: Optional["ITUserFilter"] = None
    visibility: Optional["ClassFilter"] = None


class EmployeeBoundAssociationFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AssociationRegistrationFilter"] = None
    association_type: Optional["ClassFilter"] = None
    association_types: Optional[List[UUID]] = None
    association_type_user_keys: Optional[List[str]] = None
    it_association: Optional[bool] = None


class EmployeeBoundEngagementFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["EngagementRegistrationFilter"] = None
    job_function: Optional["ClassFilter"] = None
    engagement_type: Optional["ClassFilter"] = None
    explicit_manager: Optional["ManagerFilter"] | UnsetType = UNSET
    ituser: Optional["ITUserFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class EmployeeBoundITUserFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ITUserRegistrationFilter"] = None
    itsystem: Optional["ITSystemFilter"] = None
    itsystem_uuids: Optional[List[UUID]] = None
    engagement: Optional["EngagementFilter"] = None
    rolebinding: Optional["RoleBindingFilter"] | UnsetType = UNSET
    external_ids: Optional[List[str]] | UnsetType = UNSET
    binding_types: Optional[List[str]] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class EmployeeBoundLeaveFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["LeaveRegistrationFilter"] = None


class EmployeeBoundManagerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ManagerRegistrationFilter"] = None
    engagement: Optional["EngagementFilter"] = None
    responsibility: Optional["ClassFilter"] = None
    manager_type: Optional["ClassFilter"] = None
    exclude: Optional["EmployeeFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class EmployeeCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    nickname_given_name: Optional[str] = None
    nickname_surname: Optional[str] = None
    seniority: Optional[Any] = None
    cpr_number: Optional[Any] = None
    given_name: str
    surname: str


class EmployeeFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["EmployeeRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    cpr_numbers: Optional[List[Any]] = None
    owner: Optional["OwnerFilter"] = None
    ituser: Optional["ITUserFilter"] | UnsetType = UNSET


class EmployeeRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class EmployeeTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID
    vacate: bool = False


class EmployeeUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    nickname_given_name: Optional[str] = None
    nickname_surname: Optional[str] = None
    seniority: Optional[Any] = None
    cpr_number: Optional[Any] = None
    given_name: Optional[str] = None
    surname: Optional[str] = None
    validity: "RAValidityInput"


class EngagementBoundAddressFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AddressRegistrationFilter"] = None
    address_type: Optional["ClassFilter"] = None
    address_types: Optional[List[UUID]] = None
    address_type_user_keys: Optional[List[str]] = None
    ituser: Optional["ITUserFilter"] = None
    visibility: Optional["ClassFilter"] = None


class EngagementBoundITUserFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ITUserRegistrationFilter"] = None
    itsystem: Optional["ITSystemFilter"] = None
    itsystem_uuids: Optional[List[UUID]] = None
    rolebinding: Optional["RoleBindingFilter"] | UnsetType = UNSET
    external_ids: Optional[List[str]] | UnsetType = UNSET
    binding_types: Optional[List[str]] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class EngagementCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    validity: "RAValidityInput"
    fraction: Optional[int] = None
    extension_1: Optional[str] = None
    extension_2: Optional[str] = None
    extension_3: Optional[str] = None
    extension_4: Optional[str] = None
    extension_5: Optional[str] = None
    extension_6: Optional[str] = None
    extension_7: Optional[str] = None
    extension_8: Optional[str] = None
    extension_9: Optional[str] = None
    extension_10: Optional[str] = None
    explicit_manager: Optional[UUID] = None
    employee: Optional[UUID] = None
    person: Optional[UUID] = None
    org_unit: UUID
    engagement_type: UUID
    job_function: UUID


class EngagementFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["EngagementRegistrationFilter"] = None
    job_function: Optional["ClassFilter"] = None
    engagement_type: Optional["ClassFilter"] = None
    explicit_manager: Optional["ManagerFilter"] | UnsetType = UNSET
    ituser: Optional["ITUserFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class EngagementRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class EngagementTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class EngagementUpdateInput(BaseModel):
    uuid: UUID
    validity: "RAValidityInput"
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    fraction: Optional[int] = None
    extension_1: Optional[str] = None
    extension_2: Optional[str] = None
    extension_3: Optional[str] = None
    extension_4: Optional[str] = None
    extension_5: Optional[str] = None
    extension_6: Optional[str] = None
    extension_7: Optional[str] = None
    extension_8: Optional[str] = None
    extension_9: Optional[str] = None
    extension_10: Optional[str] = None
    employee: Optional[UUID] = None
    person: Optional[UUID] = None
    org_unit: Optional[UUID] = None
    engagement_type: Optional[UUID] = None
    job_function: Optional[UUID] = None
    explicit_manager: Optional[UUID] | UnsetType = UNSET


class EventAcknowledgeInput(BaseModel):
    token: Any


class EventCreatedAtFilter(BaseModel):
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class EventFilter(BaseModel):
    listener: UUID


class EventRerunInput(BaseModel):
    listeners: Optional["ListenerFilter"] = None
    subjects: Optional[List[str]] = None
    priorities: Optional[List[int]] = None
    silenced: Optional[bool] = None


class EventSendInput(BaseModel):
    namespace: str
    routing_key: str
    subject: str
    priority: int = 10000


class EventSilenceInput(BaseModel):
    listeners: "ListenerFilter"
    subjects: List[str]


class EventUnsilenceInput(BaseModel):
    listeners: Optional["ListenerFilter"] = None
    subjects: Optional[List[str]] = None
    priorities: Optional[List[int]] = None


class FacetBoundClassFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ClassRegistrationFilter"] = None
    name: Optional[List[str]] = None
    parent: Optional["ClassFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None
    it_system: Optional["ITSystemFilter"] = None
    owner: Optional["ClassOwnerFilter"] = None
    scope: Optional[List[str]] = None


class FacetCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: str
    published: str = "Publiceret"
    validity: "ValidityInput"


class FacetFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["FacetRegistrationFilter"] = None
    parent: Optional["FacetFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None


class FacetRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class FacetTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class FacetUpdateInput(BaseModel):
    uuid: UUID
    user_key: str
    published: str = "Publiceret"
    validity: "ValidityInput"


class FileFilter(BaseModel):
    file_store: FileStore
    file_names: Optional[List[str]] = None
    file_name_contains: Optional[str] = None


class FullEventFilter(BaseModel):
    listeners: Optional["ListenerFilter"] = None
    subjects: Optional[List[str]] = None
    priorities: Optional[List[int]] = None
    silenced: Optional[bool] = None
    created_at: Optional["EventCreatedAtFilter"] = None


class HealthFilter(BaseModel):
    identifiers: Optional[List[str]] = None


class ITAssociationCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    validity: "RAValidityInput"
    org_unit: UUID
    person: UUID
    it_user: UUID
    job_function: UUID


class ITAssociationTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class ITAssociationUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    primary: Optional[UUID] = None
    validity: "RAValidityInput"
    org_unit: Optional[UUID] = None
    it_user: Optional[UUID] = None
    job_function: Optional[UUID] = None


class ITSystemCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: str
    name: str
    validity: "RAOpenValidityInput"


class ITSystemFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ITSystemRegistrationFilter"] = None


class ITSystemRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ITSystemTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class ITSystemUpdateInput(BaseModel):
    uuid: UUID
    user_key: str
    name: str
    validity: "RAOpenValidityInput"


class ITUserCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    external_id: Optional[str] = None
    binding_type: Optional[str] = None
    primary: Optional[UUID] = None
    person: Optional[UUID] = None
    org_unit: Optional[UUID] = None
    engagement: Optional[UUID] = None
    engagements: Optional[List[UUID]] = None
    validity: "RAValidityInput"
    user_key: str
    itsystem: UUID
    note: Optional[str] = None


class ITUserFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ITUserRegistrationFilter"] = None
    itsystem: Optional["ITSystemFilter"] = None
    itsystem_uuids: Optional[List[UUID]] = None
    engagement: Optional["EngagementFilter"] = None
    rolebinding: Optional["RoleBindingFilter"] | UnsetType = UNSET
    external_ids: Optional[List[str]] | UnsetType = UNSET
    binding_types: Optional[List[str]] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class ITUserRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ITUserTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class ITUserUpdateInput(BaseModel):
    uuid: UUID
    external_id: Optional[str] = None
    binding_type: Optional[str] = None
    primary: Optional[UUID] = None
    person: Optional[UUID] = None
    org_unit: Optional[UUID] = None
    engagement: Optional[UUID] = None
    engagements: Optional[List[UUID]] = None
    validity: "RAValidityInput"
    user_key: Optional[str] = None
    itsystem: Optional[UUID] = None
    note: Optional[str] = None


class ItSystemboundclassfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ClassRegistrationFilter"] = None
    name: Optional[List[str]] = None
    facet: Optional["FacetFilter"] = None
    facets: Optional[List[UUID]] = None
    facet_user_keys: Optional[List[str]] = None
    parent: Optional["ClassFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None
    owner: Optional["ClassOwnerFilter"] = None
    scope: Optional[List[str]] = None


class ItuserBoundAddressFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["AddressRegistrationFilter"] = None
    address_type: Optional["ClassFilter"] = None
    address_types: Optional[List[UUID]] = None
    address_type_user_keys: Optional[List[str]] = None
    engagement: Optional["EngagementFilter"] = None
    engagements: Optional[List[UUID]] = None
    visibility: Optional["ClassFilter"] = None


class ItuserBoundRoleBindingFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["RoleRegistrationFilter"] = None
    role: Optional["ClassFilter"] = None


class KLECreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    org_unit: UUID
    kle_aspects: List[UUID]
    kle_number: UUID
    validity: "RAValidityInput"


class KLEFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["KLERegistrationFilter"] = None


class KLERegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class KLETerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class KLEUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    kle_number: Optional[UUID] = None
    kle_aspects: Optional[List[UUID]] = None
    org_unit: Optional[UUID] = None
    validity: "RAValidityInput"


class LeaveCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    person: UUID
    engagement: UUID
    leave_type: UUID
    validity: "RAValidityInput"


class LeaveFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["LeaveRegistrationFilter"] = None


class LeaveRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class LeaveTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class LeaveUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    person: Optional[UUID] = None
    engagement: Optional[UUID] = None
    leave_type: Optional[UUID] = None
    validity: "RAValidityInput"


class ListenerCreateInput(BaseModel):
    namespace: str = "mo"
    user_key: str
    routing_key: str


class ListenerDeleteInput(BaseModel):
    uuid: UUID
    delete_pending_events: bool = False


class ListenerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    owners: Optional[List[UUID]] = None
    routing_keys: Optional[List[str]] = None
    namespaces: Optional["NamespaceFilter"] = None


class ListenersBoundFullEventFilter(BaseModel):
    subjects: Optional[List[str]] = None
    priorities: Optional[List[int]] = None
    silenced: Optional[bool] = None
    created_at: Optional["EventCreatedAtFilter"] = None


class ManagerCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    person: Optional[UUID] = None
    responsibility: List[UUID]
    engagement: Optional[UUID] = None
    org_unit: UUID
    manager_level: UUID
    manager_type: UUID
    primary: Optional[UUID] = None
    validity: "RAValidityInput"


class ManagerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ManagerRegistrationFilter"] = None
    engagement: Optional["EngagementFilter"] = None
    responsibility: Optional["ClassFilter"] = None
    manager_type: Optional["ClassFilter"] = None
    exclude: Optional["EmployeeFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class ManagerRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ManagerTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class ManagerUpdateInput(BaseModel):
    uuid: UUID
    validity: "RAValidityInput"
    user_key: Optional[str] = None
    person: Optional[UUID] = None
    engagement: Optional[UUID] | UnsetType = UNSET
    responsibility: Optional[List[UUID]] = None
    org_unit: Optional[UUID] = None
    manager_type: Optional[UUID] = None
    manager_level: Optional[UUID] = None
    primary: Optional[UUID] | UnsetType = UNSET


class ModelsUuidsBoundRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class NamespaceCreateInput(BaseModel):
    name: str
    public: bool = False


class NamespaceDeleteInput(BaseModel):
    name: str


class NamespaceFilter(BaseModel):
    names: Optional[List[str]] = None
    owners: Optional[List[UUID]] = None
    public: Optional[bool] = None


class NamespacesBoundListenerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    owners: Optional[List[UUID]] = None
    routing_keys: Optional[List[str]] = None


class OrgUnitboundaddressfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["AddressRegistrationFilter"] = None
    address_type: Optional["ClassFilter"] = None
    address_types: Optional[List[UUID]] = None
    address_type_user_keys: Optional[List[str]] = None
    engagement: Optional["EngagementFilter"] = None
    engagements: Optional[List[UUID]] = None
    ituser: Optional["ITUserFilter"] = None
    visibility: Optional["ClassFilter"] = None


class OrgUnitboundassociationfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["AssociationRegistrationFilter"] = None
    association_type: Optional["ClassFilter"] = None
    association_types: Optional[List[UUID]] = None
    association_type_user_keys: Optional[List[str]] = None
    it_association: Optional[bool] = None


class OrgUnitboundengagementfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["EngagementRegistrationFilter"] = None
    job_function: Optional["ClassFilter"] = None
    engagement_type: Optional["ClassFilter"] = None
    explicit_manager: Optional["ManagerFilter"] | UnsetType = UNSET
    ituser: Optional["ITUserFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class OrgUnitboundituserfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["ITUserRegistrationFilter"] = None
    itsystem: Optional["ITSystemFilter"] = None
    itsystem_uuids: Optional[List[UUID]] = None
    engagement: Optional["EngagementFilter"] = None
    rolebinding: Optional["RoleBindingFilter"] | UnsetType = UNSET
    external_ids: Optional[List[str]] | UnsetType = UNSET
    binding_types: Optional[List[str]] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class OrgUnitboundklefilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["KLERegistrationFilter"] = None


class OrgUnitboundleavefilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["LeaveRegistrationFilter"] = None


class OrgUnitboundmanagerfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    registration: Optional["ManagerRegistrationFilter"] = None
    engagement: Optional["EngagementFilter"] = None
    responsibility: Optional["ClassFilter"] = None
    manager_type: Optional["ClassFilter"] = None
    exclude: Optional["EmployeeFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class OrgUnitboundownerfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    owner: Optional["EmployeeFilter"] = None


class OrgUnitboundrelatedunitfilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None


class OrganisationCreate(BaseModel):
    municipality_code: Optional[int] | UnsetType = UNSET


class OrganisationUnitCreateInput(BaseModel):
    uuid: Optional[UUID] | UnsetType = UNSET
    validity: "RAValidityInput"
    name: str
    user_key: Optional[str] | UnsetType = UNSET
    parent: Optional[UUID] | UnsetType = UNSET
    org_unit_type: UUID
    time_planning: Optional[UUID] | UnsetType = UNSET
    org_unit_level: Optional[UUID] | UnsetType = UNSET
    org_unit_hierarchy: Optional[UUID] | UnsetType = UNSET


class OrganisationUnitFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["OrganisationUnitRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    names: Optional[List[str]] | UnsetType = UNSET
    parent: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    parents: Optional[List[UUID]] | UnsetType = UNSET
    child: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    hierarchy: Optional["ClassFilter"] = None
    hierarchies: Optional[List[UUID]] = None
    subtree: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    descendant: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    ancestor: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    engagement: Optional["EngagementFilter"] = None
    owner: Optional["OwnerFilter"] = None


class OrganisationUnitRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class OrganisationUnitTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class OrganisationUnitUpdateInput(BaseModel):
    uuid: UUID
    validity: "RAValidityInput"
    name: Optional[str] | UnsetType = UNSET
    user_key: Optional[str] | UnsetType = UNSET
    parent: Optional[UUID] | UnsetType = UNSET
    org_unit_type: Optional[UUID] | UnsetType = UNSET
    org_unit_level: Optional[UUID] | UnsetType = UNSET
    org_unit_hierarchy: Optional[UUID] | UnsetType = UNSET
    time_planning: Optional[UUID] | UnsetType = UNSET


class OwnerCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    org_unit: Optional[UUID] = None
    person: Optional[UUID] = None
    owner: Optional[UUID] = None
    inference_priority: Optional[OwnerInferencePriority] = None
    validity: "RAValidityInput"


class OwnerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    owner: Optional["EmployeeFilter"] = None


class OwnerTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class OwnerUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    org_unit: Optional[UUID] = None
    person: Optional[UUID] = None
    owner: Optional[UUID] = None
    inference_priority: Optional[OwnerInferencePriority] = None
    validity: "RAValidityInput"


class OwnersBoundListenerFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    routing_keys: Optional[List[str]] = None
    namespaces: Optional["NamespaceFilter"] = None


class OwnersBoundNamespaceFilter(BaseModel):
    names: Optional[List[str]] = None
    public: Optional[bool] = None


class ParentBoundClassFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ClassRegistrationFilter"] = None
    name: Optional[List[str]] = None
    facet: Optional["FacetFilter"] = None
    facets: Optional[List[UUID]] = None
    facet_user_keys: Optional[List[str]] = None
    it_system: Optional["ITSystemFilter"] = None
    owner: Optional["ClassOwnerFilter"] = None
    scope: Optional[List[str]] = None


class ParentBoundFacetFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["FacetRegistrationFilter"] = None


class ParentBoundOrganisationUnitFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["OrganisationUnitRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    names: Optional[List[str]] | UnsetType = UNSET
    child: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    hierarchy: Optional["ClassFilter"] = None
    hierarchies: Optional[List[UUID]] = None
    subtree: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    descendant: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    ancestor: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    engagement: Optional["EngagementFilter"] = None
    owner: Optional["OwnerFilter"] = None


class PolicyFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    names: Optional[List[str]] = None
    selectors: Optional[List["PolicySelectorInput"]] = None
    active: Optional[bool] = None


class PolicyReadRuleInput(BaseModel):
    collection: Collection
    fields: List[str]
    condition: Any = "true"
    graphql_version: GraphQLVersion


class PolicySelectorInput(BaseModel):
    kind: PolicySelectorKind
    value: str


class PolicyStateInput(BaseModel):
    name: str
    selectors: List["PolicySelectorInput"]
    description: str = ""
    active: bool = True
    read_rules: List["PolicyReadRuleInput"] = Field(default_factory=lambda: [])
    write_rules: List["PolicyWriteRuleInput"] = Field(default_factory=lambda: [])


class PolicyWriteRuleInput(BaseModel):
    mutator: Mutator
    condition: Any = "true"
    graphql_version: GraphQLVersion


class RAOpenValidityInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: Optional[datetime] = None


class RAValidityInput(BaseModel):
    from_: datetime = Field(alias="from")
    to: Optional[datetime] = None


class RegistrationFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    actors: Optional[List[UUID]] = None
    models: Optional[List[str]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class RelatedUnitFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None


class RelatedUnitsUpdateInput(BaseModel):
    uuid: Optional[UUID] = None
    origin: UUID
    destination: Optional[List[UUID]] = None
    validity: "RAValidityInput"


class RoleBindingCreateInput(BaseModel):
    uuid: Optional[UUID] = None
    user_key: Optional[str] = None
    org_unit: Optional[UUID] = None
    ituser: UUID
    role: UUID
    validity: "RAValidityInput"


class RoleBindingFilter(BaseModel):
    uuids: Optional[List[UUID]] = None
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["RoleRegistrationFilter"] = None
    ituser: Optional["ITUserFilter"] = None
    role: Optional["ClassFilter"] = None


class RoleBindingTerminateInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: datetime
    uuid: UUID


class RoleBindingUpdateInput(BaseModel):
    uuid: UUID
    user_key: Optional[str] = None
    org_unit: Optional[UUID] = None
    ituser: UUID
    role: Optional[UUID] = None
    validity: "RAValidityInput"


class RoleRegistrationFilter(BaseModel):
    actors: Optional[List[UUID]] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class UuidsBoundClassFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ClassRegistrationFilter"] = None
    name: Optional[List[str]] = None
    facet: Optional["FacetFilter"] = None
    facets: Optional[List[UUID]] = None
    facet_user_keys: Optional[List[str]] = None
    parent: Optional["ClassFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None
    it_system: Optional["ITSystemFilter"] = None
    owner: Optional["ClassOwnerFilter"] = None
    scope: Optional[List[str]] = None


class UuidsBoundEmployeeFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["EmployeeRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    cpr_numbers: Optional[List[Any]] = None
    owner: Optional["OwnerFilter"] = None
    ituser: Optional["ITUserFilter"] | UnsetType = UNSET


class UuidsBoundEngagementFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["EngagementRegistrationFilter"] = None
    job_function: Optional["ClassFilter"] = None
    engagement_type: Optional["ClassFilter"] = None
    explicit_manager: Optional["ManagerFilter"] | UnsetType = UNSET
    ituser: Optional["ITUserFilter"] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class UuidsBoundFacetFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["FacetRegistrationFilter"] = None
    parent: Optional["FacetFilter"] = None
    parents: Optional[List[UUID]] = None
    parent_user_keys: Optional[List[str]] = None


class UuidsBoundITSystemFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["ITSystemRegistrationFilter"] = None


class UuidsBoundITUserFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["ITUserRegistrationFilter"] = None
    itsystem: Optional["ITSystemFilter"] = None
    itsystem_uuids: Optional[List[UUID]] = None
    engagement: Optional["EngagementFilter"] = None
    rolebinding: Optional["RoleBindingFilter"] | UnsetType = UNSET
    external_ids: Optional[List[str]] | UnsetType = UNSET
    binding_types: Optional[List[str]] = None
    primary: Optional["ClassFilter"] | UnsetType = UNSET


class UuidsBoundLeaveFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    employee: Optional["EmployeeFilter"] | UnsetType = UNSET
    employees: Optional[List[UUID]] = None
    org_unit: Optional["OrganisationUnitFilter"] = None
    org_units: Optional[List[UUID]] = None
    registration: Optional["LeaveRegistrationFilter"] = None


class UuidsBoundOrganisationUnitFilter(BaseModel):
    user_keys: Optional[List[str]] = None
    from_date: Optional[datetime] | UnsetType = UNSET
    to_date: Optional[datetime] | UnsetType = UNSET
    registration_time: Optional[datetime] = None
    registration: Optional["OrganisationUnitRegistrationFilter"] = None
    query: Optional[str] | UnsetType = UNSET
    names: Optional[List[str]] | UnsetType = UNSET
    parent: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    parents: Optional[List[UUID]] | UnsetType = UNSET
    child: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    hierarchy: Optional["ClassFilter"] = None
    hierarchies: Optional[List[UUID]] = None
    subtree: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    descendant: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    ancestor: Optional["OrganisationUnitFilter"] | UnsetType = UNSET
    engagement: Optional["EngagementFilter"] = None
    owner: Optional["OwnerFilter"] = None


class ValidityInput(BaseModel):
    from_: Optional[datetime] = Field(alias="from", default=None)
    to: Optional[datetime] = None


AccessLogFilter.update_forward_refs()
ActorFilter.update_forward_refs()
AddressCreateInput.update_forward_refs()
AddressFilter.update_forward_refs()
AddressRegistrationFilter.update_forward_refs()
AddressTerminateInput.update_forward_refs()
AddressUpdateInput.update_forward_refs()
AssociationCreateInput.update_forward_refs()
AssociationFilter.update_forward_refs()
AssociationRegistrationFilter.update_forward_refs()
AssociationTerminateInput.update_forward_refs()
AssociationUpdateInput.update_forward_refs()
ClassCreateInput.update_forward_refs()
ClassFilter.update_forward_refs()
ClassOwnerFilter.update_forward_refs()
ClassRegistrationFilter.update_forward_refs()
ClassTerminateInput.update_forward_refs()
ClassUpdateInput.update_forward_refs()
DescendantParentBoundOrganisationUnitFilter.update_forward_refs()
EmployeeBoundAddressFilter.update_forward_refs()
EmployeeBoundAssociationFilter.update_forward_refs()
EmployeeBoundEngagementFilter.update_forward_refs()
EmployeeBoundITUserFilter.update_forward_refs()
EmployeeBoundLeaveFilter.update_forward_refs()
EmployeeBoundManagerFilter.update_forward_refs()
EmployeeCreateInput.update_forward_refs()
EmployeeFilter.update_forward_refs()
EmployeeRegistrationFilter.update_forward_refs()
EmployeeTerminateInput.update_forward_refs()
EmployeeUpdateInput.update_forward_refs()
EngagementBoundAddressFilter.update_forward_refs()
EngagementBoundITUserFilter.update_forward_refs()
EngagementCreateInput.update_forward_refs()
EngagementFilter.update_forward_refs()
EngagementRegistrationFilter.update_forward_refs()
EngagementTerminateInput.update_forward_refs()
EngagementUpdateInput.update_forward_refs()
EventAcknowledgeInput.update_forward_refs()
EventCreatedAtFilter.update_forward_refs()
EventFilter.update_forward_refs()
EventRerunInput.update_forward_refs()
EventSendInput.update_forward_refs()
EventSilenceInput.update_forward_refs()
EventUnsilenceInput.update_forward_refs()
FacetBoundClassFilter.update_forward_refs()
FacetCreateInput.update_forward_refs()
FacetFilter.update_forward_refs()
FacetRegistrationFilter.update_forward_refs()
FacetTerminateInput.update_forward_refs()
FacetUpdateInput.update_forward_refs()
FileFilter.update_forward_refs()
FullEventFilter.update_forward_refs()
HealthFilter.update_forward_refs()
ITAssociationCreateInput.update_forward_refs()
ITAssociationTerminateInput.update_forward_refs()
ITAssociationUpdateInput.update_forward_refs()
ITSystemCreateInput.update_forward_refs()
ITSystemFilter.update_forward_refs()
ITSystemRegistrationFilter.update_forward_refs()
ITSystemTerminateInput.update_forward_refs()
ITSystemUpdateInput.update_forward_refs()
ITUserCreateInput.update_forward_refs()
ITUserFilter.update_forward_refs()
ITUserRegistrationFilter.update_forward_refs()
ITUserTerminateInput.update_forward_refs()
ITUserUpdateInput.update_forward_refs()
ItSystemboundclassfilter.update_forward_refs()
ItuserBoundAddressFilter.update_forward_refs()
ItuserBoundRoleBindingFilter.update_forward_refs()
KLECreateInput.update_forward_refs()
KLEFilter.update_forward_refs()
KLERegistrationFilter.update_forward_refs()
KLETerminateInput.update_forward_refs()
KLEUpdateInput.update_forward_refs()
LeaveCreateInput.update_forward_refs()
LeaveFilter.update_forward_refs()
LeaveRegistrationFilter.update_forward_refs()
LeaveTerminateInput.update_forward_refs()
LeaveUpdateInput.update_forward_refs()
ListenerCreateInput.update_forward_refs()
ListenerDeleteInput.update_forward_refs()
ListenerFilter.update_forward_refs()
ListenersBoundFullEventFilter.update_forward_refs()
ManagerCreateInput.update_forward_refs()
ManagerFilter.update_forward_refs()
ManagerRegistrationFilter.update_forward_refs()
ManagerTerminateInput.update_forward_refs()
ManagerUpdateInput.update_forward_refs()
ModelsUuidsBoundRegistrationFilter.update_forward_refs()
NamespaceCreateInput.update_forward_refs()
NamespaceDeleteInput.update_forward_refs()
NamespaceFilter.update_forward_refs()
NamespacesBoundListenerFilter.update_forward_refs()
OrgUnitboundaddressfilter.update_forward_refs()
OrgUnitboundassociationfilter.update_forward_refs()
OrgUnitboundengagementfilter.update_forward_refs()
OrgUnitboundituserfilter.update_forward_refs()
OrgUnitboundklefilter.update_forward_refs()
OrgUnitboundleavefilter.update_forward_refs()
OrgUnitboundmanagerfilter.update_forward_refs()
OrgUnitboundownerfilter.update_forward_refs()
OrgUnitboundrelatedunitfilter.update_forward_refs()
OrganisationCreate.update_forward_refs()
OrganisationUnitCreateInput.update_forward_refs()
OrganisationUnitFilter.update_forward_refs()
OrganisationUnitRegistrationFilter.update_forward_refs()
OrganisationUnitTerminateInput.update_forward_refs()
OrganisationUnitUpdateInput.update_forward_refs()
OwnerCreateInput.update_forward_refs()
OwnerFilter.update_forward_refs()
OwnerTerminateInput.update_forward_refs()
OwnerUpdateInput.update_forward_refs()
OwnersBoundListenerFilter.update_forward_refs()
OwnersBoundNamespaceFilter.update_forward_refs()
ParentBoundClassFilter.update_forward_refs()
ParentBoundFacetFilter.update_forward_refs()
ParentBoundOrganisationUnitFilter.update_forward_refs()
PolicyFilter.update_forward_refs()
PolicyReadRuleInput.update_forward_refs()
PolicySelectorInput.update_forward_refs()
PolicyStateInput.update_forward_refs()
PolicyWriteRuleInput.update_forward_refs()
RAOpenValidityInput.update_forward_refs()
RAValidityInput.update_forward_refs()
RegistrationFilter.update_forward_refs()
RelatedUnitFilter.update_forward_refs()
RelatedUnitsUpdateInput.update_forward_refs()
RoleBindingCreateInput.update_forward_refs()
RoleBindingFilter.update_forward_refs()
RoleBindingTerminateInput.update_forward_refs()
RoleBindingUpdateInput.update_forward_refs()
RoleRegistrationFilter.update_forward_refs()
UuidsBoundClassFilter.update_forward_refs()
UuidsBoundEmployeeFilter.update_forward_refs()
UuidsBoundEngagementFilter.update_forward_refs()
UuidsBoundFacetFilter.update_forward_refs()
UuidsBoundITSystemFilter.update_forward_refs()
UuidsBoundITUserFilter.update_forward_refs()
UuidsBoundLeaveFilter.update_forward_refs()
UuidsBoundOrganisationUnitFilter.update_forward_refs()
ValidityInput.update_forward_refs()
