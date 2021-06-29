from functools import partial
from typing import Literal, Optional
from uuid import UUID

from pydantic import Field, conlist

from os2mo_data_import.Clients.LoRa.model_parts.interface import ConfiguredBase

single_element_list = partial(conlist, min_items=1, max_items=1)


class EffectiveTime(ConfiguredBase):
    from_date: str = Field(alias="from")
    to_date: str = Field(alias="to")


class OrganisationProperties(ConfiguredBase):
    user_key: str = Field(alias="brugervendtnoegle")
    name: str = Field(alias="organisationsnavn")
    effective_time: EffectiveTime = Field(alias="virkning")


class OrganisationAttributes(ConfiguredBase):
    properties: single_element_list(OrganisationProperties) = Field(
        alias="organisationegenskaber"
    )


class OrganisationValidState(ConfiguredBase):
    state: str = Field("Aktiv", alias="gyldighed")
    effective_time: EffectiveTime = Field(alias="virkning")


class OrganisationStates(ConfiguredBase):
    valid_state: single_element_list(OrganisationValidState) = Field(
        alias="organisationgyldighed"
    )


class Authority(ConfiguredBase):
    urn: str
    effective_time: EffectiveTime = Field(alias="virkning")


class OrganisationRelations(ConfiguredBase):
    authority: single_element_list(Authority) = Field(alias="myndighed")


class FacetProperties(ConfiguredBase):
    user_key: str = Field(alias="brugervendtnoegle")
    effective_time: EffectiveTime = Field(alias="virkning")


class FacetAttributes(ConfiguredBase):
    properties: single_element_list(FacetProperties) = Field(alias="facetegenskaber")


class Published(ConfiguredBase):
    published: str = Field("Publiceret", alias="publiceret")
    effective_time: EffectiveTime = Field(alias="virkning")


class FacetStates(ConfiguredBase):
    published_state: single_element_list(Published) = Field(alias="facetpubliceret")


class Responsible(ConfiguredBase):
    object_type: Literal["organisation"] = Field("organisation", alias="objekttype")
    uuid: UUID
    effective_time: EffectiveTime = Field(alias="virkning")


class FacetRelations(ConfiguredBase):
    responsible: single_element_list(Responsible) = Field(alias="ansvarlig")


class FacetRef(ConfiguredBase):
    object_type: Literal["facet"] = Field("facet", alias="objekttype")
    uuid: UUID
    effective_time: EffectiveTime = Field(alias="virkning")


class KlasseProperties(ConfiguredBase):
    user_key: str = Field(alias="brugervendtnoegle")
    title: str = Field(alias="titel")
    scope: Optional[str] = Field(None, alias="omfang")
    effective_time: EffectiveTime = Field(alias="virkning")


class KlasseAttributes(ConfiguredBase):
    properties: single_element_list(KlasseProperties) = Field(
        alias="klasseegenskaber"
    )


class KlasseStates(ConfiguredBase):
    published_state: single_element_list(Published) = Field(alias="klassepubliceret")


class KlasseRelations(ConfiguredBase):
    responsible: single_element_list(Responsible) = Field(alias="ansvarlig")
    facet: single_element_list(FacetRef) = Field(alias="ansvarlig")
