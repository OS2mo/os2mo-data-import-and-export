from typing import Optional
from uuid import UUID

from pydantic import Field

from os2mo_data_import.Clients.LoRa.model_parts.implementations import (
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
from os2mo_data_import.Clients.LoRa.model_parts.interface import LoraObj


class Organisation(LoraObj):
    attributes: OrganisationAttributes = Field(alias="attributter")
    states: OrganisationStates = Field(alias="tilstande")
    relations: Optional[OrganisationRelations] = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included,
    # and has to be excluded manually when converting to json
    uuid: Optional[UUID] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: UUID,
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

    def get_uuid(self) -> Optional[UUID]:
        return self.uuid


class Facet(LoraObj):
    attributes: FacetAttributes = Field(alias="attributter")
    states: FacetStates = Field(alias="tilstande")
    relations: FacetRelations = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included, and has to be excluded when converted to json
    uuid: Optional[UUID] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        uuid: UUID,
        user_key: str,
        organisation_uuid: UUID,
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

    def get_uuid(self) -> Optional[UUID]:
        return self.uuid


class Klasse(LoraObj):
    attributes: KlasseAttributes = Field(alias="attributter")
    states: KlasseStates = Field(alias="tilstande")
    relations: KlasseRelations = Field(alias="relationer")
    # TODO, PENDING: https://github.com/samuelcolvin/pydantic/pull/2231
    # for now, this value is included, and has to be excluded when converted to json
    uuid: Optional[UUID] = None  # Field(None, exclude=True)

    @classmethod
    def from_simplified_fields(
        cls,
        facet_uuid: UUID,  # uuid
        uuid: UUID,
        user_key: str,  # rarely used
        scope: Optional[str],
        organisation_uuid: UUID,
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

    def get_uuid(self) -> Optional[UUID]:
        return self.uuid
