import os
from asyncio import run, sleep
from functools import partial
from pathlib import Path
from typing import Dict, Iterable, Optional
from uuid import UUID

from more_itertools import flatten
from pydantic import AnyHttpUrl
from ramodels.lora import Klasse
from ramodels.mo import Engagement, Manager, OrganisationUnit
from ramodels.mo._shared import MOBase

from exporters.utils.uuid_helper import generate_uuid
from exporters.utils.valid_mo import ValidMo
from integrations.gir.initial_read.emd_csv import (
    AddressKlasses,
    UUIDGenerator,
    gen_single_klass,
    read_emd,
)
from integrations.gir.initial_read.mo_lora_client import MoLoRaClient
from integrations.gir.initial_read.org_unit_csv import PartialManager, read_csv


def missing_ous(outu, oulu):
    """
    We are missing some org units. We create dummies to move along
    :param outu:
    :param oulu:
    :return:
    """

    def ou(user_key):
        """
        convenience
        :param user_key:
        :return:
        """
        return [
            OrganisationUnit.from_simplified_fields(
                uuid=generate_uuid("ou_any_seed_yo" + user_key),
                user_key=user_key,
                name="MISSING INFO",
                org_unit_type_uuid=outu,
                org_unit_level_uuid=oulu,
            )
        ]

    missing_department_nos = [
        "0",
        "50001401",
        "50003553",
        "50002830",
        "50050843",
        "50057275",
        "50003724",
        "50057678",
        "50000706",
        "50001633",
        "50002336",
        "50053954",
        "50054978",
    ]
    return list(map(ou, missing_department_nos))


def klasses_from_uuid_gen(
    facet_uuid: UUID, organisation_uuid: UUID, uuid_gen: UUIDGenerator
):
    def klasse_gen(uuid, user_key):
        return Klasse.from_simplified_fields(
            facet_uuid=facet_uuid,
            uuid=uuid,
            user_key=user_key,
            scope=None,
            organisation_uuid=organisation_uuid,
            title=user_key,
        )

    return [klasse_gen(uuid, user_key) for user_key, uuid in uuid_gen.get_all().items()]


def gen_managers(
    partial_managers: Iterable[PartialManager],
    eng_mapping: Dict[str, Engagement],
    responsibility_uuid: UUID,
    manager_level_uuid: UUID,
    manager_type_uuid: UUID,
) -> Iterable[Manager]:
    def manager_gen(partial_manager: PartialManager) -> Optional[Manager]:
        try:
            eng = eng_mapping[partial_manager.engagement_user_key]
        except KeyError:
            return None

        return Manager.from_simplified_fields(
            uuid=generate_uuid(
                "manager"
                + partial_manager.engagement_user_key
                + str(partial_manager.org_unit_uuid)
            ),
            org_unit_uuid=partial_manager.org_unit_uuid,
            person_uuid=eng.person.uuid,
            responsibility_uuid=responsibility_uuid,
            manager_level_uuid=manager_level_uuid,
            manager_type_uuid=manager_type_uuid,
            from_date=eng.validity.from_date,
            to_date=eng.validity.to_date,
        )

    filtered_pm = filter(lambda x: x.engagement_user_key != "", partial_managers)
    return list(filter(lambda x: x is not None, map(manager_gen, filtered_pm)))


def read_values(base_path: Path) -> Iterable[Iterable[MOBase]]:
    base = ValidMo.from_scratch()
    gen_org_klass = partial(gen_single_klass, organisation_uuid=base.organisation.uuid)
    base_objs = [x[1] for x in base]
    address_classes = AddressKlasses.from_simplified_fields(
        organisation_uuid=base.organisation.uuid,
        engagement_address_type_uuid=base.employee_address_type.uuid,
        employee_address_type_uuid=base.employee_address_type.uuid,
    )
    address_classes_iterable = [x[1] for x in address_classes]

    ou_type_unit = gen_org_klass(
        facet_uuid=base.org_unit_type.uuid,
        title="Unit",
    )
    ou_level_l1 = gen_org_klass(
        facet_uuid=base.org_unit_level.uuid,
        title="L1",
    )

    man_type_man = gen_org_klass(
        facet_uuid=base.manager_type.uuid,
        title="Manager",
    )

    man_level_m1 = gen_org_klass(
        facet_uuid=base.manager_level.uuid,
        title="M1",
    )
    resp_r1 = gen_org_klass(
        facet_uuid=base.responsibility.uuid,
        title="R1",
    )

    not_primary = gen_org_klass(
        facet_uuid=base.primary_type.uuid,
        title="Non-primary",
    )

    visible = gen_org_klass(
        facet_uuid=base.visibility.uuid,
        title="Visible",
    )

    not_visible = gen_org_klass(
        facet_uuid=base.visibility.uuid,
        title="Not visible",
    )

    ea_assoc = gen_org_klass(
        facet_uuid=base.engagement_association_type.uuid,
        title="Association",
    )

    classes = [
        ou_type_unit,
        ou_level_l1,
        man_type_man,
        man_level_m1,
        resp_r1,
        not_primary,
        visible,
        not_visible,
        ea_assoc,
    ]
    classes.extend(address_classes_iterable)

    org_units, partial_managers = read_csv(
        path=base_path / "gir_orsted_hierarki_linje.csv",
        org_unit_type_uuid=ou_type_unit.uuid,
        org_unit_level_uuid=ou_level_l1.uuid,
    )

    org_units.extend(missing_ous(ou_type_unit.uuid, ou_level_l1.uuid))
    # department_no: uuid
    ou_mapping = {ou.user_key: ou.uuid for ou in flatten(org_units)}

    ou_fin = OrganisationUnit.from_simplified_fields(
        uuid=generate_uuid("w/e" + "fin"),
        user_key="financial_placeholder",
        name="Financial Organisation",
        org_unit_type_uuid=ou_type_unit.uuid,
        org_unit_level_uuid=ou_level_l1.uuid,
    )
    ou_legal = OrganisationUnit.from_simplified_fields(
        uuid=generate_uuid("w/e" + "legal"),
        user_key="legal_placeholder",
        name="Legal Organisation",
        org_unit_type_uuid=ou_type_unit.uuid,
        org_unit_level_uuid=ou_level_l1.uuid,
    )
    engagement_type_uuid_generator = UUIDGenerator("engagement_type")
    job_function_uuid_generator = UUIDGenerator("job_function")
    (
        employees,
        additional_engagement_ous,
        engagements,
        engagement_associations,
        addresses,
    ) = read_emd(
        path=base_path / "gir_from_emd.csv",
        org_unit_uuids=ou_mapping,
        primary_uuid=not_primary.uuid,
        engagement_type_uuid_generator=engagement_type_uuid_generator,
        job_function_uuid_generator=job_function_uuid_generator,
        visible_uuid=visible.uuid,
        not_visible_uuid=not_visible.uuid,
        address_klasses=address_classes,
        org_unit_type_uuid=ou_type_unit.uuid,
        financial_org_unit_uuid=ou_fin.uuid,
        legal_org_unit_uuid=ou_legal.uuid,
        engagement_association_type_uuid=ea_assoc.uuid,
        org_unit_level_uuid=ou_level_l1.uuid,
        org_uuid=base.organisation.uuid,
    )
    fixture2 = (
        *employees,
        *additional_engagement_ous,
        *engagements,
    )

    eng_mapping = {x.user_key: x for x in flatten(engagements)}
    managers = gen_managers(
        partial_managers=partial_managers,
        eng_mapping=eng_mapping,
        responsibility_uuid=resp_r1.uuid,
        manager_level_uuid=man_level_m1.uuid,
        manager_type_uuid=man_type_man.uuid,
    )
    # more shitty code:
    managers = list(map(lambda x: [x], managers))
    classes.extend(
        klasses_from_uuid_gen(
            facet_uuid=base.engagement_type.uuid,
            organisation_uuid=base.organisation.uuid,
            uuid_gen=engagement_type_uuid_generator,
        )
    )
    classes.extend(
        klasses_from_uuid_gen(
            facet_uuid=base.engagement_job_function.uuid,
            organisation_uuid=base.organisation.uuid,
            uuid_gen=job_function_uuid_generator,
        )
    )
    fixture = base_objs, classes, *org_units, [ou_fin], [ou_legal]
    # return *fixture, *fixture2, *engagement_associations, *managers, *addresses
    return *fixture2, *engagement_associations, *managers, *addresses


async def main(base_path: Path, mo_url: AnyHttpUrl, lora_url: AnyHttpUrl):
    client = MoLoRaClient(mo_url=mo_url, lora_url=lora_url)
    values = read_values(base_path)
    async with client.context():
        for group in values:
            await sleep(0.2)  # throttle
            await client.load_objs(group)


def str_to_url(url: str) -> AnyHttpUrl:
    scheme, host = url.split("://")
    return AnyHttpUrl(url=url, scheme=scheme, host=host)


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_PATH"])
    parsed_mo_url = str_to_url(os.environ["MO_URL"])
    parsed_lora_url = str_to_url(os.environ["LORA_URL"])

    run(main(data_path, parsed_mo_url, parsed_lora_url))
