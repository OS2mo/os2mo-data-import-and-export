import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List

import config
import uuids
from initial_classes import Class
from initial_classes import CLASSES
from mox_helpers import payloads as mox_payloads
from mox_helpers.mox_helper import create_mox_helper
from mox_helpers.mox_helper import ElementNotFound
from os2mo_data_import import ImportHelper  # type: ignore
from os2mo_data_import.mox_data_types import Itsystem

logger = logging.getLogger(__name__)


@dataclass
class ClassImportResult:
    source: Class
    lora_payload: Dict[str, Any]


async def perform_initial_setup():
    """
    Perform all initial bootstrapping of OS2mo.
    Imports an organisation if missing, and adds all base facets
    Imports all pretedetermined classes and it systems
    """
    settings = config.get_config()
    mox_helper = await create_mox_helper(settings.mox_base)
    try:
        await mox_helper.read_element_organisation_organisation(bvn="%")
    except ElementNotFound:
        logger.info("No org found in LoRa. Performing initial setup.")
        importer = ImportHelper(
            create_defaults=True,
            mox_base=settings.mox_base,
            mora_base=settings.mora_base,
            seperate_names=True,
        )
        importer.add_organisation(
            identifier="Århus Kommune",
            user_key="Århus Kommune",
            municipality_code=751,
            uuid=uuids.ORG_UUID,
        )
        # Perform initial import of org and facets
        importer.import_all()

    await import_remaining_classes()
    await import_it()


async def import_remaining_classes():
    """
    Import a set of predetermined classes. All the classes have predefined UUIDs
    which makes this function idempotent
    """
    settings = config.get_config()
    mox_helper = await create_mox_helper(settings.mox_base)
    result: List[ClassImportResult] = []

    for cls in CLASSES:
        facet_uuid = await mox_helper.read_element_klassifikation_facet(bvn=cls.facet)
        lora_payload = mox_payloads.lora_klasse(
            bvn=cls.bvn,
            title=cls.titel,
            facet_uuid=str(facet_uuid),
            org_uuid=str(uuids.ORG_UUID),
            scope=cls.scope,
        )
        await mox_helper.insert_klassifikation_klasse(lora_payload, str(cls.uuid))
        result.append(ClassImportResult(cls, lora_payload))

    return result


async def import_it():
    """
    Import predetermined IT systems. The UUID(s) are predefined which makes this
    function idempotent.
    """
    settings = config.get_config()
    if settings.azid_it_system_uuid == uuids.AZID_SYSTEM:
        mox_helper = await create_mox_helper(settings.mox_base)
        it_system = Itsystem(system_name="AZ", user_key="AZ")
        it_system.organisation_uuid = str(uuids.ORG_UUID)
        uuid = uuids.AZID_SYSTEM
        json = it_system.build()
        await mox_helper.insert_organisation_itsystem(json, str(uuid))
    else:
        logger.info(
            "Settings specify a non-default AZID IT system UUID, not creating "
            "default AZ IT system"
        )
