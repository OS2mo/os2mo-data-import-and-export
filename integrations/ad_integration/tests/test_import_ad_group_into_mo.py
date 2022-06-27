import uuid
from typing import Any
from typing import Dict
from typing import Tuple

import pytest
from os2mo_helpers.mora_helpers import MoraHelper

from ..ad_reader import ADParameterReader
from ..import_ad_group_into_mo import ADMOImporter
from .mocks import MO_AD_IT_SYSTEM_UUID
from .mocks import MO_ROOT_ORG_UNIT_UUID
from .mocks import MockADParameterReader
from .mocks import MockMoraHelper


class _MockMoraHelperNoEmployeeITUsers(MockMoraHelper):
    def get_e_itsystems(self, e_uuid, it_system_uuid=None):
        return []


class _MockADParameterReaderResults(MockADParameterReader):
    @property
    def results(self) -> Dict[str, Dict]:
        return {
            "bob": {
                "DistinguishedName": "CN=Bob,OU=Konsulenter,DC=Magenta,DC=dk",
                "SamAccountName": "bob",
                "Cpr": "bob-cpr",
                "ObjectGUID": str(uuid.uuid4()),
            }
        }


class _TestableADMOImporter(ADMOImporter):
    def _get_settings(self) -> Dict[str, Any]:
        return {
            "integrations.ad": [{"cpr_field": "Cpr"}],
            "integrations.ad.import_ou.mo_unit_uuid": str(MO_ROOT_ORG_UNIT_UUID),
            "integrations.ad.import_ou.search_string": "Konsulent",
        }

    def _get_mora_helper(self) -> MoraHelper:
        return _MockMoraHelperNoEmployeeITUsers(None, mo_uuid=uuid.uuid4())

    def _get_ad_it_system(self) -> uuid.UUID:
        return MO_AD_IT_SYSTEM_UUID

    def _get_ad_reader(self) -> ADParameterReader:
        return _MockADParameterReaderResults()  # type: ignore

    def _ensure_class_in_lora(
        self, facet_bvn: str, class_value: str
    ) -> Tuple[uuid.UUID, bool]:
        return uuid.uuid4(), True


class TestADMOImporter:
    def test_create_or_update_users_in_mo(self):
        # Regression test for #51052
        instance = _TestableADMOImporter()
        with pytest.raises(TypeError):
            # TypeError: Object of type UUID is not JSON serializable
            instance.create_or_update_users_in_mo()
