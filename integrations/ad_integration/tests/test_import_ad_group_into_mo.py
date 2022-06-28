import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Type

import pytest
from os2mo_helpers.mora_helpers import MoraHelper

from ..ad_reader import ADParameterReader
from ..import_ad_group_into_mo import ADMOImporter
from .mocks import MO_AD_IT_SYSTEM_UUID
from .mocks import MO_ROOT_ORG_UNIT_UUID
from .mocks import MockADParameterReader
from .mocks import MockMoraHelper


class _MockMoraHelperNoEmployeeITUsers(MockMoraHelper):
    """Mock a `MoraHelper` which has a MO employee, but that employee has no IT users
    nor any engagements.
    """

    def get_e_itsystems(self, e_uuid, it_system_uuid=None):
        return []

    def read_user_engagement(self, *args, **kwargs):
        return []


class _MockMoraHelperNoEmployee(_MockMoraHelperNoEmployeeITUsers):
    """Mock a `MoraHelper` which hasn't got the MO employee that we try to read."""

    def read_user(self, user_cpr=None, **kwargs):
        return []


class _MockADParameterReaderResults(MockADParameterReader):
    """Mock an AD containing a single user, having just enough information to exercise
    `ADMOImporter`.
    """

    @property
    def results(self) -> Dict[str, Dict]:
        return {
            "bob": {
                # `DistinguishedName` value must be a substring of
                # `integrations.ad.import_ou.search_string` in `_TestableADMOImporter`.
                "DistinguishedName": "CN=Bob,OU=Konsulenter,DC=Magenta,DC=dk",
                "SamAccountName": "bob",
                "Cpr": "bob-cpr",
                "ObjectGUID": str(uuid.uuid4()),
                "GivenName": "Bob",
                "Surname": "Bobson",
            }
        }


class _TestableADMOImporter(ADMOImporter):
    def __init__(self, _mora_helper_class: Type[MockMoraHelper]):
        self._mora_helper_class = _mora_helper_class
        super().__init__()

    def _get_settings(self) -> Dict[str, Any]:
        return {
            "integrations.ad": [{"cpr_field": "Cpr"}],
            "integrations.ad.import_ou.mo_unit_uuid": str(MO_ROOT_ORG_UNIT_UUID),
            "integrations.ad.import_ou.search_string": "Konsulent",
        }

    def _get_mora_helper(self) -> MoraHelper:
        return self._mora_helper_class(None, mo_uuid=uuid.uuid4())

    def _get_ad_it_system(self) -> uuid.UUID:
        return MO_AD_IT_SYSTEM_UUID

    def _get_ad_reader(self) -> ADParameterReader:
        return _MockADParameterReaderResults()  # type: ignore

    def _ensure_class_in_lora(
        self, facet_bvn: str, class_value: str
    ) -> Tuple[uuid.UUID, bool]:
        return uuid.uuid4(), True


class TestADMOImporter:
    @pytest.mark.parametrize(
        "mora_helper_class,expected_mo_post_calls",
        [
            # Case 1: MO employee does not exist - create employee, IT user and
            # engagement.
            (
                _MockMoraHelperNoEmployee,
                [
                    {
                        "url": "e/create",
                        "payload": {
                            "cpr_no": "bobcpr",
                            "givenname": "Bob",
                            "surname": "Bobson",
                        },
                        "force": True,
                    },
                    {
                        "url": "details/create",
                        "payload": {"type": "it", "user_key": "bob"},
                        "force": True,
                    },
                    {
                        "url": "details/create",
                        "payload": {"type": "engagement", "user_key": "bob"},
                        "force": True,
                    },
                ],
            ),
            # Case 2: MO employee exists, but IT user and engagement do not - create
            # IT user and engagement.
            (
                _MockMoraHelperNoEmployeeITUsers,
                [
                    {
                        "url": "details/create",
                        "payload": {"type": "it", "user_key": "bob"},
                        "force": True,
                    },
                    {
                        "url": "details/create",
                        "payload": {"type": "engagement", "user_key": "bob"},
                        "force": True,
                    },
                ],
            ),
        ],
    )
    def test_create_or_update_users_in_mo(
        self,
        mora_helper_class: MockMoraHelper,
        expected_mo_post_calls: List[dict],
    ):
        # Regression test for #51052
        instance = _TestableADMOImporter(_mora_helper_class=mora_helper_class)
        instance.create_or_update_users_in_mo()

        assert len(expected_mo_post_calls) == len(instance.helper._mo_post_calls)
        paired_calls = zip(expected_mo_post_calls, instance.helper._mo_post_calls)
        for expected_call, actual_call in paired_calls:
            # Assert that `expected_call` contains a subset of `actual_call`
            # See: https://github.com/pytest-dev/pytest/issues/2376#issuecomment-852366588
            assert {**actual_call, **expected_call} == expected_call
