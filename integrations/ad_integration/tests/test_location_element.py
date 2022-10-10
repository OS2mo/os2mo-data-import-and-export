import re
import string
from copy import deepcopy
from itertools import chain
from itertools import repeat
from typing import List
from typing import Optional

from hypothesis import given
from hypothesis import strategies as st

from ..ad_template_engine import template_powershell
from ..ad_writer import ADWriter
from .mocks import MO_UUID
from .mocks import MockMORESTSourcePreview


_base_settings = {
    "primary": {"cpr_separator": ""},
    "primary_write": {
        "cpr_field": "unused_cpr",
        "org_field": "unused_location",
        "uuid_field": "unused_uuid",
        "upn_end": "unused_upn_end",
        "mo_to_ad_fields": {},
    },
}


class _TestableADWriter(ADWriter):
    def __init__(self, location: str) -> None:
        self.datasource = MockMORESTSourcePreview()
        self.settings = _base_settings
        self._location = location

    def _find_unit_info(self, unit_uuid):
        return {"location": self._location}


class TestLocationElement:
    # Use location elements from this offset in test
    _offset = 2

    _template_to_ad_fields = {
        "department": "{{ mo_values['location']|location_element(2) }}",
        "physicalDeliveryOfficeName": "{{ mo_values['location']|location_element(3) }}",
        "extensionAttribute4": "{{ mo_values['location']|location_element(4) }}",
        "extensionAttribute5": "{{ mo_values['location']|location_element(5) }}",
        "extensionAttribute6": "{{ mo_values['location']|location_element(6) }}",
    }

    @given(
        st.lists(
            st.text(alphabet=string.ascii_letters, min_size=1), min_size=2, max_size=10
        )
    )
    def test_location(self, elems: List[str]) -> None:
        # Construct input variables based on Hypothesis data
        location: str = "\\".join(elems)
        # Construct dictionary of expected field names and values in PowerShell output
        expected_field_names = self._template_to_ad_fields.keys()
        expected_elements = elems[self._offset :]
        # Construct dictionary by zipping keys and values
        expected_pairs = dict(
            zip(
                # Take dictionary keys from `expected_field_names`, which are all the
                # keys in `_template_to_ad_fields`.
                expected_field_names,
                # Take dictionary values from `expected_elements`, padding the list of
                # values with `None` if there are not enough values in
                # `expected_elements` to match every key.
                # Paddding with `None` means that we will test that "unmapped" field
                # names will *not* be written by the generated PowerShell code.
                chain(expected_elements, repeat(None)),
            )
        )
        # Arrange
        ad_writer = _TestableADWriter(location)
        settings = deepcopy(_base_settings)
        settings["primary_write"]["template_to_ad_fields"] = self._template_to_ad_fields  # type: ignore
        # Act
        result = template_powershell(
            context={
                "mo_values": ad_writer.read_ad_information_from_mo(MO_UUID),
                "user_sam": None,  # not used
            },
            settings=settings,
            environment=ad_writer._get_jinja_environment(),
        )
        # Assert
        self._assert_matches(result, expected_pairs)

    def _assert_matches(self, result: str, expected: dict):
        for name, expected_value in expected.items():
            if expected_value:
                actual_value = self._get_value(result, name)
                assert expected_value == actual_value, f"mismatch on '{name}'"
            else:
                assert name not in result

    def _get_value(self, result: str, field_name: str) -> Optional[str]:
        # Match both "-attr=value" and '"attr"="value"' argument styles in PowerShell
        # output `result`.
        for regex in (rf"-{field_name} \"(.*?)\"", rf"{field_name}\"=\"(.*?)\""):
            match: Optional[re.Match] = re.search(regex, result)
            if match:
                return match.groups()[0]
        return None
