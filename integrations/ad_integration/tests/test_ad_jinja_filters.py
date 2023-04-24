from typing import List
from typing import Set
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st
from parameterized import parameterized

from ..ad_jinja_filters import name_to_email_address
from .mocks import MO_UUID
from .test_ad_writer import _TestRealADWriter


class TestFirstAddressOfTypeIntegration(_TestRealADWriter):
    _template_to_ad_fields = {
        "someAdField": "{{ mo_values['employee_addresses']|first_address_of_type('address-type-uuid') }}"
    }
    _expected_fragment = '"someAdField"="address-value"'

    def test_jinja_env_has_template_filter_first_address_of_type(self):
        ad_writer = self._prepare_adwriter()
        self.assertIn("first_address_of_type", ad_writer._environment.filters)

    def test_first_address_of_type_in_create_user_command(self):
        ad_writer = self._prepare_adwriter(
            template_to_ad_fields=self._template_to_ad_fields
        )
        create_cmd, _ = ad_writer._preview_create_command(MO_UUID)
        self.assertIn(self._expected_fragment, create_cmd)

    def test_first_address_of_type_in_sync_user_command(self):
        ad_writer = self._prepare_adwriter(
            template_to_ad_fields=self._template_to_ad_fields
        )
        sync_cmd, _, _, _ = ad_writer._preview_sync_command(MO_UUID, "user_sam")
        self.assertIn(self._expected_fragment, sync_cmd)


class TestNameToEmailAddressIntegration(_TestRealADWriter):
    _template_to_ad_fields = {
        "someAdField": "{{ mo_values['full_name']|name_to_email_address }}"
    }

    @parameterized.expand(
        [
            (
                # No taken emails, use variant A.
                set(),
                "tester.testesen@upn_end",
            ),
            (
                # Variant A taken, use variant B.
                {"tester.testesen@upn_end"},
                "t.testesen@upn_end",
            ),
            (
                # Variants A and B taken, use variant C.
                {"tester.testesen@upn_end", "t.testesen@upn_end"},
                "tester.t@upn_end",
            ),
            (
                # Variants A, B and C taken, use variant D.
                # Start serial number at 3 since no other D variants are taken.
                {"tester.testesen@upn_end", "t.testesen@upn_end", "tester.t@upn_end"},
                "tester.testesen.3@upn_end",
            ),
            (
                # Variants A, B, C and D taken, use variant D with next numeric suffix.
                {
                    "tester.testesen@upn_end",
                    "t.testesen@upn_end",
                    "tester.t@upn_end",
                    "tester.testesen.3@upn_end",
                    "irrelevant.4@upn_end",
                },
                "tester.testesen.4@upn_end",
            ),
        ]
    )
    def test_name_to_email_address_in_create_user_command(
        self, taken_emails: Set[str], expected_result: str
    ):
        with patch(
            "integrations.ad_integration.ad_reader.ADParameterReader.__init__",
            return_value=None,
        ):
            with patch(
                "integrations.ad_integration.ad_reader.ADParameterReader"
                ".get_all_email_values",
                return_value=taken_emails,
            ):
                ad_writer = self._prepare_adwriter(
                    template_to_ad_fields=self._template_to_ad_fields,
                )
                create_cmd, _ = ad_writer._preview_create_command(MO_UUID)

        self.assertIn(f'"someAdField"="{expected_result}"', create_cmd)


class TestNameToEmailAddress:
    _letters = st.characters(whitelist_categories=("Ll", "Lu"), max_codepoint=500)

    @given(
        st.one_of(
            # Too short input - not enough name parts
            st.lists(st.text(min_size=1, alphabet=_letters), min_size=0),
            # Sufficient input - two or more name parts
            st.lists(st.text(min_size=2, alphabet=_letters), min_size=1),
        ),
        # Separator between name parts
        st.one_of(st.just(" "), st.just("-")),
    )
    def test_input(self, name_parts: List[str], separator: str):
        name = separator.join(name_parts)
        ctx = {
            "_upn_end": "upn_end",
            "_get_all_ad_emails": lambda: set(),
        }
        if name.count(" ") or name.count("-"):
            # Name contains at least one space or dash. This means we can extract at
            # least two "name parts."
            email = name_to_email_address(ctx, name)
            assert email.endswith("@upn_end")
            value = email.split("@upn_end")[0]
            assert len(value) > 0
            assert value.count("@") == 0
            assert all(ch.isalnum() or ch == "." for ch in value)
        else:
            # Name contains fewer than two name parts, and we cannot generate an email.
            with pytest.raises(ValueError):
                name_to_email_address(ctx, name)
