import pytest
from glom import glom
from glom.core import PathAccessError
from hypothesis import given
from hypothesis import strategies as st

from ..read_ad_conf_settings import injected_settings
from ..read_ad_conf_settings import read_settings


def get_minimum_valid_settings(*extra_properties):
    properties = ["uuid_field", "org_unit_field"]
    for extra in filter(None, extra_properties):
        properties.append(extra)

    return {
        "integrations.ad": [
            {
                "system_user": "system_user",
                "password": "password",
                "search_base": "search_base",
                "cpr_field": "cpr_field",
                "properties": properties,
            }
        ],
        "integrations.ad.winrm_host": "winrm_host",
    }


def get_minimum_valid_writer_settings(level2orgunit_field=None):
    level2orgunit_field = level2orgunit_field or "l2org_field"
    settings = get_minimum_valid_settings(level2orgunit_field)
    settings.update(
        {
            "integrations.ad.write.level2orgunit_field": level2orgunit_field,
            "integrations.ad.write.level2orgunit_type": "l2org_type",
            "integrations.ad.write.upn_end": "upn_end",
            "integrations.ad.write.uuid_field": "uuid_field",
            "integrations.ad.write.org_unit_field": "org_unit_field",
        }
    )
    return settings


def test_minimum_valid_settings():
    read_settings(get_minimum_valid_settings())
    read_settings(get_minimum_valid_writer_settings())


def test_missing_winrm_setting():
    settings = get_minimum_valid_settings()
    del settings["integrations.ad.winrm_host"]
    with pytest.raises(Exception) as excinfo:
        read_settings(settings)
    assert "Missing hostname for remote management server" in str(excinfo.value)


@given(st.text(), st.text() | st.none())
def test_duplicated_field_names(ad_key, level2orgunit_field):
    settings = get_minimum_valid_writer_settings(level2orgunit_field)
    settings["integrations.ad_writer.mo_to_ad_fields"] = {"a": ad_key}
    settings["integrations.ad_writer.template_to_ad_fields"] = {ad_key: "c"}
    with pytest.raises(ValueError) as excinfo:
        read_settings(settings)
    expected = "Duplicated AD field names in settings: [%r]" % ad_key.lower()
    assert expected in str(excinfo.value)


@given(
    st.tuples(st.text(min_size=1), st.text() | st.none()).filter(lambda x: x[0] != x[1])
)
def test_missing_properties(tup):
    ad_key, level2orgunit_field = tup
    settings = get_minimum_valid_writer_settings(level2orgunit_field)
    settings["integrations.ad_writer.template_to_ad_fields"] = {ad_key: "c"}
    with pytest.raises(ValueError) as excinfo:
        read_settings(settings)
    expected = "Missing AD field names in properties: [%r]" % ad_key
    assert expected in str(excinfo.value)

    def test_injected_settings(self):
        title = "Stilling"
        old_settings = {
            "global": {
                "mora.base": "http://localhost:5000",
                "password": "pwd",
                "servers": [],
                "system_user": "os2mo",
                "winrm_host": "AD",
            },
            "primary": {
                "cpr_field": "EmployeeID",
                "method": "ntlm",
                "password": "pwd",
                "properties": [
                    "givenName",
                    "surname",
                    "distinguishedName",
                    "EmployeeID",
                    "title",
                ],
                "system_user": "os2mo",
            },
            "primary_write": {
                "cpr_field": "EmployeeID",
                "mo_to_ad_fields": {
                    "end_date": "info",
                    "unit_postal_code": "postalCode",
                },
                "org_field": "Department",
                "template_to_ad_fields": {"description": "{{" " sync_timestamp " "}}"},
            },
        }
        new_settings = {
            "ad_lifecycle_injected_settings": {
                "primary_write.mo_to_ad_fields.Title": title
            }
        }
        with pytest.raises(PathAccessError):
            assert (
                glom(old_settings, "primary_write.template_to_ad_fields.Title") != title
            )
        settings = injected_settings(
            "ad_lifecycle_injected_settings", old_settings, new_settings
        )

        assert glom(settings, "primary_write.mo_to_ad_fields.Title") == title


def test_template_to_ad_fields_when_disable():
    """Test that "template_to_ad_fields_when_disable" is read and included in settings."""
    mapping = {"ad_field": "jinja_template"}
    properties = ["ad_field"]
    settings = get_minimum_valid_writer_settings()
    settings["integrations.ad_writer.template_to_ad_fields_when_disable"] = mapping
    settings["integrations.ad"][0]["properties"] += properties
    settings_read = read_settings(settings)
    assert (
        settings_read["primary_write"]["template_to_ad_fields_when_disable"] == mapping
    )


def test_skip_locations():
    """Test that "skip_locations" is read and included in settings."""
    value = ["Some unit name"]
    settings = get_minimum_valid_writer_settings()
    settings["integrations.ad_writer.skip_locations"] = value
    settings_read = read_settings(settings)
    assert settings_read["primary_write"]["skip_locations"] == value
