import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from tools.ldap_cleanup_orphaned_addresses import cleanup_orphaned_addresses
from tools.ldap_cleanup_orphaned_addresses import load_address_type_user_keys
from tools.ldap_cleanup_orphaned_addresses import load_integrations


def test_load_integrations(tmp_path: Path):
    settings = tmp_path / "settings.json"
    settings.write_text(
        json.dumps(
            {
                "crontab.LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS": [
                    {"ad_name": "AD_NAME", "port": 8451}
                ]
            }
        ),
        encoding="utf-8",
    )

    assert load_integrations(settings) == [{"ad_name": "AD_NAME", "port": 8451}]


def test_load_integrations_missing_setting_returns_empty_list(tmp_path: Path):
    settings = tmp_path / "settings.json"
    settings.write_text("{}", encoding="utf-8")

    assert load_integrations(settings) == []


def test_load_integrations_from_prefixed_environment_setting():
    assert load_integrations(
        environ={
            "crontab_LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS": (
                '[{"ad_name": "AD_NAME", "port": 8451}]'
            )
        }
    ) == [{"ad_name": "AD_NAME", "port": 8451}]


def test_load_integrations_missing_file_returns_empty_list(tmp_path: Path):
    assert load_integrations(tmp_path / "missing-settings.json") == []


def test_load_integrations_rejects_invalid_port(tmp_path: Path):
    settings = tmp_path / "settings.json"
    settings.write_text(
        json.dumps(
            {
                "crontab.LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS": [
                    {"ad_name": "AD_NAME", "port": True}
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="valid port"):
        load_integrations(settings)


def test_load_address_type_user_keys(tmp_path: Path):
    init_config = tmp_path / "init.config.AD_NAME.yml"
    init_config.write_text(
        "facets:\n"
        "  employee_address_type:\n"
        "    EmailEmployee:\n"
        "      scope: EMAIL\n"
        "    MitIDEmployee:\n"
        "      scope: TEXT\n",
        encoding="utf-8",
    )

    assert load_address_type_user_keys(init_config) == [
        "EmailEmployee",
        "MitIDEmployee",
    ]


def test_cleanup_posts_all_address_type_keys(tmp_path: Path, monkeypatch):
    init_config = tmp_path / "init.config.AD_NAME.yml"
    init_config.write_text(
        "facets:\n"
        "  employee_address_type:\n"
        "    EmailEmployee: {}\n"
        "    MitIDEmployee: {}\n",
        encoding="utf-8",
    )
    response = Mock(text="", raise_for_status=Mock())
    post = Mock(return_value=response)
    monkeypatch.setattr("tools.ldap_cleanup_orphaned_addresses.requests.post", post)

    cleanup_orphaned_addresses("AD_NAME", 8451, tmp_path)

    post.assert_called_once_with(
        "http://localhost:8451/fixup/cleanup_orphaned_addresses",
        params=[
            ("dry_run", "false"),
            ("address_type_user_keys", "EmailEmployee"),
            ("address_type_user_keys", "MitIDEmployee"),
        ],
    )
    response.raise_for_status.assert_called_once_with()
