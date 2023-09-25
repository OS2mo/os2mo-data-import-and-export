# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from copy import deepcopy
from uuid import uuid4

import pytest
from more_itertools import one
from parameterized import parameterized
from pydantic import SecretStr, ValidationError

from sdtool.config import Settings

MANDATORY_SETTINGS = {
    "sd_user": "username",
    "sd_password": "password",
    "sd_institution_identifier": "institution"
}


@parameterized.expand(
    [
        (
            {
                "saml_token": str(uuid4()),
                "client_secret": "client_secret"
            },
            "SAML and OIDC cannot be used simultaneously"
        ),
        (
            {
                "client_secret": "client_secret"
            },
            "The following ENVs are missing: auth_server, auth_realm, client_id"
        ),
        (
            {
                "client_secret": "client_secret",
                "client_id": "client_id"
            },
            "The following ENVs are missing: auth_server, auth_realm"
        ),
        (
            {
                "client_secret": "client_secret",
                "client_id": "client_id",
                "auth_realm": "auth_realm"
            },
            "The following ENVs are missing: auth_server"
        )
    ]
)
def test_root_validator(settings_updates, msg):
    settings = deepcopy(MANDATORY_SETTINGS)
    settings.update(settings_updates)
    with pytest.raises(ValidationError) as err:
        Settings.parse_obj(settings)
    assert one(err.value.errors())["msg"] == msg


def test_auth_server_must_be_url():
    settings = deepcopy(MANDATORY_SETTINGS)
    settings.update(
        {
            "client_secret": "client_secret",
            "client_id": "client_id",
            "auth_realm": "auth_realm",
            "auth_server": "Not a URL"
        }
    )
    with pytest.raises(ValidationError):
        Settings.parse_obj(settings)


def test_sd_password_is_secret():
    settings = Settings.parse_obj(MANDATORY_SETTINGS)
    assert isinstance(settings.sd_password, SecretStr)


def test_value_error_not_raised_when_keycloak_settings_ok():
    settings = deepcopy(MANDATORY_SETTINGS)
    settings.update(
        {
            "client_secret": "client_secret",
            "client_id": "client_id",
            "auth_realm": "auth_realm",
            "auth_server": "http://keycloak-service:8080/auth"
        }
    )
    assert Settings.parse_obj(settings)


def test_set_envs_from_salt_settings():
    salt_settings = {
        "sd_username": "username",
        "sd_password": "password",
        "sd_institution": "institution"
    }
    settings = Settings.parse_obj(salt_settings)
    assert settings.sd_user == "username"
    assert settings.sd_password.get_secret_value() == "password"
    assert settings.sd_institution_identifier == "institution"
