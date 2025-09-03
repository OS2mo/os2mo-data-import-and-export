from typing import Any

import pytest


@pytest.fixture
def opus_ou_manager_not_the_same_as_eng_employee() -> dict[str, Any]:
    return {
        "org_units": {
            "objects": [
                {
                    "current": {
                        "name": "Some name",
                        "uuid": "9d1af806-f4d6-44e2-a001-a5deb3aa6703",
                        "engagements": [
                            {"uuid": "83483193-c623-4a59-a0c1-ac8887fba72e"}
                        ],
                        "parent": {"uuid": "06f200ae-a05e-4fb3-91a4-9f16a0fc0b98"},
                        "children": [],
                        "managers": [{"user_key": "54321"}],
                        "addresses": [{"value": "1234567890"}],
                        "related_units": [
                            {
                                "org_units": [
                                    {"uuid": "626ab928-a76f-46ae-bc8f-2402deb65236"},
                                    {"uuid": "abcab928-a76f-46ae-bc8f-2402deb65123"},
                                ]
                            },
                            {
                                "org_units": [
                                    {"uuid": "18b8b97d-946d-4948-a334-0582935f7c5c"},
                                    {"uuid": "abcab928-a76f-46ae-bc8f-2402deb65123"},
                                ]
                            },
                        ],
                    }
                }
            ]
        }
    }


@pytest.fixture
def opus_engagements() -> dict[str, Any]:
    return {
        "engagements": {
            "objects": [
                {
                    "validities": [
                        {
                            "validity": {
                                "from": "2021-10-22T00:00:00+02:00",
                                "to": "2023-10-31T00:00:00+01:00",
                            }
                        },
                        {
                            "validity": {
                                "from": "2023-11-01T00:00:00+01:00",
                                "to": "2025-09-30T00:00:00+02:00",
                            }
                        },
                    ],
                    "current": {
                        "user_key": "12345",
                        "person": [
                            {
                                "cpr_number": "0101011255",
                                "given_name": "Bruce",
                                "surname": "Lee",
                                "addresses": [{"value": "bruce@kung.fu"}],
                            }
                        ],
                        "job_function": {"name": "Kung Fu Master"},
                    },
                }
            ]
        }
    }
