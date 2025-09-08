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
def opus_ou_manager_the_same_as_eng_employee() -> dict[str, Any]:
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
                        "managers": [{"user_key": "12345"}],
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
def engagements() -> dict[str, Any]:
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


@pytest.fixture
def opus_ou_parent_manager() -> dict[str, Any]:
    return {
        "org_units": {
            "objects": [
                {
                    "current": {
                        "parent": {
                            "uuid": "48a3819e-c5e1-4f4d-b4ad-582caff6ea51",
                            "managers": [{"user_key": "54321"}],
                        }
                    }
                }
            ]
        }
    }


@pytest.fixture
def sd_ou_manager_not_the_same_as_eng_employee() -> dict[str, Any]:
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
                        "managers": [
                            {
                                "user_key": "e2b58581-1cbe-4397-8ca6-d8ae97d8cfa4",
                                "person": [
                                    {
                                        "engagements": [
                                            {
                                                "user_key": "54321",
                                                "org_unit_uuid": "9d1af806-f4d6-44e2-a001-a5deb3aa6703",
                                            },
                                            {
                                                "user_key": "23456",
                                                "org_unit_uuid": "4c448c55-9f2d-4b7e-a386-44ca218c977b",
                                            },
                                        ]
                                    }
                                ],
                            }
                        ],
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
def sd_ou_manager_in_ancestor() -> dict[str, Any]:
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
                        "managers": [
                            {
                                "user_key": "e2b58581-1cbe-4397-8ca6-d8ae97d8cfa4",
                                "person": [
                                    {
                                        "engagements": [
                                            {
                                                "user_key": "54321",
                                                "org_unit_uuid": "3a16f535-edfd-45b7-8c88-4db639face28",
                                            },
                                            {
                                                "user_key": "23456",
                                                "org_unit_uuid": "4c448c55-9f2d-4b7e-a386-44ca218c977b",
                                            },
                                        ]
                                    }
                                ],
                            }
                        ],
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
                        "ancestors": [{"uuid": "3a16f535-edfd-45b7-8c88-4db639face28"}],
                    }
                }
            ]
        }
    }
