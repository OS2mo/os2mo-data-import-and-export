from typing import List
from uuid import UUID

import freezegun
import pytest

from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (
    convert_person_and_engagement_data_to_csv,
)
from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (
    get_email_address_type_uuid_from_gql,
)
from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (
    get_filtered_engagements_for_ended_today,
)
from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (  # type: ignore
    get_filtered_engagements_for_started_today,  # type: ignore
)
from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (
    gql_query_persons_details_to_display,
)
from reports.os2mo_new_and_ended_engagement_reports.get_engagements import (  # type: ignore
    gql_query_validity_field,  # type: ignore
)


@pytest.mark.parametrize(
    "query_valid_from, query_valid_to, query_started_engagements, "
    "query_ended_engagements",
    [
        (
            """query EstablishedEngagements ($engagement_date_to_query_from: DateTime) {
             engagements(from_date: $engagement_date_to_query_from) {
               objects {
                 employee_uuid
                 validity {
                   from
                 }
               }
             }
           }
        """,
            """query EstablishedEngagements ($engagement_date_to_query_from: DateTime) {
             engagements(from_date: $engagement_date_to_query_from) {
               objects {
                 employee_uuid
                 validity {
                   to
                 }
               }
             }
           }
        """,
            """query PersonEngagementDetails ($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
             employees(uuids: $uuidlist) {
               objects {
                 cpr_no
                 name
                 uuid
                 addresses(address_types: $email_uuid_list) {
                   name
                 }
                 engagements {
                   org_unit {
                   name
                 }
                   validity {
                     from
                   }
                 }
                 itusers {
                   user_key
                   itsystem {
                     name
                   }
                 }
               }
             }
           }
        """,
            """query PersonEngagementDetails ($uuidlist: [UUID!], $email_uuid_list: [UUID!]) {
             employees(uuids: $uuidlist) {
               objects {
                 name
                 uuid
                 addresses(address_types: $email_uuid_list) {
                   name
                 }
                 engagements {
                   org_unit {
                   name
                 }
                   validity {
                     to
                   }
                 }
                 itusers {
                   user_key
                   itsystem {
                     name
                   }
                 }
               }
             }
           }
        """,
        )
    ],
)
def test_queries_verify_validities_and_engagements_query_string(
    query_valid_from: str,
    query_valid_to: str,
    query_started_engagements: str,
    query_ended_engagements: str,
) -> None:
    """Test to verify the propper validity "from" and "to" form is sent,
    when calling the function."""
    assert query_valid_from == gql_query_validity_field(validity_from=True)
    assert query_valid_to == gql_query_validity_field(validity_to=True)

    # The call returns None, when no from or to is provided.
    assert gql_query_validity_field() is None

    assert query_started_engagements == gql_query_persons_details_to_display(
        started_engagement=True  # type: ignore
    )
    assert query_ended_engagements == gql_query_persons_details_to_display(
        ended_engagement=True  # type: ignore
    )

    # The call returns None, when no started or ended is provided.
    assert gql_query_persons_details_to_display() is None


@freezegun.freeze_time("2023-01-09T00:00")
@pytest.mark.parametrize(
    "engagements_started_payload, engagements_ended_payload,"
    " expected_start_uuids, expected_end_uuids",
    [
        (  # Employee starting and ending engagement.
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"from": "2000-06-29T00:00:00+02:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"from": "1972-07-26T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "482ddcff-c0de-40e3-82e9-1948da9f01b1",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "71f79458-aed1-4ca5-a32a-1a4d40cbb701",
                                "validity": {"from": "1987-03-10T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "a049854d-8e75-4428-bf37-e5c20ff880ec",
                                "validity": {"from": "1998-06-10T00:00:00+02:00"},
                            }
                        ]
                    },
                ]
            },
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"to": None},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"to": None},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "b589ea2f-6e92-4a2c-80f8-fb06eba89dbd",
                                "validity": {"to": "2034-06-01T00:00:00+02:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "482ddcff-c0de-40e3-82e9-1948da9f01b1",
                                "validity": {"to": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "ecb6a918-d840-4bcc-b287-533128af6fcb",
                                "validity": {"to": "2044-01-10T00:00:00+01:00"},
                            }
                        ]
                    },
                ]
            },
            ["482ddcff-c0de-40e3-82e9-1948da9f01b1"],
            ["482ddcff-c0de-40e3-82e9-1948da9f01b1"],
        ),
        (  # Multiple employees having started and ended their engagements.
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "482ddcff-c0de-40e3-82e9-1948da9f01b1",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "71f79458-aed1-4ca5-a32a-1a4d40cbb701",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "a049854d-8e75-4428-bf37-e5c20ff880ec",
                                "validity": {"from": "1998-06-10T00:00:00+02:00"},
                            }
                        ]
                    },
                ]
            },
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"to": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"to": None},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "b589ea2f-6e92-4a2c-80f8-fb06eba89dbd",
                                "validity": {"to": "2034-06-01T00:00:00+02:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "482ddcff-c0de-40e3-82e9-1948da9f01b1",
                                "validity": {"to": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "ecb6a918-d840-4bcc-b287-533128af6fcb",
                                "validity": {"to": "2044-01-10T00:00:00+01:00"},
                            }
                        ]
                    },
                ]
            },
            [
                "b81b5097-90b7-4991-8752-c860e1e59fd3",
                "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                "482ddcff-c0de-40e3-82e9-1948da9f01b1",
                "71f79458-aed1-4ca5-a32a-1a4d40cbb701",
            ],
            [
                "b81b5097-90b7-4991-8752-c860e1e59fd3",
                "482ddcff-c0de-40e3-82e9-1948da9f01b1",
            ],
        ),
        (  # No employees starting or ending any engagements.
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"from": None},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"from": None},
                            }
                        ]
                    },
                ]
            },
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"to": None},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"to": None},
                            }
                        ]
                    },
                ]
            },
            [],
            [],
        ),
        (  # Employee starting and ending engagements multiple times on same day.
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "b81b5097-90b7-4991-8752-c860e1e59fd3",
                                "validity": {"from": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                ]
            },
            {
                "engagements": [
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"to": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "employee_uuid": "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                                "validity": {"to": "2023-01-09T00:00:00+01:00"},
                            }
                        ]
                    },
                ]
            },
            [
                "b81b5097-90b7-4991-8752-c860e1e59fd3",
                "b81b5097-90b7-4991-8752-c860e1e59fd3",
            ],
            [
                "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
                "35aa43b3-b50a-41ba-8151-4c63f5d83db8",
            ],
        ),
    ],
)
def test_filtering_engagements_from_started_and_ended_engagements_payloads(
    engagements_started_payload: dict,
    engagements_ended_payload: dict,
    expected_start_uuids: List[UUID],
    expected_end_uuids: List[UUID],
) -> None:
    """Testing filter functions ability to return correct uuid(s) on various engagements."""

    assert expected_start_uuids == get_filtered_engagements_for_started_today(
        engagements_started_payload
    )
    assert expected_end_uuids == get_filtered_engagements_for_ended_today(
        engagements_ended_payload
    )


@pytest.mark.parametrize(
    "addresses_payload, expected_eligible_email_uuids",
    [
        (  # Tests with multiple scopes of address types.
            {
                "addresses": [
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "EMAIL",
                                    "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "PHONE",
                                    "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "DAR",
                                    "uuid": "5260d4aa-e33b-48f7-ae3e-6074262cbdcf",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "DAR",
                                    "uuid": "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "TEXT",
                                    "uuid": "d34e2ee9-ec64-4259-a315-f681c3417866",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "PNUMBER",
                                    "uuid": "2e36f204-1bef-4165-bd9b-9c1981b3d240",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "DAR",
                                    "uuid": "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "PHONE",
                                    "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "EMAIL",
                                    "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "TEXT",
                                    "uuid": "60eab590-58db-44e2-aeb0-8e4c67029d33",
                                }
                            }
                        ]
                    },
                ]
            },
            [
                "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                "f376deb8-4743-4ca6-a047-3241de8fe9d2",
            ],
        ),
        (  # Tests with multiple scope of emails with different uuids.
            {
                "addresses": [
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "EMAIL",
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "EMAIL",
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "EMAIL",
                                    "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                                }
                            }
                        ]
                    },
                ]
            },
            [
                "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                "f376deb8-4743-4ca6-a047-3241de8fe9d2",
            ],
        ),
        (  # Tests with no scope of email.
            {
                "addresses": [
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "TEXT",
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "TEXT",
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": "DAR",
                                    "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                                }
                            }
                        ]
                    },
                ]
            },
            [],
        ),
        (  # Tests wth None values.
            {
                "addresses": [
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": None,
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": None,
                                    "uuid": "61c22b75-01b0-4e83-954c-9cf0c8dc79fe",
                                }
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "address_type": {
                                    "scope": None,
                                    "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                                }
                            }
                        ]
                    },
                ]
            },
            [],
        ),
    ],
)
def test_filtering_email_address_type_uuids_from_payload(
    addresses_payload: dict, expected_eligible_email_uuids: List[UUID]
) -> None:
    """Tests if filter on emails from scope of addresses returns correct list of uuid(s)."""

    assert (
        get_email_address_type_uuid_from_gql(addresses_payload)
        == expected_eligible_email_uuids
    )


@pytest.mark.parametrize(
    "engagement_started_details_to_write_from, engagement_ended_details_to_write_from,"
    " expected_csv_output_for_started, expected_csv_output_for_ended",
    [
        (  # Tests if details from payload are written correctly into CSV format.
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "1@2.dk"}],
                                "cpr_no": "0102892332",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-02-15T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [
                                    {"itsystem": {"name": "Plone"}, "user_key": "12"},
                                    {
                                        "itsystem": {"name": "Active Directory"},
                                        "user_key": "AD-konti",
                                    },
                                    {
                                        "itsystem": {"name": "OpenDesk"},
                                        "user_key": "32",
                                    },
                                ],
                                "name": "Alberte Olsen",
                                "uuid": "b1f94eca-652a-4d49-a3bf-cd34a0102dcf",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "45@1.dk"}],
                                "cpr_no": "0902874312",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-02-15T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [
                                    {"itsystem": {"name": "Plone"}, "user_key": "notAD"}
                                ],
                                "name": "Asta Jensen",
                                "uuid": "a3e500a4-b393-4923-86a8-7248ad1c20c3",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "45@0.dk"}],
                                "cpr_no": "2103782143",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-02-15T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [],
                                "name": "Liam Johansen",
                                "uuid": "694e7c1f-cf1e-46a0-a168-cd392cf11c46",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "cpr_no": "0102884312",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-02-15T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [
                                    {
                                        "itsystem": {"name": "OpenDesk"},
                                        "user_key": "43",
                                    },
                                    {
                                        "itsystem": {"name": "Active Directory"},
                                        "user_key": "AD-kontioooo",
                                    },
                                ],
                                "name": "Sofia Nielsen",
                                "uuid": "dffc9768-47da-435d-8d1b-0daf74ff97cd",
                            }
                        ]
                    },
                ]
            },
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "1@2.dk"}],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {"to": "2023-02-15T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [
                                    {"itsystem": {"name": "Plone"}, "user_key": "12"},
                                    {
                                        "itsystem": {"name": "Active Directory"},
                                        "user_key": "AD-konti",
                                    },
                                    {
                                        "itsystem": {"name": "OpenDesk"},
                                        "user_key": "32",
                                    },
                                ],
                                "name": "Alberte Olsen",
                                "uuid": "b1f94eca-652a-4d49-a3bf-cd34a0102dcf",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "45@1.dk"}],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {"to": "2023-02-15T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [
                                    {"itsystem": {"name": "Plone"}, "user_key": "notAD"}
                                ],
                                "name": "Asta Jensen",
                                "uuid": "a3e500a4-b393-4923-86a8-7248ad1c20c3",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "45@0.dk"}],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {"to": "2023-02-15T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [],
                                "name": "Liam Johansen",
                                "uuid": "694e7c1f-cf1e-46a0-a168-cd392cf11c46",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {"to": "2023-02-15T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [
                                    {
                                        "itsystem": {"name": "OpenDesk"},
                                        "user_key": "43",
                                    },
                                    {
                                        "itsystem": {"name": "Active Directory"},
                                        "user_key": "AD-kontioooo",
                                    },
                                ],
                                "name": "Sofia Nielsen",
                                "uuid": "dffc9768-47da-435d-8d1b-0daf74ff97cd",
                            }
                        ]
                    },
                ]
            },
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesdato";"CPR";"Email";"Shortname"\n'
                '"Alberte Olsen";"b1f94eca-652a-4d49-a3bf-cd34a0102dcf";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"0102892332";"1@2.dk";"AD-konti"\n'
                '"Asta Jensen";"a3e500a4-b393-4923-86a8-7248ad1c20c3";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"0902874312";"45@1.dk";""\n'
                '"Liam Johansen";"694e7c1f-cf1e-46a0-a168-cd392cf11c46";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"2103782143";"45@0.dk";""\n'
                '"Sofia Nielsen";"dffc9768-47da-435d-8d1b-0daf74ff97cd";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"0102884312";"";"AD-kontioooo"\n'
            ),
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesudløbsdato";"Email";"Shortname"\n'
                '"Alberte Olsen";"b1f94eca-652a-4d49-a3bf-cd34a0102dcf";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"1@2.dk";"AD-konti"\n'
                '"Asta Jensen";"a3e500a4-b393-4923-86a8-7248ad1c20c3";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"45@1.dk";""\n'
                '"Liam Johansen";"694e7c1f-cf1e-46a0-a168-cd392cf11c46";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"45@0.dk";""\n'
                '"Sofia Nielsen";"dffc9768-47da-435d-8d1b-0daf74ff97cd";"Social og '
                'sundhed";"2023-02-15T00:00:00+01:00";"";"AD-kontioooo"\n'
            ),
        ),
        (  # Tests if details from payload are written correctly into CSV format,
            # when payload has no email.
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "312@sd.com"}],
                                "cpr_no": "0102893211",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Kolding Kommune"}],
                                        "validity": {
                                            "from": "2023-01-11T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [],
                                "name": "Adam Nielsen",
                                "uuid": "98be7a7d-b61b-4213-9829-136f60769ba5",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "cpr_no": "0401873211",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-01-11T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [],
                                "name": "August Andersen",
                                "uuid": "3c58db88-b434-4221-9af2-a216ce4cbfb7",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "cpr_no": "0301674212",
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {
                                            "from": "2023-01-11T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [],
                                "name": "Merle Møller",
                                "uuid": "6f4a2cbf-d372-4dbf-87bb-d7f4307cb7a3",
                            }
                        ]
                    },
                ]
            },
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [{"name": "lolol@kekek.dk"}],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Budget og Planlægning"}],
                                        "validity": {"to": "2023-01-11T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [],
                                "name": "Malou Johansen",
                                "uuid": "8b227cf7-cb67-4023-8efc-b01c2a3632d1",
                            }
                        ]
                    },
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Social og sundhed"}],
                                        "validity": {"to": "2023-01-11T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [],
                                "name": "August Andersen",
                                "uuid": "3c58db88-b434-4221-9af2-a216ce4cbfb7",
                            }
                        ]
                    },
                ]
            },
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesdato";"CPR";"Email";"Shortname"\n'
                '"Adam Nielsen";"98be7a7d-b61b-4213-9829-136f60769ba5"'
                ';"Kolding Kommune";"2023-01-11T00:00:00+01:00";"0102893211";'
                '"312@sd.com";""\n'
                '"August Andersen";"3c58db88-b434-4221-9af2-a216ce4cbfb7";'
                '"Social og sundhed";"2023-01-11T00:00:00+01:00";"0401873211";"";'
                '""\n'
                '"Merle Møller";"6f4a2cbf-d372-4dbf-87bb-d7f4307cb7a3";'
                '"Social og sundhed";"2023-01-11T00:00:00+01:00";"0301674212";"";""\n'
            ),
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesudløbsdato";"Email";"Shortname"\n'
                ""
                '"Malou Johansen";"8b227cf7-cb67-4023-8efc-b01c2a3632d1";'
                '"Budget og Planlægning";"2023-01-11T00:00:00+01:00";"lolol@kekek.dk";'
                '""\n'
                '"August Andersen";"3c58db88-b434-4221-9af2-a216ce4cbfb7";'
                '"Social og sundhed";"2023-01-11T00:00:00+01:00";"";""\n'
            ),
        ),
        (  # Tests if details from payload are written correctly into CSV format,
            # when payload has no email or CPR.
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "cpr_no": None,
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Kolding Kommune"}],
                                        "validity": {
                                            "from": "2023-01-11T00:00:00+01:00"
                                        },
                                    }
                                ],
                                "itusers": [],
                                "name": "TESTFORNAVN",
                                "uuid": "77e0da12-6cbf-4cef-b2bb-ceff19944643",
                            }
                        ]
                    }
                ]
            },
            {
                "employees": [
                    {
                        "objects": [
                            {
                                "addresses": [],
                                "engagements": [
                                    {
                                        "org_unit": [{"name": "Kolding Kommune"}],
                                        "validity": {"to": "2023-01-11T00:00:00+01:00"},
                                    }
                                ],
                                "itusers": [],
                                "name": "TESTFORNAVN",
                                "uuid": "77e0da12-6cbf-4cef-b2bb-ceff19944643",
                            }
                        ]
                    }
                ]
            },
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesdato";"CPR";"Email";"Shortname"\n'
                '"TESTFORNAVN";"77e0da12-6cbf-4cef-b2bb-ceff19944643";"Kolding '
                'Kommune";"2023-01-11T00:00:00+01:00";"";"";""\n'
            ),
            (
                '"Personens navn";"Personens '
                'UUID";"Ansættelsessted";"Ansættelsesudløbsdato";"Email";"Shortname"\n'
                '"TESTFORNAVN";"77e0da12-6cbf-4cef-b2bb-ceff19944643";"Kolding '
                'Kommune";"2023-01-11T00:00:00+01:00";"";""\n'
            ),
        ),
    ],
)
def test_csv_writing_of_all_engagement_details(
    engagement_started_details_to_write_from: dict,
    engagement_ended_details_to_write_from: dict,
    expected_csv_output_for_started: str,
    expected_csv_output_for_ended: str,
) -> None:
    """Tests if all fields, also optional fields, from details
    are written correctly into CSV format"""

    assert (
        convert_person_and_engagement_data_to_csv(
            engagement_started_details_to_write_from,
            started=True,  # type: ignore
        )
        == expected_csv_output_for_started
    )
    assert (
        convert_person_and_engagement_data_to_csv(
            engagement_ended_details_to_write_from,
            ended=True,  # type: ignore
        )
        == expected_csv_output_for_ended
    )
