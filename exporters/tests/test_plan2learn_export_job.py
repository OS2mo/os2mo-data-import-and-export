from unittest.mock import patch
from uuid import UUID

import pytest
from more_itertools import first

from exporters.plan2learn.plan2learn import get_filtered_phone_addresses


@pytest.mark.parametrize(
    "address_type_uuid, person_uuid, priority_list_uuid",
    [
        (  # Expecting the correct address uuid to be returned even with duplicate entries.
            "05b69443-0c9f-4d57-bb4b-a8c719afff89",
            "ffbe5804-cf13-450a-a41b-47865e355a15",
            [
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
            ],
        ),
        (  # Expecting the first address to be returned.
            "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
            "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
            [
                UUID("e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d"),
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
            ],
        ),
        (  # First address is expected to be returned from priority list with multiple uuids.
            "f376deb8-4743-4ca6-a047-3241de8fe9d2",
            "16d08fe1-45cf-4e21-b5af-1262002534d0",
            [
                UUID("f376deb8-4743-4ca6-a047-3241de8fe9d2"),
                UUID("e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d"),
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
            ],
        ),
    ],
)
@patch("exporters.plan2learn.plan2learn.get_e_address")
def test_get_filtered_phone_addresses_takes_first_element_in_list_unit_test(
    mock_session, address_type_uuid, person_uuid, priority_list_uuid
) -> None:
    """
    Tests if filter works correctly with multiple uuid(s) in priority list.
    """

    output_dict = {"adresse_type": address_type_uuid, "user": person_uuid}

    mock_session.return_value = [output_dict]  # Return type is expected to be a list.

    # Make the call matching on the persons uuid, with a mocked helper, and a list of uuid(s) as the priority list.
    response = get_filtered_phone_addresses(
        UUID(output_dict["user"]), priority_list_uuid, mock_session
    )

    assert output_dict == response
    assert UUID(response["adresse_type"]) == UUID(address_type_uuid)
    assert UUID(response["user"]) == UUID(person_uuid)
    assert UUID(response["adresse_type"]) == first(priority_list_uuid)


@pytest.mark.parametrize(
    "output_phone_address_list, user_uuid, priority_list_uuid, expected_result",
    [
        (  # Filters on first match on address type uuid, even with multiple uuids in priority list.
            [
                {
                    "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
                }
            ],
            "16d08fe1-45cf-4e21-b5af-1262002534d0",
            [
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d"),
            ],
            {
                "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
            },
        ),
        (  # No match on address type uuid is found in priority list.
            [
                {
                    "adresse_type": "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                    "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
                }
            ],
            "eff3fca2-645c-4613-90ad-5fb47db47bc7",
            [
                UUID("f376deb8-4743-4ca6-a047-3241de8fe9d2"),
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("c803d0c2-2ef7-460c-83c0-980c58bfa7ac"),
            ],
            {},
        ),
        (  # No uuid is sent, so no match is found.
            [
                {
                    "adresse_type": None,
                    "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
                }
            ],
            "16d08fe1-45cf-4e21-b5af-1262002534d0",
            [UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89")],
            {},
        ),
        (  # Several addresses of different address_types, some are in settings. Ensure we pick the first from the list.
            [
                {
                    "adresse_type": "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
                    "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
                },
                {
                    "adresse_type": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                    "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
                },
                {
                    "adresse_type": "5a02f9c4-bb83-4ce5-b1ba-7289db912b0c",
                    "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
                },
                {
                    "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
                },
            ],
            "eff3fca2-645c-4613-90ad-5fb47db47bc7",
            [
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("5a02f9c4-bb83-4ce5-b1ba-7289db912b0c"),
                UUID("c803d0c2-2ef7-460c-83c0-980c58bfa7ac"),
            ],
            {
                "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "user": "eff3fca2-645c-4613-90ad-5fb47db47bc7",
            },
        ),
        (  # Duplicates of address_type still ensures only one match in priority_list is made and one address is sent.
            [
                {
                    "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
                },
                {
                    "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
                },
                {
                    "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
                },
            ],
            "16d08fe1-45cf-4e21-b5af-1262002534d0",
            [
                UUID("05b69443-0c9f-4d57-bb4b-a8c719afff89"),
                UUID("c803d0c2-2ef7-460c-83c0-980c58bfa7ac"),
            ],
            {
                "adresse_type": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "user": "16d08fe1-45cf-4e21-b5af-1262002534d0",
            },
        ),
    ],
)
@patch("exporters.plan2learn.plan2learn.get_e_address")
def test_get_filtered_phone_addresses_sends_correct_address_from_filter_unit_test(
    mock_session,
    output_phone_address_list,
    user_uuid,
    priority_list_uuid,
    expected_result,
) -> None:
    """
    Tests if correct address with different address types has been sent through filter.
    """

    mock_session.return_value = output_phone_address_list

    response = get_filtered_phone_addresses(
        UUID(user_uuid),
        priority_list_uuid,
        mock_session,
    )

    assert response == expected_result
