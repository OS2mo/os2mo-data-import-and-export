from unittest.mock import patch
from uuid import UUID

import pytest
from more_itertools import first

from exporters.emus.viborg_xml_emus import get_filtered_phone_addresses


@pytest.mark.parametrize(
    "address_type_uuid, person_uuid, priority_list_uuid",
    [
        (  # Expecting the correct address uuid to be returned even with duplicate entries.
            "05b69443-0c9f-4d57-bb4b-a8c719afff89",
            "ffbe5804-cf13-450a-a41b-47865e355a15",
            [
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
            ],
        ),
        (  # Expecting the first address to be returned.
            "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
            "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
            [
                "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
            ],
        ),
        (  # First address is expected to be returned from priority list with multiple uuids.
            "f376deb8-4743-4ca6-a047-3241de8fe9d2",
            "16d08fe1-45cf-4e21-b5af-1262002534d0",
            [
                "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
            ],
        ),
    ],
)
@patch("exporters.emus.viborg_xml_emus.MoraHelper")
def test_get_filtered_phone_addresses_takes_first_element_in_list_unit_test(
    mock_mh, address_type_uuid, person_uuid, priority_list_uuid
):
    """
    Tests if filter works with multiple uuid(s) in priority list.
    """

    output_dict = {
        "address_type": {
            "uuid": address_type_uuid,
        },
        "person": {"uuid": person_uuid},
    }

    mock_mh.get_e_addresses.return_value = [
        output_dict
    ]  # Return type is expected to be a list.

    # Make the call matching on the persons uuid, with a mocked helper, and a list of uuid(s) as the priority list.
    response = get_filtered_phone_addresses(
        UUID(output_dict["person"]["uuid"]),
        mock_mh,
        priority_list_uuid,
    )

    assert output_dict == response
    assert UUID(response["address_type"]["uuid"]) == UUID(address_type_uuid)
    assert UUID(response["person"]["uuid"]) == UUID(person_uuid)
    assert UUID(response["address_type"]["uuid"]) == UUID(first(priority_list_uuid))


@pytest.mark.parametrize(
    "output_phone_address_list, priority_list_uuid, expected_result",
    [
        (  # Filters on first match on address type uuid, even with multiple uuids in priority list.
            [
                {
                    "address_type": {
                        "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    },
                    "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
                }
            ],
            [
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
            ],
            {
                "address_type": {
                    "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                },
                "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
            },
        ),
        (  # No match on address type uuid is found in priority list.
            [
                {
                    "address_type": {
                        "uuid": "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
                    },
                    "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
                }
            ],
            [
                "f376deb8-4743-4ca6-a047-3241de8fe9d2"
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
            ],
            {},
        ),
        (  # No uuid is sent, so no match is found.
            [
                {
                    "address_type": {"uuid": None},
                    "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
                }
            ],
            ["05b69443-0c9f-4d57-bb4b-a8c719afff89"],
            {},
        ),
        (  # Several addresses of different address_types, some are in settings. Ensure we pick the first from the list.
            [
                {
                    "address_type": {
                        "uuid": "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
                    },
                    "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
                },
                {
                    "address_type": {
                        "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
                    },
                    "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
                },
                {
                    "address_type": {
                        "uuid": "5a02f9c4-bb83-4ce5-b1ba-7289db912b0c",
                    },
                    "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
                },
                {
                    "address_type": {
                        "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    },
                    "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
                },
            ],
            [
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "5a02f9c4-bb83-4ce5-b1ba-7289db912b0c",
                "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
            ],
            {
                "address_type": {
                    "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                },
                "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
            },
        ),
        (  # Duplicates of address_type still ensures only one match in priority_list is made and one address is sent.
            [
                {
                    "address_type": {
                        "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    },
                    "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
                },
                {
                    "address_type": {
                        "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    },
                    "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
                },
                {
                    "address_type": {
                        "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                    },
                    "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
                },
            ],
            [
                "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                "c803d0c2-2ef7-460c-83c0-980c58bfa7ac",
            ],
            {
                "address_type": {
                    "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
                },
                "person": {"uuid": "16d08fe1-45cf-4e21-b5af-1262002534d0"},
            },
        ),
    ],
)
@patch("exporters.emus.viborg_xml_emus.MoraHelper")
def test_get_filtered_phone_addresses_sends_correct_address_from_filter_unit_test(
    mock_mh, output_phone_address_list, priority_list_uuid, expected_result
):
    """
    Tests if correct address has been sent through filter.
    """

    mock_mh.get_e_addresses.return_value = output_phone_address_list

    response = get_filtered_phone_addresses(
        UUID(output_phone_address_list[0]["person"]["uuid"]),
        mock_mh,
        priority_list_uuid,
    )

    assert response == expected_result
