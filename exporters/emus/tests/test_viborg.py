from unittest.mock import patch
from uuid import UUID

from more_itertools import one

from exporters.emus.viborg_xml_emus import get_filtered_phone_addresses


@patch("exporters.emus.viborg_xml_emus.MoraHelper")
def test_send_proper_phone_number_unit_test(mock_mh):
    """
    Tests if phone numbers are successfully filtered through, and only returns phonenumbers accepted
    in our custom settings.json file.
    """

    output_dict = {
        "address_type": {
            "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
        },
        "person": {"uuid": "eff3fca2-645c-4613-90ad-5fb47db47bc7"},
    }

    mock_mh.get_e_addresses.return_value = [
        output_dict
    ]  # Return type is expected to be a list.

    # Make the call matching on the persons uuid, with a mocked helper, and a list of uuid(s) as the priority list.
    response = get_filtered_phone_addresses(
        UUID(output_dict["person"]["uuid"]),
        mock_mh,
        ["05b69443-0c9f-4d57-bb4b-a8c719afff89"],
    )

    assert output_dict == response
    assert UUID(response["address_type"]["uuid"]) == UUID(
        "05b69443-0c9f-4d57-bb4b-a8c719afff89"
    )
