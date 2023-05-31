import pytest
from more_itertools import one
from os2sync_export.config import get_os2sync_settings


# Webtests that assumes access to a running os2mo with the default "Kolding" dataset.
# Run using `pytest -v -m webtest`
# Skip using `pytest -v -m "not webtest"`
expected = {
    "a5fca2fc-1c24-4db3-b39f-70c477605793": {
        True: [
            {
                "Email": "jans@kolding.dk",
                "Person": {"Cpr": "1507732409", "Name": "Jan Frost Sørensen"},
                "Positions": [
                    {
                        "Name": "Udvikler",
                        "OrgUnitUuid": "23a2ace2-52ca-458d-bead-d1a42080579f",
                        "is_primary": True,
                    }
                ],
                "UserId": "a5fca2fc-1c24-4db3-b39f-70c477605793",
                "Uuid": "a5fca2fc-1c24-4db3-b39f-70c477605793",
            }
        ],
        False: [
            {
                "Email": "jans@kolding.dk",
                "Person": {"Cpr": None, "Name": "Jan Frost Sørensen"},
                "Positions": [
                    {
                        "Name": "Udvikler",
                        "OrgUnitUuid": "23a2ace2-52ca-458d-bead-d1a42080579f",
                        "is_primary": True,
                    }
                ],
                "UserId": "a5fca2fc-1c24-4db3-b39f-70c477605793",
                "Uuid": "a5fca2fc-1c24-4db3-b39f-70c477605793",
            }
        ],
    },
    "6442339e-da8f-49cf-beba-d6b0cb025750": {
        True: [
            {
                "Email": "erlingp@kolding.dk",
                "Person": {"Cpr": "0406453634", "Name": "Erling Pedersen"},
                "PhoneNumber": "22726287",
                "Positions": [
                    {
                        "Name": "Støttepædagog",
                        "OrgUnitUuid": "7a8e45f7-4de0-44c8-990f-43c0565ee505",
                        "is_primary": True,
                    }
                ],
                "UserId": "6442339e-da8f-49cf-beba-d6b0cb025750",
                "Uuid": "6442339e-da8f-49cf-beba-d6b0cb025750",
            }
        ],
        False: [
            {
                "Email": "erlingp@kolding.dk",
                "Person": {"Cpr": None, "Name": "Erling Pedersen"},
                "PhoneNumber": "22726287",
                "Positions": [
                    {
                        "Name": "Støttepædagog",
                        "OrgUnitUuid": "7a8e45f7-4de0-44c8-990f-43c0565ee505",
                        "is_primary": True,
                    }
                ],
                "UserId": "6442339e-da8f-49cf-beba-d6b0cb025750",
                "Uuid": "6442339e-da8f-49cf-beba-d6b0cb025750",
            }
        ],
    },
    "1586a3ef-25c9-44ae-89c9-98bbc90ef033": {
        True: [
            {
                "Email": "grejsl@kolding.dk",
                "Person": {"Cpr": "2709422807", "Name": "Grejs Rajah Lauritzen"},
                "Positions": [
                    {
                        "Name": "Pædagogmedhjælper",
                        "OrgUnitUuid": "ec127e78-3c17-5fe6-84cb-7f19dad9fb85",
                        "is_primary": True,
                    }
                ],
                "UserId": "GrejsL",
                "Uuid": "1586a3ef-25c9-44ae-89c9-98bbc90ef033",
            }
        ],
        False: [
            {
                "Email": "grejsl@kolding.dk",
                "Person": {"Cpr": None, "Name": "Grejs Rajah Lauritzen"},
                "Positions": [
                    {
                        "Name": "Pædagogmedhjælper",
                        "OrgUnitUuid": "ec127e78-3c17-5fe6-84cb-7f19dad9fb85",
                        "is_primary": True,
                    }
                ],
                "UserId": "GrejsL",
                "Uuid": "1586a3ef-25c9-44ae-89c9-98bbc90ef033",
            }
        ],
    },
}


@pytest.mark.webtest
@pytest.mark.parametrize("xfer_cpr", [True, False])
@pytest.mark.parametrize(
    "user_uuid",
    [
        "a5fca2fc-1c24-4db3-b39f-70c477605793",
        "6442339e-da8f-49cf-beba-d6b0cb025750",
        "1586a3ef-25c9-44ae-89c9-98bbc90ef033",
    ],
)
def test_user(xfer_cpr, user_uuid, mock_env):
    from os2sync_export.os2synccli import update_single_user

    settings = get_os2sync_settings(os2sync_xfer_cpr=xfer_cpr)
    payload = update_single_user(user_uuid, settings, dry_run=True)
    cpr = one(payload)["Person"]["Cpr"]
    assert cpr is not None if xfer_cpr else cpr is None
    assert payload == expected[user_uuid][xfer_cpr]
