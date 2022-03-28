import unittest
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import patch

from helpers import dummy_settings

from integrations.os2sync.os2mo import get_sts_user
from integrations.os2sync.os2mo import try_get_ad_user_key

uuid = "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b"
mo_employee_url_end = "e/{}/".format(uuid)
mo_employee_address_url_end = mo_employee_url_end + "details/address"
mo_employee_engagement_url_end = (
    mo_employee_url_end + "details/engagement?calculate_primary=true"
)
mo_employee_it_url_end = mo_employee_url_end + "details/it"

mo_employee = {
    "cpr_no": "0602602389",
    "givenname": "Solveig",
    "name": "Solveig Kuhlenhenke",
    "nickname": "",
    "nickname_givenname": "",
    "nickname_surname": "",
    "org": {
        "name": "Kolding Kommune",
        "user_key": "Kolding Kommune",
        "uuid": "3b866d97-0b1f-48e0-8078-686d96f430b3",
    },
    "surname": "Kuhlenhenke",
    "user_key": "SolveigK_user_key",
    "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
}

mo_employee_address = [
    {
        "address_type": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "full_name": "Lokation",
            "name": "Lokation",
            "owner": None,
            "scope": "TEXT",
            "top_level_facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "user_key": "LocationEmployee",
            "uuid": "d34e2ee9-ec64-4259-a315-f681c3417866",
        },
        "href": None,
        "name": "Bygning 15",
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "Bygning 15",
        "uuid": "1940ecde-c1ad-418a-bf15-194df92a0b2b",
        "validity": {"from": "2003-08-13", "to": None},
        "value": "Bygning 15",
    },
    {
        "address_type": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "full_name": "Postadresse",
            "name": "Postadresse",
            "owner": None,
            "scope": "DAR",
            "top_level_facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "user_key": "AdressePostEmployee",
            "uuid": "e75f74f5-cbc4-4661-b9f4-e6a9e05abb2d",
        },
        "href": "https://www.openstreetmap.org/?mlon=9.40634869&mlat=55.55868524&zoom=16",
        "name": "Bakkedraget 28, Vester Nebel, 6040 Egtved",
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "0a3f50ba-66b3-32b8-e044-0003ba298018",
        "uuid": "544b4172-63db-4b47-8322-fa0ab4a42de0",
        "validity": {"from": "2003-08-13", "to": None},
        "value": "0a3f50ba-66b3-32b8-e044-0003ba298018",
    },
    {
        "address_type": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "full_name": "Telefon",
            "name": "Telefon",
            "owner": None,
            "scope": "PHONE",
            "top_level_facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "user_key": "PhoneEmployee",
            "uuid": "05b69443-0c9f-4d57-bb4b-a8c719afff89",
        },
        "href": "tel:55677853",
        "name": "55677853",
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "55677853",
        "uuid": "6293d387-7504-4b04-b32e-49d93140acf1",
        "validity": {"from": "2003-08-13", "to": None},
        "value": "55677853",
        "visibility": {
            "example": None,
            "name": "Hemmelig",
            "owner": None,
            "scope": "SECRET",
            "user_key": "Hemmelig",
            "uuid": "baaabba6-829c-44bf-ad45-db9d9c72f834",
        },
    },
    {
        "address_type": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "full_name": "Email",
            "name": "Email",
            "owner": None,
            "scope": "EMAIL",
            "top_level_facet": {
                "description": "",
                "user_key": "employee_address_type",
                "uuid": "5b3a55b1-958c-416e-9054-606b2c9e4fcd",
            },
            "user_key": "EmailEmployee",
            "uuid": "f376deb8-4743-4ca6-a047-3241de8fe9d2",
        },
        "href": "mailto:solveigk@kolding.dk",
        "name": "solveigk@kolding.dk",
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "solveigk@kolding.dk",
        "uuid": "6c454f09-a2d0-4904-b64d-3ee90dfbfc8a",
        "validity": {"from": "2003-08-13", "to": None},
        "value": "solveigk@kolding.dk",
    },
]

mo_employee_engagement = [
    {
        "engagement_type": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "engagement_type",
                "uuid": "182df2a8-2594-4a3f-9103-a9894d5e0c36",
            },
            "full_name": "Ansat",
            "name": "Ansat",
            "owner": None,
            "scope": "TEXT",
            "top_level_facet": {
                "description": "",
                "user_key": "engagement_type",
                "uuid": "182df2a8-2594-4a3f-9103-a9894d5e0c36",
            },
            "user_key": "Ansat",
            "uuid": "8acc5743-044b-4c82-9bb9-4e572d82b524",
        },
        "extension_1": None,
        "extension_10": None,
        "extension_2": None,
        "extension_3": None,
        "extension_4": None,
        "extension_5": None,
        "extension_6": None,
        "extension_7": None,
        "extension_8": None,
        "extension_9": None,
        "fraction": None,
        "integration_data": {
            "Artificial import": '"be8300fe7fda60be2c1e57634dd56'
            '7d8cb8977aa5a065cace7d0a1ae03910981"STOP_DUMMY'
        },
        "is_primary": None,
        "job_function": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "engagement_job_function",
                "uuid": "82155433-489f-4ddc-84f6-89911a454c7c",
            },
            "full_name": "Bogopsætter",
            "name": "Bogopsætter",
            "owner": None,
            "scope": "TEXT",
            "top_level_facet": {
                "description": "",
                "user_key": "engagement_job_function",
                "uuid": "82155433-489f-4ddc-84f6-89911a454c7c",
            },
            "user_key": "Bogopsætter",
            "uuid": "cf84f415-a6bd-4b4d-9b06-91ea392a8543",
        },
        "org_unit": {
            "name": "Kolding Kommune",
            "user_key": "Kolding Kommune",
            "uuid": "f06ee470-9f17-566f-acbe-e938112d46d9",
            "validity": {"from": "1960-01-01", "to": None},
        },
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "primary": {
            "example": None,
            "facet": {
                "description": "",
                "user_key": "primary_type",
                "uuid": "4afba2d2-4846-42df-981b-0a85ee8de8db",
            },
            "full_name": "Primær",
            "name": "Primær",
            "owner": None,
            "scope": "3000",
            "top_level_facet": {
                "description": "",
                "user_key": "primary_type",
                "uuid": "4afba2d2-4846-42df-981b-0a85ee8de8db",
            },
            "user_key": "primary",
            "uuid": "0644cd06-b84b-42e0-95fe-ce131c21fbe6",
        },
        "user_key": "-",
        "uuid": "94f04266-f744-4c06-8f4e-9561abfbff75",
        "validity": {"from": "2003-08-13", "to": None},
    }
]

mo_employee_it = [
    {
        "itsystem": {
            "name": "OpenDesk",
            "reference": None,
            "system_type": None,
            "user_key": "OpenDesk",
            "uuid": "db519bfd-0fdd-4e5d-9337-518d1dbdbfc9",
            "validity": {"from": "1900-01-01", "to": None},
        },
        "org_unit": None,
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "SolveigKOpenDesk",
        "uuid": "a029d0ac-7ea6-4023-8680-57825e43af8c",
        "validity": {"from": "2003-08-13", "to": None},
    },
    {
        "itsystem": {
            "name": "Active Directory",
            "reference": None,
            "system_type": None,
            "user_key": "Active Directory",
            "uuid": "a1608e69-c422-404f-a6cc-b873c50af111",
            "validity": {"from": "1900-01-01", "to": None},
        },
        "org_unit": None,
        "person": {
            "givenname": "Solveig",
            "name": "Solveig Kuhlenhenke",
            "nickname": "",
            "nickname_givenname": "",
            "nickname_surname": "",
            "surname": "Kuhlenhenke",
            "uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        },
        "user_key": "SolveigK_AD_logon",
        "uuid": "a2fb2581-c57a-46ad-8a21-30118a3859b7",
        "validity": {"from": "2003-08-13", "to": None},
    },
]


class MockResponse:
    def __init__(self, value):
        self.value = value

    def raise_for_status(self):
        pass

    def json(self):
        return deepcopy(self.value)


def patched_session_get(url: str, params: Optional[Dict[Any, Any]] = None, **kwargs):
    if params:
        raise ValueError("unexpected params: {}".format(params))
    if kwargs:
        raise ValueError("unexpected kwargs: {}".format(kwargs))
    for legal_end, value in {
        mo_employee_url_end: mo_employee,
        mo_employee_it_url_end: mo_employee_it,
        mo_employee_address_url_end: mo_employee_address,
        mo_employee_engagement_url_end: mo_employee_engagement,
    }.items():
        if url.endswith(legal_end):
            return MockResponse(value)

    raise ValueError("unexpected url: {}".format(url))


class TestsMOAd(unittest.TestCase):
    @patch("integrations.os2sync.os2mo.os2mo_get", patched_session_get)
    def test_get_ad_user_key(self):
        expected = "SolveigK_AD_logon"
        self.assertEqual(expected, try_get_ad_user_key(uuid))

    @patch("integrations.os2sync.os2mo.os2mo_get", patched_session_get)
    def test_mo_client_default(self):
        expected = {
            "Email": "solveigk@kolding.dk",
            "Person": {"Cpr": "0602602389", "Name": "Solveig Kuhlenhenke"},
            "Positions": [],
            "UserId": "SolveigK_AD_logon",
            "Uuid": "23d2dfc7-6ceb-47cf-97ed-db6beadcb09b",
        }
        settings = dummy_settings
        settings.os2sync_xfer_cpr = True
        self.assertEqual(expected, get_sts_user(uuid, [], settings=settings))
