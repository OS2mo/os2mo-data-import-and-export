import time
import uuid
from datetime import datetime
from functools import partial
from random import choice, randint
from unittest import TestCase

import requests

from ad_sync import AdMoSync
from ad_writer import ADWriter
from tests.name_simulator import create_name
from user_names import CreateUserNames
from utils import AttrDict, recursive_dict_update


class MOTestMixin(object):
    """Mixin to connect to MO and check service configuration.

    Used by the MOTestCase class.
    """

    def get_mo_host(self):
        """Get the base URI for the MO test instance.

        Examples:
            http://localhost:5000
            https://moratest.magenta.dk

        Returns:
            str: URI including schema for the MO test instance.
        """
        return "http://localhost:5000"

    def _fetch_mo_service_configuration(self):
        """Fetch the /service/configuration endpoint on MO.

        Example:
            {
                "read_only": false,
                ...,
                "show_user_key": true
            }

        Returns:
            dict: The JSON response from MO as a dict or None
        """
        host = self.get_mo_host()
        url = host + "service/configuration"
        response = requests.get(url)
        if response.status_code != 200:
            return None
        return response.json()

    def _check_mo_ready_for_testing(self):
        """Check if a MO instance can be reached and is in readonly mode.

        Example:
            "Unable to reach MO instance"

        Returns:
            str: The reason why MO is not reading for testing or None if ready.
        """
        configuration = self._fetch_mo_service_configuration()
        read_only_key = "read_only"
        if configuration == None:
            return "Unable to reach MO instance"
        elif read_only_key not in configuration:
            return "MO instance did not return readonly status"
        if configuration[read_only_key] == False:
            # Consider putting MO into read-only mode using:
            # curl -X PUT -H 'Content-Type: application/json' \
            #      -d '{"status": true}' http://localhost:5000/read_only/
            return "MO instance is NOT readonly"
        return None


class MOTestCase(TestCase, MOTestMixin):
    """TestCase, which verifies MO connection in setUP()."""

    def setUp(self):
        # Fetch MO status, and skipTest if any issues are found.
        status = self._check_mo_ready_for_testing()
        if status:
            self.skipTest(status)


def dict_modifier(updates):
    return partial(recursive_dict_update, updates=updates)


def mo_modifier(updates):
    def mo_mod(mo_values, *args, **kwargs):
        return recursive_dict_update(mo_values, updates=updates)

    return mo_mod


class ADWriterTestSubclass(ADWriter):
    """Testing subclass of ADWriter."""

    def __init__(self, read_ad_information_from_mo, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # List of scripts to be executed via run_ps
        self.scripts = []
        # Transformer for mo_values return
        self.read_ad_information_from_mo = read_ad_information_from_mo
        self._find_unique_user = lambda cpr: read_ad_information_from_mo("")[
            "sam_account_name"
        ]

    def _init_name_creator(self):
        """Mocked to pretend no names are occupied.

        This method would normally use ADReader to read usernames from AD.
        """
        # Simply leave out the call to populate_occupied_names
        self.name_creator = CreateUserNames(occupied_names=set())

    def _create_session(self):
        """Mocked to return a fake-class which writes scripts to self.scripts.

        This method would normally send scripts to powershell via WinRM.
        """

        def run_ps(ps_script):
            # Add our script to the list
            self.scripts.append(ps_script)
            # Fake the WinRM run_ps return type
            return AttrDict({"status_code": 0, "std_out": b"", "std_err": b"",})

        # Fake the WinRM session object
        return AttrDict({"run_ps": run_ps,})

    def _get_retry_exceptions(self):
        """Mocked to return an empty list, i.e. never retry.

        This method would normally return the WinRM transport exception, to
        cause retrying to happen.
        """
        return []

    def read_ad_information_from_mo(self, uuid, read_manager=True, ad_dump=None):
        raise NotImplemented("Should be overridden in __init__")


class TestADMixin(object):
    # Useful for local testing
    generate_dynamic_person = True
    default_person = None

    def _no_transformation(self, default, *args, **kwargs):
        return default

    def _prepare_dynamic_person(self):
        def random_date():
            unixtime = randint(1, int(time.time()))
            return datetime.fromtimestamp(unixtime).strftime("%d%m%y")

        def random_digit():
            return choice("0123456789")

        def random_digits(num_digits):
            return "".join(random_digit() for _ in range(num_digits))

        def gen_cpr():
            return random_date() + random_digits(4)

        dynamic_person = {
            "uuid": str(uuid.uuid4()),
            "name": create_name(),
            "cpr": gen_cpr(),
            "employment_number": random_digits(4),
            "read_manager": True,
            "manager_name": create_name(),
            "manager_cpr": gen_cpr(),
        }
        return dynamic_person

    def _prepare_static_person(self):
        static_person = {
            "uuid": "7ccbd9aa-gd60-4fa1-4571-0e6f41f6ebc0",
            "name": ("Martin", "Lee", "Gore"),
            "employment_number": "101",
            "cpr": "1122334455",
            "read_manager": True,
            "manager_name": ("Daniel", "Miller"),
            "manager_cpr": "1122334455",
        }
        return static_person

    def _prepare_person(self, person_transformer=None, *args, **kwargs):
        if self.default_person is None:
            default_person = self._prepare_static_person()
            if self.generate_dynamic_person:
                default_person = self._prepare_dynamic_person()
            # Add computed fields
            sam_account_name = CreateUserNames(occupied_names=set()).create_username(
                list(default_person["name"])
            )[0]
            default_person.update(
                **{
                    "full_name": " ".join(default_person["name"]),
                    "sam_account_name": sam_account_name,
                    "manager_sam": default_person["manager_name"][0],
                    "manager_email": default_person["manager_name"][0]
                    + "@magenta.dk",
                }
            )
            # Add static fields
            default_person.update(
                **{
                    "end_date": "2089-11-11",
                    "title": "Musiker",
                    "unit": "Enhed",
                    "unit_uuid": "101bd9aa-0101-0101-0101-0e6f41f6ebc0",
                    "unit_user_key": "Musik",
                    "unit_postal_code": "8210",
                    "unit_city": "Aarhus N",
                    "unit_streetname": "Fahrenheit 451",
                    "location": "Kommune\\Forvalting\\Enhed\\",
                    "level2orgunit": "Ingen",
                    "forvaltning": "Beskæftigelse, Økonomi & Personale",
                }
            )
            transformer_func = person_transformer or self._no_transformation
            self.default_person = transformer_func(default_person, *args, **kwargs)
        return self.default_person

    def _prepare_mo_values(self, mo_values_transformer=None, *args, **kwargs):
        person = self._prepare_person()
        # Convert raw person data into mo_values data
        person["name"] = [" ".join(person["name"][:-1]), person["name"][-1]]
        person["manager_name"] = " ".join(person["manager_name"])

        #        if not read_manager:
        #            del person['manager_name']
        #            del person['manager_sam']
        #            del person['manager_email']
        #            del person['manager_cpr']
        #           person["read_manager"] = False

        transformer_func = mo_values_transformer or self._no_transformation
        return transformer_func(person, *args, **kwargs)

    def _prepare_get_from_ad(self, ad_transformer, *args, **kwargs):
        person = self._prepare_person()
        # Convert raw person data into ad_values data
        default_ad_person = {
            "ObjectGUID": person["uuid"],
            "SID": {
                "AccountDomainSid": {
                    "AccountDomainSid": "S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx",
                    "BinaryLength": 24,
                    "Value": "S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx",
                },
                "BinaryLength": 28,
                "Value": "S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx-xxxxx",
            },
            "PropertyCount": 11,
            "PropertyNames": [
                "ObjectGUID",
                "SID",
                "DistinguishedName",
                "Enabled",
                "GivenName",
                "Name",
                "ObjectClass",
                "SamAccountName",
                "Surname",
                "UserPrincipalName" "extensionAttribute1",
            ],
            "DistinguishedName": "CN="
            + person["full_name"]
            + ",OU="
            + person["unit"]
            + ",DC=lee",
            "Enabled": True,
            "GivenName": person["name"][:-1],
            "Name": person["full_name"],
            "ObjectClass": "user",
            "SamAccountName": person["sam_account_name"],
            "GivenName": person["name"][-1:],
            "UserPrincipalName": "_".join(person["full_name"]).lower()
            + "@magenta.dk",
            "extensionAttribute1": person["cpr"],
            "AddedProperties": [],
            "ModifiedProperties": [],
            "RemovedProperties": [],
        }
        transformer_func = ad_transformer or self._no_transformation
        return transformer_func(default_ad_person, *args, **kwargs)

    def _prepare_settings(self, settings_transformer=None):
        """Load default settings for AD tests.

        Args:
            settings_transformer: Function to transform settings.

        Returns:
            dict: Default settings after transformation.
        """
        default_settings = {
            "global": {},
            "mora.base": "http://example.org",
            "primary": {
                "search_base": "search_base",
                "system_user": "system_user",
                "password": "password",
                "properties": "dummy",
                "cpr_separator": "cpr_sep",
                "cpr_field": "cpr_field",
            },
            "primary_write": {
                "level2orgunit_field": "level2orgunit_field",
                "org_field": "org_field",
                "upn_end": "epn_end",
                "uuid_field": "uuid_field",
                "cpr_field": "cpr_field",
            },
            "integrations.ad.write.level2orgunit_type": "level2orgunit_type",
            "integrations.ad.cpr_separator": "ad_cpr_sep",
            "integrations.ad.ad_mo_sync_mapping": {},
            "address.visibility.public": "address_visibility_public_uuid",
            "address.visibility.internal": "address_visibility_internal_uuid",
            "address.visibility.secret": "address_visibility_secret_uuid",
        }
        transformer_func = settings_transformer or self._no_transformation
        return transformer_func(default_settings)


class TestADWriterMixin(TestADMixin):
    def _setup_adwriter(self, transform_settings=None, transform_mo_values=None):
        self.settings = self._prepare_settings(transform_settings)
        self.mo_values_func = partial(self._prepare_mo_values, transform_mo_values)
        self.ad_writer = ADWriterTestSubclass(
            all_settings=self.settings,
            read_ad_information_from_mo=self.mo_values_func,
        )


class AdMoSyncTestSubclass(AdMoSync):
    def __init__(
        self,
        mo_values_func,
        mo_e_username_func,
        ad_values_func,
        mo_seed_func,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.mo_values = mo_values_func()
        self.ad_values = ad_values_func()
        self.e_username = mo_e_username_func()

        self.mo_seed = mo_seed_func()
        self.mo_post_calls = []

    def _verify_it_systems(self):
        pass

    def _setup_mora_helper(self):
        def _mo_lookup(uuid, url):
            if url.startswith("e/{}/details/"):
                slash_index = url.rfind('/')
                parameters_index = url.rfind('?')
                if parameters_index == -1:
                    parameters_index = None
                lookup_type = url[slash_index+1:parameters_index]
                return self.mo_seed[lookup_type]
            elif url.startswith("o/{}/e?limit="):
                return {"items": [self.mo_values]}
            else:
                print("Outside mocking", url)
                raise NotImplemented

        def get_e_username(e_uuid, it_system):
            return self.e_username

        def _mo_post(url, payload, force=True):
            # Register the call, so we can test against it
            self.mo_post_calls.append(
                {"url": url, "payload": payload, "force": force}
            )
            # response.text --> "OK"
            return AttrDict({"text": "OK", "raise_for_status": lambda: None})

        return AttrDict(
            {
                "read_organisation": lambda: "org_uuid",
                "read_classes_in_facet": lambda x: [
                    [
                        {"uuid": "address_visibility_public_uuid"},
                        {"uuid": "address_visibility_internal_uuid"},
                        {"uuid": "address_visibility_secret_uuid"},
                    ]
                ],
                "get_e_username": get_e_username,
                "_mo_lookup": _mo_lookup,
                "_mo_post": _mo_post,
            }
        )

    def _setup_ad_reader_and_cache_all(self):
        def read_user(cpr, cache_only):
            # We only support one person in our mocking
            if cpr != self.mo_values["cpr"]:
                raise NotImplemented("Outside mocking")
            # If we got that one person, return it
            return self.ad_values

        self.ad_reader = AttrDict({"read_user": read_user,})


class TestADMoSyncMixin(TestADMixin):
    def _initialize_configuration(self):
        def ident(x, *args, **kwargs):
            return x

        self.settings = self._prepare_settings(ident)
        self.mo_values_func = partial(self._prepare_mo_values, ident)
        self.ad_values_func = partial(self._prepare_get_from_ad, ident)
        self.mo_e_username_func = lambda: ""
        self.mo_seed_func = lambda: {}

    def _setup_admosync(
        self,
        transform_settings=None,
        transform_mo_values=None,
        transform_ad_values=None,
        seed_e_username=None,
        seed_mo=None,
    ):
        if transform_settings:
            self.settings = self._prepare_settings(transform_settings)
        if transform_mo_values:
            self.mo_values_func = partial(
                self._prepare_mo_values, transform_mo_values
            )
        if transform_ad_values:
            self.ad_values_func = partial(
                self._prepare_get_from_ad, transform_ad_values
            )
        if seed_e_username:
            self.mo_e_username_func = seed_e_username
        if seed_mo:
            self.mo_seed_func = seed_mo
        self.ad_sync = AdMoSyncTestSubclass(
            all_settings=self.settings,
            mo_values_func=self.mo_values_func,
            ad_values_func=self.ad_values_func,
            mo_e_username_func=self.mo_e_username_func,
            mo_seed_func=self.mo_seed_func,
        )
