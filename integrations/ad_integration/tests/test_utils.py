from unittest import TestCase

import requests


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


from ad_writer import ADWriter
from user_names import CreateUserNames
from functools import partial
from utils import AttrDict, recursive_dict_update


def dict_modifier(updates):
    """Wrapper around recursive_dict_update, which provides updates beforehand.

    Example:

        updates = {'action': 'set'}

        function_with_expects_single_argument_transformer(
            dict_modifier(updates)
        )

    Args:
        updates: dictionary with updates to be applied later, when the returned
                 function is actually called.

    Returns:
        function: A partially applied recursive_dict_update function, waiting
                  for the original to apply updates to.
    """
    return partial(recursive_dict_update, updates=updates)


def mo_modifier(updates):
    """Wrapper around recursive_dict_update, which provides updates beforehand.

    Example:

        updates = {'action': 'set'}

        function_with_expects_mo_values_call(
            mo_modifier(updates)
        )

    Args:
        updates: dictionary with updates to be applied later, when the returned
                 function is actually called.

    Returns:
        function: A partially applied recursive_dict_update function, waiting
                  for the original to apply updates to.
    """
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
        self._find_unique_user = lambda cpr: read_ad_information_from_mo('')['sam_account_name']

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
        import uuid
        from tests.name_simulator import create_name

        def random_date():
            from datetime import datetime
            from random import randint
            import time
            unixtime = randint(1, int(time.time()))
            return datetime.fromtimestamp(unixtime).strftime('%d%m%y')

        def random_digit():
            from random import choice
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
                list(default_person['name'])
            )[0]
            default_person.update(**{
                'full_name': " ".join(default_person["name"]),
                "sam_account_name": sam_account_name,
                "manager_sam": default_person["manager_name"][0],
                "manager_email": default_person["manager_name"][0] + "@magenta.dk",
            })
            # Add static fields
            default_person.update(**{
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
            })
            transformer_func = person_transformer or self._no_transformation
            self.default_person = transformer_func(default_person, *args, **kwargs)
        return self.default_person

    def _prepare_mo_values(self, mo_values_transformer=None, *args, **kwargs):
        person = self._prepare_person()
        # Convert raw person data into mo_values data
        person['name'] = [" ".join(person['name'][:-1]), person['name'][-1]]
        person['manager_name'] = " ".join(person['manager_name'])

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
            'ObjectGUID': person['uuid'],
            'SID': {
                'AccountDomainSid': {
                    'AccountDomainSid': 'S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx',
                    'BinaryLength': 24,
                    'Value': 'S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx'
                },
                'BinaryLength': 28,
                'Value': 'S-x-x-xx-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx-xxxxx'
            },
            'PropertyCount': 11,
            'PropertyNames': [
                'ObjectGUID',
                'SID',
                'DistinguishedName',
                'Enabled',
                'GivenName',
                'Name',
                'ObjectClass',
                'SamAccountName',
                'Surname',
                'UserPrincipalName'
                'extensionAttribute1',
            ],
            'DistinguishedName': 'CN=' + person['full_name'] + ',OU=' + person['unit'] + ',DC=lee',
            'Enabled': True,
            'GivenName': person['name'][:-1],
            'Name': person["full_name"],
            'ObjectClass': 'user',
            'SamAccountName': person['sam_account_name'],
            'GivenName': person['name'][-1:],
            'UserPrincipalName': "_".join(person["full_name"]).lower() + '@magenta.dk',
            'extensionAttribute1': person["cpr"],
            'AddedProperties': [],
            'ModifiedProperties': [],
            'RemovedProperties': [],
        }
        transformer_func = person_transformer or self._no_transformation
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
        }
        transformer_func = settings_transformer or self._no_transformation
        return transformer_func(default_settings)


class TestADWriterMixin(TestADMixin):
    def _setup_adwriter(self, transform_settings=None, transform_mo_values=None):
        self.settings = self._prepare_settings(transform_settings)
        self.mo_values_func = partial(self._prepare_mo_values, transform_mo_values)
        self.ad_writer = ADWriterTestSubclass(
            all_settings=self.settings, read_ad_information_from_mo=self.mo_values_func
        )


