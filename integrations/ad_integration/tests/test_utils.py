import time
import uuid
from datetime import datetime
from functools import partial
from itertools import chain
from random import choice
from random import randint
from unittest.mock import patch

from ..ad_sync import AdMoSync
from ..ad_writer import ADWriter
from ..read_ad_conf_settings import read_settings
from ..user_names import UserNameGen
from ..utils import AttrDict
from ..utils import recursive_dict_update
from .name_simulator import create_name


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

    def __init__(
        self,
        read_ad_information_from_mo,
        ad_values_func=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # List of scripts to be executed via run_ps
        self.scripts = []
        # Transformer for mo_values return
        self.read_ad_information_from_mo = read_ad_information_from_mo
        # Replace real `_find_ad_user` with mock
        if kwargs.get("mock_find_ad_user", True):
            self._find_ad_user = lambda ad_user, ad_dump=None: ad_values_func()

    def _init_name_creator(self):
        """Mocked to pretend no names are occupied.

        This method would normally use ADReader to read usernames from AD.
        """
        self.name_creator = UserNameGen.get_implementation()

    def _create_session(self):
        """Mocked to return a fake-class which writes scripts to self.scripts.

        This method would normally send scripts to powershell via WinRM.
        """

        def run_ps(ps_script):
            # Add our script to the list
            self.scripts.append(ps_script)
            # Fake the WinRM run_ps return type
            return AttrDict(
                {
                    "status_code": 0,
                    "std_out": b"",
                    "std_err": b"",
                }
            )

        # Fake the WinRM session object
        return AttrDict(
            {
                "run_ps": run_ps,
            }
        )

    def _get_retry_exceptions(self):
        """Mocked to return an empty list, i.e. never retry.

        This method would normally return the WinRM transport exception, to
        cause retrying to happen.
        """
        return []

    def read_ad_information_from_mo(self, uuid, read_manager=True, ad_dump=None):
        raise NotImplementedError("Should be overridden in __init__")

    def _read_ad_information_from_mo(self, uuid, read_manager=True, ad_dump=None):
        return super().read_ad_information_from_mo(uuid, read_manager, ad_dump)


def _no_transformation(default, *args, **kwargs):
    return default


class TestADMixin(object):
    # Useful for local testing
    generate_dynamic_person = True
    default_person = None

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
            "nickname": create_name(),
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
            "nickname": ("Depeche", "Mode"),
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
            creator = UserNameGen.get_implementation()
            sam_account_name = creator.create_username(list(default_person["name"]))
            default_person.update(
                **{
                    "full_name": " ".join(default_person["name"]),
                    "sam_account_name": sam_account_name,
                    "manager_sam": default_person["manager_name"][0],
                    "manager_email": default_person["manager_name"][0] + "@magenta.dk",
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
            transformer_func = person_transformer or _no_transformation
            self.default_person = transformer_func(default_person, *args, **kwargs)
        return self.default_person

    def _prepare_mo_values(self, mo_values_transformer=None, *args, **kwargs):
        person = self._prepare_person()
        # Convert raw person data into mo_values data
        person["name"] = [" ".join(person["name"][:-1]), person["name"][-1]]
        person["nickname"] = [
            " ".join(person["nickname"][:-1]),
            person["nickname"][-1],
        ]
        person["manager_name"] = " ".join(person["manager_name"])

        #        if not read_manager:
        #            del person['manager_name']
        #            del person['manager_sam']
        #            del person['manager_email']
        #            del person['manager_cpr']
        #           person["read_manager"] = False

        transformer_func = mo_values_transformer or _no_transformation
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
                "UserPrincipalName",
                "extensionAttribute1",
            ],
            "DistinguishedName": "CN="
            + person["full_name"]
            + ",OU="
            + person["unit"]
            + ",DC=lee",
            "Enabled": True,
            "Name": person["full_name"],
            "ObjectClass": "user",
            "SamAccountName": person["sam_account_name"],
            "GivenName": person["name"][-1],
            "UserPrincipalName": "_".join(person["full_name"]).lower() + "@magenta.dk",
            "extensionAttribute1": person["cpr"],
            "AddedProperties": [],
            "ModifiedProperties": [],
            "RemovedProperties": [],
        }
        transformer_func = ad_transformer or _no_transformation
        return transformer_func(default_ad_person, *args, **kwargs)

    def _prepare_settings(self, early_settings_transformer=None):
        """Load default settings for AD tests.

        Args:
            early_settings_transformer: Function to transform settings.

        Returns:
            dict: Default settings after transformation.
        """
        default_settings = {
            "integrations.ad": [
                {
                    "cpr_field": "cpr_field",
                    "cpr_separator": "ad_cpr_sep",
                    "system_user": "system_user",
                    "password": "password",
                    "properties": [],
                    "search_base": "search_base",
                    "integrations.ad.ad_mo_sync_mapping": {},
                    "ad_mo_sync_terminate_missing": False,
                    "ad_mo_sync_terminate_missing_require_itsystem": True,
                    "ad_mo_sync_terminate_disabled": True,
                    "ad_mo_sync_pre_filters": [],
                    "ad_mo_sync_terminate_disabled_filters": [],
                    "servers": ["server123"],
                }
            ],
            "integrations.ad.winrm_host": "dummy",
            # "integrations.ad.sam_filter": "sam_filter",
            "mora.base": "http://example.org",
            "integrations.ad.write.uuid_field": "uuid_field",
            "integrations.ad.write.level2orgunit_field": "level2orgunit_field",
            "integrations.ad.write.org_unit_field": "org_field",
            "integrations.ad.write.upn_end": "epn_end",
            "integrations.ad.write.level2orgunit_type": "level2orgunit_type",
            "integrations.ad_writer.template_to_ad_fields": {
                "Name": "{{ mo_values['full_name'] }} - {{ user_sam }}",
                "Displayname": "{{ mo_values['name'][0] }} {{ mo_values['name'][1] }}",
                "GivenName": "{{ mo_values['name'][0] }}",
                "SurName": "{{ mo_values['name'][1] }}",
                "EmployeeNumber": "{{ mo_values['employment_number'] }}",
            },
            "address.visibility.public": "address_visibility_public_uuid",
            "address.visibility.internal": "address_visibility_internal_uuid",
            "address.visibility.secret": "address_visibility_secret_uuid",
        }
        transformer_func = early_settings_transformer or _no_transformation
        modified_settings = transformer_func(default_settings)
        for ad_settings in modified_settings["integrations.ad"]:
            ad_settings["properties"] = list(
                map(
                    lambda x: x.lower(),
                    chain(
                        modified_settings.get(
                            "integrations.ad_writer.template_to_ad_fields", {}
                        ).keys(),
                        modified_settings.get(
                            "integrations.ad_writer.mo_to_ad_fields", {}
                        ).values(),
                        [modified_settings["integrations.ad.write.org_unit_field"]],
                        [
                            modified_settings[
                                "integrations.ad.write.level2orgunit_field"
                            ]
                        ],
                        [modified_settings["integrations.ad.write.uuid_field"]],
                    ),
                )
            )
        return modified_settings


class TestADWriterMixin(TestADMixin):
    def _setup_adwriter(
        self,
        late_transform_settings=None,
        transform_mo_values=None,
        early_transform_settings=None,
        transform_ad_values=lambda x: x,
        **kwargs,
    ):
        transformer_func = late_transform_settings or _no_transformation
        self.settings = transformer_func(
            read_settings(self._prepare_settings(early_transform_settings))
        )
        self.mo_values_func = partial(self._prepare_mo_values, transform_mo_values)
        self.ad_values_func = partial(self._prepare_get_from_ad, transform_ad_values)

        # Avoid circular imports
        from .mocks import MockEmptyADReader
        from .mocks import MockMOGraphqlSource

        with patch(
            "integrations.ad_integration.ad_writer.MOGraphqlSource",
            new=MockMOGraphqlSource,
        ):
            with patch(
                "integrations.ad_integration.ad_writer.ADParameterReader",
                kwargs.get("mock_ad_reader_class", MockEmptyADReader),
            ):
                self.ad_writer = ADWriterTestSubclass(
                    all_settings=self.settings,
                    read_ad_information_from_mo=self.mo_values_func,
                    ad_values_func=self.ad_values_func,
                    **kwargs,
                )


class AdMoSyncTestSubclass(AdMoSync):
    def __init__(
        self,
        mo_values_func,
        mo_e_username_func,
        ad_values_func,
        mo_seed_func,
        *args,
        **kwargs,
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
        def get_e_details(detail_type):
            return lambda *args, **kwargs: self.mo_seed[detail_type]

        def _mo_post(url, payload, force=True):
            # Register the call, so we can test against it
            self.mo_post_calls.append({"url": url, "payload": payload, "force": force})
            # response.text --> "OK"
            return AttrDict({"text": "OK", "raise_for_status": lambda: None})

        def read_user(uuid):
            return self.mo_values

        def update_user(uuid, data):
            payload = {"type": "employee", "uuid": uuid, "data": data}
            return _mo_post("details/edit", payload)

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
                "read_user": read_user,
                "read_all_users": lambda: [self.mo_values],
                "read_user_engagement": get_e_details("engagement"),
                "get_e_addresses": get_e_details("address"),
                "get_e_itsystems": get_e_details("it"),
                "update_user": update_user,
                "_mo_post": _mo_post,
            }
        )

    def _setup_ad_reader_and_cache_all(self, index, cache_all=True):
        def read_user(cpr, cache_only):
            # We only support one person in our mocking
            if cpr != self.mo_values["cpr"]:
                raise NotImplementedError("Outside mocking")
            # If we got that one person, return it
            return self.ad_values

        def get_settings():
            return self.settings["integrations.ad"][0]

        ad_reader = AttrDict(
            {
                "_get_setting": get_settings,
                "read_user": read_user,
            }
        )
        return ad_reader


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
            self.mo_values_func = partial(self._prepare_mo_values, transform_mo_values)
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
