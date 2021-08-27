import datetime
import itertools
import json
import logging
import pickle
import time
from collections import defaultdict
from functools import lru_cache
from itertools import starmap
from operator import itemgetter
from typing import Tuple

import click
import dateutil.tz
import requests
from more_itertools import bucket
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_settings
from tqdm import tqdm

from integrations.dar_helper import dar_helper

logger = logging.getLogger("LoraCache")

DEFAULT_TIMEZONE = dateutil.tz.gettz("Europe/Copenhagen")

PICKLE_PROTOCOL = pickle.DEFAULT_PROTOCOL

LOG_LEVEL = logging.DEBUG
LOG_FILE = "lora_cache.log"


class LoraCache:
    def __init__(self, resolve_dar=True, full_history=False, skip_past=False):
        msg = "Start LoRa cache, resolve dar: {}, full_history: {}"
        logger.info(msg.format(resolve_dar, full_history))
        self.resolve_dar = resolve_dar

        self.settings = load_settings()

        self.additional = {"relationer": ("tilknyttedeorganisationer", "tilhoerer")}

        self.dar_map = defaultdict(list)

        self.full_history = full_history
        self.skip_past = skip_past
        self.org_uuid = self._read_org_uuid()

    @lru_cache(maxsize=None)
    def _get_mora_helper(self):
        mh = MoraHelper(hostname=self.settings["mora.base"], export_ansi=False)
        return mh

    def _validity_params(self, full_history: bool = None, skip_past: bool = None):
        return {}  # TODO
        if full_history is None:
            full_history = self.full_history
        if skip_past is None:
            skip_past = self.skip_past

        if full_history:
            if skip_past:
                return {
                    "validity": "future",
                }
            return {
                "at": "9999-12-31",
                "validity": "past",
            }
        return {
            "validity": "present",
        }

    def _read_org_uuid(self):
        mh = self._get_mora_helper()
        for attempt in range(0, 10):
            try:
                org_uuid = mh.read_organisation()
                return org_uuid
            except (
                json.decoder.JSONDecodeError,
                requests.exceptions.RequestException,
            ) as e:
                logger.error(e)
                print(e)
                time.sleep(5)
                continue
        # Unable to read org_uuid, must abort
        exit()

    @staticmethod
    def _format_optional_datetime_string(timestamp: str, fmt="%Y-%m-%d"):
        if timestamp is None:
            return None
        return datetime.datetime.fromisoformat(timestamp).strftime(fmt)

    def _cache_lora_facets(self):
        mh = self._get_mora_helper()
        facets = mh.read_facets()
        facet_tuples = map(itemgetter("uuid", "user_key"), facets)
        return {uuid: {"user_key": user_key} for uuid, user_key in facet_tuples}

    def _cache_lora_classes(self):
        mh = self._get_mora_helper()
        facets = mh.read_facets()
        classes = {}
        for facet in facets:
            facet_classes, facet_uuid = mh.read_classes_in_facet(facet["user_key"])
            for facet_class in facet_classes:
                classes[facet_class["uuid"]] = {
                    "user_key": facet_class["user_key"],
                    "title": facet_class["name"],
                    "scope": facet_class["scope"],
                    "facet": facet_uuid,
                }
        return classes

    def _cache_lora_itsystems(self):
        mh = self._get_mora_helper()
        return {
            it_system["uuid"]: {
                "user_key": it_system["user_key"],
                "name": it_system["name"],
            }
            for it_system in mh.read_it_systems()  # no v1 endpoint for this resource
        }

    def _cache_lora_users(self):
        mh = self._get_mora_helper()
        employees = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/employee",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "cpr": employee["cpr_no"],
                    "user_key": employee["user_key"],
                    "fornavn": employee["givenname"],
                    "efternavn": employee["surname"],
                    "navn": employee["name"],
                    "kaldenavn_fornavn": employee["nickname_givenname"],
                    "kaldenavn_efternavn": employee["nickname_surname"],
                    "kaldenavn": employee["nickname"],
                    "from_date": self._format_optional_datetime_string(
                        employee["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        employee["validity"]["to"]
                    ),
                }
                for employee in group
            ]
            for uuid, group in itertools.groupby(employees, key=lambda e: e["uuid"])
        }

    def _cache_lora_units(self):
        mh = self._get_mora_helper()
        units = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/org_unit",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user_key": unit["user_key"],
                    "name": unit["name"],
                    "unit_type": unit["org_unit_type"]["uuid"],
                    "level": (unit["org_unit_level"] or {}).get("uuid", None),
                    "parent": (unit["parent"] or {}).get("uuid", None),
                    "from_date": self._format_optional_datetime_string(
                        unit["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        unit["validity"]["to"]
                    ),
                }
                for unit in group
            ]
            for uuid, group in itertools.groupby(units, key=lambda u: u["uuid"])
        }

    def _cache_lora_address(self):
        mh = self._get_mora_helper()
        mo_addresses = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/address",
            params=self._validity_params(),
        )

        # The old LoRa Cache hardcoded the following scope translations:
        mo_to_lora_scope = {
            "EMAIL": "E-mail",
            "WWW": "Url",
            "PHONE": "Telefon",
            "PNUMBER": "P-nummer",
            "EAN": "EAN",
            "TEXT": "Text",
            "DAR": "DAR",
        }

        addresses = {}
        for mo_address in mo_addresses:
            uuid = mo_address["uuid"]
            scope = mo_address["address_type"]["scope"]
            address = {
                "uuid": uuid,
                "user": (mo_address["person"] or {}).get("uuid", None),
                "unit": (mo_address["org_unit"] or {}).get("uuid", None),
                "value": mo_address["value"],
                "scope": mo_to_lora_scope[scope],
                "dar_uuid": None,
                "adresse_type": mo_address["address_type"]["uuid"],
                "visibility": (mo_address["visibility"] or {}).get("uuid", None),
                "from_date": self._format_optional_datetime_string(
                    mo_address["validity"]["from"]
                ),
                "to_date": self._format_optional_datetime_string(
                    mo_address["validity"]["to"]
                ),
            }
            # DAR addresses are treated in a special way
            if scope == "DAR":
                address["dar_uuid"] = address["value"]
                address["value"] = None
            addresses[uuid] = [address]

        return addresses

    def _cache_lora_engagements(self):
        mh = self._get_mora_helper()
        engagements = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/engagement",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (engagement["person"] or {}).get("uuid", None),
                    "unit": (engagement["org_unit"] or {}).get("uuid", None),
                    "fraction": engagement["fraction"],
                    "user_key": engagement["user_key"],
                    "engagement_type": engagement["engagement_type"]["uuid"],
                    "primary_type": (engagement["primary"] or {}).get("uuid", None),
                    "job_function": engagement["job_function"]["uuid"],
                    "extensions": {
                        "udvidelse_1": engagement["extension_1"],
                        "udvidelse_2": engagement["extension_2"],
                        "udvidelse_3": engagement["extension_3"],
                        "udvidelse_4": engagement["extension_4"],
                        "udvidelse_5": engagement["extension_5"],
                        "udvidelse_6": engagement["extension_6"],
                        "udvidelse_7": engagement["extension_7"],
                        "udvidelse_8": engagement["extension_8"],
                        "udvidelse_9": engagement["extension_9"],
                        "udvidelse_10": engagement["extension_10"],
                    },
                    "from_date": self._format_optional_datetime_string(
                        engagement["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        engagement["validity"]["to"]
                    ),
                }
                for engagement in group
            ]
            for uuid, group in itertools.groupby(engagements, key=lambda e: e["uuid"])
        }

    def _cache_lora_associations(self):
        mh = self._get_mora_helper()
        associations = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/association",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (association["person"] or {}).get("uuid", None),
                    "unit": (association["org_unit"] or {}).get("uuid", None),
                    "user_key": association["user_key"],
                    "association_type": association["association_type"]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        association["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        association["validity"]["to"]
                    ),
                }
                for association in group
            ]
            for uuid, group in itertools.groupby(associations, key=lambda a: a["uuid"])
        }

    def _cache_lora_roles(self):
        mh = self._get_mora_helper()
        roles = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/role",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (role["person"] or {}).get("uuid", None),
                    "unit": (role["org_unit"] or {}).get("uuid", None),
                    "role_type": role["role_type"]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        role["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        role["validity"]["to"]
                    ),
                }
                for role in group
            ]
            for uuid, group in itertools.groupby(roles, key=lambda r: r["uuid"])
        }

    def _cache_lora_leaves(self):
        mh = self._get_mora_helper()
        leaves = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/leave",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (leave["person"] or {}).get("uuid", None),
                    "user_key": leave["user_key"],
                    "leave_type": leave["leave_type"]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        leave["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        leave["validity"]["to"]
                    ),
                }
                for leave in group
            ]
            for uuid, group in itertools.groupby(leaves, key=lambda l: l["uuid"])
        }

    def _cache_lora_it_connections(self):
        mh = self._get_mora_helper()
        it_connections = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/it",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (it_connection["person"] or {}).get("uuid", None),
                    "unit": (it_connection["org_unit"] or {}).get("uuid", None),
                    "username": it_connection["user_key"],
                    "itsystem": it_connection["itsystem"]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        it_connection["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        it_connection["validity"]["to"]
                    ),
                }
                for it_connection in group
            ]
            for uuid, group in itertools.groupby(
                it_connections, key=lambda i: i["uuid"]
            )
        }

    def _cache_lora_kles(self):
        mh = self._get_mora_helper()
        mo_kles = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/kle",
            params=self._validity_params(),
        )
        kles = defaultdict(list)
        for kle in mo_kles:
            uuid = kle["uuid"]
            for aspect in kle["kle_aspect"]:
                kles[uuid].append(
                    {
                        "uuid": uuid,
                        "unit": (kle["org_unit"] or {}).get("uuid", None),
                        "kle_number": kle["kle_number"]["uuid"],
                        "kle_aspect": aspect["uuid"],
                        "user_key": kle["user_key"],
                        "from_date": self._format_optional_datetime_string(
                            kle["validity"]["from"]
                        ),
                        "to_date": self._format_optional_datetime_string(
                            kle["validity"]["to"]
                        ),
                    }
                )
        return dict(kles)

    def _cache_lora_related(self):
        mh = self._get_mora_helper()
        related_units = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/related_unit",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "unit1_uuid": related_unit["org_unit"][0]["uuid"],
                    "unit2_uuid": related_unit["org_unit"][1]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        related_unit["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        related_unit["validity"]["to"]
                    ),
                }
                for related_unit in group
            ]
            for uuid, group in itertools.groupby(related_units, key=lambda r: r["uuid"])
        }

    def _cache_lora_managers(self):
        mh = self._get_mora_helper()
        managers = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/manager",
            params=self._validity_params(),
        )
        return {
            uuid: [
                {
                    "uuid": uuid,
                    "user": (manager["person"] or {}).get("uuid", None),
                    "unit": (manager["org_unit"] or {}).get("uuid", None),
                    "manager_type": manager["manager_type"]["uuid"],
                    "manager_level": manager["manager_level"]["uuid"],
                    "manager_responsibility": [
                        responsibility["uuid"]
                        for responsibility in manager["responsibility"]
                    ],
                    "from_date": self._format_optional_datetime_string(
                        manager["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        manager["validity"]["to"]
                    ),
                }
                for manager in group
            ]
            for uuid, group in itertools.groupby(managers, key=lambda m: m["uuid"])
        }

    def calculate_primary_engagements(self):
        if self.full_history:
            msg = """
            Calculation of primary engagements is currently not implemented for
            full historic export.
            """
            print(msg)
            return

        def extract_engagement(uuid, eng_validities):
            """Extract engagement from engagement validities."""
            assert (len(eng_validities)) == 1
            eng = eng_validities[0]
            return uuid, eng

        def convert_to_scope_value(uuid, engagement):
            """Convert engagement to scope value."""
            primary_type = self.classes.get(engagement["primary_type"])
            if primary_type is None:
                logger.debug(
                    "Primary information missing in engagement {}".format(uuid)
                )
                return uuid, 0
            primary_scope = int(primary_type["scope"])
            return uuid, primary_scope

        def get_engagement_user(tup):
            uuid, engagement = tup
            return engagement["user"]

        # List of 2-tuples: uuid, engagement validities
        engagement_validities = self.engagements.items()
        # Iterator of 2-tuples: uuid, engagement
        engagements = starmap(extract_engagement, engagement_validities)
        # Buckets of iterators of 2-tuples: uuid, engagement
        user_buckets = bucket(engagements, key=get_engagement_user)
        # Run though the user buckets in turn
        for user_uuid in user_buckets:
            # Iterator of 2-tuples: uuid, engagement
            user_engagements = user_buckets[user_uuid]
            # Iterator of 2-tuples: uuid, scope_value
            scope_values = list(starmap(convert_to_scope_value, user_engagements))

            # Find the highest scope in the users engagements, all engagements with
            # this scope value will be considered primary.
            highest_scope = max(map(itemgetter(1), scope_values))

            # Loop through all engagements and initially set them as non-primary.
            for uuid, primary_scope in scope_values:
                self.engagements[uuid][0]["primary_boolean"] = False

            # Loop through all engagements and mark the first engagement of the
            # highest scope as the primary engagement
            for uuid, primary_scope in scope_values:
                if primary_scope == highest_scope:
                    logger.debug("Primary for {} is {}".format(user_uuid, uuid))
                    self.engagements[uuid][0]["primary_boolean"] = True
                    break

    def calculate_derived_unit_data(self):
        if self.full_history:
            msg = """
            Calculation of derived unit data is currently not implemented for
            full historic export.
            """
            print(msg)
            return

        responsibility_class = self.settings.get(
            "exporters.actual_state.manager_responsibility_class", None
        )
        for unit, unit_validities in self.units.items():
            assert (len(unit_validities)) == 1
            unit_info = unit_validities[0]
            manager_uuid = None
            acting_manager_uuid = None

            def find_manager_for_org_unit(org_unit):
                def to_manager_info(manager_uuid, manager_validities):
                    assert (len(manager_validities)) == 1
                    manager_info = manager_validities[0]
                    return manager_uuid, manager_info

                def filter_invalid_managers(tup):
                    manager_uuid, manager_info = tup
                    # Wrong unit
                    if manager_info["unit"] != org_unit:
                        return False
                    # No resonsibility class
                    if responsibility_class is None:
                        return True
                    # Check responsability
                    return any(
                        resp == responsibility_class
                        for resp in manager_info["manager_responsibility"]
                    )

                managers = self.managers.items()
                managers = starmap(to_manager_info, managers)
                managers = filter(filter_invalid_managers, managers)
                managers = map(itemgetter(0), managers)
                return next(managers, None)

            # Find a direct manager, if possible
            manager_uuid = acting_manager_uuid = find_manager_for_org_unit(unit)

            location = ""
            current_unit = unit_info
            while current_unit:
                location = current_unit["name"] + "\\" + location
                current_parent = current_unit.get("parent")
                if current_parent is not None and current_parent in self.units:
                    current_unit = self.units[current_parent][0]
                else:
                    current_unit = None

                # Find the acting manager.
                if acting_manager_uuid is None:
                    acting_manager_uuid = find_manager_for_org_unit(current_parent)
            location = location[:-1]

            self.units[unit][0]["location"] = location
            self.units[unit][0]["manager_uuid"] = manager_uuid
            self.units[unit][0]["acting_manager_uuid"] = acting_manager_uuid

    def _cache_dar(self):
        # Initialize cache for entries we cannot lookup
        dar_uuids = self.dar_map.keys()
        dar_cache = dict(
            map(lambda dar_uuid: (dar_uuid, {"betegnelse": None}), dar_uuids)
        )
        total_dar = len(dar_uuids)
        total_missing = total_dar

        # Start looking entries up in DAR
        if self.resolve_dar:
            dar_addresses, missing = dar_helper.sync_dar_fetch(dar_uuids)
            dar_adgange, missing = dar_helper.sync_dar_fetch(
                list(missing), addrtype="adgangsadresser"
            )
            total_missing = len(missing)

            dar_cache.update(dar_addresses)
            dar_cache.update(dar_adgange)

        # Update all addresses with betegnelse
        for dar_uuid, uuid_list in self.dar_map.items():
            for uuid in uuid_list:
                for address in self.addresses[uuid]:
                    address["value"] = dar_cache[dar_uuid].get("betegnelse")

        logger.info("Total dar: {}, no-hit: {}".format(total_dar, total_missing))
        return dar_cache

    def populate_cache(self, dry_run=False, skip_associations=False):
        """
        Perform the actual data import.
        :param skip_associations: If associations are not needed, they can be
        skipped for increased performance.
        :param dry_run: For testing purposes it is possible to read from cache.
        """
        if self.full_history:
            facets_file = "tmp/facets_historic.p"
            classes_file = "tmp/classes_historic.p"
            users_file = "tmp/users_historic.p"
            units_file = "tmp/units_historic.p"
            addresses_file = "tmp/addresses_historic.p"
            engagements_file = "tmp/engagements_historic.p"
            managers_file = "tmp/managers_historic.p"
            associations_file = "tmp/associations_historic.p"
            leaves_file = "tmp/leaves_historic.p"
            roles_file = "tmp/roles_historic.p"
            itsystems_file = "tmp/itsystems_historic.p"
            it_connections_file = "tmp/it_connections_historic.p"
            kles_file = "tmp/kles_historic.p"
            related_file = "tmp/related_historic.p"
        else:
            facets_file = "tmp/facets.p"
            classes_file = "tmp/classes.p"
            users_file = "tmp/users.p"
            units_file = "tmp/units.p"
            addresses_file = "tmp/addresses.p"
            engagements_file = "tmp/engagements.p"
            managers_file = "tmp/managers.p"
            associations_file = "tmp/associations.p"
            leaves_file = "tmp/leaves.p"
            roles_file = "tmp/roles.p"
            itsystems_file = "tmp/itsystems.p"
            it_connections_file = "tmp/it_connections.p"
            kles_file = "tmp/kles.p"
            related_file = "tmp/related.p"

        if dry_run:
            logger.info("LoRa cache dry run - no actual read")
            with open(facets_file, "rb") as f:
                self.facets = pickle.load(f)
            with open(classes_file, "rb") as f:
                self.classes = pickle.load(f)
            with open(users_file, "rb") as f:
                self.users = pickle.load(f)
            with open(units_file, "rb") as f:
                self.units = pickle.load(f)
            with open(addresses_file, "rb") as f:
                self.addresses = pickle.load(f)
            with open(engagements_file, "rb") as f:
                self.engagements = pickle.load(f)
            with open(managers_file, "rb") as f:
                self.managers = pickle.load(f)

            if not skip_associations:
                with open(associations_file, "rb") as f:
                    self.associations = pickle.load(f)

            with open(leaves_file, "rb") as f:
                self.leaves = pickle.load(f)
            with open(roles_file, "rb") as f:
                self.roles = pickle.load(f)
            with open(itsystems_file, "rb") as f:
                self.itsystems = pickle.load(f)
            with open(it_connections_file, "rb") as f:
                self.it_connections = pickle.load(f)
            with open(kles_file, "rb") as f:
                self.kles = pickle.load(f)
            with open(related_file, "rb") as f:
                self.related = pickle.load(f)
            self.dar_cache = {}
            return

        t = time.time()
        msg = "Kørselstid: {:.1f}s, {} elementer, {:.0f}/s"

        # Here we should activate read-only mode
        def read_facets():
            logger.info("Læs facetter")
            self.facets = self._cache_lora_facets()
            return self.facets

        def read_classes():
            logger.info("Læs klasser")
            self.classes = self._cache_lora_classes()
            return self.classes

        def read_users():
            logger.info("Læs brugere")
            self.users = self._cache_lora_users()
            return self.users

        def read_units():
            logger.info("Læs enheder")
            self.units = self._cache_lora_units()
            return self.units

        def read_addresses():
            logger.info("Læs adresser:")
            self.addresses = self._cache_lora_address()
            return self.addresses

        def read_engagements():
            logger.info("Læs engagementer")
            self.engagements = self._cache_lora_engagements()
            return self.engagements

        def read_managers():
            logger.info("Læs ledere")
            self.managers = self._cache_lora_managers()
            return self.managers

        def read_associations():
            logger.info("Læs tilknytninger")
            self.associations = self._cache_lora_associations()
            return self.associations

        def read_leaves():
            logger.info("Læs orlover")
            self.leaves = self._cache_lora_leaves()
            return self.leaves

        def read_roles():
            logger.info("Læs roller")
            self.roles = self._cache_lora_roles()
            return self.roles

        def read_itsystems():
            logger.info("Læs itsystem")
            self.itsystems = self._cache_lora_itsystems()
            return self.itsystems

        def read_it_connections():
            logger.info("Læs it kobling")
            self.it_connections = self._cache_lora_it_connections()
            return self.it_connections

        def read_kles():
            logger.info("Læs kles")
            self.kles = self._cache_lora_kles()
            return self.kles

        def read_related():
            logger.info("Læs enhedssammenkobling")
            self.related = self._cache_lora_related()
            return self.related

        def read_dar():
            logger.info("Læs dar")
            self.dar_cache = self._cache_dar()
            # with open(cache_file, 'wb') as f:
            #    pickle.dump(self.dar_cache, f, pickle.HIGHEST_PROTOCOL)

        tasks = []
        tasks.append((read_facets, facets_file))
        tasks.append((read_classes, classes_file))
        tasks.append((read_users, users_file))
        tasks.append((read_units, units_file))
        tasks.append((read_addresses, addresses_file))
        tasks.append((read_engagements, engagements_file))
        tasks.append((read_managers, managers_file))
        if not skip_associations:
            tasks.append((read_associations, associations_file))
        tasks.append((read_leaves, leaves_file))
        tasks.append((read_roles, roles_file))
        tasks.append((read_itsystems, itsystems_file))
        tasks.append((read_it_connections, it_connections_file))
        tasks.append((read_kles, kles_file))
        tasks.append((read_related, related_file))
        tasks.append((read_dar, None))

        for task, filename in tqdm(tasks, desc="LoraCache", unit="task"):
            data = task()
            if filename:
                with open(filename, "wb") as f:
                    pickle.dump(data, f, PICKLE_PROTOCOL)

        # Here we should de-activate read-only mode


def fetch_loracache() -> Tuple[LoraCache, LoraCache]:
    # Here we should activate read-only mode, actual state and
    # full history dumps needs to be in sync.

    # Full history does not calculate derived data, we must
    # fetch both kinds.
    lc = LoraCache(resolve_dar=False, full_history=False)
    lc.populate_cache(dry_run=False, skip_associations=True)
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()

    # Todo, in principle it should be possible to run with skip_past True
    # This is now fixed in a different branch, remember to update when
    # merged.
    lc_historic = LoraCache(resolve_dar=False, full_history=True, skip_past=False)
    lc_historic.populate_cache(dry_run=False, skip_associations=True)
    # Here we should de-activate read-only mode
    return lc, lc_historic


@click.command()
@click.option("--historic/--no-historic", default=True, help="Do full historic export")
@click.option(
    "--resolve-dar/--no-resolve-dar", default=False, help="Resolve DAR addresses"
)
def cli(historic, resolve_dar):
    lc = LoraCache(full_history=historic, skip_past=True, resolve_dar=resolve_dar)
    lc.populate_cache(dry_run=False)

    logger.info("Now calcualate derived data")
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()


if __name__ == "__main__":

    for name in logging.root.manager.loggerDict:
        if name in ("LoraCache"):
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )

    cli()
