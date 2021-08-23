import json
import time
import pickle
import urllib
import logging
import pathlib
import datetime
import dateutil
import lora_utils
import requests
from operator import itemgetter
from itertools import starmap
from collections import defaultdict
from typing import Tuple
from tqdm import tqdm
from functools import lru_cache

import click
from more_itertools import bucket
from ra_utils.load_settings import load_settings

from retrying import retry
from os2mo_helpers.mora_helpers import MoraHelper
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

    def _get_effects(self, lora_object, relevant):
        effects = lora_utils.get_effects(
            lora_object["registreringer"][0],
            relevant=relevant,
            additional=self.additional,
        )
        # Notice, the code below will return the entire validity of an object
        # in the case of non-historic export, this could be handy in some
        # situations, eg ad->mo sync
        # if self.full_history:
        #     effects = lora_utils.get_effects(lora_object['registreringer'][0],
        #                                      relevant=relevant,
        #                                      additional=self.additional)
        # else:
        #     effects = lora_utils.get_effects(lora_object['registreringer'][0],
        #                                      relevant=self.additional,
        #                                      additional=relevant)
        return effects

    def _from_to_from_effect(self, effect):
        """
        Finds to and from date from an effect-row as returned by  iterating over the
        result of _get_effects().
        :param effect: The effect to analyse.
        :return: from_date and to_date. To date can be None, which should be
        interpreted as an infinite validity. In non-historic exports, both values
        can be None, meaning that this row is not the actual-state value.
        """
        dt_from = dateutil.parser.isoparse(str(effect[0]))
        dt_from = dt_from.astimezone(DEFAULT_TIMEZONE)
        from_date = dt_from.date().isoformat()

        if effect[1].replace(tzinfo=None) == datetime.datetime.max:
            to_date = None
        else:
            dt_to = dateutil.parser.isoparse(str(effect[1]))
            dt_to = dt_to.astimezone(DEFAULT_TIMEZONE)
            # MO considers end-dates inclusive, we need to subtract a day
            to_date = (dt_to.date() - datetime.timedelta(days=1)).isoformat()

        now = datetime.datetime.now(DEFAULT_TIMEZONE)
        # If this is an actual state export, we should only return a value if
        # the row is valid today.
        if not self.full_history:
            if to_date is None:
                # In this case, make sure dt_to is bigger than now
                dt_to = now + datetime.timedelta(days=1)
            if not dt_from < now < dt_to:
                from_date = to_date = None

        if self.skip_past:
            if to_date is None:
                # In this case, make sure dt_to is bigger than now
                dt_to = now + datetime.timedelta(days=1)
            if dt_to < now:
                from_date = to_date = None
        return from_date, to_date

    @retry(stop_max_attempt_number=7)
    def _perform_lora_lookup(self, url, params, skip_history=False, unit="it"):
        """
        Exctract a complete set of objects in LoRa.
        :param url: The url that should be used to extract data.
        :param skip_history: Force a validity of today, even if self.full_history
        is true.
        """
        t = time.time()
        logger.debug("Start reading {}, params: {}, at t={}".format(url, params, t))
        results_pr_request = 5000
        params["foersteresultat"] = 0

        # Default, this can be overwritten in the lines below
        now = datetime.datetime.today()
        params["virkningFra"] = now.strftime("%Y-%m-%d") + " 00:00:00"
        params["virkningTil"] = now.strftime("%Y-%m-%d") + " 00:00:01"
        if self.full_history and not skip_history:
            params["virkningTil"] = "infinity"
            if not self.skip_past:
                params["virkningFra"] = "-infinity"

        response = requests.get(self.settings["mox.base"] + url, params=params)
        data = response.json()
        total = len(data["results"][0])

        params["list"] = 1
        params["maximalantalresultater"] = results_pr_request

        complete_data = []

        with tqdm(total=total, desc="Fetching " + unit, unit=unit) as pbar:
            while True:
                response = requests.get(self.settings["mox.base"] + url, params=params)
                data = response.json()
                results = data["results"]
                data_list = []
                if results:
                    data_list = data["results"][0]
                pbar.update(len(data_list))
                complete_data = complete_data + data_list
                if len(data_list) == 0:
                    break
                params["foersteresultat"] += results_pr_request
                logger.debug(
                    "Mellemtid, {} læsninger: {}s".format(
                        params["foersteresultat"], time.time() - t
                    )
                )
        logger.debug(
            "LoRa læsning færdig. {} elementer, {}s".format(
                len(complete_data), time.time() - t
            )
        )
        return complete_data

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
        it_systems = (
            x["itsystem"]
            for x in mh._mo_get(
                self.settings["mora.base"] + "/api/v1/it",
                params=self._validity_params(),
            )
        )
        return {
            it_system["uuid"]: {
                "user_key": it_system["user_key"],
                "name": it_system["name"],
            }
            for it_system in it_systems
        }

    def _cache_lora_users(self):
        mh = self._get_mora_helper()
        employees = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/employee",
            params=self._validity_params(),
        )
        return {
            employee["uuid"]: [
                {
                    "uuid": employee["uuid"],
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
            ]
            for employee in employees
        }

    def _cache_lora_units(self):
        mh = self._get_mora_helper()
        units = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/org_unit",
            params=self._validity_params(),
        )
        return {
            unit["uuid"]: [
                {
                    "uuid": unit["uuid"],
                    "user_key": unit["user_key"],
                    "name": unit["name"],
                    "unit_type": unit["org_unit_type"]["uuid"],
                    "level": unit["org_unit_level"]["uuid"],
                    "parent": (unit["parent"] or {}).get(
                        "uuid", None
                    ),  # parent is optional
                    "from_date": self._format_optional_datetime_string(
                        unit["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        unit["validity"]["to"]
                    ),
                }
            ]
            for unit in units
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
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Engagement"}
        relevant = {
            "relationer": (
                "opgaver",
                "tilknyttedeenheder",
                "tilknyttedebrugere",
                "organisatoriskfunktionstype",
                "primær",
            ),
            "attributter": (
                "organisationfunktionegenskaber",
                "organisationfunktionudvidelser",
            ),
            "tilstande": ("organisationfunktiongyldighed",),
        }
        url = "/organisation/organisationfunktion"
        engagements = {}
        engagement_list = self._perform_lora_lookup(url, params, unit="engagement")
        for engagement in tqdm(
            engagement_list, desc="Processing engagement", unit="engagement"
        ):
            uuid = engagement["id"]

            effects = self._get_effects(engagement, relevant)
            engagement_effects = []
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                # Todo, this should be consistently implemented for all objects
                gyldighed = effect[2]["tilstande"]["organisationfunktiongyldighed"]
                if not gyldighed:
                    continue
                if not gyldighed[0]["gyldighed"] == "Aktiv":
                    continue

                attr = effect[2]["attributter"]
                rel = effect[2]["relationer"]

                if not rel["organisatoriskfunktionstype"]:
                    msg = "Missing in organisatoriskfunktionstype in {}"
                    logger.error(msg.format(engagement))
                    continue

                user_key = attr["organisationfunktionegenskaber"][0][
                    "brugervendtnoegle"
                ]

                engagement_type = rel["organisatoriskfunktionstype"][0]["uuid"]

                primary_type = None
                primær = rel.get("primær")
                if primær:
                    primary_type = primær[0]["uuid"]

                try:
                    job_function = rel["opgaver"][0]["uuid"]
                except:
                    continue

                user_uuid = rel["tilknyttedebrugere"][0]["uuid"]
                unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]

                udvidelser = {}
                udv_raw = attr.get("organisationfunktionudvidelser")
                if isinstance(udv_raw, list):
                    if len(udv_raw) == 1:
                        udvidelser = udv_raw[0]
                    if len(udv_raw) > 1:
                        msg = "Ugyldig organisationfunktionudvidelser: {}"
                        raise Exception(msg.format(udv_raw))
                fraction = udvidelser.get("fraktion")
                extensions = {
                    "udvidelse_1": udvidelser.get("udvidelse_1"),
                    "udvidelse_2": udvidelser.get("udvidelse_2"),
                    "udvidelse_3": udvidelser.get("udvidelse_3"),
                    "udvidelse_4": udvidelser.get("udvidelse_4"),
                    "udvidelse_5": udvidelser.get("udvidelse_5"),
                    "udvidelse_6": udvidelser.get("udvidelse_6"),
                    "udvidelse_7": udvidelser.get("udvidelse_7"),
                    "udvidelse_8": udvidelser.get("udvidelse_8"),
                    "udvidelse_9": udvidelser.get("udvidelse_9"),
                    "udvidelse_10": udvidelser.get("udvidelse_10"),
                }

                engagement_effects.append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "fraction": fraction,
                        "user_key": user_key,
                        "engagement_type": engagement_type,
                        "primary_type": primary_type,
                        "job_function": job_function,
                        "extensions": extensions,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
            if engagement_effects:
                engagements[uuid] = engagement_effects
        return engagements

    def _cache_lora_associations(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Tilknytning"}
        relevant = {
            "relationer": (
                "tilknyttedeenheder",
                "tilknyttedebrugere",
                "organisatoriskfunktionstype",
            ),
            "attributter": ("organisationfunktionegenskaber",),
        }
        url = "/organisation/organisationfunktion"
        associations = {}
        association_list = self._perform_lora_lookup(url, params, unit="association")
        for association in tqdm(
            association_list, desc="Processing association", unit="association"
        ):
            uuid = association["id"]
            associations[uuid] = []

            effects = self._get_effects(association, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                attr = effect[2]["attributter"]
                rel = effect[2]["relationer"]

                if rel["tilknyttedeenheder"]:
                    unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]
                else:
                    unit_uuid = None
                    logger.error("Error: Unable to find unit in {}".format(uuid))

                user_key = attr["organisationfunktionegenskaber"][0][
                    "brugervendtnoegle"
                ]
                association_type = rel["organisatoriskfunktionstype"][0]["uuid"]
                user_uuid = rel["tilknyttedebrugere"][0]["uuid"]

                associations[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "user_key": user_key,
                        "association_type": association_type,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return associations

    def _cache_lora_roles(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Rolle"}
        relevant = {
            "relationer": (
                "tilknyttedeenheder",
                "tilknyttedebrugere",
                "organisatoriskfunktionstype",
            )
        }
        url = "/organisation/organisationfunktion"
        roles = {}
        role_list = self._perform_lora_lookup(url, params, unit="role")
        for role in tqdm(role_list, desc="Processing role", unit="role"):
            uuid = role["id"]
            roles[uuid] = []

            effects = self._get_effects(role, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                rel = effect[2]["relationer"]
                role_type = rel["organisatoriskfunktionstype"][0]["uuid"]
                user_uuid = rel["tilknyttedebrugere"][0]["uuid"]
                unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]

                roles[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "role_type": role_type,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return roles

    def _cache_lora_leaves(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Orlov"}
        relevant = {
            "relationer": ("tilknyttedebrugere", "organisatoriskfunktionstype"),
            "attributter": ("organisationfunktionegenskaber",),
        }
        url = "/organisation/organisationfunktion"
        leaves = {}
        leave_list = self._perform_lora_lookup(url, params, unit="leave")
        for leave in tqdm(leave_list, desc="Processing leave", unit="leave"):
            uuid = leave["id"]
            leaves[uuid] = []
            effects = self._get_effects(leave, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                attr = effect[2]["attributter"]
                rel = effect[2]["relationer"]
                user_key = attr["organisationfunktionegenskaber"][0][
                    "brugervendtnoegle"
                ]
                leave_type = rel["organisatoriskfunktionstype"][0]["uuid"]
                user_uuid = rel["tilknyttedebrugere"][0]["uuid"]

                leaves[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "user_key": user_key,
                        "leave_type": leave_type,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return leaves

    def _format_optional_datetime_string(self, timestamp: str, fmt="%Y-%m-%d"):
        if timestamp is None:
            return None
        return datetime.datetime.fromisoformat(timestamp).strftime(fmt)

    def _cache_lora_it_connections(self):
        def construct_tuple(it_connection):
            return it_connection["uuid"], [
                {
                    "uuid": it_connection["uuid"],
                    "user": it_connection["person"]["uuid"],
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
            ]

        mh = self._get_mora_helper()
        it_connections = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/it",
            params=self._validity_params(),
        )
        return dict(map(construct_tuple, it_connections))

    def _cache_lora_kles(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "KLE"}
        url = "/organisation/organisationfunktion"
        kle_list = self._perform_lora_lookup(url, params, unit="KLE")
        kles = {}
        for kle in tqdm(kle_list, desc="Processing KLE", unit="KLE"):
            uuid = kle["id"]
            kles[uuid] = []

            relevant = {
                "relationer": (
                    "opgaver",
                    "tilknyttedeenheder",
                    "organisatoriskfunktionstype",
                ),
                "attributter": ("organisationfunktionegenskaber",),
            }

            effects = self._get_effects(kle, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                user_key = effect[2]["attributter"]["organisationfunktionegenskaber"][
                    0
                ]["brugervendtnoegle"]

                rel = effect[2]["relationer"]
                unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]
                kle_number = rel["organisatoriskfunktionstype"][0]["uuid"]

                for aspekt in rel["opgaver"]:
                    kle_aspect = aspekt["uuid"]
                    kles[uuid].append(
                        {
                            "uuid": uuid,
                            "unit": unit_uuid,
                            "kle_number": kle_number,
                            "kle_aspect": kle_aspect,
                            "user_key": user_key,
                            "from_date": from_date,
                            "to_date": to_date,
                        }
                    )
        return kles

    def _cache_lora_related(self):
        mh = self._get_mora_helper()
        related_units = mh._mo_get(
            self.settings["mora.base"] + "/api/v1/related_unit",
            params=self._validity_params(),
        )
        return {
            related_unit["uuid"]: [
                {
                    "uuid": related_unit["uuid"],
                    "unit1_uuid": related_unit["org_unit"][0]["uuid"],
                    "unit2_uuid": related_unit["org_unit"][1]["uuid"],
                    "from_date": self._format_optional_datetime_string(
                        related_unit["validity"]["from"]
                    ),
                    "to_date": self._format_optional_datetime_string(
                        related_unit["validity"]["to"]
                    ),
                }
            ]
            for related_unit in related_units
        }

    def _cache_lora_managers(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Leder"}
        url = "/organisation/organisationfunktion"
        manager_list = self._perform_lora_lookup(url, params, unit="manager")

        managers = {}
        for manager in tqdm(manager_list, desc="Processing manager", unit="manager"):
            uuid = manager["id"]
            managers[uuid] = []
            relevant = {
                "relationer": (
                    "opgaver",
                    "tilknyttedeenheder",
                    "tilknyttedebrugere",
                    "organisatoriskfunktionstype",
                )
            }

            if self.full_history:
                effects = lora_utils.get_effects(
                    manager["registreringer"][0],
                    relevant=relevant,
                    additional=self.additional,
                )
            else:
                effects = lora_utils.get_effects(
                    manager["registreringer"][0],
                    relevant=self.additional,
                    additional=relevant,
                )

            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                rel = effect[2]["relationer"]
                try:
                    user_uuid = rel["tilknyttedebrugere"][0]["uuid"]
                except:
                    user_uuid = None
                unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]
                manager_type = rel["organisatoriskfunktionstype"][0]["uuid"]
                manager_responsibility = []

                for opgave in rel["opgaver"]:
                    if opgave["objekttype"] == "lederniveau":
                        manager_level = opgave["uuid"]
                    if opgave["objekttype"] == "lederansvar":
                        manager_responsibility.append(opgave["uuid"])

                managers[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "manager_type": manager_type,
                        "manager_level": manager_level,
                        "manager_responsibility": manager_responsibility,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return managers

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
