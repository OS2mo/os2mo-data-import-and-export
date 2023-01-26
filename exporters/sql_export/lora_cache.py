import datetime
import json
import logging
import os
import pickle
import re
import time
import urllib
from collections import defaultdict
from itertools import starmap
from operator import itemgetter
from pathlib import Path
from typing import Dict
from typing import Optional
from typing import Set
from typing import Tuple
from uuid import UUID

import click
import lora_utils
import requests
from dateutil import parser
from dateutil import tz
from more_itertools import bucket
from os2mo_dar_client import DARClient
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm
from retrying import retry


logger = logging.getLogger("LoraCache")

DEFAULT_TIMEZONE = tz.gettz("Europe/Copenhagen")

PICKLE_PROTOCOL = pickle.DEFAULT_PROTOCOL

LOG_LEVEL = logging.DEBUG
LOG_FILE = "lora_cache.log"


def get_rel_uuid_or_none(uuid, rel, item_name) -> Optional[str]:
    """Read uuid from rel. Log if it doesn't exist"""
    try:
        return rel[item_name][0]["uuid"]
    except IndexError:
        logger.error(f"Empty rel['{item_name}'] ({uuid=}), was {rel}")
    except KeyError:
        logger.debug(f"No {item_name} found for {uuid=}, was {rel}")
    return None


class LoraCache:
    def __init__(
        self, resolve_dar=True, full_history=False, skip_past=False, settings=None
    ):
        msg = "Start LoRa cache, resolve dar: {}, full_history: {}"
        logger.info(msg.format(resolve_dar, full_history))
        self.resolve_dar = resolve_dar

        self.settings = settings or self._load_settings()

        self.additional = {"relationer": ("tilknyttedeorganisationer", "tilhoerer")}

        self.dar_map = defaultdict(list)

        self.full_history = full_history
        self.skip_past = skip_past
        self.org_uuid = self._read_org_uuid()

    def _load_settings(self):
        return load_settings()

    def _read_org_uuid(self):
        mh = MoraHelper(hostname=self.settings["mora.base"], export_ansi=False)
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
        >>> lc=LoraCache(resolve_dar=False)
        >>> lc._from_to_from_effect([datetime.datetime(2020, 1, 1), datetime.datetime(2022, 1, 2)])
        ('2020-01-01', '2022-01-01')
        """

        dt_from = parser.isoparse(str(effect[0]))
        dt_from = dt_from.astimezone(DEFAULT_TIMEZONE)
        from_date = dt_from.date().isoformat()

        if effect[1].replace(tzinfo=None) == datetime.datetime.max:
            to_date = None
        else:
            dt_to = parser.isoparse(str(effect[1]))
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
            if not dt_from <= now <= dt_to:
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
        # Copy to avoid modifying on retry
        params = params.copy()

        t = time.time()
        logger.debug("Start reading {}, params: {}, at t={}".format(url, params, t))
        results_pr_request = 1000
        params["foersteresultat"] = 0
        params["konsolider"] = True

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

    def _get_primary_class_as_boolean(self, uuid: str, rel: dict) -> Optional[bool]:
        primary_boolean = None
        primary_class_uuid = get_rel_uuid_or_none(uuid, rel, "primær")
        primary_class = self.classes.get(primary_class_uuid)
        if primary_class:
            primary_boolean = primary_class.get("user_key", "") == "primary"
        return primary_boolean

    def _cache_lora_facets(self):
        # Facets are eternal i MO and does not need a historic dump
        params = {"bvn": "%"}
        url = "/klassifikation/facet"
        facet_list = self._perform_lora_lookup(
            url, params, skip_history=True, unit="facet"
        )

        facets = {}
        for facet in tqdm(facet_list, desc="Processing facet", unit="facet"):
            uuid = facet["id"]
            reg = facet["registreringer"][0]
            user_key = reg["attributter"]["facetegenskaber"][0]["brugervendtnoegle"]
            facets[uuid] = {
                "user_key": user_key,
            }
        return facets

    def _cache_lora_classes(self):
        # MO itself will not read historic information on classes,
        # currently we replicate this behaviour here.
        params = {"bvn": "%"}
        url = "/klassifikation/klasse"
        class_list = self._perform_lora_lookup(
            url, params, skip_history=True, unit="class"
        )

        classes = {}
        for oio_class in tqdm(class_list, desc="Processing class", unit="class"):
            uuid = oio_class["id"]
            reg = oio_class["registreringer"][0]
            user_key = reg["attributter"]["klasseegenskaber"][0]["brugervendtnoegle"]
            scope = reg["attributter"]["klasseegenskaber"][0].get("omfang")
            title = reg["attributter"]["klasseegenskaber"][0]["titel"]
            facet = reg["relationer"]["facet"][0]["uuid"]
            classes[uuid] = {
                "user_key": user_key,
                "title": title,
                "scope": scope,
                "facet": facet,
            }
        return classes

    def _cache_lora_itsystems(self):
        # IT-systems are eternal i MO and does not need a historic dump
        params = {"bvn": "%"}
        url = "/organisation/itsystem"
        itsystem_list = self._perform_lora_lookup(
            url, params, skip_history=True, unit="itsystem"
        )

        itsystems = {}
        for itsystem in tqdm(
            itsystem_list, desc="Processing itsystem", unit="itsystem"
        ):
            uuid = itsystem["id"]
            reg = itsystem["registreringer"][0]
            user_key = reg["attributter"]["itsystemegenskaber"][0]["brugervendtnoegle"]
            name = reg["attributter"]["itsystemegenskaber"][0]["itsystemnavn"]

            itsystems[uuid] = {
                "user_key": user_key,
                "name": name,
            }
        return itsystems

    def _cache_lora_users(self):
        params = {"bvn": "%"}
        url = "/organisation/bruger"
        user_list = self._perform_lora_lookup(url, params, unit="user")

        relevant = {
            "attributter": ("brugeregenskaber", "brugerudvidelser"),
            "relationer": ("tilknyttedepersoner", "tilhoerer"),
            "tilstande": ("brugergyldighed",),
        }

        users = {}
        for user in tqdm(user_list, desc="Processing user", unit="user"):
            uuid = user["id"]
            users[uuid] = []

            effects = list(self._get_effects(user, relevant))
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                reg = effect[2]

                cpr = None
                tilknyttedepersoner = reg["relationer"].get("tilknyttedepersoner", [])
                if len(tilknyttedepersoner) == 0:
                    logger.warning("unable to find CPR for LoRa user %r", uuid)

                else:
                    cpr = tilknyttedepersoner[0]["urn"][-10:]

                egenskaber = reg["attributter"]["brugeregenskaber"]
                if len(egenskaber) == 0:
                    continue
                egenskaber = egenskaber[0]

                udv = reg["attributter"]["brugerudvidelser"]
                if len(udv) == 0:
                    continue
                udv = udv[0]

                user_key = egenskaber.get("brugervendtnoegle", "")
                fornavn = udv.get("fornavn", "")
                efternavn = udv.get("efternavn", "")
                kaldenavn_fornavn = udv.get("kaldenavn_fornavn", "")
                kaldenavn_efternavn = udv.get("kaldenavn_efternavn", "")
                users[uuid].append(
                    {
                        "uuid": uuid,
                        "cpr": cpr,
                        "user_key": user_key,
                        "fornavn": fornavn,
                        "efternavn": efternavn,
                        "navn": " ".join([fornavn, efternavn]).strip(),
                        "kaldenavn_fornavn": kaldenavn_fornavn,
                        "kaldenavn_efternavn": kaldenavn_efternavn,
                        "kaldenavn": " ".join(
                            [kaldenavn_fornavn, kaldenavn_efternavn]
                        ).strip(),
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return users

    def _cache_lora_units(self):
        params = {"bvn": "%"}
        skip_history = False
        if not self.full_history:
            params["gyldighed"] = "Aktiv"
            skip_history = True
        url = "/organisation/organisationenhed"
        relevant = {
            "relationer": ("overordnet", "enhedstype", "niveau", "opmærkning"),
            "attributter": ("organisationenhedegenskaber",),
        }

        unit_list = self._perform_lora_lookup(
            url, params, skip_history=skip_history, unit="unit"
        )

        units = {}
        for unit in tqdm(unit_list, desc="Processing unit", unit="unit"):
            uuid = unit["id"]
            units[uuid] = []

            effects = self._get_effects(unit, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                relationer = effect[2]["relationer"]

                orgegenskaber = effect[2]["attributter"]["organisationenhedegenskaber"]
                if len(orgegenskaber) == 0:
                    continue

                egenskaber = orgegenskaber[0]

                parent_raw = relationer["overordnet"][0]["uuid"]
                if parent_raw == self.org_uuid:
                    parent = None
                else:
                    parent = parent_raw

                if "niveau" in relationer and len(relationer["niveau"]) > 0:
                    level = relationer["niveau"][0]["uuid"]
                else:
                    level = None

                unit_type = relationer["enhedstype"][0]["uuid"]
                org_unit_hierarchy = get_rel_uuid_or_none(
                    uuid, relationer, "opmærkning"
                )

                units[uuid].append(
                    {
                        "uuid": uuid,
                        "user_key": egenskaber["brugervendtnoegle"],
                        "name": egenskaber["enhedsnavn"],
                        "unit_type": unit_type,  # class uuid
                        "level": level,  # class uuid
                        "parent": parent,  # org unit uuid
                        "org_unit_hierarchy": org_unit_hierarchy,  # class uuid
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )

        return units

    def _cache_lora_address(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Adresse"}

        url = "/organisation/organisationfunktion"
        relevant = {
            "relationer": (
                "tilknyttedeenheder",
                "tilknyttedebrugere",
                "adresser",
                "organisatoriskfunktionstype",
                "opgaver",
            ),
            "attributter": ("organisationfunktionegenskaber",),
        }
        address_list = self._perform_lora_lookup(url, params, unit="address")

        addresses = {}
        for address in tqdm(address_list, desc="Processing address", unit="address"):
            uuid = address["id"]
            addresses[uuid] = []

            effects = self._get_effects(address, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                relationer = effect[2]["relationer"]

                if (
                    "tilknyttedeenheder" in relationer
                    and len(relationer["tilknyttedeenheder"]) > 0
                ):
                    unit_uuid = relationer["tilknyttedeenheder"][0]["uuid"]
                    user_uuid = None
                elif (
                    "tilknyttedebrugere" in relationer
                    and len(relationer["tilknyttedebrugere"]) > 0
                ):
                    user_uuid = relationer["tilknyttedebrugere"][0]["uuid"]
                    unit_uuid = None
                else:
                    # Skip if address is not attached to anything
                    continue

                dar_uuid = None
                value_raw = relationer["adresser"][0]["urn"]
                address_type = relationer["adresser"][0]["objekttype"]
                if address_type == "EMAIL":
                    scope = "E-mail"
                    skip_len = len("urn:mailto:")
                    value = value_raw[skip_len:]
                elif address_type == "WWW":
                    scope = "Url"
                    skip_len = len("urn:magenta.dk:www:")
                    value = value_raw[skip_len:]
                elif address_type == "PHONE":
                    scope = "Telefon"
                    skip_len = len("urn:magenta.dk:telefon:")
                    value = value_raw[skip_len:]
                elif address_type == "PNUMBER":
                    scope = "P-nummer"
                    skip_len = len("urn:dk:cvr:produktionsenhed:")
                    value = value_raw[skip_len:]
                elif address_type == "EAN":
                    scope = "EAN"
                    skip_len = len("urn:magenta.dk:ean:")
                    value = value_raw[skip_len:]
                elif address_type == "TEXT":
                    scope = "Text"
                    skip_len = len("urn:text:")
                    value = urllib.parse.unquote(value_raw[skip_len:])
                elif address_type == "MULTIFIELD_TEXT":
                    # This address type has more than one field
                    value_raw1 = relationer["adresser"][1]["urn"]
                    scope = "Multifield_text"
                    r1 = re.compile("urn:multifield_text:(.*)")
                    r2 = re.compile("urn:multifield_text2:(.*)")
                    # Ensure correct order so that "text" is before "text2"
                    value1 = r1.match(value_raw) or r1.match(value_raw1)
                    value2 = r2.match(value_raw) or r2.match(value_raw1)
                    # Both fields are put into one field in loracache as they are shown in MO
                    value = f"{value1.group(1)} :: {value2.group(1)}"
                    value = urllib.parse.unquote(value)
                elif address_type == "DAR":
                    scope = "DAR"
                    skip_len = len("urn:dar:")
                    dar_uuid = value_raw[skip_len:]
                    value = None

                    if self.dar_map is not None:
                        self.dar_map[dar_uuid].append(uuid)
                else:
                    print("Ny type: {}".format(address_type))
                    msg = "Unknown addresse type: {}, value: {}"
                    logger.error(msg.format(address_type, value_raw))
                    raise ("Unknown address type: {}".format(address_type))

                address_type_class = relationer["organisatoriskfunktionstype"][0][
                    "uuid"
                ]

                synlighed = None
                if relationer.get("opgaver"):
                    if relationer["opgaver"][0]["objekttype"] == "synlighed":
                        synlighed = relationer["opgaver"][0]["uuid"]

                addresses[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "value": value,
                        "scope": scope,
                        "dar_uuid": dar_uuid,
                        "adresse_type": address_type_class,
                        "visibility": synlighed,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
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
                except Exception:
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
                "tilknyttedefunktioner",
                "tilknyttedeitsystemer",
                "primær",
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

                try:
                    user_key = attr["organisationfunktionegenskaber"][0][
                        "brugervendtnoegle"
                    ]
                except (IndexError, KeyError):
                    logger.warning(f"missing userkey for association {uuid}")
                    continue

                association_type_uuid = get_rel_uuid_or_none(
                    uuid, rel, "organisatoriskfunktionstype"
                )
                user_uuid = get_rel_uuid_or_none(uuid, rel, "tilknyttedebrugere")
                it_user_uuid = get_rel_uuid_or_none(uuid, rel, "tilknyttedeitsystemer")
                job_function_uuid = (
                    get_rel_uuid_or_none(uuid, rel, "tilknyttedefunktioner")
                    if it_user_uuid
                    else None
                )

                primary_boolean = self._get_primary_class_as_boolean(uuid, rel)

                associations[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "user_key": user_key,
                        "association_type": association_type_uuid,
                        "it_user": it_user_uuid,
                        "job_function": job_function_uuid,
                        "from_date": from_date,
                        "to_date": to_date,
                        "primary_boolean": primary_boolean,
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
            "relationer": (
                "tilknyttedebrugere",
                "organisatoriskfunktionstype",
                "tilknyttedefunktioner",
            ),
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

                if "tilknyttedefunktioner" in rel:
                    engagement_uuid = rel["tilknyttedefunktioner"][0]["uuid"]
                else:
                    engagement_uuid = None

                leaves[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "user_key": user_key,
                        "leave_type": leave_type,
                        "engagement": engagement_uuid,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return leaves

    def _cache_lora_it_connections(self):
        params = {"gyldighed": "Aktiv", "funktionsnavn": "IT-system"}
        url = "/organisation/organisationfunktion"
        it_connection_list = self._perform_lora_lookup(
            url, params, unit="it connection"
        )

        it_connections = {}
        for it_connection in tqdm(
            it_connection_list, desc="Processing it connection", unit="it connection"
        ):
            uuid = it_connection["id"]
            it_connections[uuid] = []

            relevant = {
                "relationer": (
                    "tilknyttedeenheder",
                    "tilknyttedebrugere",
                    "tilknyttedeitsystemer",
                    "primær",
                ),
                "attributter": ("organisationfunktionegenskaber",),
            }

            effects = self._get_effects(it_connection, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue
                user_key = effect[2]["attributter"]["organisationfunktionegenskaber"][
                    0
                ]["brugervendtnoegle"]

                rel = effect[2]["relationer"]
                itsystem = rel["tilknyttedeitsystemer"][0]["uuid"]

                if "tilknyttedeenheder" in rel:
                    unit_uuid = rel["tilknyttedeenheder"][0]["uuid"]
                    user_uuid = None
                else:
                    user_uuid = rel["tilknyttedebrugere"][0]["uuid"]
                    unit_uuid = None

                primary_boolean = self._get_primary_class_as_boolean(uuid, rel)

                it_connections[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "username": user_key,
                        "itsystem": itsystem,
                        "primary_boolean": primary_boolean,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return it_connections

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
        params = {"gyldighed": "Aktiv", "funktionsnavn": "Relateret Enhed"}
        url = "/organisation/organisationfunktion"
        related_list = self._perform_lora_lookup(url, params, unit="related")
        related = {}
        for relate in tqdm(related_list, desc="Processing related", unit="related"):
            uuid = relate["id"]
            related[uuid] = []

            relevant = {"relationer": ("tilknyttedeenheder",), "attributter": ()}

            effects = self._get_effects(relate, relevant)
            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                if from_date is None and to_date is None:
                    continue

                rel = effect[2]["relationer"]
                unit1_uuid = rel["tilknyttedeenheder"][0]["uuid"]
                unit2_uuid = rel["tilknyttedeenheder"][1]["uuid"]

                related[uuid].append(
                    {
                        "uuid": uuid,
                        "unit1_uuid": unit1_uuid,
                        "unit2_uuid": unit2_uuid,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return related

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
                # Yes, `relevant` and `additional` *appear to be* switched
                # around by mistake in this method call! However, the code
                # seems to be deliberate - the data in `effects` cannot be
                # processed by `calculate_derived_unit_data` if the code is
                # changed to `relevant=relevant, additional=self.additional`.
                # TODO: Add unittest which captures this surprising behavior.
                effects = lora_utils.get_effects(
                    manager["registreringer"][0],
                    relevant=self.additional,
                    additional=relevant,
                )

            for effect in effects:
                from_date, to_date = self._from_to_from_effect(effect)
                rel = effect[2]["relationer"]

                user_uuid = get_rel_uuid_or_none(uuid, rel, "tilknyttedebrugere")
                unit_uuid = get_rel_uuid_or_none(uuid, rel, "tilknyttedeenheder")
                manager_type = get_rel_uuid_or_none(
                    uuid, rel, "organisatoriskfunktionstype"
                )

                manager_level = None  # populated in loop over "opgaver" below
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

    def _read_from_dar(self, uuids: Set[UUID]) -> Tuple[Dict, Set[UUID]]:
        with DARClient() as dc:
            return dc.fetch(uuids)

    def _cache_dar(self):
        # Initialize cache for entries we cannot lookup
        dar_uuids = missing = set(self.dar_map.keys())
        dar_cache = dict(
            map(lambda dar_uuid: (dar_uuid, {"betegnelse": None}), dar_uuids)
        )
        if self.resolve_dar:
            dar_hits, missing = self._read_from_dar(dar_uuids)
            # dar_hits is a dict with UUIDs as keys. We need to cast them to strings.
            dar_hits_uuids_as_str = map(str, dar_hits.keys())
            dar_hits = dict(zip(dar_hits_uuids_as_str, dar_hits.values()))
            dar_cache.update(dar_hits)
            logger.info(f"Total dar: {len(dar_uuids)}, no-hit: {len(missing)}")
            for dar_uuid, uuid_list in self.dar_map.items():
                for uuid in uuid_list:
                    for address in self.addresses[uuid]:
                        address["value"] = dar_cache[dar_uuid].get("betegnelse")
        logger.info(f"Total dar: {len(dar_uuids)}, no-hit: {len(missing)}")
        return dar_cache

    def populate_cache(self, dry_run=None, skip_associations=False):
        """
        Perform the actual data import.
        :param skip_associations: If associations are not needed, they can be
        skipped for increased performance.
        :param dry_run: For testing purposes it is possible to read from cache.
        """
        if dry_run is None:
            dry_run = os.environ.get("USE_CACHED_LORACACHE", False)

        # Ensure that tmp/ exists
        Path("tmp/").mkdir(exist_ok=True)

        if self.full_history:
            if self.skip_past:
                facets_file = "tmp/facets_historic_skip_past.p"
                classes_file = "tmp/classes_historic_skip_past.p"
                users_file = "tmp/users_historic_skip_past.p"
                units_file = "tmp/units_historic_skip_past.p"
                addresses_file = "tmp/addresses_historic_skip_past.p"
                engagements_file = "tmp/engagements_historic_skip_past.p"
                managers_file = "tmp/managers_historic_skip_past.p"
                associations_file = "tmp/associations_historic_skip_past.p"
                leaves_file = "tmp/leaves_historic_skip_past.p"
                roles_file = "tmp/roles_historic_skip_past.p"
                itsystems_file = "tmp/itsystems_historic_skip_past.p"
                it_connections_file = "tmp/it_connections_historic_skip_past.p"
                kles_file = "tmp/kles_historic_skip_past.p"
                related_file = "tmp/related_historic_skip_past.p"
            else:
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

        t = time.time()  # noqa: F841
        msg = "Kørselstid: {:.1f}s, {} elementer, {:.0f}/s"  # noqa: F841

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
    lc = LoraCache(resolve_dar=True, full_history=False)
    lc.populate_cache(skip_associations=True)
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()

    # Todo, in principle it should be possible to run with skip_past True
    # This is now fixed in a different branch, remember to update when
    # merged.
    lc_historic = LoraCache(resolve_dar=True, full_history=True, skip_past=False)
    lc_historic.populate_cache(skip_associations=True)
    # Here we should de-activate read-only mode
    return lc, lc_historic


@click.command()
@click.option("--historic/--no-historic", default=True, help="Do full historic export")
@click.option(
    "--skip-past", is_flag=True, default=False, help="Skip past in historic export"
)
@click.option(
    "--resolve-dar/--no-resolve-dar", default=False, help="Resolve DAR addresses"
)
@click.option("--read-from-cache", is_flag=True)
def cli(historic, skip_past, resolve_dar, read_from_cache):
    lc = LoraCache(
        full_history=historic,
        skip_past=skip_past,
        resolve_dar=resolve_dar,
    )
    lc.populate_cache(dry_run=read_from_cache)

    logger.info("Now calcualate derived data")
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()


if __name__ == "__main__":

    for name in logging.root.manager.loggerDict:  # type: ignore
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
