import datetime
import itertools
import logging
import time
import urllib.parse

import dateutil.tz
import lora_utils
import pytest
import requests
from retrying import retry
from tqdm import tqdm

from exporters.sql_export.lora_cache import LoraCache

logger = logging.getLogger("LoraCache")

DEFAULT_TIMEZONE = dateutil.tz.gettz("Europe/Copenhagen")

skip = pytest.mark.skip
# Uncomment the line below to actually run all these equivalence tests.
# NOTE: They will run against the configured live MO / LoRa instances.
# skip = lambda func: func


class OldLoraCache(LoraCache):
    def __init__(self, resolve_dar=True, full_history=False, skip_past=False):
        super().__init__(resolve_dar, full_history, skip_past)
        self.additional = {"relationer": ("tilknyttedeorganisationer", "tilhoerer")}

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
                ),
                "attributter": ("organisationfunktionegenskaber",),
                # "tilstande": ("organisationfunktiongyldighed",)  # bug in old cache; is needed for equivalence
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

                it_connections[uuid].append(
                    {
                        "uuid": uuid,
                        "user": user_uuid,
                        "unit": unit_uuid,
                        "username": user_key,
                        "itsystem": itsystem,
                        "from_date": from_date,
                        "to_date": to_date,
                    }
                )
        return it_connections

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
                print(len(rel["tilknyttedeenheder"]))
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

                tilknyttedepersoner = reg["relationer"]["tilknyttedepersoner"]
                if len(tilknyttedepersoner) == 0:
                    continue
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
            "relationer": ("overordnet", "enhedstype", "niveau"),
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
                units[uuid].append(
                    {
                        "uuid": uuid,
                        "user_key": egenskaber["brugervendtnoegle"],
                        "name": egenskaber["enhedsnavn"],
                        "unit_type": relationer["enhedstype"][0]["uuid"],
                        "level": level,
                        "parent": parent,
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
                    # logger.error(msg.format(address_type, value_raw))
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
                    # logger.error(msg.format(engagement))
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
                    # logger.error("Error: Unable to find unit in {}".format(uuid))

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


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_facet_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    old_facets = olc._cache_lora_facets()
    new_facets = lc._cache_lora_facets()
    assert new_facets == old_facets
    assert len(new_facets) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_class_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    old_classes = olc._cache_lora_classes()
    new_classes = lc._cache_lora_classes()
    assert new_classes == old_classes
    assert len(new_classes) >= 1


@skip
def test_itsystems_equivalence():
    lc = LoraCache(resolve_dar=False)
    olc = OldLoraCache(resolve_dar=False)
    new_itsystems = lc._cache_lora_itsystems()
    old_itsystems = olc._cache_lora_itsystems()
    assert new_itsystems == old_itsystems
    assert len(new_itsystems) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_users_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_users = lc._cache_lora_users()
    old_users = olc._cache_lora_users()
    assert new_users == old_users
    assert len(new_users) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_units_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_units = lc._cache_lora_units()
    old_units = olc._cache_lora_units()
    assert new_units == old_units
    assert len(new_units) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_itconnections_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_itconnections = lc._cache_lora_it_connections()
    old_itconnections = olc._cache_lora_it_connections()
    assert new_itconnections == old_itconnections
    assert len(new_itconnections) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_related_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_related = lc._cache_lora_related()
    old_related = olc._cache_lora_related()

    # test would fail because LoRa sometimes sorts the relationship tuple differently
    for related in itertools.chain(new_related.values(), old_related.values()):
        s = sorted((related[0]["unit1_uuid"], related[0]["unit2_uuid"]))
        related[0]["unit1_uuid"], related[0]["unit2_uuid"] = s
    assert new_related == old_related
    assert len(new_related) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_addresses_equivalence(full_history, skip_past):
    """
    Test (sometimes) fails because the old LoRa cache may include historical addresses even though skip_past=True.
    """
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_addresses = lc._cache_lora_address()
    old_addresses = olc._cache_lora_address()
    assert new_addresses == old_addresses
    assert len(new_addresses) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_engagements_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_engagements = lc._cache_lora_engagements()
    old_engagements = olc._cache_lora_engagements()
    assert new_engagements == old_engagements
    assert len(new_engagements) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_associations_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_associations = lc._cache_lora_associations()
    old_associations = olc._cache_lora_associations()
    assert new_associations == old_associations
    assert len(new_associations) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_roles_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_roles = lc._cache_lora_roles()
    old_roles = olc._cache_lora_roles()
    assert new_roles == old_roles
    assert len(new_roles) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_leaves_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_leaves = lc._cache_lora_leaves()
    old_leaves = olc._cache_lora_leaves()
    assert new_leaves == old_leaves
    assert len(new_leaves) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_kles_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_kles = lc._cache_lora_kles()
    old_kles = olc._cache_lora_kles()

    # sort kle aspects to ensure we validate the data, not the order
    def sort_kles(kles: dict[str, list]):
        for kle in kles.values():
            kle.sort(key=lambda x: x["kle_aspect"])

    sort_kles(new_kles)
    sort_kles(old_kles)

    assert new_kles == old_kles
    assert len(new_kles) >= 1


@skip
@pytest.mark.parametrize("skip_past", [True, False])
@pytest.mark.parametrize("full_history", [True, False])
def test_managers_equivalence(full_history, skip_past):
    lc = LoraCache(full_history=full_history, skip_past=skip_past, resolve_dar=False)
    olc = OldLoraCache(
        full_history=full_history, skip_past=skip_past, resolve_dar=False
    )
    new_managers = lc._cache_lora_managers()
    old_managers = olc._cache_lora_managers()
    assert new_managers == old_managers
    assert len(new_managers) >= 1
