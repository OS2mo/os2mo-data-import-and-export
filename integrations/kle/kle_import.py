# SPDX-FileCopyrightText: 2023 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import logging

import payloads
import requests
import xmltodict
from ra_utils.headers import TokenSettings
from ra_utils.job_settings import JobSettings


logger = logging.getLogger(__name__)

# Facetter
# Emne: http://api.kle-online.dk/resources/kle/emneplan
# Funktion: http://api.kle-online.dk/resources/kle/handlingsfacetter


class KleImporter(object):
    """Script to import KLE into LoRa to allow easy access to relevant
    test data

    In short, KLE consists of four nested levels, one Facet level and up to
    three Klasse levels. The amount of code-reuse between the levels is fairly
    small since the details in the XML files in the three levels are
    somewhat different. The task of actually retriveing the information
    about each class is put in seperate functions, read_all_from_, in
    order to keep flexibility if it becomes relevant to extend the amount
    of imported information for some of the levels. Possibly the current
    genereic json-template will be too simple in this case, it might be
    necessary with seperate templates for the various levels.
    """

    def __init__(self, mox_base, mora_base):
        """
        Init function
        :para hostname: hostname for the rest interface
        """
        self.mox_base = mox_base
        self.mora_base = mora_base
        self.mo_session = requests.Session()
        self.mo_session.headers = TokenSettings().get_headers()

    def _read_kle_dict(self, facet="emne", local=False):
        """Read the entire KLE file
        :param facet: Either 'emne' or 'handlingsfacetter'
        :param local: If True the file is read from local cache
        :return: The document date and the KLE index as a dict
        """
        if facet == "emne":
            navn = "emneplan"
        else:
            navn = "handlingsfacetter"

        if local:
            with open("integrations/kle/" + navn + ".xml", "r") as content_file:
                xml_content = content_file.read()
        else:
            url = "http://api.kle-online.dk/resources/kle/"
            response = requests.get(url + navn)
            response.encoding = "utf-8"
            xml_content = response.text

        kle_dict = xmltodict.parse(xml_content)
        if facet == "emne":
            udgivelses_dato = kle_dict["KLE-Emneplan"]["UdgivelsesDato"]
            kle_dict = kle_dict["KLE-Emneplan"]["Hovedgruppe"]
        else:
            udgivelses_dato = kle_dict["KLE-Handlingsfacetter"]["UdgivelsesDato"]
            kle_dict = kle_dict["KLE-Handlingsfacetter"]["HandlingsfacetKategori"]
        return (udgivelses_dato, kle_dict)

    def get_or_create_facet(self, facet_name):
        """
        Creates a new facet or returns an existing one
        :param facet_name: Name of the new facet
        :return: Returns uuid of the new facet
        """

        loraurl = self.mox_base + "/klassifikation/facet" + "?bvn=" + facet_name

        lora_all = requests.get(loraurl).json()["results"][0]

        if len(lora_all) == 1:
            return lora_all[0]

        elif len(lora_all) == 0:
            template = payloads.lora_facet(bvn=facet_name, org=self.org_uuid)
            logger.info("creating facet: %r", template)
            response = requests.post(
                self.mox_base + "/klassifikation/facet", json=template
            )
            return response.json()["uuid"]

        else:
            logger.error("Mere end en instans af facetten '%s' fundet" % facet_name)
            raise RuntimeError("Facet Dublet: %s" % facet_name)

    def get_or_create_klasse(self, facet, klasse_info, overklasse=None):
        """
        Creates a new Klasse based on KLE
        :param facet: uuid for the korresponding facet
        :param klasse_info: Dict as returned by read_all_*
        :return: Returns uuid of the new klasse
        """
        uuid = klasse_info["uuid"]

        payload = payloads.lora_klasse(
            brugervendtnoegle=klasse_info["nummer"],
            beskrivelse=klasse_info["titel"],
            titel="{} - {}".format(klasse_info["nummer"], klasse_info["titel"]),
            facet=facet,
            dato=klasse_info["dato"],
            overklasse=overklasse,
            ansvarlig=self.org_uuid,
        )

        return self.get_or_create_lora_klasse(payload, uuid)

    def get_or_create_lora_klasse(self, payload, uuid):
        url = "/klassifikation/klasse/{}"
        full_url = self.mox_base + url.format(uuid)

        response = requests.get(full_url)
        if response.status_code not in [200, 404]:
            logger.error("Loraopslag fejlede")
            response.raise_for_status()

        if response.status_code == 404:
            logger.info("creating lora klasse: %r on %s", payload, uuid)
            response = requests.put(full_url, json=payload)
            lora_uuid = response.json()["uuid"]
            assert lora_uuid == uuid

        return uuid

    def _read_all_hovedgrupper(self, facet="emne"):
        """Read all Hovedgrupper from KLE
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with index as as key and
        (HovedgruppeTitel, HovedgruppeNr) as value
        """
        name = "Hovedgruppe" if facet == "emne" else "HandlingsfacetKategori"
        hovedgrupper = {}
        for i in range(0, len(self.kle_dict)):
            titel = self.kle_dict[i][name + "Titel"]
            hovedgrupper[i] = (titel, self.kle_dict[i][name + "Nr"])
        return hovedgrupper

    def _read_all_from_hovedgruppe(self, hovedgruppe_index, facet="emne"):
        """
        Read all relevant fields from a Hovedgruppe - this can
        easily be extended if more info turns out to be relevant
        :param hovedgruppe_index: Index for the wanted Hovedgruppe
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with relevant info
        """
        name = "Hovedgruppe" if facet == "emne" else "HandlingsfacetKategori"
        hovedgruppe = self.kle_dict[hovedgruppe_index]
        hovedgruppe_info = {
            "uuid": hovedgruppe["UUID"],
            "titel": hovedgruppe[name + "Titel"],
            "dato": hovedgruppe[name + "AdministrativInfo"]["OprettetDato"],
            "nummer": hovedgruppe[name + "Nr"],
        }
        # TODO: Der findes også info om rettet-dato, er dette relevant?
        return hovedgruppe_info

    def _read_all_grupper(self, hovedgruppe, facet="emne"):
        """Read all Grupper from a KLE Hovedgruppe
        :param hovedgruppe: A KLE Hovedgruppe index to be retrieved
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with index as key and (GruppeTitel, GruppeNr) as value
        """
        name = "Gruppe" if facet == "emne" else "Handlingsfacet"
        grupper = {}
        gruppe_liste = self.kle_dict[hovedgruppe][name]
        for i in range(0, len(gruppe_liste)):
            grupper[i] = (gruppe_liste[i][name + "Titel"], gruppe_liste[i][name + "Nr"])
        return grupper

    def _read_all_from_gruppe(self, hovedgruppe, gruppe, facet="emne"):
        """Read all relevant fields from a Gruppe - this can
        easily be extended if more info turns out to be relevant
        :param hovedgruppe: Index for the wanted Hovedgruppe
        :param gruppe: Index for the wanted Gruppe
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with relevant info
        """
        name = "Gruppe" if facet == "emne" else "Handlingsfacet"
        gruppe = self.kle_dict[hovedgruppe][name][gruppe]

        gruppe_info = {
            "uuid": gruppe["UUID"],
            "titel": gruppe[name + "Titel"],
            "dato": gruppe[name + "AdministrativInfo"]["OprettetDato"],
            "nummer": gruppe[name + "Nr"],
        }
        return gruppe_info

    def _read_all_emner(self, hovedgruppe, gruppe):
        """Read all Emner from a KLE Gruppe
        :param hovedgruppe: The KLE Hovedgruppe index containing the Gruppe
        :return: Dict with index as key and (EmneTitel, EmneNr) as value
        """
        emner = {}
        emne_liste = self.kle_dict[hovedgruppe]["Gruppe"][gruppe]["Emne"]
        for i in range(0, len(emne_liste)):
            try:
                emner[i] = (emne_liste[i]["EmneTitel"], emne_liste[i]["EmneNr"][6:])
            except KeyError:  # If only one element, there is no list
                emner[0] = (emne_liste["EmneTitel"], emne_liste["EmneNr"])
        return emner

    def _read_all_from_emne(self, hovedgruppe, gruppe, emne):
        """Read all relevant fields from a Gruppe - this can
        easily be extended if more info turns out to be relevant
        :param hovedgruppe: Index for the wanted Hovedgruppe
        :param gruppe: Index for the wanted Gruppe
        :param gruppe: emne for the wanted Emne
        :return: Dict with relevant info
        """
        try:
            emne = self.kle_dict[hovedgruppe]["Gruppe"][gruppe]["Emne"][emne]
        except KeyError:  # If only one element, there is no list
            emne = self.kle_dict[hovedgruppe]["Gruppe"][gruppe]["Emne"]

        emne_info = {
            "uuid": emne["UUID"],
            "titel": emne["EmneTitel"],
            "dato": emne["EmneAdministrativInfo"]["OprettetDato"],
            "nummer": emne["EmneNr"],
        }
        return emne_info

    def _import_emne(self, facet_uuid):
        """
        Read all classes from the KLE facet 'emneplan'
        :param facet_uuid: UUID of the facet the classes are created under
        """
        logger.info("Importing 'Emne'")
        kle_content = self._read_kle_dict(facet="emne")
        self.kle_dict = kle_content[1]

        hovedgrupper = self._read_all_hovedgrupper()
        for hoved_index in hovedgrupper:
            hoved_info = self._read_all_from_hovedgruppe(hoved_index)
            # Create hovedgruppe
            hoved_uuid = self.get_or_create_klasse(facet_uuid, hoved_info)

            grupper = self._read_all_grupper(hoved_index)
            for gruppe_index in grupper:
                gruppe_info = self._read_all_from_gruppe(hoved_index, gruppe_index)
                # Create gruppe
                gruppe_uuid = self.get_or_create_klasse(
                    facet_uuid, gruppe_info, hoved_uuid
                )
                emner = self._read_all_emner(hoved_index, gruppe_index)
                for emne_index in emner:
                    emne_info = self._read_all_from_emne(
                        hoved_index, gruppe_index, emne_index
                    )
                    # Create emne
                    self.get_or_create_klasse(facet_uuid, emne_info, gruppe_uuid)

    def _import_handling(self, facet_uuid):
        """
        Import the classes from the KLE facet 'handlingsfacetter'
        :param facet_uuid: UUID of the facet the classes are created under
        """
        logger.info("Importing 'Handling'")
        facet = "handling"
        kle_content = self._read_kle_dict(facet=facet)
        self.kle_dict = kle_content[1]

        hovedgrupper = self._read_all_hovedgrupper(facet=facet)
        for hoved_index in hovedgrupper:
            hoved_info = self._read_all_from_hovedgruppe(hoved_index, facet=facet)
            hoved_uuid = self.get_or_create_klasse(facet_uuid, hoved_info)

            grupper = self._read_all_grupper(hoved_index, facet=facet)
            for gruppe_index in grupper:
                gruppe_info = self._read_all_from_gruppe(
                    hoved_index, gruppe_index, facet=facet
                )
                # Create gruppe
                self.get_or_create_klasse(facet_uuid, gruppe_info, hoved_uuid)

    def set_mo_org_uuid(self):
        mora_base = self.mora_base
        r = self.mo_session.get("{}/service/o/".format(mora_base))
        r.raise_for_status()
        self.org_uuid = r.json()[0]["uuid"]

    def import_aspect_classes(self, facet_uuid):
        logger.info("Importing aspect classes")
        for key, scope, uuid in [
            ("Indsigt", "INDSIGT", "92f719f2-9e34-459e-8610-8dd160747a93"),
            ("Udførende", "UDFOERENDE", "40e91f7a-d2fc-4e07-8108-4046bde113d0"),
            ("Ansvarlig", "ANSVARLIG", "4d9d0ff4-017d-4e34-acc4-d403f7b2358c"),
        ]:
            payload = payloads.lora_klasse(
                brugervendtnoegle=key,
                beskrivelse=key,
                titel=key,
                facet=facet_uuid,
                omfang=scope,
                dato="1910-01-01 00:00:00",
                ansvarlig=self.org_uuid,
            )
            self.get_or_create_lora_klasse(payload, uuid)

    def import_kle(self):
        self.set_mo_org_uuid()
        aspect_facet_uuid = self.get_or_create_facet("kle_aspect")
        self.import_aspect_classes(aspect_facet_uuid)
        number_facet_uuid = self.get_or_create_facet("kle_number")
        self._import_emne(number_facet_uuid)
        self._import_handling(number_facet_uuid)


if __name__ == "__main__":
    settings = JobSettings()
    settings.start_logging_based_on_settings()

    mora_base = settings.mora_base
    mox_base = mora_base + "/lora"

    kle = KleImporter(mox_base, mora_base)
    kle.import_kle()
    logger.info("program has ended")
