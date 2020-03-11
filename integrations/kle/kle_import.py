import json
import pathlib
import requests
import xmltodict

from integrations.kle import payloads


# Facetter
# Emne: http://clever-gewicht-reduzieren.de/resources/kle/emneplan
# Funktion: http://clever-gewicht-reduzieren.de/resources/kle/handlingsfacetter


class KleUploader(object):
    """ Script to import KLE into LoRa to allow easy access to relevant
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

    def __init__(self):
        """
        Init function
        :para hostname: hostname for the rest interface
        """
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

    def _read_kle_dict(self, facet='emne', local=True):
        """ Read the entire KLE file
        :param facet: Either 'emne' or 'handlingsfacetter'
        :param local: If True the file is read from local cache
        :return: The document date and the KLE index as a dict
        """
        if facet == 'emne':
            navn = 'emneplan'
        else:
            navn = 'handlefacetter'

        if local:
            with open('integrations/kle/' + navn + '.xml', 'r') as content_file:
                xml_content = content_file.read()
        else:
            url = 'http://clever-gewicht-reduzieren.de/resources/kle/'
            response = requests.get(url + navn)
            xml_content = response.text

        kle_dict = xmltodict.parse(xml_content)
        if facet == 'emne':
            udgivelses_dato = kle_dict['KLE-Emneplan']['UdgivelsesDato']
            kle_dict = kle_dict['KLE-Emneplan']['Hovedgruppe']
        else:
            udgivelses_dato = kle_dict['KLE-Handlingsfacetter']['UdgivelsesDato']
            kle_dict = kle_dict['KLE-Handlingsfacetter']['HandlingsfacetKategori']
        return (udgivelses_dato, kle_dict)

    def _create_facet(self, facet_name):
        """
        Creates a new facet
        :param facet_name: Name of the new facet
        :return: Returns uuid of the new facet
        """
        url = '/klassifikation/facet'
        template = payloads.lora_facet(bvn=facet_name)
        response = requests.post(self.settings['mox.base'] + url, json=template)
        return response.json()['uuid']

    def _create_kle_klasse(self, facet, klasse_info, overklasse=None):
        """
        Creates a new Klasse based on KLE
        :param facet: uuid for the korresponding facet
        :param klasse_info: Dict as returned by read_all_*
        :return: Returns uuid of the new klasse
        """
        url = '/klassifikation/klasse/{}'
        uuid = klasse_info['uuid']
        del klasse_info['uuid']
        payload = payloads.lora_klasse(facet=facet, overklasse=overklasse,
                                       **klasse_info)
        full_url = self.settings['mox.base'] + url.format(uuid)
        response = requests.put(full_url, json=payload)
        lora_uuid = response.json()['uuid']
        assert lora_uuid == uuid

        return lora_uuid

    def _read_all_hovedgrupper(self, kle_dict, facet='emne'):
        """ Read all Hovedgrupper from KLE
        :param kle_dict: A dictinary containing KLE
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with index as as key and
        (HovedgruppeTitel, HovedgruppeNr) as value
        """
        name = 'Hovedgruppe' if facet == 'emne' else 'HandlingsfacetKategori'
        hovedgrupper = {}
        for i in range(0, len(kle_dict)):
            titel = kle_dict[i][name + 'Titel']
            hovedgrupper[i] = (titel, kle_dict[i][name + 'Nr'])
        return hovedgrupper

    def _read_all_from_hovedgruppe(self, kle_dict, hovedgruppe_index,
                                   facet='emne'):
        """
        Read all relevant fields from a Hovedgruppe - this can
        easily be extended if more info turns out to be relevant
        :param kle_dict: A dictinary containing KLE
        :param hovedgruppe_index: Index for the wanted Hovedgruppe
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with relevant info
        """
        name = 'Hovedgruppe' if facet == 'emne' else 'HandlingsfacetKategori'
        hovedgruppe = kle_dict[hovedgruppe_index]
        hovedgruppe_info = {
            'uuid': hovedgruppe['UUID'],
            'titel': hovedgruppe[name + 'Titel'],
            'dato': hovedgruppe[name + 'AdministrativInfo']['OprettetDato'],
            'nummer': hovedgruppe[name + 'Nr']
        }
        # TODO: Der findes også info om rettet-dato, er dette relevant?
        return hovedgruppe_info

    def _read_all_grupper(self, kle_dict, hovedgruppe, facet='emne'):
        """ Read all Grupper from a KLE Hovedgruppe
        :param kle_dict: A dictinary containing KLE
        :param hovedgruppe: A KLE Hovedgruppe index to be retrieved
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with index as key and (GruppeTitel, GruppeNr) as value
        """
        name = 'Gruppe' if facet == 'emne' else 'Handlingsfacet'
        grupper = {}
        gruppe_liste = kle_dict[hovedgruppe][name]
        for i in range(0, len(gruppe_liste)):
            grupper[i] = (gruppe_liste[i][name + 'Titel'],
                          gruppe_liste[i][name + 'Nr'])
        return grupper

    def _read_all_from_gruppe(self, kle_dict, hovedgruppe, gruppe,
                              facet='emne'):
        """ Read all relevant fields from a Gruppe - this can
        easily be extended if more info turns out to be relevant
        :param kle_dict: A dictinary containing KLE
        :param hovedgruppe: Index for the wanted Hovedgruppe
        :param gruppe: Index for the wanted Gruppe
        :param facet: Either 'emne' or 'handlingsfacetter'
        :return: Dict with relevant info
        """
        name = 'Gruppe' if facet == 'emne' else 'Handlingsfacet'
        gruppe = kle_dict[hovedgruppe][name][gruppe]

        gruppe_info = {
            'uuid': gruppe['UUID'],
            'titel': gruppe[name + 'Titel'],
            'dato': gruppe[name + 'AdministrativInfo']['OprettetDato'],
            'nummer': gruppe[name + 'Nr'],
        }
        return gruppe_info

    def _read_all_emner(self, kle_dict, hovedgruppe, gruppe):
        """ Read all Emner from a KLE Gruppe
        :param kle_dict: A dictinary containing KLE
        :param hovedgruppe: The KLE Hovedgruppe index containing the Gruppe
        :param GruppeNr: The KLE Gruppe index to be retrieved
        :return: Dict with index as key and (EmneTitel, EmneNr) as value
        """
        emner = {}
        emne_liste = kle_dict[hovedgruppe]['Gruppe'][gruppe]['Emne']
        for i in range(0, len(emne_liste)):
            try:
                emner[i] = (emne_liste[i]['EmneTitel'],
                            emne_liste[i]['EmneNr'][6:])
            except KeyError:  # If only one element, there is no list
                emner[0] = (emne_liste['EmneTitel'],
                            emne_liste['EmneNr'])
        return emner

    def _read_all_from_emne(self, kle_dict, hovedgruppe, gruppe, emne):
        """ Read all relevant fields from a Gruppe - this can
        easily be extended if more info turns out to be relevant
        :param kle_dict: A dictinary containing KLE
        :param hovedgruppe: Index for the wanted Hovedgruppe
        :param gruppe: Index for the wanted Gruppe
        :param gruppe: emne for the wanted Emne
        :return: Dict with relevant info
        """
        try:
            emne = kle_dict[hovedgruppe]['Gruppe'][gruppe]['Emne'][emne]
        except KeyError:  # If only one element, there is no list
            emne = kle_dict[hovedgruppe]['Gruppe'][gruppe]['Emne']

        emne_info = {
            'uuid': emne['UUID'],
            'titel': emne['EmneTitel'],
            'dato':  emne['EmneAdministrativInfo']['OprettetDato'],
            'nummer': emne['EmneNr']
        }
        return emne_info


def main():
    kle = KleUploader()

    kle_content = kle._read_kle_dict(facet='emne')
    print('Document date: ' + kle_content[0])
    kle_dict = kle_content[1]
    emne_facet_uuid = kle._create_facet('Emne')

    hovedgrupper = (kle._read_all_hovedgrupper(kle_dict))
    for hoved_index in hovedgrupper:
        hoved_info = kle._read_all_from_hovedgruppe(kle_dict, hoved_index)
        print(hoved_info['nummer'] + ': ' + hoved_info['titel'])
        # Create hovedgruppe
        hoved_uuid = kle._create_kle_klasse(emne_facet_uuid, hoved_info)

        grupper = kle._read_all_grupper(kle_dict, hoved_index)
        for gruppe_index in grupper:
            gruppe_info = kle._read_all_from_gruppe(kle_dict, hoved_index,
                                                    gruppe_index)
            print(hoved_info['nummer'] + '.' + gruppe_info['nummer'] + ': ' +
                  gruppe_info['titel'])
            # Create gruppe
            gruppe_uuid = kle._create_kle_klasse(emne_facet_uuid, gruppe_info,
                                                 hoved_uuid)
            emner = kle._read_all_emner(kle_dict, hoved_index, gruppe_index)
            for emne_index in emner:
                emne_info = kle._read_all_from_emne(kle_dict, hoved_index,
                                                    gruppe_index, emne_index)
                print(hoved_info['nummer'] + '.' + gruppe_info['nummer'] +
                      '.' + emne_info['nummer'] + ': ' + emne_info['titel'])
                # Create emne
                kle._create_kle_klasse(emne_facet_uuid, emne_info, gruppe_uuid)

    kle_content = kle._read_kle_dict(facet='handling')
    print('Document date: ' + kle_content[0])
    kle_dict = kle_content[1]

    funktion_facet_uuid = kle._create_facet('Funktion')
    hovedgrupper = (kle._read_all_hovedgrupper(kle_dict, facet='handling'))
    for hoved_index in hovedgrupper:
        hoved_info = kle._read_all_from_hovedgruppe(kle_dict, hoved_index,
                                                    facet='handling')
        print(hoved_info['nummer'] + ': ' + hoved_info['titel'])
        hoved_uuid = kle._create_kle_klasse(funktion_facet_uuid, hoved_info)

        grupper = kle._read_all_grupper(kle_dict, hoved_index, facet='handling')
        for gruppe_index in grupper:
            gruppe_info = kle._read_all_from_gruppe(kle_dict, hoved_index,
                                                    gruppe_index,
                                                    facet='handling')
            print(hoved_info['nummer'] + gruppe_info['nummer'] + ': ' +
                  gruppe_info['titel'])
            # Create gruppe
            gruppe_uuid = kle._create_kle_klasse(funktion_facet_uuid,
                                                 gruppe_info, hoved_uuid)


if __name__ == '__main__':
    main()
