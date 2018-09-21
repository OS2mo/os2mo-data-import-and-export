import pickle
import requests
import xmltodict
from datetime import datetime
from os2mo_data_import.adapters.builder import Organisation
from os2mo_data_import.http_utils import temp_import_all

def _format_time(timestring):
    try:
        dato = datetime.strptime(timestring, '%d/%m/%Y')
    except ValueError:
        dato = None
    return dato


class AposImport(object):

    def __init__(self, org_name):
        self.base_url = 'http://apos.balk.dk:8080/apos2-'
        self.org = Organisation(org_name, org_name)

    def _apos_lookup(self, url):
        path_url = url.replace('/','_')
        try:
            with open(path_url + '.p', 'rb') as f:
                response = pickle.load(f)
        except FileNotFoundError:
            response = requests.get(self.base_url + url)
            with open(path_url + '.p', 'wb') as f:
                pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

        xml_response = xmltodict.parse(response.text)
        # Response is always hidden one level down
        outer_key = list(xml_response.keys())[0]
        return xml_response[outer_key]

    def _read_apos_facetter(self, uuid):
        url = "app-klassifikation/GetFacetterForKlassifikation?uuid={}"
        r = self._apos_lookup(url.format(uuid))
        facet = r['facet']
        return facet

    def _read_apos_klasser(self, facet_uuid):
        url = "app-klassifikation/GetKlasseForFacet?uuid={}"
        r = self._apos_lookup(url.format(facet_uuid))
        return r['klasse']

    def create_typer(self, uuid, facet_type):
        """ Read all klasser from a klassifikation from Apos
        and create them in LoRa. Apos has only a single facet
        for these types, so the Apos Klassifikation will be mapped
        as a LoRa facet.
        :param uuid: The apos uuid for the klassifikation
        :param facet_type: The type of facet, only two allowed
        """
        if not facet_type in ['Enhedstype', 'Stillingsbetegnelse']:
            raise('Wrong facet type')
        # We have only a single facet in these Klassifikationer
        apos_facet = self._read_apos_facetter(uuid)
        klasser = self._read_apos_klasser(apos_facet['@uuid'])
        facet_uuid = self.org.Facet.get_uuid(facet_type)
        for klasse in klasser:
            data = {"brugervendtnoegle": klasse['@title'],
                    "omfang": None,  # TODO: Hvad er dette?
                    "titel": klasse['@title']}
            self.org.Klasse.add(identifier=klasse['@uuid'],
                                facet_ref=facet_uuid,
                                properties=data)

    def create_facetter_and_klasser(self):
        """ Her laver vi de facetter Niels efterspørger """
        url = "app-klassifikation/GetKlassifikationList"
        r = self._apos_lookup(url)
        klassifikationer = r['klassifikation']
        for k in klassifikationer:
            if k['@kaldenavn'] == 'Stillingsbetegnelser':
                self.create_typer(k['@uuid'], 'Stillingsbetegnelse')
            if k['@kaldenavn'] == 'Enhedstyper':
                self.create_typer(k['@uuid'], 'Enhedstype')

    def _read_ous_from_apos(self, re_read=False):
        if re_read:
            url = self.base_url + "app-organisation/GetEntireHierarchy"
            response = requests.get(url)
            org_units = xmltodict.parse(response.text)
            with open('ou_cache.p', 'wb') as f:
                pickle.dump(org_units, f, pickle.HIGHEST_PROTOCOL)
        else:
            with open('ou_cache.p', 'rb') as f:
                org_units = pickle.load(f)
        return org_units['hierakiResponse']['node']


    def _create_ou_from_apos(self, apos_unit, parent=None):
        url = "app-organisation/GetUnitDetails?uuid={}"
        r = self._apos_lookup(url.format(apos_unit['@uuid']))
        details = r['enhed']
        # print(apos_unit)
        fra = _format_time(details['gyldighed']['@fra'])
        til = _format_time(details['gyldighed']['@til'])
        unit = self.org.OrganisationUnit.add(
            name=apos_unit['@navn'],
            user_key=apos_unit['@brugervendtNoegle'],
            type_ref=self.org.Klasse.get_uuid(details['@enhedstype']),
            date_from=fra,
            date_to=til,
            parent_ref=parent)
        return unit

    def create_employees_for_ou(self, unit):
        url = 'composite-services/GetEngagementDetailed?unitUuid={}'
        uuid = unit['@uuid']

        medarbejdere = self._apos_lookup(url.format(uuid))

        if medarbejdere['total'] == '0':
            return
        elif medarbejdere['total'] == '1':
            medarbejdere = [medarbejdere['engagementer']['engagement']]
        else:
            medarbejdere = medarbejdere['engagementer']['engagement']

        for medarbejder in medarbejdere:
            person = medarbejder['person']
            name = person['@fornavn'] + ' '
            if person['@mellemnavn']:
                name += person['@mellemnavn']
            name += person['@efternavn']
            fra = _format_time(medarbejder['gyldighed']['@fra'])
            til = _format_time(medarbejder['gyldighed']['@til'])
            bvn = medarbejder['@brugervendtNoegle']
            self.org.Employee.add(name=name,
                                  cpr_no=person['@personnummer'],
                                  #brugervendtnoegle=bvn,
                                  date_from=fra,
                                  date_to=til)

            # NOTE: Hvorfor ser vi samme medarbejder flere gange?
            #print(medarbejder['@uuid'])
            #print(medarbejder['person'])
            #print(medarbejder['lokationer'])
            #print(medarbejder['klassifikationKontaktKanaler'])
            opgaver = medarbejder['opgaver']['opgave']
            print(opgaver[0])
            1/0
            if not opgaver['@klassifikation'] == 'stillingsbetegnelser':
                print(opgaver)
            #print(medarbejder['integrationAttributter'])

    def create_ou_tree(self):
        org_units = self._read_ous_from_apos(re_read=False)
        nodes = {}
        # The root org is always first row in APOS
        nodes[1] = self._create_ou_from_apos(org_units[0])
        del(org_units[0])

        while org_units:
            remaining_org_units = []
            new = {}
            for unit in org_units:
                over_id = int(unit['@overordnetid'])
                unit_id = int(unit['@objectid'])
                if over_id in nodes.keys():
                    print(unit['@uuid'])
                    new[unit_id] = self._create_ou_from_apos(unit,
                                                             nodes[over_id])
                    self.create_employees_for_ou(unit)
                else:
                    remaining_org_units.append(unit)
            org_units = remaining_org_units
            nodes.update(new)
            
"""
print('---- Tilknytninger ----')
url = base + "apos2-app-organisation/GetAttachedPersonsForUnit?uuid={}"
#response = requests.get(url.format(node['@uuid']))
#print(response.text)

url = base + "apos2-app-organisation/GetLocations?uuid={}"
#response = requests.get(url.format(node['@uuid']))
#print(response.text)

url = base + "apos2-composite-services/GetEngagementDetailed?unitUuid={}"
#response = requests.get(url.format(node['@uuid']))
#print(response.text)
"""

if __name__ == '__main__':
    apos_import = AposImport('Ballerup APOS 1')

    
    apos_import.create_facetter_and_klasser()

    apos_import.create_ou_tree()
