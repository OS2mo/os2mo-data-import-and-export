import re
import pickle
import requests
import xmltodict
from datetime import datetime
from os2mo_data_import import Organisation
#from os2mo_data_import.http_utils import temp_import_all


def _format_time(timestring):
    try:
        dato = datetime.strptime(timestring, '%d/%m/%Y')
    except ValueError:
        dato = None
    return dato


def _dawa_request(address, adgangsadresse=False, skip_letters=False):
    """ Perform a request to DAWA and return the json object
    :param address: An address object as returned by APOS
    :param adgangsadresse: If true, search for adgangsadresser
    :param skip_letters: If true, remove letters from the house number
    :return: The DAWA json object as a dictionary
    """
    if adgangsadresse:
        base = 'https://dawa.aws.dk/adgangsadresser?'
    else:
        base = 'https://dawa.aws.dk/adresser?'
    params = 'kommunekode={}&postnr={}&vejkode={}&husnr={}'
    if skip_letters:
        husnr = re.sub(r'\D', '', address['@husnummer'])
    else:
        husnr = address['@husnummer'].upper()
    full_url = base + params.format(address['@kommunekode'],
                                    address['@postnummer'],
                                    address['@vejkode'],
                                    husnr)
    path_url = full_url.replace('/', '_')
    try:
        with open(path_url + '.p', 'rb') as f:
            response = pickle.load(f)
    except FileNotFoundError:
        response = requests.get(full_url)
        with open(path_url + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)
    dar_data = response.json()
    return dar_data


class AposImport(object):

    def __init__(self, org_name):
        self.base_url = 'http://apos.balk.dk:8080/apos2-'
        self.org = Organisation(org_name, org_name)
        self.address_challenges = {}
        self.address_errors = {}

    def _apos_lookup(self, url):
        path_url = url.replace('/', '_')
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
        facetter = r['facet']
        if not isinstance(facetter, list):
            facetter = [facetter]
        facet_dict = {}
        for facet in facetter:
            beskrivelse = facet['@brugervendtnoegle']
            facet_dict[beskrivelse] = facet
        return facet_dict

    def _read_apos_klasser(self, facet_uuid):
        url = "app-klassifikation/GetKlasseForFacet?uuid={}"
        r = self._apos_lookup(url.format(facet_uuid))
        return r['klasse']

    def create_typer(self, klassifikation_uuid, facet_typer):
        """ Read all klasser from a klassifikation from Apos
        and create them in LoRa. Apos has only a single facet
        for these types, so the Apos Klassifikation will be mapped
        as a LoRa facet.
        :param klassifikation_uuid: The apos uuid for the klassifikation
        :param facet_type: Dict with MO facets as keys and APOS facets as
        values
        """
        apos_facetter = self._read_apos_facetter(klassifikation_uuid)
        for mo_facet_navn, apos_facet_navn in facet_typer.items():
            facet = apos_facetter[apos_facet_navn]

            klasser = self._read_apos_klasser(facet['@uuid'])
            for klasse in klasser:
                data = {"brugervendtnoegle": klasse['@title'],
                        "omfang": None,  # TODO: Hvad er dette?
                        "titel": klasse['@title']}

                if klasse['@uuid'] == '4d3ac2ff-96a5-45c8-9108-856f56110fee':
                    print('!!!!!!!!!!!!!!!!!!')
                    print(klasse)
                #if klasse['@uuid'] == '41504f53-0203-001f-4158-41504f494e54':
                #    print(klasse)
                klaf = self.org.Klasse.add(identifier=klasse['@uuid'],
                                           facet_type=mo_facet_navn,
                                           properties=data)
                if klasse['@uuid'] == '4d3ac2ff-96a5-45c8-9108-856f56110fee':
                    print('Klaf:')
                    print(klaf)
                    print(':Klaf')


    def create_facetter_and_klasser(self):
        """ Her laver vi de facetter Niels efterspørger """
        url = "app-klassifikation/GetKlassifikationList"
        r = self._apos_lookup(url)
        klassifikationer = r['klassifikation']
        for k in klassifikationer:
            if k['@kaldenavn'] == 'Stillingsbetegnelser':
                self.create_typer(k['@uuid'],
                                  {'Stillingsbetegnelse': 'Alfabetisk'})
            if k['@kaldenavn'] == 'Enhedstyper':
                self.create_typer(k['@uuid'],
                                  {'Enhedstype': 'Alfabetisk'})
            if k['@kaldenavn'] == 'Leder':
                self.create_typer(k['@uuid'], {'Lederansvar': 'Ansvar',
                                               'Ledertyper': 'Typer'})
            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'Adressetype': 'Lokation typer',
                                               'Adressetype': 'Egenskaber',
                                               'Adressetype':
                                               'Engagement typer'})

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
        fra = _format_time(details['gyldighed']['@fra'])
        til = _format_time(details['gyldighed']['@til'])
        unit = self.org.OrganisationUnit.add(
            identifier=apos_unit['@uuid'],
            name=apos_unit['@navn'],
            user_key=apos_unit['@brugervendtNoegle'],
            type_ref=details['@enhedstype'],
            date_from=fra,
            date_to=til,
            parent_ref=parent)
        return unit

    def create_employees_for_ou(self, unit):
        url = 'composite-services/GetEngagementDetailed?unitUuid={}'
        medarbejdere = self._apos_lookup(url.format(unit['@uuid']))
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

            # TODO: Hvorfor ser vi samme medarbejder flere gange?
            self.org.Employee.add(name=name,
                                  identifier=medarbejder['@uuid'],
                                  cpr_no=person['@personnummer'],
                                  user_key=bvn,
                                  date_from=fra,
                                  date_to=til)

            # print(medarbejder['lokationer'])

            # Dette er telefon og email - hold fast i dem
            kontakt = medarbejder['klassifikationKontaktKanaler']
            kontaktmuligheder = kontakt['klassifikationKontaktKanal']
            print(kontakt)

            #if kontakt['@type'] == '41504f53-0203-001f-4158-41504f494e54':
            #    print('!!!')
            #    print(kontakt['@vaerdi'])
            #1/0
            opgaver = medarbejder['opgaver']['opgave']
            if isinstance(opgaver, list):
                assert(len(opgaver) == 2)
                assert(opgaver[0]['@klassifikation'] == 'stillingsbetegnelser')
                assert(opgaver[1]['@klassifikation'] == 'tilknytningstyper')
                stilling = opgaver[0]['@uuid']
                tilknytning = opgaver[1]['@uuid']
                if not tilknytning == '56e1214a-330f-4592-89f3-ae3ee8d5b2e6':
                    # TODO: Disse findes ikke i klassifikation
                    pass
            else:
                assert(opgaver['@klassifikation'] == 'stillingsbetegnelser')
                stilling = opgaver['@uuid']

            # engagementstype = self.org.Klasse.get_uuid("Ansat")
            print(stilling)
            job = self.org.Employee.add_type_engagement(
                identifier=stilling,
                org_unit_ref=unit['@uuid'],
                job_function_ref=stilling,
                engagement_type_ref='56e1214a-330f-4592-89f3-ae3ee8d5b2e6',
                date_from=fra)
            # print(medarbejder['integrationAttributter'])

    def create_associations_for_ou(self, unit):
        """ De returnerede personer findes tilsyneladende ikke?
        Skal undersøges!!! """
        uuid = unit['@uuid']
        url = "app-organisation/GetAttachedPersonsForUnit?uuid={}"
        associations = self._apos_lookup(url.format(uuid))
        if int(associations['total']) > 0:
            for person in associations['person']:
                uuid = person['@uuid']

                url = "app-organisation/GetBruger?uuid={}"
                # print(self.org.Employee.get_data(uuid))

    def dawa_lookup(self, address):
        """ Lookup an APOS address object in DAWA and find a UUID
        for the address.
        :param address: APOS address object
        :return: DAWA UUID for the address, or None if it is not found
        """
        dar_uuid = None
        dar_data = _dawa_request(address)
        if len(dar_data) == 0:
            # Found no hits, first attempts is to remove the letter
            # from the address and note it for manual verifikation
            self.address_challenges[address['@uuid']] = address
            dar_data = _dawa_request(address, skip_letters=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
            else:
                self.address_errors[address['@uuid']] = address
        elif len(dar_data) == 1:
            # Everyting is as expected
            dar_uuid = dar_data[0]['id']
        else:
            # Multiple results typically means we have found an
            # adgangsadresse
            dar_data = _dawa_request(address, adgangsadresse=True)
            if len(dar_data) == 1:
                dar_uuid = dar_data[0]['id']
            else:
                del self.address_challenges[address['@uuid']]
                self.address_errors[address['@uuid']] = address
        return dar_uuid

    def read_locations(self, unit):
        url = 'app-organisation/GetLocations?uuid={}'
        locations = self._apos_lookup(url.format(unit['@uuid']))
        mo_locations = []
        if int(locations['total']) == 0:
            # Return imidiately of no locations are returned
            return mo_locations

        locations = locations['location']
        if not isinstance(locations, list):
            locations = [locations]
        for location in locations:
            # TODO: What to do with information regarding primary?
            uuid = location['@adresse']
            url = 'app-part/GetAdresseList?uuid={}'
            opus_address = self._apos_lookup(url.format(uuid))

            dawa_uuid = self.dawa_lookup(opus_address['adresse'])
            pnummer = location.get('@pnummer', None)
            primary = (location.get('@primary', None) == 'JA')
            mo_location = {'pnummer': pnummer,
                           'primary': primary,
                           'dawa_uuid': dawa_uuid}
            mo_locations.append(mo_location)
        return mo_locations

    def get_ou_functions(self, unit):
        stop = False
        url = 'app-organisation/GetFunctionsForUnit?uuid={}'
        org_funcs = self._apos_lookup(url.format(unit['@uuid']))
        if int(org_funcs['total']) == 0:
            return

        for func in org_funcs['function']:
            persons = func['persons']
            if not persons:
                continue
            if persons['person']['@uuid']:
                print(func.keys())
                print(func['@bvn'])
                print(func['@navn'])
                person = func['persons']['person']['@uuid']
                assert(len(func['persons']) == 1)
                assert(func['units']['unit']['@uuid'] == unit['@uuid'])
                tasks = func['tasks']['task']
                print(tasks)
                #for task in tasks:
                #    self.org.Employee.add_type_manager(
                #        org_unit_ref=unit['@uuid'],
                #        manager_type_ref=func['tasks']['task']['@uuid']
                # These task corresond to klasser in Leder
                print('--**--')
                #print(persons['person'])
                #print(type(persons['person']))
                # print(persons['person'].keys())
                stop = True
        if stop:
            1/0
    
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
                unit_locations = self.read_locations(unit)
                over_id = int(unit['@overordnetid'])
                unit_id = int(unit['@objectid'])
                if over_id in nodes.keys():
                    print(unit['@uuid'])                  
                    new[unit_id] = self._create_ou_from_apos(unit,
                                                             nodes[over_id])
                    self.create_employees_for_ou(unit)
                    self.get_ou_functions(unit)

                else:
                    remaining_org_units.append(unit)
            org_units = remaining_org_units
            nodes.update(new)


if __name__ == '__main__':
    apos_import = AposImport('Ballerup APOS 1')

    apos_import.create_facetter_and_klasser()
    apos_import.create_ou_tree()
    #temp_import_all(apos_import.org)

    print('********************************')
    print('Address challenges:')
    for challenge in apos_import.address_challenges:
        print(apos_import.address_challenges[challenge])

    print('Address Errors:')
    for error in apos_import.address_errors:
        print(apos_import.address_errors[error])
