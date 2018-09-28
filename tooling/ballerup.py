import re
import pickle
import requests
import xmltodict
import collections
from datetime import datetime
from os2mo_data_import import Organisation, ImportUtility


def _format_time(gyldighed):
    from_time = None
    to_time = None
    if not gyldighed['@fra'] == '-INFINITY':
        from_time = datetime.strptime(gyldighed['@fra'], '%d/%m/%Y')
        from_time = from_time.strftime('%Y-%m-%d')
    if not gyldighed['@til'] == 'INFINITY':
        to_time = datetime.strptime(gyldighed['@til'], '%d/%m/%Y')
        to_time = to_time.strftime('%Y-%m-%d')
    return from_time, to_time


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
        self.duplicate_persons = {}
        self.address_errors = {}
        self.klassifikation_errors = {}

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

    def dawa_lookup(self, address):
        """ Lookup an APOS address object in DAWA and find a UUID
        for the address.
        :param address: APOS address object
        :return: DAWA UUID for the address, or None if it is not found
        """
        dar_uuid = None
        dar_data = _dawa_request(address)
        if len(dar_data) == 0:
            # Found no hits, first attempt is to remove the letter
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
            # Return imidiately if no locations are found
            return mo_locations
        locations = locations['location']
        if not isinstance(locations, list):
            locations = [locations]

        for location in locations:
            if '@adresse' not in location:
                continue  # This entry is broken and should be ignored
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
        for mo_facet_navn, apos_facet_navne in facet_typer.items():
            for apos_facet_navn in apos_facet_navne:
                facet = apos_facetter[apos_facet_navn]
                klasser = self._read_apos_klasser(facet['@uuid'])
                if not isinstance(klasser, list):
                    klasser = [klasser]
                for klasse in klasser:
                    print(klasse['@uuid'] + ': ' + klasse['@title'])
                    self.org.Klasse.add(identifier=klasse['@uuid'],
                                        titel=klasse['@title'],
                                        brugervendtnoegle=klasse['@title'],
                                        omfang=None,  # TODO: Hvad er dette?
                                        facet_type_ref=mo_facet_navn)

    def create_facetter_and_klasser(self):
        url = "app-klassifikation/GetKlassifikationList"
        r = self._apos_lookup(url)
        klassifikationer = r['klassifikation']
        for k in klassifikationer:
            if k['@kaldenavn'] == 'Stillingsbetegnelser':
                self.create_typer(k['@uuid'],
                                  {'Stillingsbetegnelse': ['Alfabetisk']})

            if k['@kaldenavn'] == 'Enhedstyper':
                self.create_typer(k['@uuid'],
                                  {'Enhedstype': ['Alfabetisk']})

            if k['@kaldenavn'] == 'Tilknytningstyper':
                self.create_typer(k['@uuid'],
                                  {'Engagementstype': ['Alfabetisk']})

            if k['@kaldenavn'] == 'Leder':
                self.create_typer(k['@uuid'], {'Lederansvar': ['Ansvar'],
                                               'Ledertyper': ['Typer']})

            if k['@kaldenavn'] == 'SD løn enhedstyper':
                self.create_typer(k['@uuid'], {'Enhedstyper':
                                               ['sd_loen_enhedstyper']})

            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'Adressetype':
                                               ['Lokation typer',
                                                'Egenskaber',
                                                'Engagement typer']})

    def _read_ous_from_apos(self, org_uuid):
        url = "app-organisation/GetEntireHierarchy?uuid={}"
        r = self._apos_lookup(url.format(org_uuid))
        org_units = r['node']
        return org_units

    def _create_ou_from_apos(self, apos_unit, parent=None, enhedstype=None):
        """ Create a MO org_unit from the corresponding APOS object,
        includes looking up address information """
        url = "app-organisation/GetUnitDetails?uuid={}"
        r = self._apos_lookup(url.format(apos_unit['@uuid']))
        details = r['enhed']
        fra, til = _format_time(details['gyldighed'])

        # If enhedstype is not hard-coded, we take it from APOS
        if not enhedstype:
            enhedstype = details['@enhedstype']

        unit = self.org.OrganisationUnit.add(
            identifier=apos_unit['@uuid'],
            name=apos_unit['@navn'],
            user_key=apos_unit['@brugervendtNoegle'],
            org_unit_type_ref=enhedstype,
            date_from=fra,
            date_to=til,
            parent_ref=parent)

        location = self.read_locations(apos_unit)
        if 'pnummer' in location:
            self.org.OrganisationUnit.add_type_address(
                identifier=apos_unit['@uuid'],
                address_type_ref='PNUMBER',
                value=location['pnummer'],
                date_from=None)
        if 'dawa_uuid' in location:
            self.org.OrganisationUnit.add_type_address(
                identifier=apos_unit['@uuid'],
                address_type_ref='AdressePost',
                value=location['dawa_uuid'],
                date_from=None)
        return unit

    def create_associations_for_ou(self, unit):
        """ Returnerer tilsyneladende personer som ikke er ansat.
        Dette skal undersges.
        """
        url = "app-organisation/GetAttachedPersonsForUnit?uuid={}"
        associations = self._apos_lookup(url.format(unit))
        if int(associations['total']) > 0:
            for person in associations['person']:
                for p in person:
                    pass

    def update_contact_information(self, employee):
        kontakt = employee['klassifikationKontaktKanaler']
        if kontakt is None:
            return
        kontaktmuligheder = kontakt['klassifikationKontaktKanal']
        for kontaktmulighed in kontaktmuligheder:
            if isinstance(kontaktmulighed, str):
                return
            value = kontaktmulighed['@vaerdi']
            if value:
                apos_type = kontaktmulighed['@type']
                data = self.org.Klasse.get(apos_type)['data']
                employee_identifier = employee['person']['@uuid']

                if data['titel'] == 'E-mail':
                    klasse_ref = 'Email'
                elif data['titel'] == 'Kontakt Tlf.':
                    klasse_ref = 'Telefon'
                elif data['titel'] == 'Alt. Tlf.':
                    klasse_ref = apos_type
                else:  # This should never happen
                    print(data['titel'])
                    raise Exception('Ukendt kontaktmulighed')
                try:
                    self.org.Employee.add_type_address(
                        owner_ref=employee_identifier,
                        value=value,
                        address_type_ref=klasse_ref,
                        date_from=None,
                        date_to=None
                    )
                except AssertionError:
                    pass  # Already inserted

    def update_tasks(self, employee, unit):
        tasks = employee['opgaver']['opgave']
        fra, til = _format_time(employee['gyldighed'])
        if isinstance(tasks, list):
            assert(len(tasks) == 2)
            assert(tasks[0]['@klassifikation'] == 'stillingsbetegnelser')
            assert(tasks[1]['@klassifikation'] == 'tilknytningstyper')
            stilling = tasks[0]['@uuid']
            tilknytning = tasks[1]['@uuid']
            if not tilknytning == '56e1214a-330f-4592-89f3-ae3ee8d5b2e6':
                # Tilknyting bør altid være 'Ansat', vi ignorerer # derfor
                # denne oplysning, men kan debugge her hvis der opstår behov
                pass
        else:
            assert(tasks['@klassifikation'] == 'stillingsbetegnelser')
            stilling = tasks['@uuid']

        engagement_ref = '56e1214a-330f-4592-89f3-ae3ee8d5b2e6'  # Ansat
        self.org.Employee.add_type_engagement(
            owner_ref=employee['person']['@uuid'],
            org_unit_ref=unit,
            job_function_ref=stilling,
            engagement_type_ref=engagement_ref,
            date_from=fra)

    def create_employees_for_ou(self, unit):
        url = 'composite-services/GetEngagementDetailed?unitUuid={}'
        medarbejdere = self._apos_lookup(url.format(unit))
        if 'total' not in medarbejdere:
            return   # This unit has no employees
        if medarbejdere['total'] == '0':
            return
        elif medarbejdere['total'] == '1':
            medarbejdere = [medarbejdere['engagementer']['engagement']]
        else:
            medarbejdere = medarbejdere['engagementer']['engagement']

        # Be carefull, the employee has uuids both as persons and as
        # employees
        for medarbejder in medarbejdere:
            person = medarbejder['person']
            name = person['@fornavn'] + ' '
            if person['@mellemnavn']:
                name += person['@mellemnavn']
            name += person['@efternavn']
            fra, til = _format_time(medarbejder['gyldighed'])
            bvn = medarbejder['@brugervendtNoegle']

            try:
                self.org.Employee.get(person['@uuid'])
                self.duplicate_persons[person['@uuid']] = person
                # Some employees are duplicated, skip them and remember them.
                continue
            except KeyError:
                pass
            self.org.Employee.add(name=name,
                                  identifier=person['@uuid'],
                                  cpr_no=person['@personnummer'],
                                  user_key=bvn,
                                  date_from=fra,
                                  date_to=til)
            self.update_contact_information(medarbejder)
            self.update_tasks(medarbejder, unit)

    def get_ou_functions(self, unit):
        url = 'app-organisation/GetFunctionsForUnit?uuid={}'
        org_funcs = self._apos_lookup(url.format(unit))
        if int(org_funcs['total']) == 0:
            return
        for func in org_funcs['function']:
            if not isinstance(func, collections.OrderedDict):
                continue  # This func is empty

            fra, til = _format_time(func['gyldighed'])
            opus_persons = func['persons']
            if not opus_persons:
                continue
            if not opus_persons['person']:
                continue  # Vacant manager?

            personer = opus_persons['person']
            if not isinstance(personer, list):
                personer = [personer]

            for person in personer:
                if not person['@uuid']:
                    continue

                assert(func['units']['unit']['@uuid'] == unit)
                tasks = func['tasks']['task']

                manager_type = None
                manager_responsibility = []

                for task in tasks:
                    try:
                        klasse = task['@uuid']
                    except TypeError:
                        continue

                    try:  # We have a few problematic Klasser, chack manually
                        self.org.Klasse.get(klasse)
                    except KeyError:
                        self.klassifikation_errors[klasse] = True
                        continue

                    klasse_ref = self.org.Klasse.get(klasse)
                    facet = klasse_ref['facet_type_ref']

                    if facet == 'Ledertyper':
                        manager_type = klasse
                    elif facet == 'Lederansvar':
                        manager_responsibility.append(klasse)
                    elif facet in ('Stillingsbetegnelse', 'Engagementstype'):
                        pass
                    else:
                        print('WARNING')

                if manager_type:
                    try:
                        self.org.Employee.add_type_manager(
                            owner_ref=person['@uuid'],
                            org_unit_ref=unit,
                            manager_level_ref=None,  # TODO?
                            address_uuid=None,  # TODO?
                            manager_type_ref=manager_type,
                            responsibility_list=manager_responsibility,
                            date_from=fra,
                            date_to=til
                        )
                    except KeyError:
                        print('Problem adding manager:')
                        print(person['@uuid'])

    def create_ou_tree(self, org_uuid, enhedstype=None):
        org_units = self._read_ous_from_apos(org_uuid)
        nodes = {}
        # The root org is always first row in APOS
        objectid = int(org_units[0]['@objectid'])
        nodes[objectid] = self._create_ou_from_apos(org_units[0])
        del(org_units[0])

        while org_units:
            remaining_org_units = []
            new = {}
            for unit in org_units:
                over_id = int(unit['@overordnetid'])
                unit_id = int(unit['@objectid'])
                if over_id in nodes.keys():
                    new[unit_id] = self._create_ou_from_apos(
                        unit,
                        nodes[over_id],
                        enhedstype=enhedstype)
                    self.create_employees_for_ou(unit['@uuid'])
                else:
                    remaining_org_units.append(unit)
            print(len(remaining_org_units))
            org_units = remaining_org_units
            nodes.update(new)

    def create_managers(self):
        """ Create org_funcs, at the momen this means managers """
        units = apos_import.org.OrganisationUnit.export()
        for unit in units:
            self.get_ou_functions(unit[0])


if __name__ == '__main__':
    apos_import = AposImport('Ballerup APOS 1')

    apos_import.create_facetter_and_klasser()
    apos_import.create_ou_tree('b78993bb-d67f-405f-acc0-27653bd8c116')
    sd_enhedstype = '324b8c95-5ff9-439b-a49c-1a6a6bba4651'
    apos_import.create_ou_tree('945bb286-9753-4f77-9082-a67a5d7bdbaf',
                               enhedstype=sd_enhedstype)
    apos_import.create_managers()

    ballerup = ImportUtility(apos_import.org, dry_run=True)
    #ballerup.import_all()

    #exit()  # <---- NOTICE!

    print('********************************')
    print('Address challenges:')
    for challenge in apos_import.address_challenges:
        print(apos_import.address_challenges[challenge])
    print()

    print('Address Errors:')
    for error in apos_import.address_errors:
        print(apos_import.address_errors[error])
    print()

    print('Klassifikation Errors:')
    for uuid, error in apos_import.klassifikation_errors.items():
        print(uuid)
    print()

    # print('Duplicate people:')
    # for uuid, person in apos_import.duplicate_persons.items():
    #    print(person['@adresseringsnavn'])
