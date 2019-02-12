#!/usr/bin/env python3
#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import re
import pickle
import requests
import xmltodict
import collections
# from datetime import datetime
from uuid import UUID


MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'APOS Import')
GLOBAL_DATE = os.environ.get('GLOBAL_DATE', '1977-01-01')
BASE_APOS_URL = os.environ.get('BASE_APOS_URL', 'http://localhost:8080/apos2-')

# Phone names will become a list once we have more examples of naming of
# the concept phone numbers
PHONE_NAMES = os.environ.get('PHONE_NAMES', 'Telefon').split(':')

ANSAT_UUID = os.environ.get('ANSAT_UUID', '00000000-0000-0000-0000-000000000000')
CREATE_UDVALGS_CLASSES = os.environ.get('CREATE_UDVALGS_CLASSES', 'No') == 'yes'
EMAIL_NAME = os.environ.get('EMAIL_NAME', 'Email')
MAIN_PHONE_NAME = os.environ.get('MAIN_PHONE_NAME', 'Telefon')


def _format_time(gyldighed):
    from_time = GLOBAL_DATE
    to_time = None
    # NOTICE: DATES ARE INCONSISTENT, CURRENTLY, WE RETURN A
    # FIXED DATE!!!!!!!!
    """
    if not gyldighed['@fra'] == '-INFINITY':
        from_time = datetime.strptime(gyldighed['@fra'], '%d/%m/%Y')
        from_time = from_time.strftime('%Y-%m-%d')
    if not gyldighed['@til'] == 'INFINITY':
        to_time = datetime.strptime(gyldighed['@til'], '%d/%m/%Y')
        to_time = to_time.strftime('%Y-%m-%d')
    """
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

    def __init__(self, importer, org_name, municipality_code):
        self.base_url = BASE_APOS_URL

        self.importer = importer
        self.importer.add_organisation(
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        self.object_to_uuid = {}  # Mapping of Opus object ID to Opus UUID
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
            print(self.base_url + url)
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
            apos_address = self._apos_lookup(url.format(uuid))
            dawa_uuid = self.dawa_lookup(apos_address['adresse'])
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
                    # Add more here if necessary
                    if klasse['@title'] in PHONE_NAMES:
                        scope = 'PHONE'
                    else:
                        scope = 'TEXT'
                    try:
                        UUID(klasse['@uuid'], version=4)
                        uuid = klasse['@uuid']
                    except ValueError:
                        uuid = None

                    self.importer.add_klasse(identifier=klasse['@uuid'],
                                             uuid=uuid,
                                             title=klasse['@title'],
                                             user_key=klasse['@title'],
                                             scope=scope,
                                             facet_type_ref=mo_facet_navn)

    def create_facetter_and_klasser(self):
        url = "app-klassifikation/GetKlassifikationList"
        r = self._apos_lookup(url)
        klassifikationer = r['klassifikation']
        for k in klassifikationer:
            if k['@kaldenavn'] == 'Stillingsbetegnelser':
                self.create_typer(k['@uuid'],
                                  {'engagement_job_function': ['Alfabetisk']})
            if k['@kaldenavn'] == 'Rolle':
                self.create_typer(k['@uuid'],
                                  {'role_type': ['Rolle']})

            if k['@kaldenavn'] == 'Enhedstyper':
                self.create_typer(k['@uuid'],
                                  {'org_unit_type': ['Alfabetisk']})

            if k['@kaldenavn'] == 'Tilknytningstyper':
                self.create_typer(k['@uuid'],
                                  {'engagement_type': ['Alfabetisk']})

            if k['@kaldenavn'] == 'AM/MED':
                self.create_typer(k['@uuid'],
                                  {'association_type': ['Repræsentanttyper',
                                                        'Medlemstyper',
                                                        'Forbund']})

            if k['@kaldenavn'] == 'Leder':
                self.create_typer(k['@uuid'], {'responsibility': ['Ansvar'],
                                               'manager_type': ['Typer']})

            if k['@kaldenavn'] == 'SD løn enhedstyper':
                self.create_typer(k['@uuid'], {'org_unit_type':
                                               ['sd_loen_enhedstyper']})

            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'employee_address_type':
                                               ['Lokation typer',
                                                'Egenskaber',
                                                'Engagement typer']})

        if CREATE_UDVALGS_CLASSES:
            specific_klasser = [
                {'titel': 'AMR', 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'titel': 'H-MED', 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'titel': 'C-MED', 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'titel': 'L-MED', 'facet': 'org_unit_type', 'scope': 'TEXT'}
            ]
        else:
            specific_klasser = []
        standard_klasser = [
            {'titel': 'Lederniveau',
             'facet': 'manager_level',
             'scope': 'TEXT'},
            {'titel': 'Email',
             'facet': 'employee_address_type',
             'scope': 'EMAIL'},
            {'titel': 'Telefon',
             'facet': 'employee_address_type',
             'scope': 'PHONE'},
            {'titel': 'p-nummer',
             'facet': 'org_unit_address_type',
             'scope': 'PNUMBER'},
            {'titel': 'AdressePost',
             'facet': 'org_unit_address_type',
             'scope': 'DAR'}
        ]

        for klasse in specific_klasser + standard_klasser:
            self.importer.add_klasse(identifier=klasse['titel'],
                                     title=klasse['titel'],
                                     user_key=klasse['titel'],
                                     scope=klasse['scope'],
                                     facet_type_ref=klasse['facet'])

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
        if 'overordnet' in details:
            gyldighed = {'@fra': details['overordnet']['@fra'],
                         '@til': details['overordnet']['@til']
                         }
            fra, til = _format_time(gyldighed)
        else:
            fra, til = _format_time(details['gyldighed'])
        unit_id = int(apos_unit['@objectid'])

        # If enhedstype is not hard-coded, we take it from APOS
        if not enhedstype:
            enhedstype = details['@enhedstype']

        unit = self.importer.add_organisation_unit(
            identifier=unit_id,
            uuid=apos_unit['@uuid'],
            name=apos_unit['@navn'],
            user_key=apos_unit['@brugervendtNoegle'],
            type_ref=enhedstype,
            date_from=fra,
            date_to=til,
            parent_ref=parent)

        locations = self.read_locations(apos_unit)

        for location in locations:
            if location['pnummer']:
                try:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='p-nummer',
                        value=location['pnummer'],
                        date_from=GLOBAL_DATE)
                except AssertionError:  # pnumber added multiple times
                    pass
            if location['dawa_uuid']:
                try:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='AdressePost',
                        value=location['dawa_uuid'],
                        date_from=GLOBAL_DATE)
                except AssertionError:  # Address already added
                    pass
        return unit

    def create_associations_for_ou(self, unit):
        """ Returnerer tilsyneladende personer som ikke er ansat.
        Dette skal undersges.
        """
        url = "app-organisation/GetAttachedPersonsForUnit?uuid={}"
        uuid = self.object_to_uuid[unit]
        added_persons = []
        associations = self._apos_lookup(url.format(uuid))
        if int(associations['total']) > 0:
            persons = associations['person']
            if not isinstance(persons, list):
                persons = [persons]

            for person in persons:
                for p in person.values():
                    if p in added_persons:
                        # Some persons are reported more than once?
                        break
                    else:
                        added_persons.append(p)
                    if not self.importer.check_if_exists('employee', p):
                        print('Association error: {}'.format(p))
                        break

                    details = self.importer.get_details(owner_type="employee",
                                                        owner_ref=p,
                                                        type_id="engagement")
                    from_date = details[0].date_from
                    job_function = details[0].job_function_ref

                    ansat = ANSAT_UUID

                    self.importer.add_association(
                        employee=p,
                        organisation_unit=unit,
                        job_function_ref=job_function,
                        association_type_ref=ansat,
                        date_from=from_date
                    )

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

                klasse = self.importer.get('klasse', apos_type)
                employee_identifier = employee['person']['@uuid']

                if klasse.title == EMAIL_NAME:
                    klasse_ref = 'Email'
                elif klasse.title == MAIN_PHONE_NAME:
                    klasse_ref = 'Telefon'
                elif klasse.title in PHONE_NAMES:
                    klasse_ref = apos_type
                else:  # This should never happen
                    print(klasse.title)
                    raise Exception('Ukendt kontaktmulighed')
                try:
                    self.importer.add_address_type(
                        employee=employee_identifier,
                        value=value,
                        type_ref=klasse_ref,
                        date_from=GLOBAL_DATE,
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

        if not (
                self.importer.check_if_exists('klasse', stilling) and
                self.importer.check_if_exists('organisation_unit', unit) and
                self.importer.check_if_exists('employee',
                                              employee['person']['@uuid'])
        ):
            print(employee)
            print('Medarbejder ukendt')
            1/0

        engagement_ref = '56e1214a-330f-4592-89f3-ae3ee8d5b2e6'  # Ansat
        self.importer.add_engagement(
            employee=employee['person']['@uuid'],
            uuid=employee['@uuid'],
            organisation_unit=unit,
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
            objectid = int(medarbejder['pathToRoot']['enhed'][-1]['@objectid'])
            person = medarbejder['person']
            name = person['@fornavn'] + ' '
            if person['@mellemnavn']:
                name += person['@mellemnavn']
            name += person['@efternavn']
            fra, til = _format_time(medarbejder['gyldighed'])
            bvn = medarbejder['@brugervendtNoegle']

            """
            try:
                self.org.Employee.get(person['@uuid'])
                self.duplicate_persons[person['@uuid']] = person
                # Some employees are duplicated, skip them and remember them.
                continue
            except KeyError:
                pass
            """
            # TODO: VERIFY THAT THIS IS THE SAME AS THE ABOVE
            if self.importer.check_if_exists('employee', person['@uuid']):
                self.duplicate_persons[person['@uuid']] = person
                # Some employees are duplicated, skip them and remember them.
                continue

            self.importer.add_employee(name=name,
                                       uuid=person['@uuid'],
                                       identifier=person['@uuid'],
                                       cpr_no=person['@personnummer'],
                                       user_key=bvn)
            self.update_contact_information(medarbejder)
            self.update_tasks(medarbejder, objectid)

    def get_ou_functions(self, unit):
        url = 'app-organisation/GetFunctionsForUnit?uuid={}'
        uuid = self.object_to_uuid[unit]
        org_funcs = self._apos_lookup(url.format(uuid))

        if int(org_funcs['total']) == 0:
            return
        for func in org_funcs['function']:
            if not isinstance(func, collections.OrderedDict):
                continue  # This func is empty

            fra, til = _format_time(func['gyldighed'])
            apos_persons = func['persons']
            if not apos_persons:
                continue
            if not apos_persons['person']:
                continue  # Vacant manager?

            personer = apos_persons['person']
            if not isinstance(personer, list):
                personer = [personer]

            # Only one person should ever be in this list, but sometimes
            # empty entries are also included. These are filtered out by
            # abcense of a person uuid.
            for person in personer:
                if not person['@uuid']:
                    continue

                assert(func['units']['unit']['@uuid'] == uuid)
                tasks = func['tasks']['task']

                manager_type = None
                manager_responsibility = []

                for task in tasks:
                    try:
                        klasse = task['@uuid']
                    except TypeError:
                        continue

                    """ This is the old procedure, check the new one works
                    try:  # We have a few problematic Klasser, chack manually
                        self.importer.get('klasse', klasse)
                    except KeyError:
                        self.klassifikation_errors[klasse] = True
                        continue
                    """
                    klasse_ref = self.importer.get('klasse', klasse)
                    # We have a few problematic Klasser, chack manually
                    if klasse_ref is None:
                        self.klassifikation_errors[klasse] = True
                        continue

                    facet = klasse_ref.facet_type_ref

                    if facet == 'manager_type':
                        manager_type = klasse
                    elif facet == 'responsibility':
                        manager_responsibility.append(klasse)
                    elif facet in ('engagement_job_function', 'engagement_type'):
                        pass
                    else:
                        print('WARNING')

                if manager_type:
                    try:
                        self.importer.add_manager(
                            employee=person['@uuid'],
                            organisation_unit=unit,
                            manager_level_ref='Lederniveau',
                            address_uuid=None,  # TODO?
                            manager_type_ref=manager_type,
                            responsibility_list=manager_responsibility,
                            uuid=func['@uuid'],
                            date_from=fra,
                            date_to=til
                        )
                    except ReferenceError:
                        print('Problem adding manager:')
                        print(person['@uuid'])

    def create_ou_tree(self, org_uuid, enhedstype=None):
        org_units = self._read_ous_from_apos(org_uuid)
        nodes = {}
        # The root org is always first row in APOS
        objectid = int(org_units[0]['@objectid'])
        self.object_to_uuid[objectid] = org_units[0]['@uuid']
        nodes[objectid] = self._create_ou_from_apos(org_units[0])
        del(org_units[0])

        while org_units:
            remaining_org_units = []
            new = {}
            for unit in org_units:
                over_id = int(unit['@overordnetid'])
                unit_id = int(unit['@objectid'])
                if over_id in nodes.keys():
                    self.object_to_uuid[unit_id] = unit['@uuid']
                    new[unit_id] = self._create_ou_from_apos(
                        unit,
                        over_id,
                        enhedstype=enhedstype)
                    self.create_employees_for_ou(unit['@uuid'])
                else:
                    remaining_org_units.append(unit)
            print(len(remaining_org_units))
            org_units = remaining_org_units
            nodes.update(new)

    def create_managers_and_associatins(self):
        """ Create org_funcs, at the momen this means managers """
        # units = self.org.OrganisationUnit.export()
        units = self.importer.export('organisation_unit')
        for unit in units:
            self.get_ou_functions(unit)
            self.create_associations_for_ou(unit)
