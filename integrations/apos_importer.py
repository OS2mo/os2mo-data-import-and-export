#!/usr/bin/env python3
#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import pickle
import logging
import requests
import xmltodict
import collections
from integrations import dawa_helper
from datetime import datetime
from uuid import UUID

LOG_LEVEL = logging.DEBUG
LOG_FILE = 'mo_integrations.log'

logger = logging.getLogger("aposImport")

for name in logging.root.manager.loggerDict:
    if name in ('aposImport', 'moImporterMoraTypes', 'moImporterMoxTypes',
                'moImporterUtilities', 'moImporterHelpers', 'mora-helper'):
        logging.getLogger(name).setLevel(LOG_LEVEL)
    else:
        logging.getLogger(name).setLevel(logging.WARNING)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(name)s %(message)s',
    level=LOG_LEVEL,
    filename=LOG_FILE
)

logger.info('Apos import started')

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'APOS Import')
BASE_APOS_URL = os.environ.get('BASE_APOS_URL', 'http://localhost:8080/apos2-')

# Phone names will become a list once we have more examples of naming of
# the concept phone numbers
PHONE_NAMES = os.environ.get('PHONE_NAMES', 'Telefon').split(':')

ANSAT_UUID = os.environ.get('ANSAT_UUID', '00000000-0000-0000-0000-000000000000')
CREATE_UDVALGS_CLASSES = os.environ.get('CREATE_UDVALGS_CLASSES', 'no') == 'yes'
EMAIL_NAME = os.environ.get('EMAIL_NAME', 'Email')
MAIN_PHONE_NAME = os.environ.get('MAIN_PHONE_NAME', 'Telefon')
ALT_PHONE_NAME = os.environ.get('ALT_PHONE_NAME', None)


def _format_time(gyldighed):
    from_time = '1920-01-01'
    to_time = None
    if not gyldighed['@fra'] == '-INFINITY':
        from_time = datetime.strptime(gyldighed['@fra'], '%d/%m/%Y')
        from_time = from_time.strftime('%Y-%m-%d')
    if not gyldighed['@til'] == 'INFINITY':
        to_time = datetime.strptime(gyldighed['@til'], '%d/%m/%Y')
        to_time = to_time.strftime('%Y-%m-%d')

    if from_time is None and to_time is None:
        raise Exception('Invalid validity: {}'.format(gyldighed))
    return from_time, to_time


class AposImport(object):

    def __init__(self, importer, org_name, municipality_code, org_uuid=None, ean={}):
        self.base_url = BASE_APOS_URL

        self.importer = importer
        self.importer.add_organisation(
            uuid=org_uuid,
            identifier=org_name,
            user_key=org_name,
            municipality_code=municipality_code
        )

        self.ean = ean
        self.object_to_uuid = {}  # Mapping of Apos object ID to Apos UUID
        self.address_challenges = {}  # Needs support in dawa helper
        self.duplicate_persons = {}
        self.address_errors = {}  # Needs support in dawa helper
        self.klassifikation_errors = {}

    def _apos_lookup(self, url):
        path_url = url.replace('/', '_')
        try:
            with open('tmp/' + path_url + '.p', 'rb') as f:
                response = pickle.load(f)
        except FileNotFoundError:
            response = requests.get(self.base_url + url)
            with open('tmp/' + path_url + '.p', 'wb') as f:
                pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

        xml_response = xmltodict.parse(response.text)
        # Response is always hidden one level down
        outer_key = list(xml_response.keys())[0]
        return xml_response[outer_key]

    def read_locations(self, unit):
        # url = 'app-organisation/GetOrganisationEnhedIntegration?uuid={}'
        # integration_values = self._apos_lookup(url.format(unit['@uuid']))
        # print(integration_values)

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
            address_string = None
            if '@adresse' not in location:
                if len(location['@navn']) > 0:
                    address = location['@navn'].split(' ')
                    if len(address) == 1:
                        continue
                    zip_code = address[-2][:4]
                    address_string = ''
                    for i in range(0, len(address) - 2):
                        address_string += address[i] + ' '
                    address_string = address_string[:-2]
            else:
                uuid = location['@adresse']
                url = 'app-part/GetAdresseList?uuid={}'
                apos_address = self._apos_lookup(url.format(uuid))
                address_string = apos_address['adresse']['@vejadresseringsnavn']
                zip_code = apos_address['adresse']['@postnummer']

            if address_string:
                dawa_uuid = dawa_helper.dawa_lookup(address_string, zip_code)
            else:
                dawa_uuid = None

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
                    logger.debug('{}: {}'.format(klasse['@uuid'], klasse['@title']))
                    if klasse['@uuid'] in PHONE_NAMES:
                        scope = 'PHONE'
                    elif klasse['@uuid'] == EMAIL_NAME:
                        scope = 'EMAIL'
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

            if k['@kaldenavn'] == 'SD løn enhedstyper':
                self.create_typer(k['@uuid'], {'time_planning':
                                               ['Tidsregistrering']})

            """
            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'employee_address_type':
                                               ['Lokation typer',
                                                'Egenskaber',
                                                'Engagement typer']})
            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'org_unit_address_type':
                                               ['Lokation typer']})
            if k['@kaldenavn'] == 'Kontaktkanaler':
                self.create_typer(k['@uuid'], {'employee_address_type':
                                               ['Engagement typer']})
            """

        if CREATE_UDVALGS_CLASSES:
            specific_klasser = [
                {'id': 'AMG', 'titel': 'AMG',
                 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'id': 'H-MED', 'titel': 'H-MED',
                 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'id': 'C-MED', 'titel': 'C-MED',
                 'facet': 'org_unit_type', 'scope': 'TEXT'},
                {'id': 'L-MED', 'titel': 'L-MED',
                 'facet': 'org_unit_type', 'scope': 'TEXT'}
            ]
        else:
            specific_klasser = []
        standard_klasser = [
            {'id': 'Lederniveau',
             'titel': 'Lederniveau',
             'facet': 'manager_level',
             'scope': 'TEXT'},
            {'id': 'EmailEmployee',
             'titel': 'Email',
             'facet': 'employee_address_type',
             'scope': 'EMAIL'},
            {'id': 'PhoneEmployee',
             'titel': 'Telefon',
             'facet': 'employee_address_type',
             'scope': 'PHONE'},
            {'id': 'AltPhoneEmployee',
             'titel': 'Alt Tlf',
             'facet': 'employee_address_type',
             'scope': 'PHONE'},
            {'id': 'p-nummer',
             'titel': 'p-nummer',
             'facet': 'org_unit_address_type',
             'scope': 'PNUMBER'},
            {'id': 'EmailUnit',
             'titel': 'Email',
             'facet': 'org_unit_address_type',
             'scope': 'EMAIL'},
            {'id': 'AddressMailUnit',
             'titel': 'Adresse',
             'facet': 'org_unit_address_type',
             'scope': 'DAR'},
            {'id': 'PhoneUnit',
             'titel': 'Telefon',
             'facet': 'org_unit_address_type',
             'scope': 'PHONE'},
            {'id': 'EAN',
             'titel': 'EAN',
             'facet': 'org_unit_address_type',
             'scope': 'EAN'},
            {'id': 'WebUnit',
             'titel': 'Webadresse',
             'facet': 'org_unit_address_type',
             'scope': 'WWW'},
            {'id': 'Ekstern',
             'titel': 'Må vises eksternt',
             'facet': 'visibility',
             'scope': 'PUBLIC'},
            {'id': 'Intern',
             'titel': 'Må vises internt',
             'facet': 'visibility',
             'scope': 'INTERNAL'},
            {'id': 'Hemmelig',
             'titel': 'Hemmelig',
             'facet': 'visibility',
             'scope': 'SECRET'},
            {'id': 'Teammedarbejder',
             'titel': 'Teammedarbejder',
             'facet': 'association_type',
             'scope': 'TEXT'},
            {'id': 'Projektleder',
             'titel': 'Projektleder',
             'facet': 'association_type',
             'scope': 'TEXT'},
            {'id': 'Projektgruppemedlem',
             'titel': 'Projektgruppemedlem',
             'facet': 'association_type',
             'scope': 'TEXT'},
            # Last three possbily not general to all APOS imports
            {'id': 'Afdelingskode',
             'titel': 'Afdelingskode',
             'facet': 'org_unit_address_type',
             'scope': 'TEXT'},
            {'id': 'Formaalskode',
             'titel': 'Formålskode',
             'facet': 'org_unit_address_type',
             'scope': 'TEXT'},
            {'id': 'Skolekode',
             'titel': 'Skolekode',
             'facet': 'org_unit_address_type',
             'scope': 'TEXT'},
        ]

        for klasse in specific_klasser + standard_klasser:
            self.importer.add_klasse(identifier=klasse['id'],
                                     title=klasse['titel'],
                                     user_key=klasse['id'],
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

        """
        The handling of apos integration-data might turn out to be specfic for
        Ballerup in the current  implementation. Once other imports also needs these
        values, we should make a parameter list.
        """

        # Default i Ballerup:
        time_planning = '41504f53-0203-0028-4158-41504f494e54'
        if details['opgaver']:
            opgaver = details['opgaver']['opgave']
            if not isinstance(opgaver, list):
                opgaver = [opgaver]

            for opgave in opgaver:
                if opgave['@uuid'] in ('41504f53-0203-0028-4158-41504f494e54',
                                       '41504f53-0203-0029-4158-41504f494e54',
                                       '41504f53-0203-0030-4158-41504f494e54'):
                    time_planning = opgave['@uuid']

        unit_id = int(apos_unit['@objectid'])
        if 'overordnet' in details:
            gyldighed = {'@fra': details['overordnet']['@fra'],
                         '@til': details['overordnet']['@til']
                         }
            fra, til = _format_time(gyldighed)
        else:
            fra, til = _format_time(details['gyldighed'])

        enhedstype = details['@enhedstype']

        # NY7
        if enhedstype == '58380fd3-b3fb-4f84-a56f-15c9716972c1':
            enhedstype = '42b9042f-5f20-4998-b0e5-c4deb6c5f42e'

        # NY6
        if enhedstype == '25eebd5c-d774-469c-97af-f8d9ca2780c9':
            enhedstype = '414a035e-9c22-42eb-b035-daa7d7f2ade8'

        # NY5
        if enhedstype == 'b4bd5908-c724-4544-bff3-324832156ff3':
            enhedstype = '819ae28e-04e0-4030-880e-7b699faeaff9'

        # NY4
        if enhedstype == 'f16e9d0c-2692-43d3-8def-62c0c7a75fdf':
            enhedstype = 'ff8c3f53-85ec-44d7-a9d6-07c619ac50df'

        # NY3
        if enhedstype == '2c73e9b0-785d-48a4-b14b-656c596be759':
            enhedstype = '70c69826-4ba1-4e1e-82f0-4c47c89a7ecc'

        # NY2
        if enhedstype == '68fea789-87ce-451c-8ad7-8b7525c26978':
            enhedstype = 'ec882c49-3cc2-4bc9-994f-a6f29136401b'

        # NY1
        if enhedstype == '5cbde7bf-7632-4b6a-ae51-691a89d8df7a':
            enhedstype = 'd9bd186b-3c11-4dbf-92d1-4e3b61140302'

        # Afdelingsniveau
        if enhedstype in ('81fc2195-d782-4ae0-8ab7-56a1f7ad92e2',
                          '58380fd3-b3fb-4f84-a56f-15c9716972c1'):
            enhedstype = '345a8893-eb1f-4e20-b76d-63b95b5809f6'

        apos_int = {}
        if 'integrationAttributter' in details:
            if details['integrationAttributter'] is not None:
                attributter = details['integrationAttributter']['attribut']
                if not isinstance(attributter, list):
                    attributter = [attributter]

                for attribut in attributter:
                    if isinstance(attribut, str):
                        continue

                    if len(attribut['@vaerdi']) > 0:
                        apos_int[attribut['@navn']] = attribut['@vaerdi']

        if 'Afdelingskode' in apos_int:
            user_key = apos_int['Afdelingskode']
            apos_int['Afdelingskode'] = apos_unit['@brugervendtNoegle']
        else:
            user_key = apos_unit['@brugervendtNoegle']

        unit = self.importer.add_organisation_unit(
            identifier=unit_id,
            uuid=apos_unit['@uuid'],
            name=apos_unit['@navn'],
            user_key=user_key,
            time_planning_ref=time_planning,
            type_ref=enhedstype,
            date_from=fra,
            date_to=til,
            parent_ref=parent)

        for key, value in apos_int.items():
            logger.debug('Add address type {}: value {}'.format(key, value))
            self.importer.add_address_type(
                organisation_unit=unit_id,
                type_ref=key,
                value=value,
                date_from=fra
            )

        if apos_unit['@uuid'] in self.ean:
            self.importer.add_address_type(
                organisation_unit=unit_id,
                type_ref='EAN',
                value=self.ean[apos_unit['@uuid']],
                date_from=fra
            )

        # This is most likely the orgunit relation information
        # print(details['tilknyttedeEnheder']))

        locations = self.read_locations(apos_unit)
        for location in locations:
            if location['pnummer']:
                try:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='p-nummer',
                        value=location['pnummer'],
                        date_from=fra)
                except AssertionError:  # pnumber added multiple times
                    pass

            if location['dawa_uuid']:
                try:
                    self.importer.add_address_type(
                        organisation_unit=unit_id,
                        type_ref='AddressMailUnit',
                        value=location['dawa_uuid'],
                        date_from=fra)
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
                        logger.warning('Association error: {}'.format(p))
                        break

                    details = self.importer.get_details(owner_type="employee",
                                                        owner_ref=p,
                                                        type_id="engagement")
                    from_date = details[0].date_from

                    self.importer.add_association(
                        employee=p,
                        organisation_unit=unit,
                        association_type_ref=ANSAT_UUID,
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

                # klasse = self.importer.get('klasse', apos_type)
                employee_identifier = employee['person']['@uuid']
                if apos_type == EMAIL_NAME:
                    klasse_ref = 'EmailEmployee'
                elif apos_type == MAIN_PHONE_NAME:
                    klasse_ref = 'PhoneEmployee'
                elif apos_type == ALT_PHONE_NAME:
                    klasse_ref = 'AltPhoneEmployee'
                else:  # This should never happen
                    logger.error('Ukendt kontaktmulighed: {}'.format(apos_type))
                    raise Exception('Ukendt kontaktmulighed')
                try:
                    fra, til = _format_time(employee['gyldighed'])

                    self.importer.add_address_type(
                        employee=employee_identifier,
                        value=value,
                        type_ref=klasse_ref,
                        # date_from=GLOBAL_DATE,
                        # date_to=None
                        date_from=fra,
                        date_to=til
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
            logger.error('Medarbejder ukendt: {}'.format(employee))
            1/0

        engagement_ref = '56e1214a-330f-4592-89f3-ae3ee8d5b2e6'  # Ansat
        logger.debug(
            'Add engagement, person: {}, from: {}, to: {}'.format(
                employee['person']['@uuid'], fra, til
            )
        )

        self.importer.add_engagement(
            employee=employee['person']['@uuid'],
            uuid=employee['@uuid'],
            user_key=employee['@uuid'],
            organisation_unit=unit,
            job_function_ref=stilling,
            engagement_type_ref=engagement_ref,
            date_from=fra,
            date_to=til)

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
            given_name = person['@fornavn']
            if person['@mellemnavn']:
                given_name = given_name + ' ' + person['@mellemnavn']
            sur_name = person['@efternavn']
            fra, til = _format_time(medarbejder['gyldighed'])
            bvn = medarbejder['@brugervendtNoegle']

            if not self.importer.check_if_exists('employee', person['@uuid']):
                logger.info('New employee: {}'.format(person))

                name = (given_name, sur_name)
                logger.info('Add employee: {}, user_key: {}'.format(name, bvn))
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
                        print(klasse)
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
                        logger.error('Unkown facet: {}'.format(facet))

                if manager_type:
                    try:
                        logger.debug('Add manager: {}'.format(person['@uuid']))
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
                        logger.warning(
                            'Problem adding manager: {}'.format(person['@uuid'])
                        )

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
            logger.debug('Remaining org units: {}'.format(len(remaining_org_units)))
            org_units = remaining_org_units
            nodes.update(new)

    def create_managers_and_associatins(self):
        """ Create org_funcs, at the momen this means managers """
        # units = self.org.OrganisationUnit.export()
        units = self.importer.export('organisation_unit')
        for unit in units:
            self.get_ou_functions(unit)
            self.create_associations_for_ou(unit)
        logger.info('Finished creating managers')

    def add_all_missing_employees(self):
        """
        Call this function to retrive any person known by apos and add them to MO
        if they are not already imported at this point.
        """
        url = 'app-part/GetPersonList'
        persons = self._apos_lookup(url)

        for person in persons['person']:
            given_name = person['@fornavn'] + ' '
            if person['@mellemnavn']:
                given_name += person['@mellemnavn']
            sur_name = person['@efternavn']
            name = (given_name, sur_name)

            cpr = person['@personnummer']
            if not len(cpr) == 10:
                # print('Unable to import {}'.format(person))
                continue

            if not self.importer.check_if_exists('employee', person['@uuid']):
                logger.info('New employee: {}'.format(person))
                self.importer.add_employee(
                    name=name,
                    uuid=person['@uuid'],
                    identifier=person['@uuid'],
                    cpr_no=person['@personnummer'],
                    user_key=person['@uuid']
                )
        logger.info('Finished importig remaining employees')
