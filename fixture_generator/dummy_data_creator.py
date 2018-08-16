""" Create dummy data to populate MO """
import csv
import uuid
import random


class CreateDummyOrg(object):
    """ Create a dummy organisation to use as test data """

    def __init__(self, root_org):
        self.root_org = root_org
        self.names = {}
        self.export_path = './fixtures'
        self.used_bvns = []
        self.names['first'] = self._load_names('fornavne.txt')
        self.names['middle'] = self._load_names('mellemnavne.txt')
        self.names['last'] = self._load_names('efternavne.txt')
        self.uuids = {}
        self.uuids['org_id'] = ''
        self.uuids['klassifikation'] = ''
        self.uuids['email'] = ''
        self.uuids['adresse'] = ''
        self.uuids['telefon'] = ''

    def _load_names(self, name_file):
        """ Load a weighted list of names
        :param name_file: Name of the text file with names
        :return: A weighted list of names
        """
        with open(name_file, 'r') as f:
            names_file = f.read()
        name_lines = names_file.split('\n')
        names = []
        for name_set in name_lines:
            try:
                parts = name_set.split('\t')
                if parts[2].find(',') > 0:
                    subnames = parts[2].split(',')
                    for subname in subnames:
                        names.append([int(parts[1]), subname])
                else:
                    names.append([int(parts[1]), parts[2]])
            except IndexError:
                pass
        return names

    def _pick_name_from_list(self, name_type):
        """ Pick a name
        :param name_type: Type of name, first, middle or last
        :return: A name
        """
        names = self.names[name_type]
        total_weight = 0
        for name in names:
            total_weight += name[0]
        weight = 0
        stop_weight = random.randrange(total_weight)
        for name in names:
            weight += name[0]
            if weight > stop_weight:
                break
        return name[1]

    def _telefon(self):
        tlf = str(random.randrange(1, 9))
        for i in range(0, 6):
            tlf += str(random.randrange(0, 9))
        return tlf

    def _adresse(self):
        """ Create a Danish adresse """
        # TODO: We should use the open adresse data to create realistic data
        # in the same way names are currently created
        adresse = {}
        adresse['postnummer'] = 2300
        adresse['postdistrikt'] = 'København S'
        adresse['adresse'] = 'Vejlands Alle {}'.format(random.randrange(999))
        return adresse

    def create_name(self, bvn=False):
        """ Create a full name
        :return: The full name as a string
        """
        first = self._pick_name_from_list('first')

        middle = ''
        if random.random() > 0.3:
            middle = middle + ' ' + self._pick_name_from_list('middle') + ' '
        if random.random() > 0.9:
            middle = middle + ' ' + self._pick_name_from_list('middle') + ' '

        last = self._pick_name_from_list('last')
        name = first + middle + last
        bvn = first + last[0]
        i = 0
        while bvn in self.used_bvns:
            i = i + 1
            bvn = first + last[0:i]
        self.used_bvns.append(bvn)

        if bvn:
            return name, bvn
        else:
            return name

    def create_organisation(self):
        org_uuid = uuid.uuid4()
        self.uuids['org_id'] = org_uuid
        organisation = {}
        organisation['objektid'] = org_uuid
        organisation['note'] = 'Plads til noter'
        organisation['fra'] = '1964-05-24 00:00:00'
        organisation['til'] = 'infinity'
        organisation['brugervendtnoegle'] = self.root_org
        organisation['virksomhed'] = 99999999  # CVR Nummer
        organisation['gyldighed'] = 'Aktiv'
        organisation['myndighed'] = 151  # Hvad er dette?
        organisation['mydighedstype'] = 'Kommune'
        return organisation

    def create_bruger(self, tilhoerer_uuid):
        bruger_id = uuid.uuid4()
        navn, bvn = self.create_name(bvn=True)
        bruger = {}
        bruger['operation'] = ''
        bruger['objektid'] = bruger_id
        bruger['note'] = 'Note'
        bruger['fra'] = '1964-05-24 00:00:00'
        bruger['til'] = 'infinity'
        bruger['brugervendtnoegle'] = bvn
        bruger['brugernavn'] = navn
        adresse = self_adresse()
        bruger['adresse'] = adresse['adresse']
        bruger['postnummer'] = adresse['postnummer']
        bruger['postdistrikt'] = adresse['postdistrikt']
        bruger['adresse_type'] = self.uuids['adresse']
        bruger['email'] = bvn + '@' + self.root_org + '.dk'
        bruger['email_type'] = self.uuids['email']
        bruger['telefon'] = self._telefon()
        bruger['telefon_type'] = self.uuids['telefon']
        bruger['brugertype'] = 'demo'
        bruger['tilhoerer'] = tilhoerer_uuid
        bruger['gyldighed'] = 'Aktiv'
        bruger['tilknyttedepersoner'] = ''
        bruger['tilknyttedeitsystemer'] = ''  # TODO: Get a random it-system
        return bruger

    def create_it_system(self, bvn, navn):
        it_sys = {}
        it_sys['objektid'] = uuid.uuid4()
        it_sys['note'] = 'Note'
        it_sys['fra'] = '1900-01-01 00:00:00'
        it_sys['til'] = 'infinity'
        it_sys['brugervendtnoegle'] = bvn
        it_sys['itsystemnavn'] = navn
        it_sys['itsystemtype'] = ''
        it_sys['konfigurationreference'] = ''
        it_sys['tilhoerer'] = self.uuids['org_id']
        it_sys['tilhoerer_type'] = 'organisation'
        it_sys['tilknyttedeorganisationer'] = ''
        it_sys['tilknyttedeorganisationer_bvn'] = ''
        it_sys['gyldighed'] = 'Aktiv'
        return it_sys

    def create_klassifikation(self):
        klassifikation = {}
        klassifikation['objektid'] = uuid.uuid4()
        klassifikation['note'] = 'Note'
        klassifikation['fra'] = '1900-01-01 00:00:00'
        klassifikation['til'] = 'infinity'
        klassifikation['brugervendtnoegle'] = '{}s typer'.format(self.root_org)
        klassifikation['brugerref_bvn'] = ''
        klassifikation['beskrivelse'] = '{}s typer'.format(self.root_org)
        klassifikation['kaldenavn'] = '{}s typer'.format(self.root_org)
        klassifikation['ophavsret'] = ''
        klassifikation['brugerref'] = ''
        klassifikation['registreret'] = ''
        klassifikation['livscykluskode'] = ''
        klassifikation['ansvarlig_type'] = ''
        klassifikation['ansvarlig'] = self.uuids['org_id']
        klassifikation['ansvarlig_bvn'] = ''
        klassifikation['ejer_type'] = ''
        klassifikation['ejer'] = ''
        klassifikation['ejer_bvn'] = ''
        klassifikation['publiceret'] = 'Publiceret'
        return klassifikation

    def create_klasse(self, titel, facet):
        klasse = {}
        klasse['objektid'] = uuid.uuid4()
        klasse['note'] = 'Plads til noter'
        klasse['fra'] = '1900-01-01 00:00:00'
        klasse['til'] = 'infinity'
        klasse['brugervendtnoegle'] = titel
        klasse['brugerref_bvn'] = ''
        klasse['brugerref'] = ''
        klasse['registreret'] = '2016-04-11 15:38:58.691932'
        klasse['livscykluskode'] = 'Opstaaet'
        klasse['titel'] = titel
        klasse['ansvarlig_type'] = 'organisation'
        klasse['ansvarlig'] = self.uuids['org_id']
        klasse['ejer_type'] = ''
        klasse['ejer'] = ''
        klasse['ejer_bvn'] = ''
        klasse['facet_type'] = 'facet'
        klasse['facet'] = facet
        klasse['publiceret'] = 'Publiceret'
        klasse['omfang'] = ''
        klasse['beskrivelse'] = ''
        klasse['eksempel'] = ''
        klasse['overordnetklasse'] = ''
        klasse['overordnetklasse_bvn'] = ''
        klasse['aendringsnotat'] = ''
        klasse['mapninger'] = ''
        klasse['mapninger_bvn'] = ''
        return klasse

    def create_facet(self, facet):
        facet = {}
        facet['objektid'] = uuid.uuid4()
        facet['note'] = 'Note'
        facet['fra'] = '1900-01-01 00:00:00'
        facet['til'] = 'infinity'
        facet['brugervendtnoegle'] = facet
        facet['beskrivelse'] = facet
        facet['ansvarlig_type'] = 'organisation'
        facet['ansvarlig'] = self.uuids['org_id']
        facet['ansvarlig_bvn'] = self.root_org
        facet['ejer_type'] = 'organisation'
        facet['ejer'] = self.uuids['org_id']
        facet['ejer_bvn'] = self.root_org
        facet['facettilhoerer'] = self.uuids['klassifikation']
        facet['facettilhoerer_type'] = 'klassifikation'
        facet['publiceret'] = 'Publiceret'
        return facet

    def create_facet_list(self, klasser):
        used_facets = []
        facet_list = []
        for klasse in klasser:
            facet = klasse['facet']
            if facet not in used_facets:
                used_facets.append(facet)
                facet_list.append(self.create_facet(facet))
        return facet_list

    def create_org_enhed(self, enhedsnavn, enhedstype,
                         fra='1993-01-01 00:00:00',
                         til='infinity', overordnet=''):
        adresse = self._adresse()
        org_enhed = {}
        org_enhed['objektid'] = uuid.uuid4()
        org_enhed['note'] = 'Plads til noter'
        org_enhed['fra'] = fra
        org_enhed['til'] = til
        bvn = enhedsnavn.split(' ')[-1]
        org_enhed['brugervendtnoegle'] = bvn
        org_enhed['enhedsnavn'] = enhedsnavn
        org_enhed['enhedstype'] = enhedstype
        org_enhed['adresse'] = adresse['adresse']
        # Kan også være en henvendelsessted
        org_enhed['adresse_type'] = self.uuids['adresse']

        org_enhed['postnummer'] = adresse['postnummer']
        org_enhed['postdistrikt'] = adresse['postdistrikt']
        org_enhed['telefon'] = self._telefon()
        org_enhed['telefon_type'] = self.uuids['telefon']
        org_enhed['email'] = bvn + '@' + self.root_org + '.dk'
        org_enhed['email_type'] = self.uuids['email']
        org_enhed['tilknyttedeenheder'] = ''
        org_enhed['overordnet'] = overordnet
        org_enhed['tilhoerer'] = self.uuids['org_id']
        org_enhed['gyldighed'] = 'Aktiv'
        return org_enhed

    def export(self, filename, content_list):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=content_list[0].keys())
            writer.writeheader()
            for content in content_list:
                writer.writerow(content)

    def create_all_org_units(self, klasser):
        org_enheder = []
        brugere = []

        # TODO: Randomize the 'til' field
        # The root organisationshed has the name as the organsation itself
        klasser.append(self.create_klasse('Kommune', facet='Enhedstype'))
        enhedstype = klasser[-1]['objektid']
        org_enheder.append(self.create_org_enhed(self.root_org,
                                                 enhedstype=enhedstype))
        # org_enheder[0] will now contain the root_organisations_enhed

        # Now make a number Forvaltninger
        over_uuid = org_enheder[0]['objektid']
        klasser.append(self.create_klasse('Forvaltning', facet='Enhedstype'))
        enhedstype = klasser[-1]['objektid']
        org_enheder.append(self.create_org_enhed('Borgmesterens Afdeling',
                                                 enhedstype=enhedstype,
                                                 overordnet=over_uuid))
        for i in range(random.randrange(5, 50)):
            brugere.append(self.create_bruger(org_enheder[-1]['objektid']))

        org_enheder.append(self.create_org_enhed('Teknik og Miljø',
                                                 enhedstype=enhedstype,
                                                 overordnet=over_uuid))
        for i in range(random.randrange(5, 50)):
            brugere.append(self.create_bruger(org_enheder[-1]['objektid']))

        org_enheder.append(self.create_org_enhed('Skole og Børn',
                                                 enhedstype=enhedstype,
                                                 overordnet=over_uuid))
        for i in range(random.randrange(5, 50)):
            brugere.append(self.create_bruger(org_enheder[-1]['objektid']))

        org_enheder.append(self.create_org_enhed('Social Job og sundhed',
                                                 enhedstype=enhedstype,
                                                 overordnet=over_uuid))
        for i in range(random.randrange(5, 50)):
            brugere.append(self.create_bruger(org_enheder[-1]['objektid']))

        # TODO: Sub-units to the units
        return org_enheder, brugere, klasser

    def make_dummy_unit(self):
        organisation = [self.create_organisation()]
        self.uuids['org_id'] = organisation[0]['objektid']
        klassifikation = [self.create_klassifikation()]
        self.uuids['klassifikation'] = klassifikation[0]['objektid']

        it_systemer = []
        it_systemer.append(self.create_it_system('AD', 'Active Directory'))
        it_systemer.append(self.create_it_system('LoRa',
                                                 'Lokal Rammearkitektur'))
        it_systemer.append(self.create_it_system('OPUS', 'KMD OPUS'))
        it_systemer.append(self.create_it_system('MO', 'MORA'))

        klasser = []
        klasser.append(self.create_klasse('Email', facet='Adressetype'))
        self.uuids['email'] = klasser[-1]['objektid']
        klasser.append(self.create_klasse('Adresse', facet='Adressetype'))
        self.uuids['adresse'] = klasser[-1]['objektid']
        klasser.append(self.create_klasse('Telefon', facet='Adressetype'))
        self.uuids['adresse'] = klasser[-1]['objektid']
        klasse_list = [('Ansat', 'Tilknytningstype'),
                       ('Folkevalgt', 'Engagementstype'),
                       ('Afdelingssygeplejeske', 'Stillingsbetegnelse'),
                       ('IT-medarbejer', 'Stillingsbetegnelse'),
                       ('Skolelærer', 'Stillingsbetegnelse'),
                       ('SOSU-assistent', 'Stillingsbetegnelse'),
                       ('Direktørområde', 'Enhedstype'),
                       ('Fagligt center', 'Enhedstype'),
                       ('Bibliotek', 'Enhedstype'),
                       ('Svømmehal', 'Enhedstype'),
                       ('Kommunaldirektør', 'Ledertype')]
        for klasse in klasse_list:
            klasser.append(self.create_klasse(klasse[0], facet=klasse[1]))

        org_enheder, brugere, klasser = self.create_all_org_units(klasser)

        facetter = self.create_facet_list(klasser)

        #self.export('klassifikation.csv', organisation)
        #self.export('facetter.csv', facetter)
        #self.export('itsystem.csv', it_systemer)
        #self.export('organisation.csv', klassifikation)
        #self.export('klasse.csv', klasser)
        #self.export('organisationsenhed.csv', org_enheder)
        #self.export('brugere.csv', brugere)


if __name__ == '__main__':
    # TODO: Use cvr-data to extract realistic names
    dummy_creator = CreateDummyOrg('Magenta')
    dummy_creator.make_dummy_unit()
