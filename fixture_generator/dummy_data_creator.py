""" Create dummy data to populate MO """
import pickle
import random
import requests
import pathlib

class CreateDummyOrg(object):
    """ Create a dummy organisation to use as test data """

    def __init__(self, kommune, navne):
        self.kommunekoder = {'København': 101,
                             'Frederiksberg': 147,
                             'Ballerup': 151,
                             'Næstved': 370}
        self.kommune = kommune
        try:
            with open(kommune + '.p', 'rb') as file_handle:
                self.adresser = pickle.load(file_handle)
        except OSError:
            addr = ('http://dawa.aws.dk/adgangsadresser' +
                    '?kommunekode={}&struktur=mini')
            r = requests.get(addr.format(self.kommunekoder[kommune]))
            self.adresser = r.json()
            with open(kommune + '.p', 'wb') as file_handle:
                pickle.dump(self.adresser, file_handle)

        self.names = {}
        self.names['first'] = self._load_names(navne[0])
        self.names['middle'] = self._load_names(navne[1])
        self.names['last'] = self._load_names(navne[2])
        self.used_bvns = []

    def _load_names(self, name_file):
        """ Load a weighted list of names
        :param name_file: Name of the text file with names
        :return: A weighted list of names
        """
        with name_file.open('r') as f:
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
        """ Create a random phone number
        :return: A random phone number
        """
        tlf = str(random.randrange(1, 9))
        for i in range(0, 6):
            tlf += str(random.randrange(0, 9))
        return tlf

    def _adresse(self):
        """ Create a Danish adresse """
        # TODO: We should use the open adresse data to create realistic data
        # in the same way names are currently created
        addr = self.adresser[random.randrange(len(self.adresser))]
        adresse = {}
        adresse['postnummer'] = addr['postnr']
        adresse['postdistrikt'] = addr['postnrnavn']
        adresse['adresse'] = addr['vejnavn'] + ' ' + addr['husnr']
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

    def create_bruger(self):
        """ Create a MO bruger with a random name and phone
        :return: A Dict with information about the bruger
        """
        navn, bvn = self.create_name(bvn=True)
        bruger = {}
        bruger['fra'] = '1964-05-24 00:00:00'
        bruger['til'] = 'infinity'
        bruger['brugervendtnoegle'] = bvn
        bruger['brugernavn'] = navn
        bruger['email'] = bvn + '@' + self.kommune + '.dk'
        bruger['telefon'] = self._telefon()
        return bruger

    def _create_org_level(self, org_list):
        """ Create a dict with names, adresses and room for subunits
        :param org_list: List of names of the organisation
        :return: A flat dict with name, random adress and room for sub-units
        """
        org_dict = {}
        for org in org_list:
            org_dict[org] = {}
            org_dict[org]['adresse'] = self._adresse()
            org_dict[org]['subunits'] = {}
        return org_dict

    def _postdistrikter(self):
        """ Create a list of all unique postal areas
        :return: List of all unique postal areas
        """
        postdistrikter = []
        for adresse in self.adresser:
            if adresse['postnrnavn'] not in postdistrikter:
                postdistrikter.append(adresse['postnrnavn'])
        return postdistrikter

    def create_org_func_list(self):
        orgs = ['Borgmesterens Afdeling',
                'Teknik og Miljø',
                'Skole og Børn',
                'Social og sundhed']
        org_func_list = self._create_org_level(orgs)

        for org in org_func_list.keys():
            if org == 'Teknik og Miljø':
                orgs = ['Kloakering',
                        'Park og vej',
                        'Renovation',
                        'Belysning',
                        'IT-Support']
                org_func_list[org]['subunits'] = self._create_org_level(orgs)

            if org == 'Borgmesterens Afdeling':
                orgs = ['Budget og Planlægning',
                        'HR og organisation',
                        'Erhverv',
                        'Byudvikling',
                        'IT-Support']
                org_func_list[org]['subunits'] = self._create_org_level(orgs)

            if org == 'Skole og Børn':
                orgs = ['Skole', 'Børnehaver', 'Social Indsats', 'IT-Support']
                org_func_list[org]['subunits'] = self._create_org_level(orgs)

                skoler = self._create_org_level([dist + " skole"
                                                 for dist in
                                                 self._postdistrikter()])
                børnehaver = self._create_org_level([dist + " børnehus"
                                                     for dist in
                                                     self._postdistrikter()])

                org_func_list[org]['subunits']['Skole']['subunits'] = skoler
                org_func_list[org]['subunits']['Børnehaver']['subunits'] = børnehaver
        return org_func_list

if __name__ == '__main__':
    # TODO: Use cvr-data to extract realistic names
    path = pathlib.Path.cwd()
    path = path /  'navne'
    navne_list = [path / 'fornavne.txt',
                  path / 'mellemnavne.txt',
                  path / 'efternavne.txt']
    dummy_creator = CreateDummyOrg('Næstved', navne_list)
    org = dummy_creator.create_org_func_list()

    brugere = []
    for i in range(0, 5):
        brugere.append(dummy_creator.create_bruger())

    print(org)
    print(brugere)
