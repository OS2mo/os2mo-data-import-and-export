""" Create dummy data to populate MO """
import pickle
import random
import pathlib
import requests
from uuid import uuid4
from anytree import Node, PreOrderIter


# Name handling
def _path_to_names():
    path = pathlib.Path.cwd()
    path = path / 'navne'
    navne_list = [path / 'fornavne.txt',
                  path / 'mellemnavne.txt',
                  path / 'efternavne.txt']
    return navne_list


def _load_names(name_file):
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


def _telefon():
    """ Create a random phone number
    :return: A random phone number
    """
    tlf = str(random.randrange(1, 9))
    for i in range(0, 6):
        tlf += str(random.randrange(0, 9))
    return tlf


class CreateDummyOrg(object):
    """ Create a dummy organisation to use as test data """

    def __init__(self, kommunekode, kommunenavn, path_to_names):
        self.nodes = {}
        self.kommunenavn = kommunenavn
        self.nodes['root'] = Node(kommunenavn)

        try:
            with open(str(kommunekode) + '.p', 'rb') as file_handle:
                self.adresser = pickle.load(file_handle)
        except OSError:
            addr = ('http://dawa.aws.dk/adgangsadresser' +
                    '?kommunekode={}&struktur=mini')
            r = requests.get(addr.format(kommunekode))
            self.adresser = r.json()
            with open(str(kommunekode) + '.p', 'wb') as file_handle:
                pickle.dump(self.adresser, file_handle)

        self.names = {'first': _load_names(path_to_names[0]),
                      'middle': _load_names(path_to_names[1]),
                      'last': _load_names(path_to_names[2])}

        # Used to keep track of used bvns to keep them unique
        self.used_bvns = []

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

    def _postdistrikter(self):
        """ Create a list of all unique postal areas
        :return: List of all unique postal areas
        """
        postdistrikter = []
        for adresse in self.adresser:
            if adresse['postnrnavn'] not in postdistrikter:
                postdistrikter.append(adresse['postnrnavn'])
        return postdistrikter

    def _adresse(self):
        """ Create a Danish adresse """
        addr = self.adresser[random.randrange(len(self.adresser))]
        adresse = {'postnummer': addr['postnr'],
                   'postdistrikt': addr['postnrnavn'],
                   'adresse': addr['vejnavn'] + ' ' + addr['husnr'],
                   'dar-uuid': addr['id']}
        return adresse

    def create_name(self, bvn=False):
        """ Create a full name
        :return: The full name as a string
        """
        first = self._pick_name_from_list('first')

        middle = ''
        if random.random() > 0.3:
            middle = middle + self._pick_name_from_list('middle')
        if random.random() > 0.9:
            middle = middle + ' ' + self._pick_name_from_list('middle')

        last = self._pick_name_from_list('last')
        name = first + ' ' + middle + ' ' + last
        bvn = first + last[0]
        i = 0
        while bvn in self.used_bvns:
            i = i + 1
            bvn = first[0:i+2] + last[0:i]
            if i > len(last):
                bvn = bvn + str(random.randrange(1, 999))
        self.used_bvns.append(bvn)

        if bvn:
            return name, bvn
        else:
            return name

    def create_bruger(self, manager=False):
        """ Create a MO bruger with a random name and phone
        :return: A Dict with information about the bruger
        """
        navn, bvn = self.create_name(bvn=True)
        bruger = {'fra': '1964-05-24',  # TODO
                  'til': None,  # TODO
                  'brugervendtnoegle': bvn,
                  'brugernavn': navn,
                  'email': bvn + '@' + self.kommunenavn + '.dk',
                  'telefon': _telefon(),
                  'manager': manager,
                  'adresse': self._adresse()
                  }
        return bruger

    def _create_org_level(self, org_list, parent):
        """ Create a dict with names, adresses and parents
        :param org_list: List of names of the organisation
        :return: A flat dict with name, random adress and room for sub-units
        """
        uuid_list = []
        for org in org_list:
            uuid = uuid4()
            uuid_list.append(uuid)
            self.nodes[uuid] = Node(org, adresse=self._adresse(),
                                    type='ou', parent=parent)
        return uuid_list

    def create_org_func_tree(self):
        orgs = ['Borgmesterens Afdeling',
                'Teknik og Miljø',
                'Skole og Børn',
                'Social og sundhed']
        self._create_org_level(orgs, parent=self.nodes['root'])

        for node in list(self.nodes.keys()):
            org = self.nodes[node].name
            if org == 'Teknik og Miljø':
                orgs = ['Kloakering',
                        'Park og vej',
                        'Renovation',
                        'Belysning',
                        'IT-Support']
                self._create_org_level(orgs, self.nodes[node])

            if org == 'Borgmesterens Afdeling':
                orgs = ['Budget og Planlægning',
                        'HR og organisation',
                        'Erhverv',
                        'Byudvikling',
                        'IT-Support']
                self._create_org_level(orgs, self.nodes[node])

            if org == 'Skole og Børn':
                orgs = ['Social Indsats', 'IT-Support']
                self._create_org_level(orgs, self.nodes[node])

                org = ['Skoler og børnehaver']
                uuid = self._create_org_level(org, self.nodes[node])[0]

                skoler = [dist + " skole" for dist in self._postdistrikter()]
                self._create_org_level(skoler, self.nodes[uuid])

                børnehaver = [dist + " børnehus"
                              for dist in self._postdistrikter()]
                self._create_org_level(børnehaver, self.nodes[uuid])

    def add_users_to_tree(self, ou_size_scale):
        new_nodes = {}
        for node in PreOrderIter(self.nodes['root']):
            size = ou_size_scale * (node.depth + 1)
            ran_size = random.randrange(round(size/4), size)
            for _ in range(0, ran_size):
                user = self.create_bruger()
                new_nodes[uuid4()] = {'name': user['brugernavn'], 'user': user,
                                      'parent': node}
            # In version one we always add a single manager to a OU
            # This should be randomized and also sometimes be a vacant
            # position
            user = self.create_bruger(manager=True)
            new_nodes[uuid4()] = {'name': user['brugernavn'], 'user': user,
                                  'parent': node}

        for key, user_info in new_nodes.items():
            user_node = Node(user_info['user']['brugernavn'],
                             user=user_info['user'], type='user',
                             parent=user_info['parent'])
            self.nodes[key] = user_node


if __name__ == '__main__':
    dummy_creator = CreateDummyOrg(860, 'Hjørring Kommune',
                                   _path_to_names())
    dummy_creator.create_org_func_tree()
    dummy_creator.add_users_to_tree(ou_size_scale=1)

    # Iterate over all nodes:
    for node in PreOrderIter(dummy_creator.nodes['root']):
        print(node)
