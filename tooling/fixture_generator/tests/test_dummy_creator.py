import random
import unittest
from anytree import PreOrderIter
import dummy_data_creator


class DummyTest(unittest.TestCase):

    def setUp(self):
        random.seed(1)
        name_path = dummy_data_creator._path_to_names()
        self.ddc = dummy_data_creator.CreateDummyOrg(825, 'Læsø Kommune', name_path)
        self.ddc.create_org_func_tree()

    def test_create_names(self):
        names = []
        for i in range(0, 10):
            names.append(self.ddc.create_name(return_user_key=False))
        expected_names = [
            'Bjarne Tornbjerg', 'Elsebeth Silkjær Schelde', 'Inge Graverholt Brandt',
            'Sonja Kjær Jamil', 'Tage Vendelbo Stanimirovic', 'Inga Nielsen',
            'Maria Brejnholt Skinbjerg', 'Christian Ankjær Gustavsen',
            'Ralf Greve Christensen', 'Marie Mauritzen Werenfeldt Christensen',
        ]
        self.assertEqual(names, expected_names)

    def test_postal_codes(self):
        """ Test that we get all postal codes in the municipality
        (one in Læsø). """
        postal_codes = self.ddc._postdistrikter()
        self.assertEqual(postal_codes, ['Læsø'])

    def test_number_of_ous(self):
        """ Test that we produce the expected number of units """
        number_of_ous = 0
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'ou':
                number_of_ous += 1
        self.assertEqual(number_of_ous, 23)

    def test_cpr(self):
        """ Test that the cpr generator is deterministic when the random
        seed is fixed at 1 """
        cprs = []
        for i in range(0, 20):
            cpr = dummy_data_creator._cpr()
            cprs.append(cpr)
        expected_cprs = ['2812580526', '2312723746', '0711592857', '2808723851',
                         '1601553631', '0611632215', '2208720301', '2709652721',
                         '0112700498', '2812793929', '0603721357', '3009951619',
                         '1209942689', '2205750595', '1712911766', '0709670719',
                         '1906751743', '1607922639', '0106742685', '0110911912']
        self.assertEqual(expected_cprs, cprs)

    def test_number_of_users(self):
        """ Test that we get the expected number of users. """
        self.ddc.add_users_to_tree(ou_size_scale=2)
        number_of_users = 0
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                number_of_users += 1
        self.assertEqual(number_of_users, 98)

    def test_it_systems(self):
        """ Test that all it-systems are given to users """
        expected_systems = {}
        for it_system in dummy_data_creator.IT_SYSTEMS:
            expected_systems[it_system] = 0
        self.ddc.add_users_to_tree(ou_size_scale=2)
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                it_systems = node.user[0]['it_systemer']
                for it_system in it_systems:
                    expected_systems[it_system] += 1
        for it_system, count in expected_systems.items():
            self.assertTrue(count > 0)

    def test_roles(self):
        """ Test that all roles are given to users and that the
        roles are given in a valid unit """
        expected_roles = {}
        for role in dummy_data_creator.CLASSES['Rolletype']:
            expected_roles[role] = 0
        self.ddc.add_users_to_tree(ou_size_scale=2)
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                role = node.user[0]['role']
                if role is not None:
                    unit = role['unit']
                    self.assertTrue(unit in self.ddc.nodes.keys())
                    expected_roles[role['type']] += 1
        for role, count in expected_roles.items():
            self.assertTrue(count > 0)

    def test_consistent_associations(self):
        """ Test that associations are deterministic """
        self.ddc.add_users_to_tree(ou_size_scale=2)
        unit = None
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                if node.user[0]['cpr'] == '1404433829':
                    unit = node.user[0]['association']['unit']
        assert(unit == '4efa9a5f-6185-54e8-9dbc-d4d8518e9754')

    def test_managers_in_ous(self):
        """ Test that all units have a manager """
        self.ddc.add_users_to_tree(ou_size_scale=2)
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'ou':
                depth = node.depth
                has_manager = False
                for user in PreOrderIter(self.ddc.nodes[node.key]):
                    if user.is_leaf and user.depth == depth + 1:
                        user_info = user.user[0]
                        if user_info['manager']:
                            if has_manager is False:
                                has_manager = True
                            else:
                                has_manager = False
                                break
                self.assertTrue(has_manager)
