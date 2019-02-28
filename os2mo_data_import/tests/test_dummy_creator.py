import random
import unittest
from anytree import PreOrderIter
from fixture_generator import dummy_data_creator


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
            'Kristine Serup Jørgensen', 'Arne Larsen',
            'Christian Heldgaard Frederiksen', 'Flemming Lindgaard Skorstengaard',
            'Maria Ahmednor Brøsted', 'Anders Maagaard Pedersen',
            'Anne Balsgaard Barløse', 'Margit Hesselbjerg', 'Tina Petersen',
            'Christen Bødker Weinell'
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
        self.assertEqual(number_of_ous, 24)

    def test_cpr(self):
        """ Test that the cpr generator is deterministic when the random
        seed is fixed at 1 """
        cprs = []
        for i in range(0, 20):
            cpr = dummy_data_creator._cpr()
            cprs.append(cpr)
        expected_cprs = [
            '1603902889', '2306542888', '0812940899', '1107671443', '0304801840',
            '1908981878', '1505731278', '2903853568', '2505651246', '0712852186',
            '0802652739', '0602420868', '2901881971', '1601853538', '2711570880',
            '0612461619', '1604683315', '0804922695', '1909641671', '0912613852'
        ]
        self.assertEqual(expected_cprs, cprs)

    def test_number_of_users(self):
        """ Test that we get the expected number of users. """
        self.ddc.add_users_to_tree(ou_size_scale=2)
        number_of_users = 0
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                number_of_users += 1
        self.assertEqual(number_of_users, 106)

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
        for role in dummy_data_creator.CLASSES['role_type']:
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
                if node.user[0]['cpr'] == '2808443964':
                    unit = node.user[0]['association']['unit']
        assert(unit == '4ba054d2-9f69-5b09-8c2a-862da1429e31')

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
