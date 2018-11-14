import random
import unittest
from anytree import PreOrderIter
import dummy_data_creator


class DummyTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
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
        postal_codes = self.ddc._postdistrikter()
        self.assertEqual(postal_codes, ['Læsø'])

    def test_number_of_ous(self):
        number_of_ous = 0
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'ou':
                number_of_ous += 1
        self.assertEqual(number_of_ous, 23)

    def test_number_of_users(self):
        self.ddc.add_users_to_tree(ou_size_scale=2)
        number_of_users = 0
        for node in PreOrderIter(self.ddc.nodes['root']):
            if node.type == 'user':
                number_of_users += 1
        print(number_of_users)
        self.assertIn(number_of_users, range(63, 161))

    # Test IT systems
    # Test associations
    # Test roles
    # Test managers
