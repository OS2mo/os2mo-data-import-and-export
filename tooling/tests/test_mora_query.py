import os
import csv
import unittest
from mora_query import MoraQuery
# from mora import lora
# from tests import util


class QueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # c = lora.Connector(virkningfra='-infinity', virkningtil='infinity')
        start_ou = '82b42d4e-f7c0-4787-aa2d-9312b284e519'
        self.moraq = MoraQuery()
        self.nodes = self.moraq.read_ou_tree(start_ou)
        self.moraq.export_orgs(self.nodes, 'all_employees.csv')
        self.moraq.export_orgs(self.nodes, 'all_orgs.csv',
                               include_employees=False)
        self.moraq.export_managers(self.nodes, 'all_managers.csv')

    @classmethod
    def tearDownClass(self):
        os.remove('all_employees.csv')
        os.remove('all_managers.csv')
        os.remove('all_orgs.csv')

    def _load_csv(self, filename):
        rows = []
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rows.append(row)
        return rows

    def test_node_levels(self):
        """ Test that the tree has the expected height """
        height = self.nodes['root'].height
        self.assertEqual(height, 3)

    def test_user_names(self):
        """ Test that we did not mistakingly write the same username for
        multiple users. This could for example happen if we mistankingly
        exports the same use for every employee in an ou. We allow a
        small difference (15%) since it actally happens that an employee
        is listed more than once."""
        rows = self._load_csv('all_employees.csv')
        usernames = []
        for row in rows:
            if row['Brugernavn'] not in usernames:
                usernames.append(row['Brugernavn'])
        self.assertTrue(len(usernames) > len(rows) * 0.85)

    def test_all_employees(self):
        """ Test that we have exported all employees exactly once """
        rows = self._load_csv('all_employees.csv')
        self.assertEqual(len(rows), 206)

    def test_consistency(self):
        rows = self._load_csv('all_orgs.csv')
        self.assertEqual(len(rows), len(self.nodes))

    def test_all_managers(self):
        rows = self._load_csv('all_managers.csv')
        self.assertEqual(len(rows), 9)

if __name__ == '__main__':
        unittest.main()
