import unittest
import user_names
from . name_simulator import create_name

# PIMJE is missing in specification documen
pmj_first = ['PMUNJ', 'PMUJE', 'PJENS', 'PIMUJ', 'PIMJE', 'PIJEN', 'PIAMJ', 'PIAJE',
             'PIAMU', 'PIMUN', 'PMUNK']
pmj_second = ['PMUJ', 'PIMJ', 'PJEN', 'PIJE', 'PIAJ', 'PMJE']
pmj_third = ['PMJ', 'PJE', 'PIJ', 'PIM', 'PMU']
pmj_fourth = ['PMUNKJ', 'PIMUNJ', 'PIAMUJ', 'PMUNJE', 'PMUJEN', 'PMJENS', 'PJENSE',
              'PIJENS', 'PIAJEN']
pmj = [pmj_first, pmj_second, pmj_third, pmj_fourth]

# Error in specification document AKRJL should be AKRJA
akjpa_first = [
    'AKJPA', 'AKJEA', 'AKRIA', 'AKRJA', 'AKRPA', 'AKPEA', 'AJEPA', 'AJPEA', 'APETA',
    'AKRAN', 'AKJAN', 'AKPAN', 'AJEAN', 'AJPAN', 'APEAN', 'AANDE', 'ANKPA', 'ANKJA',
    'ANKRA', 'ANJPA', 'ANJEA', 'ANPEA', 'ANKAN', 'ANJAN', 'ANPAN', 'ANAND', 'ANDKA',
    'ANDJA', 'ANDPA', 'ANDAN', 'ANDKR', 'ANDKJ', 'ANDKP', 'ANDJE', 'ANDJP', 'ANDPE',
    'ANDEA', 'ANDEK', 'ANDEJ', 'ANDEP', 'ANKJP', 'ANKJE', 'ANKRP', 'ANKRJ', 'ANKRI',
    'ANKPE', 'ANJPE', 'ANJEP', 'ANJEN', 'ANPET', 'AKJPE', 'AKJEP', 'AKRJP', 'AKRIP',
    'AKRIJ', 'AKRJE', 'AKJEN', 'AKRPE', 'AKPET', 'AJPET', 'AJEPE', 'AJENP', 'AJENS',
    'APETE', 'AKRIS'
]
akjpa_second = ['AKRA', 'AKJA', 'AKPA', 'AJEA', 'AJPA', 'APEA', 'ANKA', 'ANJA',
                'ANPA', 'AAND', 'ANAN', 'ANDA', 'AKAN', 'AJAN', 'APAN']
# Second ANK in the specifikation document should be ANJ
akjpa_third = ['AKA', 'AJA', 'APA', 'AAN', 'ANA', 'ANK', 'ANJ', 'ANP', 'AKR', 'AKJ',
               'AKP', 'AJE', 'AJP', 'APE']
# Error in specification document ANKRIJ should be ANKRIA
akjpa_fourth = [
    'AKRISA', 'AKRIJA', 'AKRIPA', 'AKRJEA', 'AKRJPA', 'AKRPEA', 'AKJENA', 'AKJEPA',
    'AKJPEA', 'AJENSA', 'AJENPA', 'AJEPEA', 'AJPETA', 'APETEA', 'ANKRIA', 'ANKRJA',
    'ANKRPA', 'ANKJEA', 'ANKJPA', 'ANJENA', 'ANJEPA', 'ANJPEA', 'ANPETA', 'ANDKRA',
    'ANDKJA', 'ANDKPA', 'ANDJEA', 'ANDJPA', 'ANDPEA', 'ANDEKA', 'ANDEJA', 'ANDEPA',
    'AKRIAN', 'AKRJAN', 'AKRPAN', 'AKJEAN', 'AKJPAN', 'AKPEAN', 'AJENAN', 'AJEPAN',
    'AJPEAN', 'APETAN', 'AKRAND', 'AKJAND', 'AKPAND', 'AJEAND', 'AJPAND', 'APEAND',
    'AKANDE', 'AJANDE', 'APANDE', 'AANDER', 'ANANDE', 'ANDAND', 'ANDEAN', 'ANDERA'
]
akjpa = [akjpa_first, akjpa_second, akjpa_third, akjpa_fourth]

kj_first = ['KJENS', 'KAJEN', 'KARJE', 'KARIJ']
kj_second = ['KJEN', 'KAJE', 'KARJ']
kj_third = ['KJE', 'KAJ']
kj_fourth = ['KJENSE', 'KAJENS', 'KARJEN', 'KARIJE', 'KARINJ']
kj = [kj_first, kj_second, kj_third, kj_fourth]

# KAMJE is missing in the specification document
kmj_first = ['KMUNJ', 'KMUJE', 'KJENS', 'KAMUJ', 'KAMJE', 'KAJEN', 'KARMJ', 'KARJE',
             'KARMU', 'KARIJ', 'KARIM', 'KAMUN', 'KMUNK']
kmj_second = ['KMUJ', 'KAMJ', 'KJEN', 'KAJE', 'KARJ', 'KMJE']
kmj_third = ['KMJ', 'KJE', 'KAJ', 'KAM', 'KMU']
kmj_fourth = ['KMUNKJ', 'KAMUNJ', 'KARMUJ', 'KARIMJ', 'KMUNJE', 'KMUJEN', 'KMJENS',
              'KJENSE', 'KAJENS', 'KARJEN', 'KARIJE', 'KARINJ']
kmj = [kmj_first, kmj_second, kmj_third, kmj_fourth]


class TestUsernameCreattion(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        pass

    def setUp(self):
        pass

    def _test_person(self, name, reference, max_level):
        name_creator = user_names.CreateUserNames(occupied_names=set())
        success = True
        for level in range(0, max_level):
            for correct_user_name in reference[level]:
                user_name = name_creator.create_username(name)
                if not user_name[0] == correct_user_name:
                    success = False
                    print('Got: {}, expected: {}'.format(user_name,
                                                         correct_user_name))
        return success

    def test_pia_munk_jensen(self):
        name = ['Pia', 'Munk', 'Jensen']
        for i in range(1, 5):
            test_result = self._test_person(name, pmj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_anders_kristian_jens_peter_andersen(self):
        name = ['Anders', 'Kristian', 'Jens', 'Peter', 'Andersen']
        for i in range(1, 5):
            test_result = self._test_person(name, akjpa, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_karina_jensen(self):
        name = ['Karina', 'Jensen']
        for i in range(1, 5):
            test_result = self._test_person(name, kj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_karina_munk_jensen(self):
        name = ['Karina', 'Munk', 'Jensen']
        for i in range(1, 5):
            test_result = self._test_person(name, kmj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_too_many_karina_jensens(self):
        """
        Tests behaviour of creating so many identical names, that
        the algorithm runs out of options.
        """
        name = ['Karina', 'Jensen']
        name_creator = user_names.CreateUserNames(occupied_names=set())
        for i in range(1, 18):
            user_name = name_creator.create_username(name)
        self.assertTrue(user_name[0] is '')

    def test_dry_run(self):
        """
        Tests that with dry_run set to True, we can keep adding the
        same person and never run into an answer of None.
        """
        name = ['Karina', 'Jensen']
        name_creator = user_names.CreateUserNames(occupied_names=set())
        for i in range(1, 25):
            user_name = name_creator.create_username(name, dry_run=True)
        self.assertFalse(user_name[0] is None)

    def test_multiple_names(self):
        """
        Test that we can create a large number of random names without hitting a
        bug that stops the program. Test succeeds if the code does not raise an
        exception.
        """
        name_creator = user_names.CreateUserNames(occupied_names=set())
        for i in range(0, 1000):
            name = create_name()
            name_creator.create_username(name)
