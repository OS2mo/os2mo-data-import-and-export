import unittest
import user_names
from . name_simulator import create_name

# PIMJE is missing in specification documen
pmj_first = ['pmunj', 'pmuje', 'pjens', 'pimuj', 'pimje', 'pijen', 'piamj', 'piaje',
             'piamu', 'pimun', 'pmunk', 'jense', 'mjens', 'mujen', 'munje', 'munkj']
pmj_second = ['pmuj', 'pimj', 'pjen', 'pije', 'piaj', 'pmje']
pmj_third = ['pmj', 'pje', 'pij', 'pim', 'pmu']
pmj_fourth = ['pmu2j', 'pm2je', 'p2jen', 'p2muj', 'p2mje', 'pim2j', 'pi2je',
              'pi2mj', 'pia2j', 'piam2', 'pimu2', 'pmun2', '2pmuj', '2pmun',
              '2pjen', '2pimj', '2pije', '2pimu', '2piam', '2jens']

pmj_fifth = ['pmj2', 'pmu2', 'pij2', 'pim2', 'pia2', 'mun2', 'jen2', 'pm2j',
             'pi2j', 'pi2m', 'p2mj', 'p2je', 'p2mu', '2pmj', '2pmu', '2pij',
             '2pim', '2pia', '2muj', '2mje']
pmj_sixth = ['pmunkj', 'pimunj', 'piamuj', 'pmunje', 'pmujen', 'pmjens', 'pjense',
             'pijens', 'piajen']
pmj = [pmj_first, pmj_second, pmj_third, pmj_fourth, pmj_fifth, pmj_sixth]

akjpa_first = [
    'akjpa', 'akjea', 'akria', 'akrja', 'akrpa', 'akpea', 'ajepa', 'ajpea', 'apeta',
    'akran', 'akjan', 'akpan', 'ajean', 'ajpan', 'apean', 'aande', 'ankpa', 'ankja',
    'ankra', 'anjpa', 'anjea', 'anpea', 'ankan', 'anjan', 'anpan', 'anand', 'andka',
    'andja', 'andpa', 'andan', 'andkr', 'andkj', 'andkp', 'andje', 'andjp', 'andpe',
    'andea', 'andek', 'andej', 'andep', 'ankjp', 'ankje', 'ankrp', 'ankrj', 'ankri',
    'ankpe', 'anjpe', 'anjep', 'anjen', 'anpet', 'akjpe', 'akjep', 'akrjp', 'akrip',
    'akrij', 'akrje', 'akjen', 'akrpe', 'akpet', 'ajpet', 'ajepe', 'ajenp', 'ajens',
    'apete', 'akris', 'ander', 'kande', 'krand', 'krian', 'krisa', 'kjand', 'kjean',
    'kjena', 'kjpan', 'kjpea', 'kpand', 'kpean', 'kpeta', 'jande', 'jeand', 'jenan',
    'jensa', 'jpand', 'jpean', 'jpeta', 'pande', 'peand', 'petan', 'petea'
]
akjpa_second = ['akra', 'akja', 'akpa', 'ajea', 'ajpa', 'apea', 'anka', 'anja',
                'anpa', 'aand', 'anan', 'anda', 'akan', 'ajan', 'apan']
akjpa_third = ['aka', 'aja', 'apa', 'aan', 'ana', 'ank', 'anj', 'anp', 'akr', 'akj',
               'akp', 'aje', 'ajp', 'ape']
akjpa_fourth = [
    'akj2a', 'akr2a', 'akp2a', 'aje2a', 'ajp2a', 'ape2a', 'ak2an', 'aj2an', 'ap2an',
    'a2and', 'a2kpa', 'a2kja', 'a2kra', 'a2jpa', 'a2jea', 'a2pea', 'a2kan', 'a2jan',
    'a2pan', 'ank2a', 'anj2a', 'anp2a', 'an2an', 'an2ka', 'an2ja', 'an2pa', 'and2a',
    'ande2', 'andk2', 'andj2', 'andp2', 'ankr2', 'ankj2', 'ankp2', 'anje2', 'anjp2',
    'anpe2', 'akjp2', 'akri2', 'akrj2', 'akrp2', 'akje2', 'akpe2', 'ajen2', 'ajep2',
    'ajpe2', 'apet2', '2akja', '2akpa', '2akra', '2ajpa', '2ajea', '2akjp', '2akje',
    '2akrj', '2akri', '2ajep', '2ajpe', '2apet', '2aand', '2anka', '2anja', '2anpa',
    '2anan', '2ankj', '2ankp', '2ankr', '2anje', '2anjp', '2anpe', '2andk', '2andj',
    '2andp', '2ande', '2kjpa', '2krja', '2krpa', '2kjea', '2kpea', '2jepa', '2jpea',
    '2peta'
]
akjpa_fifth = [
    'aka2', 'aja2', 'apa2', 'akj2', 'akp2', 'akr2', 'ajp2', 'aje2', 'ape2', 'ana2',
    'ank2', 'anj2', 'anp2', 'and2', 'kjp2', 'kje2', 'kri2', 'jep2', 'jpe2', 'pet2',
    'ak2a', 'aj2a', 'ap2a', 'an2a', 'ak2j', 'ak2p', 'aj2p', 'an2k', 'an2j',
    'an2p', 'a2ka', 'a2ja', 'a2pa', 'a2an', 'a2kj', 'a2kp', 'a2kr', 'a2jp', 'a2je',
    'a2pe', '2aka', '2aja', '2apa', '2akj', '2akp', '2akr', '2ana', '2ank', '2anj',
    '2anp', '2and', '2kjp', '2kje', '2kja', '2kpa', '2kra', '2kan', '2kpe', '2jpa',
    '2jea', '2jan', '2jpe', '2pea', '2pan'
]
akjpa_sixth = [
    'akrisa', 'akrija', 'akripa', 'akrjea', 'akrjpa', 'akrpea', 'akjena', 'akjepa',
    'akjpea', 'ajensa', 'ajenpa', 'ajepea', 'ajpeta', 'apetea', 'ankria', 'ankrja',
    'ankrpa', 'ankjea', 'ankjpa', 'anjena', 'anjepa', 'anjpea', 'anpeta', 'andkra',
    'andkja', 'andkpa', 'andjea', 'andjpa', 'andpea', 'andeka', 'andeja', 'andepa',
    'akrian', 'akrjan', 'akrpan', 'akjean', 'akjpan', 'akpean', 'ajenan', 'ajepan',
    'ajpean', 'apetan', 'akrand', 'akjand', 'akpand', 'ajeand', 'ajpand', 'apeand',
    'akande', 'ajande', 'apande', 'aander', 'anande', 'andand', 'andean', 'andera'
]
akjpa = [akjpa_first, akjpa_second, akjpa_third, akjpa_fourth, akjpa_fifth,
         akjpa_sixth]

kj_first = ['kjens', 'kajen', 'karje', 'karij', 'jense']
kj_second = ['kjen', 'kaje', 'karj']
kj_third = ['kje', 'kaj']
kj_fourth = ['k2jen', 'ka2je', 'kar2j', 'kari2', '2kjen', '2kaje', '2kari', '2jens']
kj_fifth = ['kaj2', 'kar2', 'jen2', 'ka2j', 'k2je', '2kaj', '2kar']
kj_sixth = ['kjense', 'kajens', 'karjen', 'karije', 'karinj']
kj = [kj_first, kj_second, kj_third, kj_fourth, kj_fifth, kj_sixth]

# KAMJE is missing in the specification document
kmj_first = ['kmunj', 'kmuje', 'kjens', 'kamuj', 'kamje', 'kajen', 'karmj', 'karje',
             'karmu', 'karij', 'karim', 'kamun', 'kmunk', 'jense', 'mjens', 'mujen',
             'munje', 'munkj']
kmj_second = ['kmuj', 'kamj', 'kjen', 'kaje', 'karj', 'kmje']
kmj_third = ['kmj', 'kje', 'kaj', 'kam', 'kmu']
kmj_fourth = ['kmu2j', 'km2je', 'k2jen', 'k2muj', 'k2mje', 'kam2j', 'ka2je', 'ka2mj',
              'kar2j', 'kari2', 'karm2', 'kamu2', 'kmun2', '2kmuj', '2kmun', '2kjen',
              '2kamj', '2kaje', '2kamu', '2karm', '2kari', '2jens']
kmj_fifth = ['kmj2', 'kmu2', 'kaj2', 'kam2', 'kar2', 'mun2', 'jen2', 'km2j', 'ka2j',
             'ka2m', 'k2mj', 'k2je', 'k2mu', '2kmj', '2kmu', '2kaj', '2kam', '2kar',
             '2muj', '2mje']
kmj_sixth = ['kmunkj', 'kamunj', 'karmuj', 'karimj', 'kmunje', 'kmujen', 'kmjens',
             'kjense', 'kajens', 'karjen', 'karije', 'karinj']
kmj = [kmj_first, kmj_second, kmj_third, kmj_fourth, kmj_fifth, kmj_sixth]

ooha_first = ['oohoa', 'oosta', 'oosha', 'oosar', 'oohar', 'ohoar', 'oaroe',
              'oloha', 'olosa', 'olhoa', 'oloar', 'olhar', 'olaro', 'oleoa',
              'oleha', 'olear', 'oleos', 'oleoh', 'oleho', 'oloho', 'olosh',
              'olost', 'olhos', 'oosth', 'oosho', 'oohos', 'ohost', 'ooste',
              'osaro', 'ostar', 'ostea', 'oharo', 'ohosa', 'haroe',
              'hoaro', 'hosar', 'hosta']
ooha_second = ['oosa', 'ooha', 'ohoa', 'oloa', 'olha', 'oaro', 'olar', 'olea',
               'ooar', 'ohar']
ooha_third = ['ooa', 'oha', 'oar', 'ola', 'olo', 'olh', 'oos', 'ooh', 'oho']
ooha_fourth = ['ooh2a', 'oos2a', 'oho2a', 'oo2ar', 'oh2ar', 'o2aro', 'o2oha',
               'o2osa', 'o2hoa', 'o2oar', 'o2har', 'olo2a', 'olh2a', 'ol2ar',
               'ol2oa', 'ol2ha', 'ole2a', 'oleo2', 'oleh2', 'olos2', 'oloh2',
               'olho2', 'oost2', 'oosh2', 'ooho2', 'ohos2', '2ooha', '2oosa',
               '2ohoa', '2ooho', '2oosh', '2oost', '2oaro', '2oloa', '2olha',
               '2olar', '2oloh', '2olos', '2olho', '2oleo', '2oleh', '2osha',
               '2aroe']
ooha_fifth = ['ooa2', 'oha2', 'ooh2', 'oos2', 'oho2', 'ola2', 'olo2', 'olh2', 'ole2',
              'ost2', 'aro2', 'oo2a', 'oh2a', 'ol2a', 'oo2h', 'ol2o', 'ol2h',
              'o2oa', 'o2ha', 'o2ar', 'o2oh', 'o2os', 'o2ho', '2ooa', '2oha', '2ooh',
              '2oos', '2ola', '2olo', '2olh', '2ole', '2oho', '2osa',
              '2oar', '2hoa', '2har']
ooha_sixth = ['oostea', 'oostha', 'ooshoa', 'oohosa', 'ohosta', 'olosta', 'olosha',
              'olohoa', 'olhosa', 'oleosa', 'oleoha', 'olehoa', 'oostar', 'ooshar',
              'oohoar', 'ohosar', 'oosaro', 'ooharo', 'ohoaro', 'ooaroe', 'oharoe',
              'olaroe', 'olearo']
ooha = [ooha_first, ooha_second, ooha_third, ooha_fourth, ooha_fifth, ooha_sixth]


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
        for i in range(1, 7):
            test_result = self._test_person(name, pmj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_anders_kristian_jens_peter_andersen(self):
        name = ['Anders', 'Kristian', 'Jens', 'Peter', 'Andersen']
        for i in range(1, 7):
            test_result = self._test_person(name, akjpa, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_karina_jensen(self):
        name = ['Karina', 'Jensen']
        for i in range(1, 7):
            test_result = self._test_person(name, kj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_karina_munk_jensen(self):
        name = ['Karina', 'Munk', 'Jensen']
        for i in range(1, 7):
            test_result = self._test_person(name, kmj, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_Olê_Østergård_Høst_Ærøe(self):
        name = ['Olê', 'Østergård', 'Høst', 'Ærøe']
        for i in range(1, 7):
            test_result = self._test_person(name, ooha, i)
            self.assertTrue(test_result, 'Priority: {}'.format(i))

    def test_too_many_karina_jensens(self):
        """
        Tests behaviour of creating so many identical names, that
        the algorithm runs out of options.
        """
        name = ['Karina', 'Jensen']
        name_creator = user_names.CreateUserNames(occupied_names=set())
        with self.assertRaisesRegex(RuntimeError, 'Failed to create user name'):
            for i in range(1, 137):
                name_creator.create_username(name)

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
        for i in range(0, 2500):
            name = create_name()
            name_creator.create_username(name)
