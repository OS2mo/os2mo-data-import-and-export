import unittest
from typing import Any
from unittest import mock

from hypothesis import given
from hypothesis import strategies as st
from parameterized import parameterized
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session
from sqlalchemy.orm import sessionmaker

from ..ad_exceptions import ImproperlyConfigured
from ..user_names import UserNameGen
from ..user_names import UserNameGenMethod2
from ..user_names import UserNameGenPermutation
from ..user_names import UserNameSet
from ..user_names import UserNameSetCSVFile
from ..user_names import UserNameSetCSVFileSubstring
from ..user_names import UserNameSetInAD
from ..user_names import UserNameSetInDatabase
from .mocks import MockADParameterReader
from .name_simulator import create_name


class TestUserNameGen(unittest.TestCase):
    _load_setting_path = "integrations.ad_integration.user_names.load_setting"

    @parameterized.expand(
        [
            ("UserNameGenMethod2", UserNameGenMethod2),
            ("UserNameGenUnknown", UserNameGenMethod2),
            ("UserNameGenPermutation", UserNameGenPermutation),
        ]
    )
    def test_override_implementation(self, name, expected_class):
        """Test that `get_implementation` returns the expected implementation
        configured by setting 'integrations.ad_writer.user_names.class'.
        """
        with mock.patch(self._load_setting_path, return_value=lambda: name):
            impl = UserNameGen.get_implementation()
            self.assertIsInstance(impl, expected_class)
            self.assertEqual(len(impl._loaded_occupied_name_sets), 0)

    def test_default_if_no_settings(self):
        """Test that `get_implementation` returns the default implementation
        if 'settings.json' could not be read.
        """

        def raise_filenotfound():
            raise FileNotFoundError()

        with mock.patch(self._load_setting_path, return_value=raise_filenotfound):
            impl = UserNameGen.get_implementation()
            self.assertIsInstance(impl, UserNameGenMethod2)
            self.assertEqual(len(impl._loaded_occupied_name_sets), 0)

    def test_load_occupied_names(self):
        """Test that we can configure `UserNameGen` to load one or more
        `UserNameSet` classes and add their individual sets of occupied names.
        """
        # Get `UserNameGen` implementation which loads occupied usernames from
        # `UserNameSet` (= an empty implementation.)
        impl = self._get_instance("UserNameSet")

        # Assert that we loaded two username sets (from AD and from the
        # `UserNameSet` specified in settings.)
        self.assertEqual(len(impl._loaded_occupied_name_sets), 2)

        # Assert that the first username set is the AD username set.
        self.assertIsInstance(impl._loaded_occupied_name_sets[0], UserNameSetInAD)

        # Assert that the second username set is the one we specified
        # via settings, and is an empty set.
        self.assertIsInstance(impl._loaded_occupied_name_sets[1], UserNameSet)
        self.assertSetEqual(set(impl._loaded_occupied_name_sets[1]), set())

        # Assert that the total set of occupied usernames is equal to
        # the usernames "found" by our mock `ADParameterReader`.
        self.assertSetEqual(
            impl.occupied_names,
            set(impl._loaded_occupied_name_sets[0]),
        )

    def test_load_occupied_names_invalid_name_raises(self):
        with self.assertRaises(ImproperlyConfigured):
            self._get_instance("InvalidUserNameSetName")

    def test_is_username_occupied_is_case_insensitive(self):
        impl = self._get_instance("UserNameSet")
        username = list(impl.occupied_names)[0]
        self.assertEqual(username, username.lower())
        self.assertTrue(impl.is_username_occupied(username.upper()))

    def _get_instance(self, usernameset_class_name: str) -> "UserNameGen":
        cls_names = [usernameset_class_name]
        settings = {
            f"{UserNameGen._setting_prefix}.extra_occupied_name_classes": cls_names
        }
        with self._patch_settings(settings):
            # Use mock `ADParameterReader` in `UserNameSetInAD`
            with mock.patch(
                "integrations.ad_integration.user_names.ADParameterReader",
                new=MockADParameterReader,
            ):
                impl = UserNameGen.get_implementation()
                impl.load_occupied_names()
                return impl

    def _patch_settings(self, settings: dict):
        return mock.patch(
            self._load_setting_path,
            new=lambda name, default=None: lambda: settings.get(name, default),
        )


# PIMJE is missing in specification documen
pmj_first = [
    "pmunj",
    "pmuje",
    "pjens",
    "pimuj",
    "pimje",
    "pijen",
    "piamj",
    "piaje",
    "piamu",
    "pimun",
    "pmunk",
    "jense",
    "mjens",
    "mujen",
    "munje",
    "munkj",
]
pmj_second = ["pmuj", "pimj", "pjen", "pije", "piaj", "pmje"]
pmj_third = ["pmj", "pje", "pij", "pim", "pmu"]
pmj_fourth = [
    "pmu2j",
    "pm2je",
    "p2jen",
    "p2muj",
    "p2mje",
    "pim2j",
    "pi2je",
    "pi2mj",
    "pia2j",
    "piam2",
    "pimu2",
    "pmun2",
]

pmj_fifth = [
    "pmj2",
    "pmu2",
    "pij2",
    "pim2",
    "pia2",
    "mun2",
    "jen2",
    "pm2j",
    "pi2j",
    "pi2m",
    "p2mj",
    "p2je",
    "p2mu",
]
pmj_sixth = [
    "pmunkj",
    "pimunj",
    "piamuj",
    "pmunje",
    "pmujen",
    "pmjens",
    "pjense",
    "pijens",
    "piajen",
]
pmj = [pmj_first, pmj_second, pmj_third, pmj_fourth, pmj_fifth, pmj_sixth]

akjpa_first = [
    "akjpa",
    "akjea",
    "akria",
    "akrja",
    "akrpa",
    "akpea",
    "ajepa",
    "ajpea",
    "apeta",
    "akran",
    "akjan",
    "akpan",
    "ajean",
    "ajpan",
    "apean",
    "aande",
    "ankpa",
    "ankja",
    "ankra",
    "anjpa",
    "anjea",
    "anpea",
    "ankan",
    "anjan",
    "anpan",
    "anand",
    "andka",
    "andja",
    "andpa",
    "andan",
    "andkr",
    "andkj",
    "andkp",
    "andje",
    "andjp",
    "andpe",
    "andea",
    "andek",
    "andej",
    "andep",
    "ankjp",
    "ankje",
    "ankrp",
    "ankrj",
    "ankri",
    "ankpe",
    "anjpe",
    "anjep",
    "anjen",
    "anpet",
    "akjpe",
    "akjep",
    "akrjp",
    "akrip",
    "akrij",
    "akrje",
    "akjen",
    "akrpe",
    "akpet",
    "ajpet",
    "ajepe",
    "ajenp",
    "ajens",
    "apete",
    "akris",
    "ander",
    "kande",
    "krand",
    "krian",
    "krisa",
    "kjand",
    "kjean",
    "kjena",
    "kjpan",
    "kjpea",
    "kpand",
    "kpean",
    "kpeta",
    "jande",
    "jeand",
    "jenan",
    "jensa",
    "jpand",
    "jpean",
    "jpeta",
    "pande",
    "peand",
    "petan",
    "petea",
]
akjpa_second = [
    "akra",
    "akja",
    "akpa",
    "ajea",
    "ajpa",
    "apea",
    "anka",
    "anja",
    "anpa",
    "aand",
    "anan",
    "anda",
    "akan",
    "ajan",
    "apan",
]
akjpa_third = [
    "aka",
    "aja",
    "apa",
    "aan",
    "ana",
    "ank",
    "anj",
    "anp",
    "akr",
    "akj",
    "akp",
    "aje",
    "ajp",
    "ape",
]
akjpa_fourth = [
    "akj2a",
    "akr2a",
    "akp2a",
    "aje2a",
    "ajp2a",
    "ape2a",
    "ak2an",
    "aj2an",
    "ap2an",
    "a2and",
    "a2kpa",
    "a2kja",
    "a2kra",
    "a2jpa",
    "a2jea",
    "a2pea",
    "a2kan",
    "a2jan",
    "a2pan",
    "ank2a",
    "anj2a",
    "anp2a",
    "an2an",
    "an2ka",
    "an2ja",
    "an2pa",
    "and2a",
    "ande2",
    "andk2",
    "andj2",
    "andp2",
    "ankr2",
    "ankj2",
    "ankp2",
    "anje2",
    "anjp2",
    "anpe2",
    "akjp2",
    "akri2",
    "akrj2",
    "akrp2",
    "akje2",
    "akpe2",
    "ajen2",
    "ajep2",
    "ajpe2",
    "apet2",
]
akjpa_fifth = [
    "aka2",
    "aja2",
    "apa2",
    "akj2",
    "akp2",
    "akr2",
    "ajp2",
    "aje2",
    "ape2",
    "ana2",
    "ank2",
    "anj2",
    "anp2",
    "and2",
    "kjp2",
    "kje2",
    "kri2",
    "jep2",
    "jpe2",
    "pet2",
    "ak2a",
    "aj2a",
    "ap2a",
    "an2a",
    "ak2j",
    "ak2p",
    "aj2p",
    "an2k",
    "an2j",
    "an2p",
    "a2ka",
    "a2ja",
    "a2pa",
    "a2an",
    "a2kj",
    "a2kp",
    "a2kr",
    "a2jp",
    "a2je",
    "a2pe",
]
akjpa_sixth = [
    "akrisa",
    "akrija",
    "akripa",
    "akrjea",
    "akrjpa",
    "akrpea",
    "akjena",
    "akjepa",
    "akjpea",
    "ajensa",
    "ajenpa",
    "ajepea",
    "ajpeta",
    "apetea",
    "ankria",
    "ankrja",
    "ankrpa",
    "ankjea",
    "ankjpa",
    "anjena",
    "anjepa",
    "anjpea",
    "anpeta",
    "andkra",
    "andkja",
    "andkpa",
    "andjea",
    "andjpa",
    "andpea",
    "andeka",
    "andeja",
    "andepa",
    "akrian",
    "akrjan",
    "akrpan",
    "akjean",
    "akjpan",
    "akpean",
    "ajenan",
    "ajepan",
    "ajpean",
    "apetan",
    "akrand",
    "akjand",
    "akpand",
    "ajeand",
    "ajpand",
    "apeand",
    "akande",
    "ajande",
    "apande",
    "aander",
    "anande",
    "andand",
    "andean",
    "andera",
]
akjpa = [
    akjpa_first,
    akjpa_second,
    akjpa_third,
    akjpa_fourth,
    akjpa_fifth,
    akjpa_sixth,
]

kj_first = ["kjens", "kajen", "karje", "karij", "jense"]
kj_second = ["kjen", "kaje", "karj"]
kj_third = ["kje", "kaj"]
kj_fourth = ["k2jen", "ka2je", "kar2j", "kari2"]
kj_fifth = ["kaj2", "kar2", "jen2", "ka2j", "k2je"]
kj_sixth = ["kjense", "kajens", "karjen", "karije", "karinj"]
kj = [kj_first, kj_second, kj_third, kj_fourth, kj_fifth, kj_sixth]

kmj_first = [
    "kmunj",
    "kmuje",
    "kjens",
    "kamuj",
    "kamje",
    "kajen",
    "karmj",
    "karje",
    "karmu",
    "karij",
    "karim",
    "kamun",
    "kmunk",
    "jense",
    "mjens",
    "mujen",
    "munje",
    "munkj",
]
kmj_second = ["kmuj", "kamj", "kjen", "kaje", "karj", "kmje"]
kmj_third = ["kmj", "kje", "kaj", "kam", "kmu"]
kmj_fourth = [
    "kmu2j",
    "km2je",
    "k2jen",
    "k2muj",
    "k2mje",
    "kam2j",
    "ka2je",
    "ka2mj",
    "kar2j",
    "kari2",
    "karm2",
    "kamu2",
    "kmun2",
]
kmj_fifth = [
    "kmj2",
    "kmu2",
    "kaj2",
    "kam2",
    "kar2",
    "mun2",
    "jen2",
    "km2j",
    "ka2j",
    "ka2m",
    "k2mj",
    "k2je",
    "k2mu",
]
kmj_sixth = [
    "kmunkj",
    "kamunj",
    "karmuj",
    "karimj",
    "kmunje",
    "kmujen",
    "kmjens",
    "kjense",
    "kajens",
    "karjen",
    "karije",
    "karinj",
]
kmj = [kmj_first, kmj_second, kmj_third, kmj_fourth, kmj_fifth, kmj_sixth]

ooha_first = [
    "oohoa",
    "oosta",
    "oosha",
    "oosar",
    "oohar",
    "ohoar",
    "oaroe",
    "oloha",
    "olosa",
    "olhoa",
    "oloar",
    "olhar",
    "olaro",
    "oleoa",
    "oleha",
    "olear",
    "oleos",
    "oleoh",
    "oleho",
    "oloho",
    "olosh",
    "olost",
    "olhos",
    "oosth",
    "oosho",
    "oohos",
    "ohost",
    "ooste",
    "osaro",
    "ostar",
    "ostea",
    "oharo",
    "ohosa",
    "haroe",
    "hoaro",
    "hosar",
    "hosta",
]
ooha_second = [
    "oosa",
    "ooha",
    "ohoa",
    "oloa",
    "olha",
    "oaro",
    "olar",
    "olea",
    "ooar",
    "ohar",
]
ooha_third = ["ooa", "oha", "oar", "ola", "olo", "olh", "oos", "ooh", "oho"]
ooha_fourth = [
    "ooh2a",
    "oos2a",
    "oho2a",
    "oo2ar",
    "oh2ar",
    "o2aro",
    "o2oha",
    "o2osa",
    "o2hoa",
    "o2oar",
    "o2har",
    "olo2a",
    "olh2a",
    "ol2ar",
    "ol2oa",
    "ol2ha",
    "ole2a",
    "oleo2",
    "oleh2",
    "olos2",
    "oloh2",
    "olho2",
    "oost2",
    "oosh2",
    "ooho2",
    "ohos2",
]
ooha_fifth = [
    "ooa2",
    "oha2",
    "ooh2",
    "oos2",
    "oho2",
    "ola2",
    "olo2",
    "olh2",
    "ole2",
    "ost2",
    "aro2",
    "oo2a",
    "oh2a",
    "ol2a",
    "oo2h",
    "ol2o",
    "ol2h",
    "o2oa",
    "o2ha",
    "o2ar",
    "o2oh",
    "o2os",
    "o2ho",
]
ooha_sixth = [
    "oostea",
    "oostha",
    "ooshoa",
    "oohosa",
    "ohosta",
    "olosta",
    "olosha",
    "olohoa",
    "olhosa",
    "oleosa",
    "oleoha",
    "olehoa",
    "oostar",
    "ooshar",
    "oohoar",
    "ohosar",
    "oosaro",
    "ooharo",
    "ohoaro",
    "ooaroe",
    "oharoe",
    "olaroe",
    "olearo",
]
ooha = [
    ooha_first,
    ooha_second,
    ooha_third,
    ooha_fourth,
    ooha_fifth,
    ooha_sixth,
]


class TestUserNameGenMethod2(unittest.TestCase):
    def _test_person(self, name, reference, max_level):
        name_creator = UserNameGenMethod2()
        success = True
        for level in range(0, max_level):
            for correct_user_name in reference[level]:
                user_name = name_creator.create_username(name)
                if user_name != correct_user_name:
                    success = False
                    print("Got: {}, expected: {}".format(user_name, correct_user_name))
        return success

    def test_pia_munk_jensen(self):
        name = ["Pia", "Munk", "Jensen"]
        for i in range(1, 7):
            test_result = self._test_person(name, pmj, i)
            self.assertTrue(test_result, "Priority: {}".format(i))

    def test_anders_kristian_jens_peter_andersen(self):
        name = ["Anders", "Kristian", "Jens", "Peter", "Andersen"]
        for i in range(1, 7):
            test_result = self._test_person(name, akjpa, i)
            self.assertTrue(test_result, "Priority: {}".format(i))

    def test_karina_jensen(self):
        name = ["Karina", "Jensen"]
        for i in range(1, 7):
            test_result = self._test_person(name, kj, i)
            self.assertTrue(test_result, "Priority: {}".format(i))

    def test_karina_munk_jensen(self):
        name = ["Karina", "Munk", "Jensen"]
        for i in range(1, 7):
            test_result = self._test_person(name, kmj, i)
            self.assertTrue(test_result, "Priority: {}".format(i))

    def test_Olê_Østergård_Høst_Ærøe(self):
        name = ["Olê", "Østergård", "Høst", "Ærøe"]
        for i in range(1, 7):
            test_result = self._test_person(name, ooha, i)
            self.assertTrue(test_result, "Priority: {}".format(i))

    def test_too_many_karina_jensens(self):
        """
        Tests behaviour of creating so many identical names, that
        the algorithm runs out of options. For 'Karina Jensen' this
        should happen after 89 attempts.
        """
        name = ["Karina", "Jensen"]
        name_creator = UserNameGenMethod2()
        for i in range(1, 88):
            name_creator.create_username(name)
        with self.assertRaisesRegex(RuntimeError, "Failed to create user name"):
            name_creator.create_username(name)

    def test_dry_run(self):
        """
        Tests that with dry_run set to True, we can keep adding the
        same person and never run into an answer of None.
        """
        name = ["Karina", "Jensen"]
        name_creator = UserNameGenMethod2()
        for i in range(1, 100):
            user_name = name_creator.create_username(name, dry_run=True)
        self.assertFalse(user_name is None)

    def test_multiple_names(self):
        """
        Test that we can create a large number of random names without hitting a
        bug that stops the program. Test succeeds if the code does not raise an
        exception.
        """
        name_creator = UserNameGenMethod2()
        for i in range(0, 250):
            name = create_name()
            name_creator.create_username(name)

    def test_name_fixer(self):
        """
        Test that the name fixer allows a-z and does not allow values outside this
        range.
        """
        name_creator = UserNameGenMethod2()
        name = ["Anders", "abzæ-{øå", "Andersen"]
        fixed_name = name_creator._name_fixer(name)
        expected_name = ["Anders", "abzaoa", "Andersen"]
        self.assertTrue(fixed_name == expected_name)

    def test_check_is_case_insensitive(self):
        name = ["Fornavn", "Efternavn"]
        name_creator = UserNameGenMethod2()
        # Generate username (no occupied names yet)
        first_username = name_creator.create_username(name)
        # Add upper-case version of generated username to list of occupied names
        name_creator.add_occupied_names({first_username.upper()})
        # Generate second username from same name
        second_username = name_creator.create_username(name)
        # Assert new username is different, even when case is ignored
        self.assertNotEqual(first_username.lower(), second_username.lower())


class TestUserNameGenPermutation(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.instance = UserNameGenPermutation()

    @given(
        st.lists(
            st.text(
                min_size=1,
                alphabet=st.characters(
                    whitelist_characters="bcdfghjklmnpqrstvwxz",
                    whitelist_categories=(),
                ),
            ),
            min_size=2,
        )
    )
    def test_valid_input(self, name):
        # If `name` has at least two items, each item being a string of at least
        # one consonant, we should be able to create a username.
        self.instance.create_username(name)

    def test_suffix_increments(self):
        name = ["B", "C", "D"]
        for expected_suffix in range(1, 100):
            username = self.instance.create_username(name)
            self.assertEqual(username, "bcd%d" % expected_suffix)

    def test_skips_names_already_taken(self):
        name = ["First Name", "Last-Name"]
        self.instance.add_occupied_names({"fnm1", "fnm4"})
        for expected_username in ("fnm2", "fnm3", "fnm5"):
            username = self.instance.create_username(name)
            self.assertEqual(username, expected_username)

    @parameterized.expand(
        [
            ("Abel Spendabel", "asp1"),  # First name starts with a vowel
            ("Erik Ejegod", "ejg1"),  # Both first name and last name start with a vowel
            ("Erik Episk Ejegod", "eps1"),  # All parts start with a vowel
            ("Gorm Den Gamle", "gdn1"),  # All parts start with a consonant
            ("Ba Ca Da", "bcd1"),  # All parts start with a consonant
            ("Theodor Fælgen", "tfl1"),  # Last name contains non-ASCII character
            ("Øjvind Ørn", "jrn1"),  # All parts begin with non-ASCII characters
            ("Ea Obe", "ebb1"),  # Last name contains just one consonant
            ("Ivan Aaaa", "ivn1"),  # Last name contains *only* vocals
            ("Ab Aaa", "abb1"),  # Only *one* consonant across *all* name parts
        ]
    )
    def test_by_example(self, name, expected_username):
        name = name.split(maxsplit=1)
        actual_username = self.instance.create_username(name)
        self.assertEqual(actual_username, expected_username)

    def test_check_is_case_insensitive(self):
        name = ["Fornavn", "Efternavn"]
        # Generate username (no occupied names yet)
        first_username = self.instance.create_username(name)
        # Add upper-case version of generated username to list of occupied names
        self.instance.add_occupied_names({first_username.upper()})
        # Generate second username from same name
        second_username = self.instance.create_username(name)
        # Assert new username is different, even when case is ignored
        self.assertNotEqual(first_username.lower(), second_username.lower())

    def test_max_iterations(self):
        with self.assertRaises(ValueError):
            self.instance.create_username(["A"])


class TestUserNameSet(unittest.TestCase):
    def test_contains_is_case_insensitive(self):
        username_set = UserNameSet()
        username_set._usernames = {"aaa", "BBB"}
        self.assertIn("Aaa", username_set)
        self.assertIn("bbb", username_set)


class _UserNameCSVFileHelperMixin:
    csv_path = "some/fs/path"
    csv_lines = [
        "%s,foo" % UserNameSetCSVFile._column_name,
        "abcd1234,",
        "efgh5678",
    ]

    def _get_instance(self, cls):
        load_setting_path = "integrations.ad_integration.user_names.load_setting"
        with mock.patch(load_setting_path, return_value=lambda: self.csv_path):
            with mock.patch("io.open") as mock_open:
                mock_open.return_value.__enter__.return_value = self.csv_lines
                instance = cls()
                instance._mock_open = mock_open
                return instance


class TestUserNameSetCSVFile(_UserNameCSVFileHelperMixin, unittest.TestCase):
    def test_can_read_csv(self):
        instance = self._get_instance(UserNameSetCSVFile)
        instance._mock_open.assert_called_once_with(
            self.csv_path,
            "r",
            encoding=UserNameSetCSVFile._encoding,
        )
        self.assertEqual(instance._usernames, {"abcd1234", "efgh5678"})

    def test_contains(self):
        instance = self._get_instance(UserNameSetCSVFile)
        self.assertIn("abcd1234", instance)
        self.assertNotIn("abc123", instance)

    def test_iter(self):
        instance = self._get_instance(UserNameSetCSVFile)
        self.assertSetEqual(instance._usernames, set(instance))


class TestUserNameCSVFileSubstring(_UserNameCSVFileHelperMixin, unittest.TestCase):
    def test_contains(self):
        instance = self._get_instance(UserNameSetCSVFileSubstring)
        # List of taken names contains an exact match for the new username
        self.assertIn("abcd1234", instance)
        # List of taken names contains a substring match for the new username
        self.assertIn("abcd12345678", instance)
        # New username is not a substring of any of the taken names
        self.assertNotIn("abcd", instance)


Base = declarative_base()  # type: Any


class MockedUserNameTable(Base):
    __tablename__ = "usernames"

    uid = Column(Integer, primary_key=True)
    username = Column(String)
    other_random_info = Column(String, default="test")


class TestUserNameSetInDatabase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        self.session = sessionmaker(bind=self.engine, autoflush=False)()

        Base.metadata.create_all(self.engine)
        name1 = MockedUserNameTable(uid=1, username="Alice")
        self.session.add(name1)
        name2 = MockedUserNameTable(uid=2, username="Bob")
        self.session.add(name2)
        name3 = MockedUserNameTable(uid=3, username="Bob")
        self.session.add(name3)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    @mock.patch.object(
        UserNameSetInDatabase,
        "_get_settings",
        return_value=("", "usernames", "username"),
    )
    def test_load_from_database(self, settings_mock):
        with mock.patch.object(
            UserNameSetInDatabase, "_get_session", return_value=self.session
        ) as sql_mock:

            instance = UserNameSetInDatabase()
            assert sql_mock.call_count == 1
            assert instance._usernames == {"Alice", "Bob"}

    @mock.patch.object(UserNameSetInDatabase, "__init__", return_value=None)
    def test_get_session(self, init_mock):
        instance = UserNameSetInDatabase()
        s = instance._get_session("sqlite://")
        assert isinstance(s, session.Session)
