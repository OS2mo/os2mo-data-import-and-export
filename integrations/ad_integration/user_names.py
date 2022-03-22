import csv
import io
import logging
import string
from operator import itemgetter
from typing import List

from more_itertools import interleave_longest
from more_itertools import nth_permutation
from ra_utils.load_settings import load_setting

from .ad_exceptions import ImproperlyConfigured
from .ad_reader import ADParameterReader
from .username_rules import method_2


logger = logging.getLogger(__name__)

NameType = List[str]


class UserNameGen:
    _setting_prefix = "integrations.ad_writer.user_names"

    @classmethod
    def get_implementation(cls) -> "UserNameGen":
        """Returns an implementation of `UserNameGen`.

        If you want to load occupied usernames from AD or elsewhere, you can
        call `load_occupied_names` on the instance returned by this method.

        By default, no occupied names are loaded on the instance returned by
        this method. This can be used in the cases where we want to spare the
        overhead of retrieving all AD usernames, for instance.
        """
        implementation_class = UserNameGenMethod2  # this is the default

        get_class_name = load_setting(f"{cls._setting_prefix}.class")

        try:
            name = get_class_name()
            implementation_class = cls._lookup_class_by_name(name)
        except (ValueError, FileNotFoundError):
            # ValueError: "not in settings file and no default"
            # FileNotFoundError: could not find "settings.json"
            logger.warning(
                "could not read settings, defaulting to %r",
                implementation_class,
            )
        except NameError:
            logger.warning(
                "could not find class %r, defaulting to %r",
                name,
                implementation_class,
            )

        instance = implementation_class()
        return instance

    @classmethod
    def _lookup_class_by_name(cls, name):
        if name and name in globals():
            return globals()[name]
        raise NameError(f"could not find class {name}")

    def __init__(self):
        self.occupied_names = set()
        self._loaded_occupied_name_sets = []

    def add_occupied_names(self, occupied_names: set) -> None:
        self.occupied_names.update(set(occupied_names))
        self._loaded_occupied_name_sets.append(occupied_names)

    def create_username(self, name: NameType, dry_run=False) -> str:
        raise NotImplementedError("must be implemented by subclass")

    def load_occupied_names(self):
        # Always load AD usernames when this method is called
        self.add_occupied_names(UserNameSetInAD())

        # Load any extra username sets specified in settings
        setting_name = f"{self._setting_prefix}.extra_occupied_name_classes"
        get_usernameset_class_names = load_setting(setting_name, default=[])
        try:
            usernameset_class_names = get_usernameset_class_names()
        except FileNotFoundError:  # could not find "settings.json"
            logger.info("could not read settings, not adding extra username sets")
        else:
            for name in usernameset_class_names:
                try:
                    cls = self._lookup_class_by_name(name)
                except NameError:
                    raise ImproperlyConfigured(
                        f"{setting_name!r} refers to unknown class {name!r}"
                    )
                else:
                    username_set = cls()
                    self.add_occupied_names(username_set)
                    logger.debug("added %r to set of occupied names", username_set)


class UserNameGenMethod2(UserNameGen):
    """
    An implementation of metode 2 in the AD MOX specification document
    (Bilag: Tildeling af brugernavne).
    """

    def __init__(self):
        super().__init__()
        self.combinations = [
            method_2.FIRST,
            method_2.SECOND,
            method_2.THIRD,
            method_2.FOURTH,
            method_2.FIFTH,
            method_2.SIXTH,
        ]

    def _name_fixer(self, name: NameType) -> NameType:
        for i in range(0, len(name)):
            # Replace according to replacement list
            for char, replacement in method_2.CHAR_REPLACEMENT.items():
                name[i] = name[i].replace(char, replacement)

            # Remove all remaining characters outside a-z
            for char in name[i].lower():
                if ord(char) not in range(ord("a"), ord("z") + 1):
                    name[i] = name[i].replace(char, "")

        return name

    def _readable_combi(self, combi):
        readable_combi = []
        max_position = -1
        for i in range(0, len(combi)):
            # First name
            if combi[i] == "F":
                position = 0
            # First middle name
            if combi[i] == "1":
                position = 1
            # Second middle name
            if combi[i] == "2":
                position = 2
            # Third middle name
            if combi[i] == "3":
                position = 3
            # Last name (independent of middle names)
            if combi[i] == "L":
                position = -1
            if combi[i] == "X":
                position = None
            if position is not None and position > max_position:
                max_position = position
            readable_combi.append(position)
        return (readable_combi, max_position)

    def _create_from_combi(self, name, combi):
        """
        Create a username from a name and a combination.
        """
        (code, max_position) = self._readable_combi(combi)

        # Do not use codes that uses more names than the actual person has
        if max_position > len(name) - 2:
            return None

        # First letter is always first letter of first position
        if code[0] is not None:
            relevant_name = code[0]
            username = name[relevant_name][0].lower()
        else:
            username = "X"

        current_char = 0
        for i in range(1, len(code)):
            if code[i] == code[i - 1]:
                current_char += 1
            else:
                current_char = 0

            if code[i] is not None:
                relevant_name = code[i]
                if current_char >= len(name[relevant_name]):
                    username = None
                    break
                username += name[relevant_name][current_char].lower()
            else:
                username += "X"
        return username

    def create_username(self, name: NameType, dry_run: bool = False) -> str:
        """
        Create a new username in accodance with the rules specified in this file.
        The username will be the highest quality available and the value will be
        added to list of used names, so consequtive calles with the same name
        will keep returning new names until the algorithm runs out of options
        and a RuntimeException is raised.

        :param name: Name of the user given as a list with at least two elements.
        :param dry_run: If true the name will be returned, but will be added to
        the interal list of reserved names.
        :return: New username generated.
        """
        final_user_name = ""
        name = self._name_fixer(name)

        for permutation_counter in range(2, 10):
            for prioritation in range(0, len(self.combinations)):
                if final_user_name != "":
                    break
                i = 0
                for combi in self.combinations[prioritation]:
                    i = i + 1
                    username = self._create_from_combi(name, combi)
                    if not username:
                        continue
                    indexed_username = username.replace("X", str(permutation_counter))
                    if indexed_username not in self.occupied_names:
                        if not dry_run:
                            self.occupied_names.add(indexed_username)
                        final_user_name = indexed_username
                        return final_user_name

        # If we get to here, we completely failed to make a username
        raise RuntimeError("Failed to create user name")


class UserNameGenPermutation(UserNameGen):
    def __init__(self):
        super().__init__()
        self.length = 3
        self.allowed_chars = "".join(set(string.ascii_lowercase) - set("aeiouy"))

    def create_username(self, name: NameType, dry_run: bool = False) -> str:
        suffix = 1
        permutation_index = 0

        feed = self._get_feed(name)

        while True:
            letters = self._create_permutation(feed, permutation_index)
            new_username = "%s%d" % (letters, suffix)
            if new_username not in self.occupied_names:
                # An unused username was found, add it to the list of
                # occupied names and return.
                self.occupied_names.add(new_username)
                return new_username
            else:
                # We are still looking for an available username.
                # Bump the `suffix` variable until it reaches 10.
                suffix += 1
                if suffix > 9:
                    # Reset the `suffix` back to 1, and go to the next
                    # permutation.
                    suffix = 1
                    permutation_index += 1

    def _get_feed(self, name: NameType):
        name_cleaned = self._remove_unwanted_letters(name)
        feed = list(interleave_longest(*name_cleaned))

        # If there are not enough consonants in `feed`, pad it using 'x' chars
        feed_length = sum(len(item) for item in feed)
        if feed_length < self.length:
            feed.extend("x" * (self.length - feed_length))

        return feed

    def _create_permutation(self, feed, index=0):
        permutation = nth_permutation(feed, r=self.length, index=index)
        username = "".join(permutation).lower()
        return username

    def _remove_unwanted_letters(self, name: NameType):
        return [
            "".join(char for char in name_part if char.lower() in self.allowed_chars)
            for name_part in name
        ]


class UserNameSet:
    def __init__(self):
        self._usernames = set()

    def __contains__(self, username: str) -> bool:
        return username in self._usernames

    def __iter__(self):
        return iter(self._usernames)


class UserNameSetCSVFile(UserNameSet):
    _encoding = "utf-8-sig"
    _column_name = "Brugernavn"

    def __init__(self):
        get_path = load_setting("integrations.ad_writer.user_names.disallowed.csv_path")
        with io.open(get_path(), "r", encoding=self._encoding) as stream:
            reader = csv.DictReader(stream)
            self._usernames = set(row[self._column_name] for row in reader)


class UserNameSetInAD(UserNameSet):
    def __init__(self):
        reader = ADParameterReader()
        all_users = reader.read_it_all()
        self._usernames = set(map(itemgetter("SamAccountName"), all_users))
