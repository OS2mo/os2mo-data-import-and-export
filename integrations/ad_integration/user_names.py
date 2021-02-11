#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random

import click
from ad_reader import ADParameterReader
from operator import itemgetter

import username_rules.method_2


METHOD = 'metode 2'

CHAR_REPLACEMENT = {
    'â': 'a',
    'ä': 'a',
    'à': 'a',
    'å': 'a',
    'Ä': 'a',
    'Å': 'a',
    'æ': 'a',
    'Æ': 'a',
    'á': 'a',
    'Á': 'a',
    'Â': 'a',
    'À': 'a',
    'ã': 'a',
    'Ã': 'a',
    'é': 'e',
    'ê': 'e',
    'ë': 'e',
    'è': 'e',
    'É': 'e',
    'Ê': 'e',
    'Ë': 'e',
    'È': 'e',
    'ï': 'i',
    'î': 'i',
    'ì': 'i',
    'í': 'i',
    'Î': 'i',
    'Ï': 'i',
    'ô': 'o',
    'ö': 'o',
    'ò': 'o',
    'Ö': 'o',
    'ø': 'o',
    'Ø': 'o',
    'ó': 'o',
    'Ó': 'o',
    'Ô': 'o',
    'Ò': 'o',
    'õ': 'o',
    'Õ': 'o',
    'ü': 'u',
    'û': 'u',
    'ù': 'u',
    'Ü': 'u',
    'ú': 'u',
    'Ú': 'u',
    'Û': 'u',
    'Ù': 'u',
    'ÿ': 'y',
    'ý': 'y',
    'Ý': 'y',
    'Ç': 'c',
    'ç': 'c',
    'ñ': 'n',
    'Ñ': 'n'
}


def _name_fixer(name):
    for i in range(0, len(name)):
        # Replace accoding to replacement list
        for char, replacement in CHAR_REPLACEMENT.items():
            name[i] = name[i].replace(char, replacement)

        # Remove all remaining charecters outside a-z
        for char in name[i].lower():
            if ord(char) not in range(ord('a'), ord('z') + 1):
                name[i] = name[i].replace(char, '')
    return name


class CreateUserNames(object):
    """
    An implementation of metode 2 in the AD MOX specification document
    (Bilag: Tildeling af brugernavne).
    """
    def __init__(self, occupied_names: set = None):
        self.method = METHOD
        self.occupied_names = self.set_occupied_names(occupied_names)
        self.combinations = [
            username_rules.method_2.FIRST,
            username_rules.method_2.SECOND,
            username_rules.method_2.THIRD,
            username_rules.method_2.FOURTH,
            username_rules.method_2.FITFTH,
            username_rules.method_2.SIXTH
        ]

    def _create_from_combi(self, name, combi):
        """
        Create a username from a name and a combination.
        """
        (code, max_position) = username_rules.method_2._readable_combi(combi)

        # Do not use codes that uses more names than the actual person has
        if max_position > len(name) - 2:
            return None

        # First letter is always first letter of first position
        if code[0] is not None:
            relevant_name = code[0]
            username = name[relevant_name][0].lower()
        else:
            username = 'X'

        current_char = 0
        for i in range(1, len(code)):
            if code[i] == code[i-1]:
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
                username += 'X'
        return username

    def set_occupied_names(self, occupied_names: set = None):
        occupied_names = occupied_names or set()
        self.occupied_names = occupied_names

    def populate_occupied_names(self, **kwargs):
        """
        Read all usernames from AD and add them to the list of reserved names.
        :return: The amount of added users.
        """
        reader = ADParameterReader(**kwargs)
        all_users = reader.read_it_all()
        occupied_names = set(map(itemgetter('SamAccountName'), all_users))
        self.set_occupied_names(occupied_names)
        del reader
        return len(occupied_names)

    def disqualify_unwanted_names(self, banned_words_list):
        """
        Read a list of non-allowed usernames and add them to the list of
        reserved names to avoid that they will be given to an actual user.
        """
        pass

    def _metode_1(self, name: list, dry_run=False) -> tuple:
        pass

    def _metode_2(self, name: list, dry_run=False) -> tuple:
        """
        Create a new username in accodance with the rules specified in this file.
        The username will be the highest quality available and the value will be
        added to list of used names, so consequtive calles with the same name
        will keep returning new names until the algorithm runs out of options
        and a RuntimeException is raised.

        :param name: Name of the user given as a list with at least two elements.
        :param dry_run: If true the name will be returned, but will be added to
        the interal list of reserved names.
        :return: A tuple with first element being the username and the second
        element being a tuple describing the quality of the username, first
        element is the prioritazion level (1-6) and second element being the actual
        rule that ended up suceeding. Lower numbers means better usernames.
        """
        final_user_name = ''
        quality = (0, 0)
        name = _name_fixer(name)

        for permutation_counter in range(2, 10):
            for prioritation in range(0, 6):
                if final_user_name != '':
                    break
                i = 0
                for combi in self.combinations[prioritation]:
                    i = i + 1
                    username = self._create_from_combi(name, combi)
                    if not username:
                        continue

                    indexed_username = username.replace('X',
                                                        str(permutation_counter))
                    if indexed_username not in self.occupied_names:
                        if not dry_run:
                            self.occupied_names.add(indexed_username)
                        final_user_name = indexed_username
                        quality = (prioritation + 1, i + 1, permutation_counter)
                        return final_user_name, quality

        # If we get to here, we completely failed to make a username
        raise RuntimeError('Failed to create user name')

    def _metode_3(self, name=[], dry_run=False) -> tuple:
        """
        Create a new random user name
        :param name: Not used in this algorithm.
        :param dry_run: Return a random valid name, but do not
        mark it as used.
        :return: A tuple with first element being the username, and second
        element being the tuple (0,0)
        """
        username = ''
        while username == '':
            for _ in range(0, 6):
                username += chr(random.randrange(97, 123))
            if username in self.occupied_names:
                username = ''
            else:
                if not dry_run:
                    self.occupied_names.add(username)
        return (username, (0, 0, 0))

    def create_username(self, name: list, dry_run=False) -> tuple:
        if self.method == 'metode 2':
            return self._metode_2(name, dry_run)
        if self.method == 'metode 3':
            return self._metode_3(name, dry_run)

    def stats(self, names, size=None, find_quality=None):
        if size is not None:
            difficult_names = set()
            quality_dist = {2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0,
                            'broken': 0}
            for i in range(0, size):
                name = names[i]
                try:
                    quality = self.create_username(name)
                    quality_dist[quality[1][2]] += 1
                except RuntimeError:
                    difficult_names.add(str(name))
                    quality_dist['broken'] += 1
            # print(difficult_names)
            print('Size: {}, qualiy: {}'.format(size, quality_dist))

        if find_quality is not None:
            user_count = 1
            hits = 0
            name = names[user_count]
            user_name = self.create_username(name)
            while hits < find_quality[1]:
                if user_name[1][2] >= find_quality[0]:
                    hits += 1
                if user_name[1][0] == 0:
                    hits = 100000000
                name = names[user_count]
                user_name = self.create_username(name)
                print('Count: {}, User name {}, hits: {}'.format(user_count,
                                                                 user_name,
                                                                 hits))
                user_count += 1
            print(user_count)
            print('------')


@click.command(help="User name creator")
@click.option(
    "--method",
    required=True,
    type=click.Choice(["2", "3"]),
    help='User name method (2 or 3)',
)
@click.option("--name", required=True, help='Name of user')
@click.option("-N", required=True, type=int, help='Number of user names to create')
@click.option(
    '--populate-occupied-names',
    is_flag=True,
    help='Populate occupied names from AD',
)
def cli(**args):
    name_creator = CreateUserNames(occupied_names=set())
    name_creator.method = "metode %s" % args["method"]

    if args['populate_occupied_names']:
        name_creator.populate_occupied_names()

    name = args['name'].split(' ')
    for i in range(0, args['n']):
        print(name_creator.create_username(name))


if __name__ == '__main__':
    cli()

    # name_creator = CreateUserNames(occupied_names=set())
    # name_creator.populate_occupied_names()
    # name_creator._cli()
    # name = ['Anders', 'Kristian', 'Jens', 'abzæ-{øå', 'Peter', 'Andersen']
    # print(name_creator.create_username(name))

    # import pickle
    # from pathlib import Path

    # names = []
    # p = Path('.')
    # name_files = p.glob('*.p')
    # for name in name_files:
    #     with open(str(name), 'rb') as f:
    #         names = names + pickle.load(f)

    # unrealistic_names = []
    # for i in range(0, len(names)):
    #     for name in names[i]:
    #         if len(name) < 2:
    #             unrealistic_names.append((names[i]))

    # for bad_name in unrealistic_names:
    #     names.remove(bad_name)

    # for i in range(1, 50):
    #     name_creator.occupied_names = set()
    #     name_creator.stats(names, size=i*5000)

    # print()
    # print('3, 1')
    # name_creator.stats(names, find_quality=(3, 1))

    # from tests.name_simulator import create_name
    # import time
    # t = time.time()
    # for i in range(19, 100):
    #     print(time.time() - t)
    #     name_list = []
    #     for _ in range(0, 10000):
    #         name = create_name()
    #         name_list.append(name)
    #     with open('name_list_{:04d}.p'.format(i), 'wb') as f:
    #         pickle.dump(name_list, f, pickle.HIGHEST_PROTOCOL)
