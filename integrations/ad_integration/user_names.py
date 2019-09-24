#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import argparse

from ad_reader import ADParameterReader

METHOD = 'metode 2'

FIRST = [
    'F123L', 'F122L', 'F111L', 'F112L', 'F113L', 'F133L', 'F223L', 'F233L', 'F333L',
    'F11LL', 'F12LL', 'F13LL', 'F22LL', 'F23LL', 'F33LL', 'FLLLL', 'FF13L', 'FF12L',
    'FF11L', 'FF23L', 'FF22L', 'FF33L', 'FF1LL', 'FF2LL', 'FF3LL', 'FFLLL', 'FFF1L',
    'FFF2L', 'FFF3L', 'FFFLL', 'FFF11', 'FFF12', 'FFF13', 'FFF22', 'FFF23', 'FFF33',
    'FFFFL', 'FFFF1', 'FFFF2', 'FFFF3', 'FF123', 'FF122', 'FF113', 'FF112', 'FF111',
    'FF133', 'FF233', 'FF223', 'FF222', 'FF333', 'F1233', 'F1223', 'F1123', 'F1113',
    'F1112', 'F1122', 'F1222', 'F1133', 'F1333', 'F2333', 'F2233', 'F2223', 'F2222',
    'F3333', 'F1111', 'LLLLL', '1LLLL', '11LLL', '111LL', '1111L', '12LLL', '122LL',
    '1222L', '123LL', '1233L', '13LLL', '133LL', '1333L', '2LLLL', '22LLL', '222LL',
    '2222L', '23LLL', '233LL', '2333L', '3LLLL', '33LLL', '333LL', '3333L'
]

SECOND = [
    'F11L', 'F12L', 'F13L', 'F22L', 'F23L', 'F33L', 'FF1L', 'FF2L', 'FF3L', 'FLLL',
    'FFLL', 'FFFL', 'F1LL', 'F2LL', 'F3LL'
]

THIRD = [
    'F1L', 'F2L', 'F3L', 'FLL', 'FFL', 'FF1', 'FF2', 'FF3', 'F11', 'F12',
    'F13', 'F22', 'F23', 'F33'
]

FOURTH = [
    'F12XL', 'F11XL', 'F13XL', 'F22XL', 'F23XL', 'F33XL', 'F1XLL', 'F2XLL', 'F3XLL',
    'FXLLL', 'FX13L', 'FX12L', 'FX11L', 'FX23L', 'FX22L', 'FX33L', 'FX1LL', 'FX2LL',
    'FX3LL', 'FF1XL', 'FF2XL', 'FF3XL', 'FFXLL', 'FFX1L', 'FFX2L', 'FFX3L', 'FFFXL',
    'FFFFX', 'FFF1X', 'FFF2X', 'FFF3X', 'FF11X', 'FF12X', 'FF13X', 'FF22X', 'FF23X',
    'FF33X', 'F123X', 'F111X', 'F112X', 'F113X', 'F122X', 'F133X', 'F222X', 'F223X',
    'F233X', 'F333X', 'XF12L', 'XF13L', 'XF11L', 'XF23L', 'XF22L', 'XF123', 'XF122',
    'XF112', 'XF111', 'XF223', 'XF233', 'XF333', 'XFLLL', 'XFF1L', 'XFF2L', 'XFF3L',
    'XFFLL', 'XFF12', 'XFF13', 'XFF11', 'XFF22', 'XFF23', 'XFF33', 'XFFF1', 'XFFF2',
    'XFFF3', 'XFFFF', 'X123L', 'X112L', 'X113L', 'X122L', 'X133L', 'X223L', 'X233L',
    'X333L', 'XLLLL'
]

FITFTH = [
    'F1LX', 'F2LX', 'F3LX', 'F12X', 'F13X', 'F11X', 'F23X', 'F22X', 'F33X', 'FFLX',
    'FF1X', 'FF2X', 'FF3X', 'FFFX', '123X', '122X', '111X', '223X', '233X', '333X',
    'LLLX', 'F1XL', 'F2XL', 'F3XL', 'FFXL', 'F1X2', 'F1X3', 'F2X3', 'FFX1', 'FFX2',
    'FFX3', 'FX1L', 'FX2L', 'FX3L', 'FXLL', 'FX12', 'FX13', 'FX11', 'FX23', 'FX22',
    'FX33', 'XF1L', 'XF2L', 'XF3L', 'XF12', 'XF13', 'XF11', 'XFFL', 'XFF1', 'XFF2',
    'XFF3', 'XFFF', 'X123', 'X122', 'X12L', 'X13L', 'X11L', 'X1LL', 'X133', 'X23L',
    'X22L', 'X2LL', 'X233', 'X33L', 'X3LL'
]

SIXTH = [
    'F1111L', 'F1112L', 'F1113L', 'F1122L', 'F1123L', 'F1133L', 'F1222L', 'F1223L',
    'F1233L', 'F2222L', 'F2223L', 'F2233L', 'F2333L', 'F3333L', 'FF111L', 'FF112L',
    'FF113L', 'FF122L', 'FF123L', 'FF222L', 'FF223L', 'FF233L', 'FF333L', 'FFF11L',
    'FFF12L', 'FFF13L', 'FFF22L', 'FFF23L', 'FFF33L', 'FFFF1L', 'FFFF2L', 'FFFF3L',
    'F111LL', 'F112LL', 'F113LL', 'F122LL', 'F123LL', 'F133LL', 'F222LL', 'F223LL',
    'F233LL', 'F333LL', 'F11LLL', 'F12LLL', 'F13LLL', 'F22LLL', 'F23LLL', 'F33LLL',
    'F1LLLL', 'F2LLLL', 'F3LLLL', 'FLLLLL', 'FFLLLL', 'FFFLLL', 'FFFFLL', 'FFFFFL'
]

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


def _readable_combi(combi):
    readable_combi = []
    max_position = -1
    for i in range(0, len(combi)):
        # First name
        if combi[i] == 'F':
            position = 0
        # First middle name
        if combi[i] == '1':
            position = 1
        # Second middle name
        if combi[i] == '2':
            position = 2
        # Third middle name
        if combi[i] == '3':
            position = 3
        # Last name (independant of middle names)
        if combi[i] == 'L':
            position = -1
        if combi[i] == 'X':
            position = None
        if position is not None and position > max_position:
            max_position = position
        readable_combi.append(position)
    return (readable_combi, max_position)


def _name_fixer(name):
    for i in range(0, len(name)):
        for char, replacement in CHAR_REPLACEMENT.items():
            name[i] = name[i].replace(char, replacement)

    return name


class CreateUserNames(object):
    """
    An implementation of metode 2 in the AD MOX specification document
    (Bilag: Tildeling af brugernavne).
    """
    def __init__(self, occupied_names: set):
        self.method = METHOD
        self.occupied_names = occupied_names
        self.combinations = [FIRST, SECOND, THIRD, FOURTH, FITFTH, SIXTH]

    def _create_from_combi(self, name, combi):
        """
        Create a username from a name and a combination.
        """
        (code, max_position) = _readable_combi(combi)

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

    def populate_occupied_names(self):
        """
        Read all usernames from AD and add them to the list of reserved names.
        :return: The amount of added users.
        """
        reader = ADParameterReader()
        all_users = reader.read_it_all()
        for user in all_users:
            self.occupied_names.add(user['SamAccountName'])
        del reader
        return len(all_users)

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
        added to list of used name, so consequtive calles with the same name
        will keep returning new names until the algorithm runs out of options
        and the returned name will be None.

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
                if final_user_name is not '':
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
                user_name = ''
            else:
                if not dry_run:
                    self.occupied_names.add(username)
        return (username, (0, 0))


    def create_username(self, name: list, dry_run=False) -> tuple:
        if self.method == 'metode 2':
            return self._metode_2(name, dry_run)
        if self.method == 'metode 3':
            return self._metode_3(name, dry_run)
    
    def stats(self, init_size=None, sample=None, find_quality=None):
        from tests.name_simulator import create_name
        if init_size is not None:
            for i in range(0, init_size):
                name = create_name()
                self.create_username(name)

            for i in range(0, sample):
                name = create_name()
                user_name = self.create_username(name)
                print('{}: {}'.format(name, user_name))

        if find_quality is not None:
            user_count = 1
            hits = 0
            name = create_name()
            user_name = self.create_username(name)
            while hits < find_quality[1]:
                if user_name[1][0] >= find_quality[0]:
                    hits += 1
                if user_name[1][0] == 0:
                    hits = 100000000
                name = create_name()
                user_name = self.create_username(name)
                print('Count: {}, User name {}, hits: {}'.format(user_count,
                                                                 user_name,
                                                                 hits))
                user_count += 1
            print('------')
            print(user_count)

    def _cli(self):
        parser = argparse.ArgumentParser(description='User name creator')
        parser.add_argument('--method', nargs=1, metavar='method',
                           help='User name method (2 or 3)')
        parser.add_argument('-N', nargs=1, type=int, help='Number of usernames')
        parser.add_argument('--name', nargs=1, metavar='name', help='Name of user')

        args = vars(parser.parse_args())

        name = args.get('name')[0].split(' ')

        if args.get('method')[0]=='2':
            self.method = 'metode 2'
        elif args.get('method')[0]=='3':
            self.method = 'metode 3'
        else:
            exit('No valid method given')

        for i in range(0, args.get('N')[0]):
            print(self.create_username(name))


if __name__ == '__main__':
    name_creator = CreateUserNames(occupied_names=set())
    # name_creator.populate_occupied_names()
    name_creator._cli()
    # name = ['Anders', 'Kristian', 'Jens', 'Peter', 'Andersen']
    # print(name_creator.create_username(name))
