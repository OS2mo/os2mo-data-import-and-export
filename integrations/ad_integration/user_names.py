FIRST = [
    'F123L', 'F122L', 'F111L', 'F112L', 'F113L', 'F133L', 'F223L', 'F233L', 'F333L',
    'F11LL', 'F12LL', 'F13LL', 'F22LL', 'F23LL', 'F33LL', 'FLLLL', 'FF13L', 'FF12L',
    'FF11L', 'FF23L', 'FF22L', 'FF33L', 'FF1LL', 'FF2LL', 'FF3LL', 'FFLLL', 'FFF1L',
    'FFF2L', 'FFF3L', 'FFFLL', 'FFF11', 'FFF12', 'FFF13', 'FFF22', 'FFF23', 'FFF33',
    'FFFFL', 'FFFF1', 'FFFF2', 'FFFF3', 'FF123', 'FF122', 'FF113', 'FF112', 'FF111',
    'FF133', 'FF233', 'FF223', 'FF222', 'FF333', 'F1233', 'F1223', 'F1123', 'F1113',
    'F1112', 'F1122', 'F1222', 'F1133', 'F1333', 'F2333', 'F2233', 'F2223', 'F2222',
    'F3333', 'F1111'
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
    'F1111L', 'F1112L', 'F1113L', 'F1122L', 'F1123L', 'F1133L', 'F1222L', 'F1223L',
    'F1233L', 'F2222L', 'F2223L', 'F2233L', 'F2333L', 'F3333L', 'FF111L', 'FF112L',
    'FF113L', 'FF122L', 'FF123L', 'FF222L', 'FF223L', 'FF233L', 'FF333L', 'FFF11L',
    'FFF12L', 'FFF13L', 'FFF22L', 'FFF23L', 'FFF33L', 'FFFF1L', 'FFFF2L', 'FFFF3L',
    'F111LL', 'F112LL', 'F113LL', 'F122LL', 'F123LL', 'F133LL', 'F222LL', 'F223LL',
    'F233LL', 'F333LL', 'F11LLL', 'F12LLL', 'F13LLL', 'F22LLL', 'F23LLL', 'F33LLL',
    'F1LLLL', 'F2LLLL', 'F3LLLL', 'FLLLLL', 'FFLLLL', 'FFFLLL', 'FFFFLL', 'FFFFFL'
]


def _readable_combi(combi):
    readable_combi = []
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
        # Last name (independan of middle names)
        if combi[i] == 'L':
            position = -1
        readable_combi.append(position)
    return readable_combi


class CreateUserNames(object):
    """
    An implementation of metode 2 in the AD MOX specification document
    (Bilag: Tildeling af brugernavne).
    """
    def __init__(self, occupied_names: set):
        self.occupied_names = occupied_names
        self.combinations = [FIRST, SECOND, THIRD, FOURTH]

    def _create_from_combi(self, name, combi):
        """
        Create a username from a name and a combination.
        """
        code = _readable_combi(combi)

        if max(code) > len(name) - 2:
            return None

        # First letter is always first letter of first position
        relevant_name = code[0]
        username = name[relevant_name][0]

        current_char = 0
        for i in range(1, len(code)):
            if code[i] == code[i-1]:
                current_char += 1
            else:
                current_char = 0

            relevant_name = code[i]
            if current_char >= len(name[relevant_name]):
                username = None
                break
            username += name[relevant_name][current_char].upper()
        return username

    def populate_occupied_names(self):
        """
        Read all usernames from AD and add them to the list of reserved names.
        """
        pass

    def disqualify_unwanted_names(self, banned_words_list):
        """
        Read a list of non-allowed usernames and add them to the list of
        reserved names to avoid that they will be given to an actual user.
        """
        pass

    def create_username(self, name: list, dry_run=False) -> tuple:
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
        element is the prioritazion level (1-4) and second element being the actual
        rule that ended up suceeding. Lower numbers means better usernames.
        """
        final_user_name = ''
        quality = (0, 0)

        for prioritation in range(0, 4):
            if final_user_name is not '':
                break
            i = 0
            for combi in self.combinations[prioritation]:
                i = i + 1
                username = self._create_from_combi(name, combi)
                if not username:
                    continue
                if username not in self.occupied_names:
                    if not dry_run:
                        self.occupied_names.add(username)
                    final_user_name = username
                    quality = (prioritation + 1, i + 1)
                    break
        return final_user_name, quality


if __name__ == '__main__':
    name = ['Pia', 'Munk', 'Jensen']
    # name = ['Karina', 'Jensen']
    # name = ['Karina', 'Munk', 'Jensen']
    # name = ['Anders', 'Kristian', 'Jens', 'Peter', 'Andersen']
    name_creator = CreateUserNames(occupied_names=set())

    print(name_creator.create_username(name))
