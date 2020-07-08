import time
import random
from pathlib import Path

import sys
from os.path import dirname
sys.path.append(dirname(__file__) + "/..")
sys.path.append(dirname(__file__) + "/../../..")
sys.path.append(dirname(__file__) + "/../../../os2mo_data_import")

from fixture_generator import dummy_data_creator

base = '../..//os2mo_data_import/fixture_generator/navne/'
fornavne = dummy_data_creator._load_names(Path(base + 'fornavne.txt'))
mellemnavne = dummy_data_creator._load_names(Path(base + 'mellemnavne.txt'))
efternavne = dummy_data_creator._load_names(Path(base + 'efternavne.txt'))


def _pick_name_from_list(names):
    total_weight = 0
    for name in names:
        total_weight += name[0]
    weight = 0
    stop_weight = random.randrange(total_weight)
    for name in names:
        weight += name[0]
        if weight > stop_weight:
            break
    return name[1]


def create_name():
    names = []
    names.append(_pick_name_from_list(fornavne))
    if random.random() > 0.25:
        names.append(_pick_name_from_list(mellemnavne))
    if random.random() > 0.8:
        names.append(_pick_name_from_list(mellemnavne))
    if random.random() > 0.99:
        names.append(_pick_name_from_list(mellemnavne))

    names.append(_pick_name_from_list(efternavne))
    return names


if __name__ == '__main__':
    t = time.time()
    for i in range(0, 1000):
        create_name()
    print(time.time() - t)
