from collections import defaultdict
from typing import Dict, List, TypeVar

DictKeyType = TypeVar("DictKey")
DictValueType = TypeVar("DictValue")


def transpose_dict(
    mydict: Dict[DictKeyType, DictValueType]
) -> Dict[DictValueType, List[DictKeyType]]:
    """Switches values in a dict to keys, with a list of the old keys as new values.

    >>> test_dict = {'test_key1': 'test_value1'}
    >>> transpose_dict(test_dict)
    {'test_value1': ['test_key1']}

    >>> test_dict = {'test_key1': 'test_value1','test_key2': 'test_value2','test_key3': 'test_value1'}
    >>> transpose_dict(test_dict)
    {'test_value1': ['test_key1', 'test_key3'], 'test_value2': ['test_key2']}
    """
    reversed_dict = defaultdict(list)
    for key, value in mydict.items():
        reversed_dict[value].append(key)
    return dict(reversed_dict)
