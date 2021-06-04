from collections.abc import Mapping
from functools import wraps


class AttrDict(dict):
    """Enable dot.notation access for a dict object.

    Example:
        script_result = AttrDict({"exit_code": 0})
        self.assertEqual(script_result.exit_code, 0)
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__  # type: ignore
    __delattr__ = dict.__delitem__  # type: ignore


def recursive_dict_update(original, updates):
    """Recursively update 'original' with keys from 'updates'.

    Example:
        original = {'alfa': {'beta': 2, 'charlie': 3}},
        updates = {'alfa': {'beta': 4}}
        # Non recursive update
        updated = {**original, **updates}
        self.assertEqual(updated, {'alfa': {'beta': 4}})
        # Recursive update
        r_updated = recursive_dict_update(original, updates)
        self.assertEqual(r_updated, {'alfa': {'beta': 4, 'charlie': 3}})

    Returns:
        dict: modified 'original'
    """
    for key, value in updates.items():
        if isinstance(value, Mapping):
            original[key] = recursive_dict_update(original.get(key, {}), value)
        else:
            original[key] = value
    return original


def dict_map(dicty, key_func=None, value_func=None, func=None):
    """Map the dict values.

    Example:
        input_dict = {1: 1, 2: 2, 3: 3}
        output_dict = dict_map(input_dict, value_func=lambda value: value ** 2)
        self.assertEqual(output_dict, {1: 1, 2: 4, 3: 6})

    Returns:
        dict: A dict where func has been applied to every value.
    """

    def identity(x):
        return x

    def tuple_identity(x, y):
        return (x, y)

    def help_call(func):
        def inner(x, **kwargs):
            try:
                return func(x, **kwargs)
            except TypeError:
                return func(x)

        return inner

    key_func = help_call(key_func or identity)
    value_func = help_call(value_func or identity)
    func = func or tuple_identity
    return dict(
        [
            func(key_func(key, value=value), value_func(value, key=key))
            for key, value in dicty.items()
        ]
    )


def dict_partition(func, dicty):
    """Partition the input dict into two using the predicate function.

    Example:
        input_dict = {0: 'value', 1: 'value': 2: 'value'}
        odd_dict, even_dict = dict_partition(
            lambda key, value: value % 2 == 0, input_dict
        )
        self.assertEqual(odd_dict, {1: 'value'})
        self.assertEqual(even_dict, {0: 'value', 2: 'value'})

    Returns:
        (dict, dict): A dict containing key-value pairs that failed the
                      predicate and a dict containing the key-value pairs
                      that passed the predicate.
    """
    falsy, truesy = {}, {}
    for key, value in dicty.items():
        write_dict = truesy if func(key, value) else falsy
        write_dict[key] = value
    return falsy, truesy


def dict_filter(func, dicty):
    return dict_partition(func, dicty)[1]


def dict_exclude(dicty, keys):
    return dict_filter(lambda key, value: key not in keys, dicty)


def dict_subset(dicty, keys):
    return dict_filter(lambda key, value: key in keys, dicty)


def duplicates(iterable):
    """Return set of duplicates from iterable.

    Example:
        input_list = [1, 5, 2, 4, 2, 1]
        dup_set = duplicates(input_list)
        self.assertEqual(dup_set, {1, 2})

    Returns:
        set: A set of the elements which are duplicates.
    """
    seen = set()
    return set(x for x in iterable if x in seen or seen.add(x))


def lower_list(listy):
    """Convert each element in the list to lower-case.

    Example:
        result = lower_list(['Alfa', 'BETA', 'gamma'])
        self.assertEqual(result, ['alfa', 'beta', 'gamma'])

    Args:
        listy: The list of strings to force into lowercase.

    Returns:
        list: A list where all contained the strings are lowercase.
    """
    return [x.lower() for x in listy]


def apply(func):
    """Return decorated / applied version of func.

    Example:
        @apply
        def tuple_expand(num, char):
            return char * num

        elements = [(1,'a'), (2, 'b'), (3, 'c')]
        elements = list(map(tuple_expand, elements))
        self.assertEqual(elements, ['a', 'bb', 'ccc'])

    Args:
        func: The function to apply arguments for

    Returns:
        wrapped function: Which has its one argument applied.
    """

    @wraps(func)
    def applied(tup):
        return func(*tup)

    return applied


def progress_iterator(elements, outputter=print, mod=10):
    """Output progress as iteration progresses.

    Example:
        elements = [(1,'a'), (2, 'b'), (3, 'c')]
        elements = progress_iterator(elements)
        elements = map(itemgetter(1), elements)
        self.assertEqual(list(elements), [1,2,3])

    Args:
        elements: list of elements,
        outputter: Function to call with progress strings,
        mod: The modulus for printing operations.

    Returns:
        Generator of objects in elements.
    """
    total = len(elements)
    for i, element in enumerate(elements, start=1):
        if i == 1 or i % mod == 0 or i == total:
            outputter("{}/{}".format(i, total))
        yield element
