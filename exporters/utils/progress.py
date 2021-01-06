def progress_iterator(elements, outputter, mod=10, total=None):
    """Write progress reports as an iterator is being iterated.

    Args:
        elements: An iterator of elements
        outputter: A function taking two integers as arguments (current, total)
        mod: The modulus for when to generate progress outputs
        total: The total number of elements in elements or None for len(elements)

    Returns:
        Generator: Generating the elements from elements.
    """
    if total is None:
        total = len(elements)
    for i, element in enumerate(elements, start=1):
        if i == 1 or i % mod == 0 or i == total:
            outputter(i, total)
        yield element


def format_progress_iterator(elements, outputter, mod=10, total=None):
    """Write progress reports as an iterator is being iterated.

    Similar to progress_iterator, but outputter is being called with a single
    string instead, with the format of "current / total".
    """

    def helper(current, total):
        outputter("{} / {}".format(current, total))

    yield from progress_iterator(elements, helper, mod=mod, total=total)


def print_progress_iterator(elements, mod=10, total=None):
    """Write progress reports as an iterator is being iterated.

    Similar to format_progress_iterator, but outputter is bound to print.
    """
    yield from format_progress_iterator(elements, print, mod=mod, total=total)
