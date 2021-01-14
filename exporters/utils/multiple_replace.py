import re
from more_itertools import unzip


def multiple_replace(string, replacement_dict):
    """Make multiple replacements in string.

    Example:
        >>> multiple_replace("I like tea", {"like": "love", "tea": "coffee"})
        I love coffee

    Args:
        string: The string to make replacements in.
        replacement_dict: Dictionary of replacements.
            Keys are replaced with their values.

    Returns:
        Modified string, with all replacements made.
    """
    # Replacing empty string is a mess
    keys = replacement_dict.keys()
    assert "" not in keys, "Cannot replace empty string"

    # Make a regex pattern, which matches all (escaped) keys
    escaped_keys = map(re.escape, keys)
    pattern = re.compile("|".join(escaped_keys))

    # For each match, replace found key with corresponding value
    return pattern.sub(
        lambda x: replacement_dict.get(x.group(0), ''), string
    )
