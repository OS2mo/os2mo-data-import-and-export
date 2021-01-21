import re

from more_itertools import unzip


def multiple_replace_compile(replacement_dict):
    """Make a regex pattern for finding all keys in replacement dict.

    Calling this directly with multiple_replace_run allows one to generate the regex
    only once, but using it multiple times, which is advantageous for performance.

    Args:
        replacement_dict: Dictionary of replacements. Keys are made into a regex.

    Returns:
        Regex matching all keys in replacement dict.
    """
    # Replacing empty string is a mess
    keys = replacement_dict.keys()
    assert "" not in keys, "Cannot replace empty string"

    # Make a regex pattern, which matches all (escaped) keys
    escaped_keys = map(re.escape, keys)
    pattern = re.compile("|".join(escaped_keys))

    return pattern


def multiple_replace_run(pattern, replacement_dict, string):
    """Run a a regex pattern to replace matches.

    Calling this directly with a regex from multiple_replace_compile allows one to
    only generate the regex once, but using it multiple times, which is advantageous
    for performance.

    Args:
        pattern: A regex pattern produced by multiple_replace_compile, using the
            same replacment_dict provided here.
        replacement_dict: Dictionary of replacements.
            Keys are replaced with their values.
        string: The string to make replacements in.

    Returns:
        Modified string, with all replacements made.
    """
    # For each match, replace found key with corresponding value
    return pattern.sub(lambda x: replacement_dict.get(x.group(0), ""), string)


def multiple_replace(replacement_dict, string):
    """Make multiple replacements in string.

    Example:
        >>> multiple_replace({"like": "love", "tea": "coffee"}, "I like tea")
        I love coffee

    Args:
        replacement_dict: Dictionary of replacements.
            Keys are replaced with their values.
        string: The string to make replacements in.

    Returns:
        Modified string, with all replacements made.
    """
    pattern = multiple_replace_compile(replacement_dict)
    return multiple_replace_run(pattern, replacement_dict, string)
