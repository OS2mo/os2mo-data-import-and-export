from functools import partial
from typing import Any, Callable, Dict, Iterator, List, Tuple

from jinja2 import Template

InnerFilterType = Callable[[List[str], List[Any]], bool]
FilterType = Callable[[List[Any]], bool]


def string_to_bool(v: str) -> bool:
    """Convert a string to a boolean.

    Note: The values for truth are chosen semi-arbitrarily.
    """
    return v.lower() in ("yes", "true", "1", "1.0")


def jinja_filter(
    template: Template, tuple_keys: List[str], tuple_values: List[Any]
) -> bool:
    """Filter function to evaluate the filter on the provided argument list.

    Note: Utilizes tuple_keys as keys to map the incoming list of arguments to
          jinja2 key-value context variables.
    Note: Utilizes string_to_bool to convert the output of the template into a
          boolean value.

    Args:
        tempalte: The jinja2 template to put our context into.
        tuple_keys: List of keys to put into the jinja2 template context.
        tuple_values: List of values to put into the jinja2 template context.

    Returns:
        boolean: Whether the filter passed or not.
    """
    context: Iterator[Tuple[str, Any]] = zip(tuple_keys, tuple_values)
    context_dict: Dict[str, Any] = dict(context)
    result: str = template.render(**context_dict)
    print("*", result)
    return string_to_bool(result)


def create_filter(
    jinja_string: str,
    tuple_keys: List[str],
) -> FilterType:
    """Convert a jinja2 filter strings into a filter function.

    Args:
        jinja_string: The filter string to be converted into a function.
        tuple_keys: List of keys to put into the jinja2 template context.

    Returns:
        filter function: The generated filter function.
    """
    filter_function: FilterType = partial(
        jinja_filter, Template(jinja_string), tuple_keys
    )
    return filter_function


def create_filters(
    jinja_strings: List[str],
    tuple_keys: List[str],
) -> List[FilterType]:
    """Convert a list of jinja2 filter strings into filter functions.

    For more details see create_filter.
    """

    filter_functions: Iterator[FilterType] = map(
        partial(create_filter, tuple_keys=tuple_keys), jinja_strings
    )
    return list(filter_functions)
