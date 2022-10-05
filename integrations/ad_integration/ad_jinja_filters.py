import re
import sys
from functools import partial
from typing import Iterator
from typing import List
from typing import Match
from typing import Optional

from jinja2 import contextfilter
from more_itertools import first
from unidecode import unidecode


def first_address_of_type(value: List[dict], address_type_uuid: str) -> Optional[str]:
    return first(
        (
            addr["value"]
            for addr in value
            if addr["address_type"]["uuid"] == address_type_uuid
        ),
        None,
    )


def location_element(value, index, sep="\\"):
    try:
        return value.split(sep)[index]
    except IndexError:
        return None


@contextfilter
def name_to_email_address(ctx, value):
    upn_end = ctx["_upn_end"]
    all_emails = ctx["_get_all_ad_emails"]()
    serial_start = 3

    def _split_and_normalize(name: str) -> List[str]:
        special = {ord("@"): None, ord("'"): None}

        parts: List[str] = re.split(r"[\-\s+]", name)
        parts = list(map(unidecode, parts))
        parts = list(map(str.lower, parts))
        parts = list(map(lambda s: s.translate(special), parts))

        if len(parts) < 2:
            raise ValueError("name must have at least two parts (name=%r)" % name)
        elif len(parts) == 2:
            return parts
        elif len(parts) > 2:
            return [parts[0], parts[1], parts[-1]]

        return []  # never reached, but makes mypy happy

    def _variant_a(parts: List[str]) -> str:
        # "firstname.lastname" if two name parts
        # "firstname.secondname.lastname" if more than two name parts
        return ".".join(parts)

    def _variant_b(parts: List[str]) -> str:
        # "f.lastname" if two name parts
        # "f.secondname.lastname" if more than two name parts
        return ".".join([parts[0][0]] + parts[1:])

    def _variant_c(parts: List[str]) -> str:
        # "firstname.l" if two name parts
        # "firstname.secondname.l" if more than two name parts
        return ".".join(parts[:-1] + [parts[-1][0]])

    def _variant_d(parts: List[str]) -> str:
        # Like variant A, except a serial number is appended.
        # If variants A, B, C and possibly D are already taken, find the next available
        # suffixed variant, e.g. "firstname.lastname.4" if "firstname.lastname.1" to
        # "firstname.lastname..3" are already taken.
        prefix = _variant_a(parts)

        def taken_suffixes() -> List[int]:
            # Look for emails matching e.g. "firstname.lastname.42@kommune.dk" and
            # extract the numeric part. Filter out non-matches.
            pattern: str = rf"{re.escape(prefix)}\.(\d+)@{re.escape(upn_end)}"
            prefix_matches: Iterator[Match] = filter(
                None, map(partial(re.search, pattern), all_emails)
            )
            # Extract numeric part, and cast to int.
            numeric_suffixes: Iterator[int] = map(
                lambda match: int(match.groups()[0]),
                prefix_matches,
            )
            return sorted(numeric_suffixes)

        def next_suffix():
            # Find next available suffix higher than or equal to `serial_start`.
            # E.g. if `taken_suffixes` is `{0, 1, 3, 4, 6}`, the next available suffix
            # is 5 - and not 2, as that would be below `serial_start` (which is 3.)
            taken = taken_suffixes()
            for new, curr in enumerate(taken + [sys.maxsize], serial_start):
                if new not in taken:
                    return new

        return f"{prefix:s}.{next_suffix():d}"

    def _gen_email(name: str) -> Optional[str]:
        parts = _split_and_normalize(name)

        for fn in (_variant_a, _variant_b, _variant_c, _variant_d):
            email = "%s@%s" % (fn(parts), upn_end)
            if email not in all_emails:
                return email

        raise ValueError("could not generate unique email address for %r" % value)

    return _gen_email(value)
