import re

CPR_REGEX = re.compile("[0-9]{10}")


def anonymize_cpr(cpr: str) -> str:
    assert CPR_REGEX.match(cpr)
    return cpr[:6] + "xxxx"
