import datetime
import re

# TODO: move constants elsewhere
MO_INFINITY: str = "infinity"
SD_INFINITY: str = "9999-12-31"

# TODO: Create "MoValidity" and "SdValidity" classes based on the RA Models
#  "Validity" class and use these as input to the function below


def sd_to_mo_termination_date(sd_date: str) -> str:
    """
    Convert SD termination date to MO termination date.

    In MO, the termination date is the last day of work, while in SD it is the
    first day of non-work.

    Args:
        sd_date: SD termination date formatted as "YYYY-MM-DD"

    Returns:
        MO termination date formatted as "YYYY-MM-DD"
    """

    assert isinstance(sd_date, str)
    date_regex = re.compile("[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])")
    assert date_regex.match(sd_date)

    if sd_date == SD_INFINITY:
        return MO_INFINITY

    # In MO, the termination date is the last day of work,
    # in SD it is the first day of non-work.
    _sd_date = datetime.datetime.strptime(sd_date, "%Y-%m-%d")
    mo_date = _sd_date - datetime.timedelta(days=1)

    return mo_date.strftime("%Y-%m-%d")
