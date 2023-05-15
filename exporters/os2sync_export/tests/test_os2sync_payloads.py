import pytest
from more_itertools import one
from os2sync_export.config import get_os2sync_settings


# Webtests that assumes access to a running os2mo with the default "Kolding" dataset.
# Run using `pytest -v -m webtest`
# Skip using `pytest -v -m "not webtest"`


@pytest.mark.webtest
@pytest.mark.parametrize("xfer_cpr", [True, False])
@pytest.mark.parametrize(
    "user_uuid",
    [
        "a5fca2fc-1c24-4db3-b39f-70c477605793",
        "6442339e-da8f-49cf-beba-d6b0cb025750",
        "1586a3ef-25c9-44ae-89c9-98bbc90ef033",
    ],
)
def test_user(xfer_cpr, user_uuid, mock_env):
    from os2sync_export.os2synccli import update_single_user

    settings = get_os2sync_settings(os2sync_xfer_cpr=xfer_cpr)
    payload = update_single_user(user_uuid, settings, dry_run=True)
    cpr = one(payload)["Person"]["Cpr"]
    assert cpr is not None if xfer_cpr else cpr is None
