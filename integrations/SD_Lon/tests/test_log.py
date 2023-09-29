import pytest

from sdlon.log import anonymize_cpr


def test_anonymize_cpr():
    assert anonymize_cpr("1212127890") == "121212xxxx"


def test_anonymize_cpr_assert():
    with pytest.raises(AssertionError):
        anonymize_cpr("xyz")
