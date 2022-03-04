from integrations.SD_Lon.engagement import is_external


def test_is_external_is_false_for_number_employment_id():
    assert not is_external("12345")
    assert not is_external("1")


def test_is_external_is_true_for_non_number_employment_id():
    assert is_external("ABCDE")
