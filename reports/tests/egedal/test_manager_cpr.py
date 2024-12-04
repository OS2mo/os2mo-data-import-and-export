from unittest.mock import MagicMock

from reports.egedal.manager_cpr import find_cpr_manager_cpr


def gen_eng_response(employee_cpr: str, manager_cpr: str, has_parent: bool = True):
    return {
        "engagements": {
            "objects": [
                {
                    "current": {
                        "is_primary": True,
                        "person": [{"cpr_number": employee_cpr}],
                        "org_unit": [
                            {
                                "managers": [{"person": [{"cpr_number": manager_cpr}]}],
                                "parent": {
                                    "uuid": "2665d8e0-435b-5bb6-a550-f275692984ef"
                                }
                                if has_parent
                                else None,
                            }
                        ],
                    }
                }
            ]
        }
    }


def gen_orgunit_response(manager_cpr: str):
    return {
        "org_units": {
            "objects": [
                {
                    "current": {
                        "parent": {"uuid": "7a8e45f7-4de0-44c8-990f-43c0565ee505"},
                        "managers": [{"person": [{"cpr_number": manager_cpr}]}],
                    }
                }
            ]
        }
    }


def test_find_cpr_list_has_manager():
    """Test that the cpr-numbers are extracted from the graphql response correctly"""
    client = MagicMock()
    client.execute.return_value = gen_eng_response("1111111111", "2222222222")
    res = find_cpr_manager_cpr(client)
    client.execute.assert_called_once()
    assert res == [("1111111111", "2222222222")]


def test_find_cpr_list_is_manager():
    """Test that we search up the hierarchy until we find another manager"""
    client = MagicMock()
    client.execute.side_effect = [
        gen_eng_response("1111111111", "1111111111"),
        gen_orgunit_response("1111111111"),
        gen_orgunit_response("2222222222"),
    ]
    res = find_cpr_manager_cpr(client)
    assert client.execute.call_count == 3
    assert res == [("1111111111", "2222222222")]


def test_find_cpr_list_has_no_manager():
    """Test that the top manager has the same cpr number in both columns"""
    client = MagicMock()
    client.execute.return_value = gen_eng_response(
        "1111111111", "1111111111", has_parent=False
    )
    res = find_cpr_manager_cpr(client)
    client.execute.assert_called_once()
    assert res == [("1111111111", "1111111111")]
