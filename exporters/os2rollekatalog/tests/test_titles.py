from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st

from exporters.os2rollekatalog.titles import check_update_titles
from exporters.os2rollekatalog.titles import read_engagement_job_function
from exporters.os2rollekatalog.titles import Title
from exporters.os2rollekatalog.titles import Titles


@given(st.uuids(), st.text())
def test_title_class(uuid, user_key):
    t = Title(uuid=uuid, user_key=user_key)
    assert t
    assert t.dict() == {"uuid": uuid, "name": user_key}
    assert Titles(titles=[t])


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ([], []),
        (
            [{"uuid": "5d4be2a1-e977-44f0-ae27-d7b0bebf1ae7", "user_key": "1"}],
            [{"uuid": "5d4be2a1-e977-44f0-ae27-d7b0bebf1ae7", "name": "1"}],
        ),
    ],
)
def test_read_titles(test_input, expected):
    with patch("exporters.os2rollekatalog.titles.GraphQLClient") as mock_session:
        mock_return = {
            "facets": [{"user_key": "engagement_job_function", "classes": test_input}]
        }
        mock_session.execute.return_value = mock_return
        t = read_engagement_job_function(mock_session)
    assert t == expected


@patch("requests.get")
@patch("requests.post")
@pytest.mark.parametrize(
    "dry_run,titles_input",
    [
        (True, []),
        (False, []),
        (True, [{"uuid": "5d4be2a1-e977-44f0-ae27-d7b0bebf1ae7", "name": "1"}]),
        (False, [{"uuid": "5d4be2a1-e977-44f0-ae27-d7b0bebf1ae7", "name": "1"}]),
    ],
)
def test_check_update_titles(mock_post, mock_get, dry_run, titles_input):
    check_update_titles(
        url="dummy", api_key="UUID", titles=titles_input, dry_run=dry_run
    )
    mock_get.assert_called_once_with("dummy", headers={"ApiKey": "UUID"}, verify=False)
    if dry_run:
        mock_post.assert_not_called()
    else:
        mock_post.assert_called_once_with(
            "dummy", json=titles_input, headers={"ApiKey": "UUID"}, verify=False
        )
