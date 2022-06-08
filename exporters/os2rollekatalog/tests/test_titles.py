from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st

from exporters.os2rollekatalog.os2rollekatalog_integration import read_engagement_types
from exporters.os2rollekatalog.os2rollekatalog_integration import Title
from exporters.os2rollekatalog.os2rollekatalog_integration import Titles


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
    with patch(
        "exporters.os2rollekatalog.os2rollekatalog_integration.GraphQLClient"
    ) as mock_session:
        mock_return = {
            "facets": [{"user_key": "engagement_type", "classes": test_input}]
        }
        mock_session.execute.return_value = mock_return
        t = read_engagement_types(mock_session)
    assert t == expected
