# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from unittest.mock import Mock

import payload_db
from pytest import MonkeyPatch


def test_log_payload(monkeypatch: MonkeyPatch) -> None:
    # Arrange
    mock_session: Mock = Mock()
    mock_session_maker: Mock = Mock(return_value=mock_session)
    monkeypatch.setattr(payload_db, "get_engine", lambda: None)
    monkeypatch.setattr(payload_db, "Session", mock_session_maker)
    # Act
    payload_db.log_payload("full_url", "params", "response")
    # Assert
    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()
    payload: payload_db.Payload = mock_session.add.call_args.args[0]
    assert payload.full_url == "full_url"
    assert payload.params == "params"
    assert payload.response == "response"
