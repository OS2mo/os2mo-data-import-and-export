import pytest


@pytest.fixture()
def mock_env(monkeypatch):
    """Set the USER env var to assert the behavior."""
    monkeypatch.setenv("MUNICIPALITY", "1234")
    monkeypatch.setenv("OS2SYNC_TOP_UNIT_UUID", "f06ee470-9f17-566f-acbe-e938112d46d9")
    return
