from ra_utils.async_to_sync import async_to_sync

from constants import FK_org_uuid_it_system
from integrations.fkorg_it_systems.import_fkorg_orgunits import check_it_system_value


class MockResponse:
    def __init__(self, return_value):
        self.return_value = return_value

    def json(self):
        return self.return_value


class MockClient:
    def __init__(self, return_value):
        self.return_value = return_value

    async def get(self, url):
        return MockResponse(self.return_value)


@async_to_sync
async def test_no_it_system():
    moModel = MockClient([])
    uuid, changed = await check_it_system_value(moModel, "dummy_org", "new")
    assert not uuid
    assert changed


@async_to_sync
async def test_it_system_changed():
    account_uuid = "account_uuid"
    moModel = MockClient(
        [
            {
                "uuid": account_uuid,
                "user_key": "old",
                "itsystem": {"name": FK_org_uuid_it_system},
            }
        ]
    )
    uuid, changed = await check_it_system_value(moModel, "dummy_org", "new")
    assert uuid == account_uuid
    assert changed


@async_to_sync
async def test_it_system_unchanged():
    account_uuid = "account_uuid"
    moModel = MockClient(
        [
            {
                "uuid": account_uuid,
                "user_key": "old",
                "itsystem": {"name": FK_org_uuid_it_system},
            }
        ]
    )
    uuid, changed = await check_it_system_value(moModel, "dummy_org", "old")
    assert uuid == account_uuid
    assert not changed
