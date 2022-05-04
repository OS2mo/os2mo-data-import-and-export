from unittest.mock import Mock
from uuid import uuid4

from integrations.os2sync.lcdb_os2mo import is_ignored
from integrations.os2sync.tests.helpers import dummy_settings


def test_lcdb_is_ignored():
    ignored_level_uuid = uuid4()
    ignored_type_uuid = uuid4()
    used_level_uuid = uuid4()
    used_type_uuid = uuid4()
    settings = dummy_settings

    settings.os2sync_ignored_unit_levels = [ignored_level_uuid, uuid4()]
    settings.os2sync_ignored_unit_types = [ignored_type_uuid, uuid4()]

    # random uuids, not in settings
    unit = Mock(enhedsniveau_uuid=str(uuid4()), enhedstype_uuid=str(uuid4()))
    assert not is_ignored(unit, settings)
    # Unit type is ignored:
    unit.enhedstype_uuid = str(ignored_type_uuid)
    assert is_ignored(unit, settings)
    # Unit type is used:
    unit.enhedstype_uuid = str(used_type_uuid)
    assert not is_ignored(unit, settings)
    # ignored unit_level:
    unit.enhedsniveau_uuid = str(ignored_level_uuid)
    assert is_ignored(unit, settings)
    # Used unit_level:
    unit.enhedsniveau_uuid = str(used_level_uuid)
    assert not is_ignored(unit, settings)

    # No unit_level:
    unit.enhedsniveau_uuid = None
    assert not is_ignored(unit, settings)
    # No unit_type:
    unit.enhedstype_uuid = None
    assert not is_ignored(unit, settings)
