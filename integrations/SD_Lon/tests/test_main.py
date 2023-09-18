from datetime import datetime
from unittest.mock import patch, MagicMock
from uuid import uuid4

from fastapi.testclient import TestClient
from freezegun import freeze_time

from sdlon.main import create_app
from tests.test_fix_departments import _TestableFixDepartments


@patch("sdlon.main.get_changed_at_settings")
@patch("sdlon.main.FixDepartments")
def test_trigger_fix_departments(
    mock_fix_dep: MagicMock,
    mock_get_changed_at_settings: MagicMock,
):
    # Arrange
    fix_departments = _TestableFixDepartments.get_instance()
    fix_departments.fix_department = MagicMock()
    fix_departments.fix_NY_logic = MagicMock()
    mock_fix_dep.return_value = fix_departments

    app = create_app()
    client = TestClient(app)

    ou = str(uuid4())
    today = datetime.today().date()

    # Act
    r = client.post(f"/trigger/fix-departments/{ou}")

    # Assert
    fix_departments.fix_department.assert_called_once_with(ou, today)
    fix_departments.fix_NY_logic.assert_called_once_with(ou, today)

    assert r.status_code == 200
    assert r.json() == {"msg": "success"}


@patch("sdlon.main.get_changed_at_settings")
@patch("sdlon.main.FixDepartments")
def test_trigger_fix_departments_on_error(
    mock_fix_dep: MagicMock,
    mock_get_changed_at_settings: MagicMock,
):
    # Arrange
    fix_departments = _TestableFixDepartments.get_instance()
    error = Exception("some error")
    fix_departments.fix_department = MagicMock(side_effect=error)
    mock_fix_dep.return_value = fix_departments

    app = create_app()
    client = TestClient(app)

    # Act
    r = client.post(f"/trigger/fix-departments/{str(uuid4())}")

    # Assert
    assert r.status_code == 500
    assert r.json() == {"msg": str(error)}
