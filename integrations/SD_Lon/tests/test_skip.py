from copy import deepcopy
from unittest.mock import patch

from parameterized import parameterized

from sdlon.config import ChangedAtSettings
from sdlon.skip import cpr_env_filter
from .test_config import DEFAULT_CHANGED_AT_SETTINGS


class TestCprEnvFilter:
    @parameterized.expand(
        [
            (True, [], True),
            (True, ["5555555555"], False),
            (False, [], False),
            (False, ["5555555555"], True),
        ]
    )
    @patch("sdlon.skip.get_changed_at_settings")
    def test_return_true_for_exclude_mode_when_cpr_not_in_list(
        self, exclude_mode, cprs, expected, mock
    ):
        settings_dict = deepcopy(DEFAULT_CHANGED_AT_SETTINGS)
        settings_dict.update({"sd_exclude_cprs_mode": exclude_mode, "sd_cprs": cprs})
        mock.return_value = ChangedAtSettings.parse_obj(settings_dict)

        entity = {"PersonCivilRegistrationIdentifier": "5555555555"}

        assert cpr_env_filter(entity) == expected
