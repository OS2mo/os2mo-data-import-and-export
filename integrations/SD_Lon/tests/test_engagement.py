import unittest.mock
from copy import deepcopy

from more_itertools import one
from parameterized import parameterized

from .fixtures import get_read_employment_changed_fixture
from sdlon.engagement import _is_external
from sdlon.engagement import (
    create_engagement,
    is_employment_id_and_no_salary_minimum_consistent,
)


class TestIsExternal(unittest.TestCase):
    def test_is_external_is_false_for_number_employment_id(self):
        assert not _is_external("12345")
        assert not _is_external("1")

    def test_is_external_is_true_for_non_number_employment_id(self):
        assert _is_external("ABCDE")


class TestIsEmploymentIdAndNoSalaryMinimumConsistent(unittest.TestCase):
    @parameterized.expand(
        [
            (None, "12345", 1, True),
            (9000, "External", 10000, True),
            (9000, "External", 8000, False),
            (9000, "12345", 8000, True),
            (9000, "12345", 10000, False),
        ]
    )
    def test_return_values(
        self, no_salary_minimum, employment_id, job_pos_id, expected
    ):
        engagement = one(
            get_read_employment_changed_fixture(
                employment_id=employment_id,
                job_pos_id=job_pos_id,
            )
        )["Employment"]

        assert (
            is_employment_id_and_no_salary_minimum_consistent(
                engagement, no_salary_minimum
            )
            == expected
        )

    @parameterized.expand(
        [
            ("1001", True),
            ("9001", False),
        ]
    )
    def test_job_pos_ids_consistent_but_different(self, job_pos_id2, expected):
        # Arrange
        engagement = one(get_read_employment_changed_fixture())["Employment"]

        profession1 = deepcopy(engagement.get("Profession"))
        profession2 = deepcopy(engagement.get("Profession"))
        profession2["JobPositionIdentifier"] = job_pos_id2
        engagement.update({"Profession": [profession1, profession2]})

        # Assert
        assert (
            is_employment_id_and_no_salary_minimum_consistent(engagement, 9000)
            == expected
        )


class TestCreateEngagement(unittest.TestCase):
    @unittest.mock.patch("sdlon.engagement.read_employment_at")
    def test_return_none_when_sd_employment_not_found(self, mock_read_employment_at):
        mock_read_employment_at.return_value = None
        mock_sd_updater = unittest.mock.MagicMock()
        create_engagement(mock_sd_updater, 12345, "person_uuid")

        mock_sd_updater.assert_not_called()
