from unittest import TestCase
from unittest.mock import patch

from parameterized import parameterized
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError

from ..mora_helpers import MoraHelper


class TestMoraHelper(TestCase):
    def setUp(self):
        super().setUp()
        self._instance = MoraHelper()

    @parameterized.expand(
        [
            (ConnectionError, 5),
            (HTTPError, 5),
            (Exception, 1),
        ]
    )
    def test_mo_lookup_retries_on_error(self, exception_class, expected_attempts):
        """Check that the expected number of retry attempts are performed, depending
        on the specific exception raised by `requests.get`.
        """
        with self.assertRaises(exception_class):
            with patch("requests.get", side_effect=exception_class):
                self._instance._mo_lookup("not-a-uuid", "not-a-url")
                self.assertEqual(
                    self._instance._mo_lookup.retry.statistics["attempt_number"],
                    expected_attempts,
                )
