from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

from parameterized import parameterized
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError

from ..mora_helpers import MoraHelper


class MoPostMock:
    def json(self):
        return str(uuid4())

    def raise_for_status(self):
        pass


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

    # Inputs are titles and user_keys of current class and input. Lastly a bool indicating if it needs to post to MO or not.
    @parameterized.expand(
        [
            # Same title and bvn
            ("Current title", "Current bvn", "Current title", "Current bvn", False),
            # Same title, other bvn
            ("Current title", "Current bvn", "Current title", "Another bvn", False),
            # Same bvn, other title
            ("Current title", "Current bvn", "Another title", "Current bvn", False),
            # Other title and bvn
            ("Current title", "Current bvn", "Another title", "Another bvn", True),
        ]
    )
    def test_ensure_class(self, name, user_key, title, bvn, expected_post):
        """Test to check behavior of ensure_class_in_facet"""
        current_class_uuid = uuid4()
        org = uuid4
        owner = uuid4()
        with patch.object(
            self._instance,
            "read_classes_in_facet",
            return_value=[
                [{"uuid": str(current_class_uuid), "name": name, "user_key": user_key}],
                "",
            ],
        ):
            with patch.object(
                self._instance, "read_organisation", return_value=str(org)
            ):
                with patch.object(
                    self._instance, "_mo_post", return_value=MoPostMock()
                ) as _mo_post_mock:
                    return_uuid = self._instance.ensure_class_in_facet(
                        "facet_name", bvn, title=title, owner=owner
                    )
        if expected_post:
            _mo_post_mock.assert_called_once_with(
                "f/facet_name/",
                {
                    "name": title,
                    "user_key": bvn,
                    "scope": "TEXT",
                    "org_uuid": str(org),
                    "owner": str(owner),
                },
            )
            assert return_uuid != current_class_uuid
        else:
            _mo_post_mock.assert_not_called()
            assert return_uuid == current_class_uuid
