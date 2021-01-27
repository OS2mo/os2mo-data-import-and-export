from unittest import TestCase
<<<<<<< HEAD
from unittest.mock import mock_open, patch
=======
from unittest.mock import patch, mock_open
>>>>>>> e84e9da (Test utils)

from exporters.utils.load_settings import load_settings


class LoadSettingsTests(TestCase):
    def setUp(self):
        # Clear the lru_cache in between tests
        load_settings.cache_clear()

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_cache(self, mock_file):
        """Test that calling load_settings multiple times only reads the file once.

        The test only tries twice, but could do it 'n' times.
        """
        result1 = load_settings()
        result2 = load_settings()
        mock_file.assert_called_once()
        self.assertEqual(result1, result2)

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_missing_file(self, mock_file):
        """Test that load_settings propergates FileNotFound errors."""
        mock_file.side_effect = FileNotFoundError()
        with self.assertRaises(FileNotFoundError):
            result1 = load_settings()
