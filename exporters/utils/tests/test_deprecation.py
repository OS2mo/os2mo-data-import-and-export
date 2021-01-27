import warnings
from unittest import TestCase

from exporters.utils.deprecation import deprecated


@deprecated
def old_function():
    """Stand-in for an old deprecated function."""
    pass


class DeprecationTests(TestCase):

    def assertIsSubclass(self, cls, parent_cls):
        """Assert that cls is a subclass of parent_cls."""
        assert issubclass(cls, parent_cls)

    def test_warning(self):
        """Test that calling old_function yields a deprecation warning."""
        # Setup a warning catcher
        with warnings.catch_warnings(record=True) as warning_catcher:
            # Call the old function
            old_function()

            # Verify the warning was triggered
            self.assertEqual(len(warning_catcher), 1)
            warning = warning_catcher[0]
            self.assertIsSubclass(warning.category, DeprecationWarning)
            self.assertEqual(
                str(warning.message),
                "Call to deprecated function old_function."
            )
