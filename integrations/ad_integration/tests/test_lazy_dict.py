import unittest

from parameterized import parameterized

from ..utils import LazyDict

LOOKUP_KEYS = ["key", "john", "deere"]


class TestLazyDict(unittest.TestCase):
    def test_happy_path(self):
        lazy_dict = LazyDict()

        # Initializer with call counter
        def initializer():
            initializer.call_count += 1
            return {"key": "value"}

        initializer.call_count = 0
        # Setup initializer
        lazy_dict.set_initializer(initializer)
        # Setting initializer should not call it
        self.assertEqual(initializer.call_count, 0)
        self.assertFalse(lazy_dict.is_initialized())
        # Getting key should call initializer
        self.assertEqual(lazy_dict.get("key"), "value")
        self.assertEqual(initializer.call_count, 1)
        self.assertTrue(lazy_dict.is_initialized())
        # Initializer should not be called on subsequent accesses
        self.assertEqual(lazy_dict.get("key"), "value")
        self.assertEqual(initializer.call_count, 1)
        # Accesssing invalid keys should not call the initializer
        self.assertEqual(lazy_dict.get("invalid_key"), None)
        self.assertEqual(initializer.call_count, 1)

    @parameterized.expand([(key,) for key in LOOKUP_KEYS])
    def test_uninitialized_get_fails(self, key):
        lazy_dict = LazyDict()
        # Accesssing an uninitialized dict is an error
        with self.assertRaises(ValueError) as context:
            lazy_dict.get(key)
        self.assertTrue("No initializer provided" in str(context.exception))

    def test_uninitialized_len_fails(self):
        lazy_dict = LazyDict()
        # Checking length of an uninitialized dict is an error
        with self.assertRaises(ValueError) as context:
            len(lazy_dict)
        self.assertTrue("No initializer provided" in str(context.exception))

    def test_uninitialized_iter_fails(self):
        lazy_dict = LazyDict()
        # Iterating an uninitialized dict is an error
        with self.assertRaises(ValueError) as context:
            iter(lazy_dict)
        self.assertTrue("No initializer provided" in str(context.exception))

    def test_setting_initializer_after_initialization(self):
        lazy_dict = LazyDict()
        # Can change initializer as much as we want until first access
        lazy_dict.set_initializer(lambda: {"alfa": 1})
        lazy_dict.set_initializer(lambda: {"alfa": 2})
        lazy_dict.set_initializer(lambda: {"alfa": 3})
        # On access the last set initializer is run
        self.assertEqual(lazy_dict.get("alfa"), 3)
        # After first access changing the initializer is an error
        with self.assertRaises(ValueError) as context:
            lazy_dict.set_initializer(lambda: {"alfa": 4})
        self.assertTrue("Already initialized" in str(context.exception))
