import unittest

from parameterized import parameterized

from ..utils import AttrDict


class TestDotMapping(unittest.TestCase):
    @parameterized.expand(
        [
            (0,),
            ("value",),
            (3.14,),
        ]
    )
    def test_getattr_static(self, value):
        attr_dict = AttrDict({"status": value})
        self.assertEqual(attr_dict.status, value)

    @parameterized.expand(
        [
            (0,),
            ("value",),
            (3.14,),
        ]
    )
    def test_setattr_static(self, value):
        attr_dict = AttrDict({})
        attr_dict.status = value
        self.assertEqual(attr_dict.status, value)

    @parameterized.expand(
        [
            (0,),
            ("value",),
            (3.14,),
        ]
    )
    def test_delattr_static(self, value):
        attr_dict = AttrDict({"status": value})
        del attr_dict.status
        self.assertEqual(attr_dict, {})

    @parameterized.expand(
        [
            (
                "exit_code",
                0,
            ),
            (
                "key",
                "value",
            ),
            (
                "PI",
                3.14,
            ),
        ]
    )
    def test_getattr_dynamic(self, key, value):
        attr_dict = AttrDict({key: value})
        self.assertEqual(getattr(attr_dict, key), value)

    @parameterized.expand(
        [
            (
                "exit_code",
                0,
            ),
            (
                "key",
                "value",
            ),
            (
                "PI",
                3.14,
            ),
        ]
    )
    def test_setattr_dynamic(self, key, value):
        attr_dict = AttrDict({})
        setattr(attr_dict, key, value)
        self.assertEqual(attr_dict[key], value)

    @parameterized.expand(
        [
            (
                "exit_code",
                0,
            ),
            (
                "key",
                "value",
            ),
            (
                "PI",
                3.14,
            ),
        ]
    )
    def test_delattr_dynamic(self, key, value):
        attr_dict = AttrDict({key: value})
        delattr(attr_dict, key)
        self.assertEqual(attr_dict, {})
