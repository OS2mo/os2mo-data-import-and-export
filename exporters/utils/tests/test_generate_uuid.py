from unittest import TestCase
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import text

from exporters.utils.generate_uuid import generate_uuid, uuid_generator


class test_generate_uuid(TestCase):
    @given(text(), text())
    def test_generate_uuid(self, value, base):
        uuid1 = generate_uuid(value, base)
        uuid2 = generate_uuid(value, base)
        uuid3 = generate_uuid(value, base + "A different string")
        assert uuid1 == uuid2
        assert uuid1 != uuid3
        assert isinstance(uuid1, UUID)

    @given(text(), text())
    def test_create_generator(self, value, base):
        gen = uuid_generator(base)
        uuid1 = gen(value)
        assert uuid1 == gen(value)
        assert uuid_generator != gen(value + "Another string")
        assert isinstance(uuid1, UUID)
