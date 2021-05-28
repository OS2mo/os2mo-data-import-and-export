import hashlib
import uuid
from functools import lru_cache, partial


@lru_cache(maxsize=None)
def generate_uuid(base: str, value: str) -> uuid.UUID:
    """
    Generate a predictable uuid based on org name and a unique value.
    """
    base_hash = hashlib.md5(base.encode())
    base_digest = base_hash.hexdigest()
    base_uuid = uuid.UUID(base_digest)

    combined_value = (str(base_uuid) + str(value)).encode()
    value_hash = hashlib.md5(combined_value)
    value_digest = value_hash.hexdigest()
    value_uuid = uuid.UUID(value_digest)
    return value_uuid


def uuid_generator(base):
    """Make a uuid generator with a fixed base"""
    return partial(generate_uuid, base)
