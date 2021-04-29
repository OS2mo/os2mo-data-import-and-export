import hashlib
import uuid
from functools import lru_cache


@lru_cache(maxsize=None)
def generate_uuid(seed):
    """
    Generate an UUID based on a seed in a deterministic way
    This allows us generate the same uuid for objects across different imports,
    without having to maintain a separate list of UUIDs, or fetch the relevant uuids
    from MO
    """
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    return uuid.UUID(m.hexdigest())
