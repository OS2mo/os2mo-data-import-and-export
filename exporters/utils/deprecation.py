import warnings
from functools import wraps


def deprecated(func):
    """Mark the decorated function as deprecated."""

    @wraps(func)
    def new_func(*args, **kwargs):
        warnings.warn(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return new_func
