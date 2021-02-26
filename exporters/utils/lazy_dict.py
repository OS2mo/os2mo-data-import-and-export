from collections.abc import Mapping
from typing import Any, Callable, Iterator


class LazyEval:
    """Lazily evaluated dict member, used in tandem with LazyDict.

    For details and usage see LazyDict.
    """

    def __init__(self, cally: Callable, cache: bool = True) -> None:
        """Initializer.

        Note: If the callable needs arguments, these should be provided using before
              calling this initializer using functools.partial or similar.
              Alternatively arguments can be provided and fetched from the LazyDict
              itself, as this is provided during the __call__ method.

        Args:
            cally: The callable to execute for lazy evaluation.
            cache: Whether to cache the result in the LazyDict after execution.
        """

        self.cally = cally
        self.cache = cache

    def do_cache(self) -> bool:
        """Return whether the return-value of the __call__ method should be cached."""
        return self.cache

    def __call__(self, key: Any, dictionary: "LazyDict") -> Any:
        """Evaluate the callable.

        Is first tried with the key and LazyDict as argument, and if this fails,
        retried without any arguments being provided.
        """
        try:
            return self.cally(key, dictionary)
        except TypeError:
            return self.cally()

    def __str__(self) -> str:
        return "LazyEval" + str(self.cally)

    def __repr__(self) -> str:
        return "LazyEval" + repr(self.cally)


class LazyDict(Mapping):
    """Dictionary supporting lazy evaluation of some keys.

    Usage:

        def expensive_func(n: int = 5):
            time.sleep(n)
            return n

        # Initialization finishes without sleeping
        d = LazyDicy({'a': LazyEval(expensive_func), 'b': 2})
        print(d['b'])  # --> Prints 2 immediately
        print(d['a'])  # --> Prints 5 after 5 seconds
        print(d['a'])  # --> Prints 5 immediately (cached)
    """

    def __init__(self, *args, **kw) -> None:
        self._raw_dict = dict(*args, **kw)

    def __getitem__(self, key: Any) -> Any:
        """Implementation of evaluation of self[key].

        Fetches a value from the underlying dictionary, if the value is a LazyEval it
        is evaluated using _handle_lazy, otherwise it is returned as-is.
        """
        value = self._raw_dict.__getitem__(key)
        # Check if we got back a LazyEval item
        if isinstance(value, LazyEval):
            return self._handle_lazy(key, value)
        return value

    def __setitem__(self, key: Any, value: Any) -> None:
        self._raw_dict.__setitem__(key, value)

    def _handle_lazy(self, key: Any, lazy_eval: LazyEval) -> Any:
        """Evaluate the LazyEval and cache result if configured to do so."""
        value = lazy_eval(key, self)
        if lazy_eval.do_cache():
            self._raw_dict[key] = value
        return value

    def __str__(self) -> str:
        return str(self._raw_dict)

    def __repr__(self) -> str:
        return repr(self._raw_dict)

    def __iter__(self) -> Iterator:
        return iter(self._raw_dict)

    def __len__(self) -> int:
        return len(self._raw_dict)
