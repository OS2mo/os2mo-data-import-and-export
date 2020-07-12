from collections.abc import Mapping


class LazyDict(Mapping):
    """Lazily initialized dictionary.

    Initialized on first access using the initializer set with set_initializer.
    """

    def __init__(self, *args, **kw):
        self._raw_dict = None
        self._initializer = None

    def set_initializer(self, func):
        """Set initalizer for the LazyDict.

        Can be called multiple times to override initializer, until the
        LazyDict has been accessed and thus the initializer has been run.

        Throws ValueError if the LazyDict has already been initialized.

        Args:
            func: Function which takes no arguments and returns python dict.

        Returns:
            None
        """
        if self._raw_dict:
            raise ValueError("Already initialized")
        self._initializer = func

    def is_initialized(self):
        """Check whether the LazyDict has already been initialized.
        
        Returns:
            bool: Whether the LazyDict has run the initializer.
        """
        return self._raw_dict != None

    def _run_initializer_if_required(self):
        """Helper method called on access to run initializer.
        
        Throws ValueError if the LazyDict has no initializer configured.
        Throws ValueError if the LazyDict has already been initialized.

        Returns:
            None
        """
        if not self._initializer:
            raise ValueError("No initializer provided")
        if not self._raw_dict:
            self._raw_dict = self._initializer()

    def __getitem__(self, key):
        self._run_initializer_if_required()
        return self._raw_dict.__getitem__(key)

    def __iter__(self):
        self._run_initializer_if_required()
        return iter(self._raw_dict)

    def __len__(self):
        self._run_initializer_if_required()
        return len(self._raw_dict)


class AttrDict(dict):
    """Enable dot.notation access for a dict object.

    Example:
        script_result = AttrDict({"exit_code": 0})
        self.assertEqual(script_result.exit_code, 0)
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
