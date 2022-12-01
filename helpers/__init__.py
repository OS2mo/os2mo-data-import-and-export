from tqdm import tqdm as _tqdm


def tqdm(iterable=None, **kwargs) -> _tqdm:
    """Wrap `tqdm`, ensuring that we only print progress bars in interactive sessions,
    and *not* when running from cron or other non-interactive environments.
    """
    disable: bool = kwargs.pop("disable", None)
    return _tqdm(iterable=iterable, disable=disable, **kwargs)
