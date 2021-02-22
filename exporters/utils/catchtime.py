from contextlib import contextmanager
from time import perf_counter, process_time
from typing import Callable, Iterator, Tuple, Union


@contextmanager
def catchtime(
    include_process_time: bool = False,
) -> Iterator[Union[Callable[[], float], Callable[[], Tuple[float, float]]]]:
    """Measure time spend within the contextmanager.

    Usage:

        with catchtime() as t:
            time.sleep(1)
        time_spent = t()
        print(time_spent)  # --> Prints 1.0...

        with catchtime(True) as t:
            time.sleep(1)
        time_spent, process_time = t()
        print(time_spent)  # --> Prints 1.0...
        print(process_time)  # --> Prints 0.0...
    """
    real_start = perf_counter()
    process_start = process_time()
    result_func = lambda: (perf_counter() - real_start, process_time() - process_start)
    if include_process_time:
        yield result_func
    else:
        yield lambda: result_func()[0]
