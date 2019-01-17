# -*- coding: utf-8 -*-
import itertools
from threading import Lock


class ThreadSafeTee:
    """ Thread-safe wrapper for `itertools.tee` objects.

    Copied from https://stackoverflow.com/questions/6703594/itertools-tee-thread-safe
    """

    def __init__(self, tee, lock):
        self.tee = tee
        self.lock = lock

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.tee)

    def __copy__(self):
        return ThreadSafeTee(self.tee.__copy__(), self.lock)


def safetee(iterable, n):
    """ Replacement for `itertools.tee` that returns `ThreadSafeTee` objects.
    """
    lock = Lock()
    return (ThreadSafeTee(tee, lock) for tee in itertools.tee(iterable, n))


class Teeable:
    """ Wrapper for `iterable`s that allows them to later be `tee`d
    (as in `itertools.tee`) *and* used in a thread-safe manner.

    This is useful for wrapping generators and other iterables that cannot be copied,
    such as streaming requests and responses. It is required by extensions which
    inspect requests and responses, such as the Nameko Tracer.
    """

    def __init__(self, iterable):
        self.iterable = iter(iterable)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterable)

    def tee(self):
        self.iterable, safe_tee = safetee(self.iterable, 2)
        return safe_tee
