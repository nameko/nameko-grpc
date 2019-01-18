# -*- coding: utf-8 -*-
import collections
from threading import Lock


def raisetee(iterable, n=2):
    """ Alternative to `itertools.tee` that will raise from all iterators if the
    source iterable raises.

    Modified from the "roughly equivalent" example in the documentation at
    https://docs.python.org/3/library/itertools.html#itertools.tee
    """
    source = iter(iterable)
    deques = [collections.deque() for i in range(n)]

    def gen(mydeque):
        while True:
            if not mydeque:
                try:
                    val = next(source)
                except StopIteration:
                    return
                except Exception as exc:
                    val = exc
                for d in deques:
                    d.append(val)
            yield mydeque.popleft()

    def read(generator):
        for item in generator:
            if isinstance(item, Exception):
                raise item
            yield item

    return tuple(read(gen(d)) for d in deques)


class ThreadSafeTee:
    """ Thread-safe wrapper for `itertools.tee` (or `raisetee`) objects.

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
    return (ThreadSafeTee(tee, lock) for tee in raisetee(iterable, n))


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
