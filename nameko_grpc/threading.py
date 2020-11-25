# -*- coding: utf-8 -*-
import sys


def target_with_callback(target, args=(), kwargs=None, callback=None):
    """ Executes `target(*args, **kwargs)` and optionally calls a `callback`
    with the result or any exception raised.
    """
    if kwargs is None:
        kwargs = {}

    def execute():
        try:
            res = target(*args, **kwargs)
        except Exception:
            res = None
            exc_info = sys.exc_info()
            raise
        else:
            exc_info = None
        finally:
            if callback:
                callback(res, exc_info)

    return execute
