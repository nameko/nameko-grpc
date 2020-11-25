# -*- coding: utf-8 -*-
import sys


def target_with_callback(target, args=(), kwargs=None, callback=None):

    if kwargs is None:
        kwargs = {}

    def execute():
        try:
            res = target(*args, **kwargs)
        except Exception:
            res = None
            exc_info = sys.exc_info()
            # print traceback?
        else:
            exc_info = None
        if callback:
            callback(res, exc_info)

    return execute
