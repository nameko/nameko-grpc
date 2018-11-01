# -*- coding: utf-8 -*-
from collections import OrderedDict


buckets = OrderedDict(
    [
        (0.000000001, "n"),
        (0.000001, "u"),
        (0.001, "m"),
        (1, "S"),
        (60, "M"),
        (3600, "H"),
    ]
)
inverse_buckets = {unit: value for value, unit in buckets.items()}


def unbucket_timeout(value):
    unit = value[-1]
    count = int(value[:-1])
    multiplier = inverse_buckets[unit]
    return count * multiplier


def bucket_timeout(value):

    for period in buckets:
        if value // period > 1:
            last_period = period
        else:
            break
    return "{}{}".format(int(value / last_period), buckets[last_period])
