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

    periods = list(buckets)
    bucket_period = periods.pop(0)

    for period in periods:
        if value // period > 1:
            bucket_period = period
        else:
            break
    return "{}{}".format(round(value / bucket_period), buckets[bucket_period])
