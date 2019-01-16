# -*- coding: utf-8 -*-
import pytest

from nameko_grpc.timeout import bucket_timeout, unbucket_timeout


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, "0n"),
        (0.0000000001, "0n"),
        (0.000000001, "1n"),
        (0.000001999, "1999n"),
        (0.000002001, "2u"),
        (1, "1000m"),
        (2, "2S"),
        (60, "60S"),
        (119, "119S"),
        (120, "2M"),
        (3600, "60M"),
        (7200, "2H"),
        (86400, "24H"),
    ],
)
def test_bucket_timeout(value, expected):
    assert bucket_timeout(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("0n", 0),
        ("1n", 0.000000001),
        ("100000n", 0.0001),
        ("1S", 1),
        ("200000M", 12000000),
        ("24H", 86400),
    ],
)
def test_unbucket_timeout(value, expected):
    assert unbucket_timeout(value) == expected
