# -*- coding: utf-8 -*-
import gzip
import zlib
from collections import OrderedDict


ENCODERS = OrderedDict(
    [
        ("deflate", {"compress": zlib.compress, "decompress": zlib.decompress}),
        ("gzip", {"compress": gzip.compress, "decompress": gzip.decompress}),
    ]
)
SUPPORTED_ENCODINGS = tuple(ENCODERS.keys()) + ("identity",)


class UnsupportedEncoding(Exception):
    pass


def decompress(data):
    for algorithm in ENCODERS.values():
        try:
            return algorithm["decompress"](data)
        except Exception:
            pass
    raise UnsupportedEncoding(
        "Could not decompress data. Supported algorithms: {})".format(
            ", ".join(SUPPORTED_ENCODINGS)
        )
    )


def compress(data, encoding):  # level?
    if encoding == "identity":
        return False, data
    if encoding in ENCODERS:
        # TODO: heuristic to decide whether we _should_ compress or not
        return True, ENCODERS[encoding]["compress"](data)
    raise UnsupportedEncoding(encoding)


def select_algorithm(acceptable_encodings, preferred_encoding):
    if preferred_encoding and preferred_encoding in SUPPORTED_ENCODINGS:
        return preferred_encoding
    if not acceptable_encodings:
        return "identity"
    for encoding in SUPPORTED_ENCODINGS:
        if encoding in acceptable_encodings:
            return encoding
    raise UnsupportedEncoding(
        "No supported algorithm acceptable: {} (supported: {})".format(
            acceptable_encodings, ", ".join(SUPPORTED_ENCODINGS)
        )
    )
