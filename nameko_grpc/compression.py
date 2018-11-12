# -*- coding: utf-8 -*-
import gzip
import zlib


ENCODING_PREFERENCES = ("deflate", "gzip")
SUPPORTED_ENCODINGS = ",".join(ENCODING_PREFERENCES + ("identity",))


class UnsupportedEncoding(Exception):
    pass


def decompress(data):
    try:
        zlib.decompress(data)
    except Exception:
        raise UnsupportedEncoding(
            "Could not decompress data. Supported algorithms: {})".format(
                ", ".join(ENCODING_PREFERENCES)
            )
        )


def compress(data, encoding):  # level?
    if encoding == "identity":
        return False, data
    if encoding == "deflate":
        # TODO: heuristic to decide whether we _should_ compress or not
        return True, zlib.compress(data)
    if encoding == "gzip":
        # TODO: heuristic to decide whether we _should_ compress or not
        return True, gzip.compress(data)

    raise UnsupportedEncoding(encoding)


def select_algorithm(acceptable_encodings):
    for encoding in ENCODING_PREFERENCES:
        if encoding in acceptable_encodings:
            return encoding
    raise UnsupportedEncoding(
        "No supported algorithm acceptable: {} (supported: {})".format(
            acceptable_encodings, ", ".join(ENCODING_PREFERENCES)
        )
    )
