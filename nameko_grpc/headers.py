# -*- coding: utf-8 -*-
import base64


def is_grpc_header(name):
    return name.startswith("grpc-")


def is_pseudo_header(name):
    return name.startswith(":")


def is_http2_header(name):
    return name in ("te", "content-type", "user-agent", "accept-encoding")


def maybe_base64_decode(header):
    name, value = header
    if name.endswith("-bin"):
        value = base64.b64decode(value)
    return name, value


def maybe_base64_encode(header):
    name, value = header
    if name.endswith("-bin"):
        value = base64.b64encode(value)
    return name, value


def filter_headers_for_application(headers):
    def include(header):
        name, _ = header
        if is_pseudo_header(name) or is_grpc_header(name) or is_http2_header(name):
            return False
        return True

    return filter(include, headers)


def sort_headers_for_wire(headers):
    def weight(header):
        name, _ = header
        if is_pseudo_header(name):
            return 0
        if is_http2_header(name):
            return 1
        if is_grpc_header(name):
            return 2
        return 3

    # XXX needs unit test!
    return sorted(headers, key=weight)


class HeaderManager:
    def __init__(self, data=None):
        """
        Object for managing headers (or trailers).

        Accepts a list of tuples in the form `("<name>", "<value>")`
        """
        if data is None:
            data = []
        self.data = data

    def get(self, name, default=None):
        """ Get a header by `name`.

        Performs base64 decoding of binary values, and joins duplicate headers into a
        comma-separated string of values.
        """
        matches = []
        for key, value in self.data:
            if key == name:
                _, value = maybe_base64_decode((key, value))
                matches.append(value)
        if not matches:
            return default
        return ",".join(matches)

    def set(self, *headers):
        """ Set headers.

        Overwrites any existing header with the same name.
        """
        for name, _ in headers:
            self.clear(name)
        self.append(*headers)

    def clear(self, name):
        """ Clear a header by `name`.
        """
        self.data = list(filter(lambda header: header[0] != name, self.data))

    def append(self, *headers):
        """ Add new headers.

        Preserves and appends to any existing header with the same name.
        """
        self.data.extend(headers)

    def __len__(self):
        return len(self.data)

    @property
    def for_wire(self):
        """ A sorted list of headers for transmitting over the wire.
        """
        return list(map(maybe_base64_encode, sort_headers_for_wire(self.data)))

    @property
    def for_application(self):
        """ A filtered and processed list of headers for use by the application.
        """
        return list(map(maybe_base64_decode, filter_headers_for_application(self.data)))
