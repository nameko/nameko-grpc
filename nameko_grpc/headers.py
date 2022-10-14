# -*- coding: utf-8 -*-
import base64


def is_grpc_header(name):
    return name.startswith("grpc-")


def is_pseudo_header(name):
    return name.startswith(":")


def is_http2_header(name):
    return name in ("te", "content-type", "user-agent", "accept-encoding")


def filter_headers_for_application(headers):
    def include(header):
        name, _ = header
        if is_pseudo_header(name) or is_grpc_header(name) or is_http2_header(name):
            return False
        return True

    return list(filter(include, headers))


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

    return sorted(headers, key=weight)


def check_encoded(headers):
    for name, value in headers:
        assert isinstance(name, bytes)
        assert isinstance(value, bytes)


def check_decoded(headers):
    for name, value in headers:
        assert not isinstance(name, bytes)
        if name.endswith("-bin"):
            assert isinstance(value, bytes)
        else:
            assert not isinstance(value, bytes)


def decode_header(header):
    name, value = header
    name = name.decode("utf-8")
    if name.endswith("-bin"):
        value = base64.b64decode(
            value + b"=="
        )  # add padding, per https://stackoverflow.com/a/49459036/128749
    else:
        value = value.decode("utf-8")
    return name, value


def encode_header(header):
    name, value = header
    name = name.encode("utf-8")
    if name.endswith(b"-bin"):
        value = base64.b64encode(value)
    else:
        value = value.encode("utf-8")
    return name, value


def comma_join(values):
    if all(map(lambda item: isinstance(item, bytes), values)):
        separator = b","
    else:
        separator = ","
    return separator.join(values)


class HeaderManager:
    def __init__(self):
        """
        Object for managing headers (or trailers). Headers are encoded before
        transmitting over the wire and stored in their non-encoded form.
        """
        self.data = []

    @staticmethod
    def decode(headers):
        return list(map(decode_header, headers))

    @staticmethod
    def encode(headers):
        return list(map(encode_header, headers))

    def get(self, name, default=None):
        """Get a header by `name`.

        Joins duplicate headers into a comma-separated string of values.
        """
        matches = []
        for key, value in self.data:
            if key == name:
                matches.append(value)
        if not matches:
            return default
        return comma_join(matches)

    def set(self, *headers, from_wire=False):
        """Set headers.

        Overwrites any existing header with the same name. Optionally decodes
        the headers first.
        """
        # XXX why isn't this clobbered?
        if from_wire:
            headers = self.decode(headers)
        check_decoded(headers)

        # clear existing headers with these names
        to_clear = dict(headers).keys()
        self.data = [(key, value) for (key, value) in self.data if key not in to_clear]

        self.data.extend(headers)

    def append(self, *headers, from_wire=False):
        """Add new headers.

        Preserves and appends to any existing header with the same name. Optionally
        decodes the headers first.
        """
        if from_wire:
            headers = self.decode(headers)
        check_decoded(headers)
        self.data.extend(headers)

    def __len__(self):
        return len(self.data)

    @property
    def for_wire(self):
        """A sorted list of encoded headers for transmitting over the wire."""
        return self.encode(sort_headers_for_wire(self.data))

    @property
    def for_application(self):
        """A filtered list of headers for use by the application."""
        return filter_headers_for_application(self.data)
