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


class AlreadySent(Exception):
    pass


class HeaderManager:
    def __init__(self, data=None):
        """
        Object for managing headers (or trailers). Maintains state tracking whether
        or not the headers have been transmitted over the wire.
        XXX really seems like this should live on the stream/

        Accepts a list of tuples in the form `("<name>", "<value>")`
        """
        if data is None:
            data = []
        self.data = data

        self.sent = False

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

        Preseveves and appends to any existing header with the same name.
        """
        for name, value in headers:
            self.data.append((name, value))

    @property
    def for_wire(self):
        """ A sorted list of headers for transmitting over the wire.
        """
        if self.sent:
            raise AlreadySent()
        self.sent = True
        return sort_headers_for_wire(self.data)

    @property
    def for_application(self):
        """ A filtered and processed list of headers for use by the application.
        """
        return map(maybe_base64_decode, filter_headers_for_application(self.data))


class GrpcContext:
    def __init__(self, request_stream, response_stream):
        self.request_stream = request_stream
        self.response_stream = response_stream

    def invocation_metadata(self):
        return self.request_stream.headers.for_application

    def send_initial_metadata(self, metadata):
        self.response_stream.headers.set(*metadata)

    def set_trailing_metadata(self, metadata):
        self.response_stream.trailers.set(*metadata)
