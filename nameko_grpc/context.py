# -*- coding: utf-8 -*-
import base64


def maybe_base64_decode(header):
    name, value = header
    if name.endswith("-bin"):
        value = base64.b64decode(value)
    return name, value


def filter_headers_for_application(headers):
    def include(header):
        name, _ = header
        if name.startswith(":"):
            return False
        if name.startswith("grpc-"):
            return False
        if name in ("te", "content-type", "user-agent", "accept-encoding"):
            return False
        return True

    return filter(include, headers)


class Headers:
    def __init__(self, data):
        self.data = data

    def get(self, name, default=None):
        matches = []
        for key, value in self.data:
            if key == name:
                _, value = maybe_base64_decode((key, value))
                matches.append(value)
        if not matches:
            return default
        return ",".join(matches)

    def application_headers(self):
        return map(maybe_base64_decode, filter_headers_for_application(self.data))


class GrpcContext:
    def __init__(self, cardinality, response_stream, headers):
        self.cardinality = cardinality
        self.response_stream = response_stream
        self.headers = headers

        self.initial_metadata = None
        self.trailing_metadata = None

    def invocation_metadata(self):
        return self.headers

    # def send_initial_metadata(self, value):
    #     self.initial_metadata = value

    # def set_trailing_metadata(self, value):
    #     self.trailing_metadata = value
