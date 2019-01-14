# -*- coding: utf-8 -*-
import json


METADATA_PREFIX = "x-nameko-"


def encode_value(value):
    # TODO use nameko.serialization
    return json.dumps(value)


def decode_value(value):
    # TODO use nameko.serialization
    return json.loads(value)


def metadata_from_context_data(data):
    """ Utility function transforming a `data` dictionary into metadata suitable
    for a GRPC stream.
    """
    metadata = []
    for key, value in data.items():
        name = "{}{}".format(METADATA_PREFIX, key)
        value = encode_value(value)
        metadata.append((name, value))
    return metadata


def context_data_from_metadata(metadata):
    """ Utility function transforming `metadata` into a context data dictionary.

    Metadata may have been encoded at the client by `metadata_from_context_data`, or
    it may be "normal" GRPC metadata. In this case, duplicate values are allowed;
    they become a list in the context data.
    """
    data = {}

    for name, value in metadata:
        if name.startswith(METADATA_PREFIX):
            _, key = name.split(METADATA_PREFIX, 1)
            data[key] = decode_value(value)
        else:
            if name in data:
                try:
                    data[name].append(value)
                except AttributeError:
                    data[name] = [data[name], value]
            else:
                data[name] = value

    return data


class GrpcContext:
    """ Context object passed to GRPC methods.

    Gives access to request metadata and allows response metadata to be set.
    """

    def __init__(self, request_stream, response_stream):
        self.request_stream = request_stream
        self.response_stream = response_stream

    def set_code(self, code):
        self.response_stream.trailers.set(("grpc-status", str(code)))

    def set_details(self, details):
        self.response_stream.trailers.set(("grpc-message", details))

    def invocation_metadata(self):
        return self.request_stream.headers.for_application

    def send_initial_metadata(self, metadata):
        self.response_stream.headers.set(*metadata)

    def set_trailing_metadata(self, metadata):
        self.response_stream.trailers.set(*metadata)
