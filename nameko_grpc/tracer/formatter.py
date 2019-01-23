# -*- coding: utf-8 -*-
import json

from google.protobuf.json_format import MessageToJson
from nameko_tracer import constants
from nameko_tracer.formatters import JSONFormatter

from nameko_grpc.context import GrpcContext


def default(obj):
    try:
        return MessageToJson(obj)
    except Exception:
        pass

    if isinstance(obj, GrpcContext):
        return {
            "request_metadata": obj.invocation_metadata(),
            "response_headers": obj.response_stream.headers.for_application,
            "response_trailers": obj.response_stream.trailers.for_application,
        }

    return str(obj)


def serialise(obj):
    return json.dumps(obj, default=default)


class GrpcFormatter(JSONFormatter):

    extra_serialise_keys = (
        constants.CONTEXT_DATA_KEY,
        constants.EXCEPTION_ARGS_KEY,
        "grpc_context",
    )

    def format(self, record):

        trace = getattr(record, constants.TRACE_KEY)

        for key in self.extra_serialise_keys:
            if key in trace:
                trace[key] = serialise(trace[key])

        return serialise(trace)
