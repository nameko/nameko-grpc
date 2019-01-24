# -*- coding: utf-8 -*-
from nameko_tracer import constants
from nameko_tracer.adapters import DefaultAdapter

from nameko_grpc.constants import Cardinality


GRPC_STREAM = "GrpcStream"
GRPC_CONTEXT = "GrpcContext"
GRPC_REQUEST = "GrpcRequest"
GRPC_RESPONSE = "GrpcResponse"


def clean_call_args(extra):
    # TODO can fix this by overriding get_call_args

    trace_data = extra[constants.TRACE_KEY]
    trace_data["call_args"]["context"] = GRPC_CONTEXT

    cardinality = get_cardinality(extra)
    if cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
        trace_data["call_args"]["request"] = GRPC_STREAM
    else:
        trace_data["call_args"]["request"] = GRPC_REQUEST


def clean_response(extra):
    # TODO can fix this by overriding get_result

    if not is_response(extra) or extra["exc_info_"]:
        return

    trace_data = extra[constants.TRACE_KEY]

    cardinality = get_cardinality(extra)
    if not is_stream(extra) and cardinality in (
        Cardinality.UNARY_STREAM,
        Cardinality.STREAM_STREAM,
    ):
        trace_data["response"] = GRPC_STREAM
    else:
        trace_data["response"] = GRPC_RESPONSE


def clean_response_state(extra):
    # TODO can fix this by overriding get_result (probably)

    if not is_response(extra):
        return

    cardinality = get_cardinality(extra)
    if not is_stream(extra) and cardinality in (
        Cardinality.UNARY_STREAM,
        Cardinality.STREAM_STREAM,
    ):
        trace_data = extra[constants.TRACE_KEY]
        trace_data["response_status"] = None


def is_request(extra):
    return extra["stage"] == constants.Stage.request


def is_response(extra):
    return extra["stage"] == constants.Stage.response


def is_stream(extra):
    return "stream_part" in extra


def has_streaming_request(extra):
    cardinality = get_cardinality(extra)
    return cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM)


def has_streaming_response(extra):
    cardinality = get_cardinality(extra)
    return cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM)


def get_cardinality(extra):
    return extra["worker_ctx"].entrypoint.cardinality


def add_cardinality(extra):
    trace_data = extra[constants.TRACE_KEY]
    trace_data["cardinality"] = get_cardinality(extra)


def add_stream_part(extra):
    if not is_stream(extra):
        return
    trace_data = extra[constants.TRACE_KEY]
    trace_data["stream_part"] = extra["stream_part"]


def add_stream_age(extra):
    if not is_stream(extra):
        return
    trace_data = extra[constants.TRACE_KEY]
    trace_data["stream_age"] = extra["stream_age"]


def add_grpc_request(extra):
    trace_data = extra[constants.TRACE_KEY]

    if has_streaming_request(extra):
        if is_request(extra) and is_stream(extra):
            trace_data["grpc_request"] = extra["request"]
        else:
            trace_data["grpc_request"] = GRPC_STREAM
    else:
        request, context = extra["worker_ctx"].args
        trace_data["grpc_request"] = request


def add_grpc_response(extra):
    if not is_response(extra):
        return

    trace_data = extra[constants.TRACE_KEY]

    if not has_streaming_response(extra):
        trace_data["grpc_response"] = extra["result"]
        return

    if is_stream(extra):
        trace_data["grpc_response"] = extra["result"]
    else:
        trace_data["grpc_response"] = GRPC_STREAM


def add_grpc_context(extra):
    request, context = extra["worker_ctx"].args

    trace_data = extra[constants.TRACE_KEY]
    trace_data["grpc_context"] = context


class GrpcEntrypointAdapter(DefaultAdapter):
    def process(self, message, kwargs):
        message, kwargs = super().process(message, kwargs)

        extra = kwargs["extra"]

        add_cardinality(extra)
        add_stream_part(extra)
        add_stream_age(extra)

        add_grpc_request(extra)
        add_grpc_response(extra)
        add_grpc_context(extra)

        clean_call_args(extra)
        clean_response(extra)
        clean_response_state(extra)

        return message, kwargs

    def get_result(self, result):
        # override to avoid serialization?
        return result
