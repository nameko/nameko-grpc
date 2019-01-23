# -*- coding: utf-8 -*-
from nameko_tracer import constants
from nameko_tracer.adapters import DefaultAdapter

from nameko_grpc.constants import Cardinality


def clean_call_args(extra):
    # TODO can fix this by overriding get_call_args
    cardinality = get_cardinality(extra)
    if cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
        trace_data = extra[constants.TRACE_KEY]
        trace_data["call_args"]["request"] = "streaming"


def clean_response(extra):
    # TODO can fix this by overriding get_result
    if not is_response(extra):
        return

    cardinality = get_cardinality(extra)
    if cardinality in (
        Cardinality.UNARY_STREAM,
        Cardinality.STREAM_STREAM,
    ) and not is_stream(extra):
        trace_data = extra[constants.TRACE_KEY]
        trace_data["response"] = "streaming"


def is_request(extra):
    return extra["stage"] == constants.Stage.request


def is_response(extra):
    return extra["stage"] == constants.Stage.response


def is_stream(extra):
    return "stream_part" in extra


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


def add_request(extra):
    if not is_request(extra) or not is_stream(extra):
        return

    trace_data = extra[constants.TRACE_KEY]
    trace_data["request"] = extra["request"]


class GrpcEntrypointAdapter(DefaultAdapter):

    # EXPOSE REQUEST separately (RESPONSE already exists)
    # AND GRPC CONTEXT?

    def process(self, message, kwargs):
        message, kwargs = super().process(message, kwargs)

        extra = kwargs["extra"]

        add_cardinality(extra)
        add_stream_part(extra)
        add_stream_age(extra)
        add_request(extra)

        clean_call_args(extra)
        clean_response(extra)

        return message, kwargs

    def get_result(self, result):
        # override to avoid serialization?
        return result
