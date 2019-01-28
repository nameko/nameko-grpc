# -*- coding: utf-8 -*-
from nameko_tracer import constants
from nameko_tracer.adapters import DefaultAdapter

from nameko_grpc.constants import Cardinality


GRPC_STREAM = "GrpcStream"
GRPC_CONTEXT = "GrpcContext"
GRPC_REQUEST = "GrpcRequest"
GRPC_RESPONSE = "GrpcResponse"


def is_request_record(extra):
    """ Determine whether the record represents a request.
    """
    return extra["stage"] == constants.Stage.request


def is_response_record(extra):
    """ Determine whether the record represents a response.
    """
    return extra["stage"] == constants.Stage.response


def is_stream_record(extra):
    """ Determine whether the record represents part of a streaming request or response.
    """
    return "stream_part" in extra


def is_error_record(extra):
    """ Determine whether the record represents an error response
    """
    return is_response_record(extra) and extra["exc_info_"]


def is_streaming_request_method(extra):
    """ Determine whether the record relates to a method that has a streaming request.

    Note that the record may be the request or response trace, or part of one of these.
    """
    cardinality = get_cardinality(extra)
    return cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM)


def is_streaming_response_method(extra):
    """ Determine whether the record relates to a method that has a streaming response.

    Note that the record may be the request or response trace, or part of one of these.
    """
    cardinality = get_cardinality(extra)
    return cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM)


def get_cardinality(extra):
    """ Extract the cardinality of the method that this record relates to.
    """
    return extra["worker_ctx"].entrypoint.cardinality


def add_cardinality(extra):
    """ Add the cardinality of the method to which this record relates to the trace
    data.
    """
    trace_data = extra[constants.TRACE_KEY]
    trace_data["cardinality"] = get_cardinality(extra)


def add_stream_part(extra):
    """ If this record represents part of a stream, add the stream part identifier to
    the trace data.
    """
    if not is_stream_record(extra):
        return
    trace_data = extra[constants.TRACE_KEY]
    trace_data["stream_part"] = extra["stream_part"]


def add_stream_age(extra):
    """ If this record represents part of a stream, add the commulative stream age to
    the trace data.
    """
    if not is_stream_record(extra):
        return
    trace_data = extra[constants.TRACE_KEY]
    trace_data["stream_age"] = extra["stream_age"]


def add_grpc_request(extra):
    """ Add the GRPC request message to the trace data for this record, under the
    `grpc_request` key.

    All records receive a value for this key.

    If this record relates to a method that has a streaming request and the record
    does not represent part that stream (i.e. it's a "top-level" record, or a response),
    the value is the GRPC_STREAM placeholder.
    """
    trace_data = extra[constants.TRACE_KEY]

    if is_streaming_request_method(extra):
        if is_request_record(extra) and is_stream_record(extra):
            trace_data["grpc_request"] = extra["request"]
        else:
            trace_data["grpc_request"] = GRPC_STREAM
    else:
        request, context = extra["worker_ctx"].args
        trace_data["grpc_request"] = request


def add_grpc_response(extra):
    """ Add the GRPC response message to the trace data for this record, under the
    `grpc_response` key.

    Only response records receive a value for this key.

    If this record relates to a method that has a streaming response and the record
    does not represent part of that stream (i.e. it's the "top-level" record),
    the value is the GRPC_STREAM placeholder.
    """
    if not is_response_record(extra):
        return

    trace_data = extra[constants.TRACE_KEY]

    if is_streaming_response_method(extra):
        if is_stream_record(extra):
            trace_data["grpc_response"] = extra["result"]
        else:
            trace_data["grpc_response"] = GRPC_STREAM
    else:
        trace_data["grpc_response"] = extra["result"]


def add_grpc_context(extra):
    """ Add the GRPC context object to the trace data for this record, under the
    `grpc_context` key.
    """
    request, context = extra["worker_ctx"].args

    trace_data = extra[constants.TRACE_KEY]
    trace_data["grpc_context"] = context


def clean_call_args(extra):
    """ Replace the `context` and `request` keys of `call_args` in the trace data for
    this record.

    These objects are exposed in the `grpc_context` and `grpc_request` fields
    respectively and don't need to be in multiple places. See `add_grpc_context` and
    `add_grpc_request` respectively.

    The value for `context` is the GRPC_CONTEXT placeholder. The value for `request`
    is the GRPC_REQUEST placeholder, or GRPC_STREAM placeholder if this record relates
    to a method that has a streaming request.
    """

    trace_data = extra[constants.TRACE_KEY]
    trace_data["call_args"]["context"] = GRPC_CONTEXT

    if is_streaming_request_method(extra):
        trace_data["call_args"]["request"] = GRPC_STREAM
    else:
        trace_data["call_args"]["request"] = GRPC_REQUEST


def clean_response(extra):
    """ Replaces the `response` key in the trace data for this record.

    Only successful response records have a value for this key.

    The GRPC response message is exposed in the `grpc_response` field and doesn't need
    to be in multiple places. See `add_grpc_response`.

    The value for `response` is the GRPC_RESPONSE placeholder, or GRPC_STREAM
    placeholder if this record relates to a method that has a streaming response.

    """

    if not is_response_record(extra) or is_error_record(extra):
        return

    trace_data = extra[constants.TRACE_KEY]

    if is_streaming_response_method(extra) and not is_stream_record(extra):
        trace_data["response"] = GRPC_STREAM
    else:
        trace_data["response"] = GRPC_RESPONSE


def clean_response_status(extra):
    """ Replaces `response_status` keys in the trace data for this record.

    Only response records have a value for this key.

    The value for is unchanged unless this record relates to a method that has a
    streaming response and the record does not represent part of that stream
    (i.e. it's the "top-level" record), and the record does not already indicate an
    error (i.e. the method immediately failed).

    The status of the response is therefore not yet known, so the value is set to
    `None`.

    """
    if not is_response_record(extra) or is_error_record(extra):
        return

    trace_data = extra[constants.TRACE_KEY]

    if is_streaming_response_method(extra) and not is_stream_record(extra):
        # response status still unknown
        trace_data["response_status"] = None


class GrpcEntrypointAdapter(DefaultAdapter):
    """ Logging adapter for methods decorated with the Grpc entrypoint.

    Records may represent one of the following:

    * The request to a "unary request" RPC method
    * The response from a "unary response" RPC method
    * The "top-level" request to a "streaming request" RPC method
    * Each "part" of the stream to a "streaming request" RPC method
    * The "top-level" response from a "streaming response" RPC method
    * Each "part" of the stream from a "streaming response" RPC method

    """

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
        clean_response_status(extra)

        return message, kwargs

    def get_result(self, result):
        """ Override get_result to remove serialization.
        """
        return result
