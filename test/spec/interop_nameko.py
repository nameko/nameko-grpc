# -*- coding: utf-8 -*-
from nameko_grpc.entrypoint import Grpc

import empty_pb2
import interop_pb2_grpc
import messages_pb2


grpc = Grpc.implementing(interop_pb2_grpc.TestServiceStub)


_INITIAL_METADATA_KEY = "x-grpc-test-echo-initial"
_TRAILING_METADATA_KEY = "x-grpc-test-echo-trailing-bin"


def _maybe_echo_metadata(servicer_context):
    """Copies metadata from request to response if it is present."""
    invocation_metadata = dict(servicer_context.invocation_metadata())
    if _INITIAL_METADATA_KEY in invocation_metadata:
        initial_metadatum = (
            _INITIAL_METADATA_KEY,
            invocation_metadata[_INITIAL_METADATA_KEY],
        )
        servicer_context.send_initial_metadata((initial_metadatum,))
    if _TRAILING_METADATA_KEY in invocation_metadata:
        trailing_metadatum = (
            _TRAILING_METADATA_KEY,
            invocation_metadata[_TRAILING_METADATA_KEY],
        )
        servicer_context.set_trailing_metadata((trailing_metadatum,))


def _maybe_echo_status_and_message(request, servicer_context):
    """Sets the response context code and details if the request asks for them"""
    if request.HasField("response_status"):
        servicer_context.set_code(request.response_status.code)
        servicer_context.set_details(request.response_status.message)


class TestService:
    name = "test"

    @grpc
    def EmptyCall(self, request, context):
        _maybe_echo_metadata(context)
        return empty_pb2.Empty()

    @grpc
    def UnaryCall(self, request, context):
        _maybe_echo_metadata(context)
        _maybe_echo_status_and_message(request, context)
        return messages_pb2.SimpleResponse(
            payload=messages_pb2.Payload(
                type=messages_pb2.COMPRESSABLE, body=b"\x00" * request.response_size
            )
        )

    @grpc
    def StreamingOutputCall(self, request, context):
        _maybe_echo_status_and_message(request, context)
        for response_parameters in request.response_parameters:
            yield messages_pb2.StreamingOutputCallResponse(
                payload=messages_pb2.Payload(
                    type=request.response_type, body=b"\x00" * response_parameters.size
                )
            )

    @grpc
    def StreamingInputCall(self, request_iterator, context):
        aggregate_size = 0
        for index, request in enumerate(request_iterator):
            print(
                index,
                request.payload and request.payload.body and len(request.payload.body),
            )
            if request.payload is not None and request.payload.body:
                aggregate_size += len(request.payload.body)
        return messages_pb2.StreamingInputCallResponse(
            aggregated_payload_size=aggregate_size
        )

    @grpc
    def FullDuplexCall(self, request_iterator, context):
        _maybe_echo_metadata(context)
        for request in request_iterator:
            _maybe_echo_status_and_message(request, context)
            for response_parameters in request.response_parameters:
                yield messages_pb2.StreamingOutputCallResponse(
                    payload=messages_pb2.Payload(
                        type=request.payload.type,
                        body=b"\x00" * response_parameters.size,
                    )
                )

    # NOTE(nathaniel): Apparently this is the same as the full-duplex call?
    # NOTE(atash): It isn't even called in the interop spec (Oct 22 2015)...
    @grpc
    def HalfDuplexCall(self, request_iterator, context):
        return self.FullDuplexCall(request_iterator, context)
