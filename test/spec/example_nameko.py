# -*- coding: utf-8 -*-
from nameko_grpc.entrypoint import Grpc
from grpc import StatusCode

from helpers import extract_metadata, instrumented, maybe_echo_metadata, maybe_sleep

import example_pb2_grpc
from example_pb2 import ExampleReply


class Error(Exception):
    pass


grpc = Grpc.implementing(example_pb2_grpc.exampleStub)


class example:
    name = "example"

    @grpc
    @instrumented
    def unary_unary(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        maybe_sleep(request)
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message, metadata=metadata)

    @grpc
    @instrumented
    def unary_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            maybe_sleep(request)
            yield ExampleReply(message=message, seqno=i + 1, metadata=metadata)

    @grpc
    @instrumented
    def stream_unary(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        messages = []
        for index, req in enumerate(request):
            maybe_sleep(req)
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return ExampleReply(message=",".join(messages), metadata=metadata)

    @grpc
    @instrumented
    def stream_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        for index, req in enumerate(request):
            maybe_sleep(req)
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(message=message, seqno=index + 1, metadata=metadata)

    @grpc(expected_exceptions=Error)
    @instrumented
    def unary_error(self, request, context):
        maybe_echo_metadata(context)
        maybe_sleep(request)
        raise Error("boom")

    @grpc
    @instrumented
    def unary_error_via_context(self, request, context):
        context.set_code(StatusCode.UNAUTHENTICATED)
        context.set_message("Not allowed!")

    @grpc(expected_exceptions=Error)
    @instrumented
    def stream_error(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            maybe_sleep(request)
            # raise on the last message
            if i == request.response_count - 1:
                raise Error("boom")
            yield ExampleReply(message=message, seqno=i + 1, metadata=metadata)

    @grpc
    @instrumented
    def stream_error_via_context(self, request, context):
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            maybe_sleep(request)
            # break on the last message
            if i == request.response_count - 1:
                context.set_code(StatusCode.RESOURCE_EXHAUSTED)
                context.set_message("Out of tokens!")
                break
            yield ExampleReply(message=message, seqno=i + 1)
