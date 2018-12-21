# -*- coding: utf-8 -*-
from helpers import extract_metadata, instrumented, maybe_echo_metadata, maybe_sleep

import example_pb2_grpc
from example_pb2 import ExampleReply


class Error(Exception):
    pass


class example(example_pb2_grpc.exampleServicer):
    @instrumented
    def unary_unary(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        maybe_sleep(request)
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message, metadata=metadata)

    @instrumented
    def unary_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            maybe_sleep(request)
            yield ExampleReply(message=message, seqno=i + 1, metadata=metadata)

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

    @instrumented
    def stream_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        for index, req in enumerate(request):
            maybe_sleep(req)
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(message=message, seqno=index + 1, metadata=metadata)

    @instrumented
    def unary_error(self, request, context):
        maybe_echo_metadata(context)
        maybe_sleep(request)
        raise Error("boom")

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
