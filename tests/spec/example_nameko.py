# -*- coding: utf-8 -*-
import time

from nameko_grpc.entrypoint import Grpc

import example_pb2_grpc
from example_pb2 import ExampleReply


grpc = Grpc.decorator(example_pb2_grpc.exampleStub)


class example:
    name = "example"

    @grpc
    def unary_unary(self, request, context):
        if request.delay:
            time.sleep(request.delay / 1000)
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message)

    @grpc
    def unary_stream(self, request, context):
        if request.delay:
            time.sleep(request.delay / 1000)
        message = request.value * (request.multiplier or 1)
        yield ExampleReply(message=message, seqno=1)
        yield ExampleReply(message=message, seqno=2)

    @grpc
    def stream_unary(self, request, context):
        messages = []
        for req in request:
            if req.delay:
                time.sleep(req.delay / 1000)
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return ExampleReply(message=",".join(messages))

    @grpc
    def stream_stream(self, request, context):
        for index, req in enumerate(request):
            if req.delay:
                time.sleep(req.delay / 1000)
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(message=message, seqno=index + 1)
