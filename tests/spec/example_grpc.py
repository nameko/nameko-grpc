# -*- coding: utf-8 -*-
import example_pb2_grpc
from example_pb2 import ExampleReply


class example(example_pb2_grpc.exampleServicer):
    def unary_unary(self, request, context):
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message)

    def unary_stream(self, request, context):
        message = request.value * (request.multiplier or 1)
        yield ExampleReply(message=message, seqno=1)
        yield ExampleReply(message=message, seqno=2)

    def stream_unary(self, request, context):
        messages = []
        for req in request:
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return ExampleReply(message=",".join(messages))

    def stream_stream(self, request, context):
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(message=message, seqno=index + 1)
