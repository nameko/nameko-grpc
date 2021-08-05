# -*- coding: utf-8 -*-
"""
Usage: nameko run performance_nameko
"""
import grpc

from nameko_grpc.entrypoint import Grpc


example_pb2, example_pb2_grpc = grpc.protos_and_services("example.proto")
entrypoint = Grpc.implementing(example_pb2_grpc.exampleStub)


class example:
    name = "example"

    @entrypoint
    def unary_unary(self, request, context):
        message = request.value * (request.multiplier or 1)
        return example_pb2.ExampleReply(message=message)

    @entrypoint
    def unary_stream(self, request, context):
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            yield example_pb2.ExampleReply(message=message, seqno=i + 1)

    @entrypoint
    def stream_unary(self, request, context):
        messages = []
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return example_pb2.ExampleReply(message=",".join(messages))

    @entrypoint
    def stream_stream(self, request, context):
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            yield example_pb2.ExampleReply(message=message, seqno=index + 1)
