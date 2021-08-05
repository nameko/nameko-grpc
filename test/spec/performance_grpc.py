# -*- coding: utf-8 -*-
"""
Usage: python performance_grpc.py
"""
import time
from concurrent import futures

import grpc


example_pb2, example_pb2_grpc = grpc.protos_and_services("example.proto")

PORT = 50052
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class example(example_pb2_grpc.exampleServicer):
    def unary_unary(self, request, context):
        message = request.value * (request.multiplier or 1)
        return example_pb2.ExampleReply(message=message)

    def unary_stream(self, request, context):
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            yield example_pb2.ExampleReply(message=message, seqno=i + 1)

    def stream_unary(self, request, context):
        messages = []
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return example_pb2.ExampleReply(message=",".join(messages))

    def stream_stream(self, request, context):
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            yield example_pb2.ExampleReply(message=message, seqno=index + 1)


def serve():

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    example_pb2_grpc.add_exampleServicer_to_server(example(), server)
    server.add_insecure_port("[::]:{}".format(PORT))

    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


serve()
