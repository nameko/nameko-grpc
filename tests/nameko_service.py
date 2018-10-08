import os
import sys
from nameko_grpc.entrypoint import Grpc

sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))

from example_pb2 import ExampleReply
from example_pb2_grpc import exampleStub


grpc = Grpc.decorator(exampleStub)


class ExampleService:
    name = "example"

    @grpc
    def unary_unary(self, request, context):
        return ExampleReply(message="Hello, %s!" % request.name)

    @grpc
    def unary_stream(self, request, context):
        yield ExampleReply(message="Hello, %s!" % request.name)
        yield ExampleReply(message="Goodbye, %s!" % request.name)

    @grpc
    def stream_unary(self, request, context):
        names = []
        for message in request:
            names.append(message.name)

        return ExampleReply(message="Hi " + ", ".join(names) + "!")

    @grpc
    def stream_stream(self, request, context):
        for message in request:
            yield ExampleReply(message="Hi " + message.name)
