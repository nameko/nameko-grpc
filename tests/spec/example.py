from nameko_grpc.entrypoint import Grpc
from helloworld_pb2 import HelloReply
from helloworld_pb2_grpc import greeterStub


grpc = Grpc.decorator(greeterStub)


class Service:
    name = "greeter"

    @grpc
    def say_hello(self, request, context):
        return HelloReply(message="Hello, %s!" % request.name)

    @grpc
    def say_hello_goodbye(self, request, context):
        yield HelloReply(message="Hello, %s!" % request.name)
        yield HelloReply(message="Goodbye, %s!" % request.name)

    @grpc
    def say_hello_to_many(self, request, context):
        for message in request:
            yield HelloReply(message="Hi " + message.name)

    @grpc
    def say_hello_to_many_at_once(self, request, context):
        names = []
        for message in request:
            names.append(message.name)

        return HelloReply(message="Hi " + ", ".join(names) + "!")
