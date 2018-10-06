import os
import sys
import grpc
from concurrent import futures
import time

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import helloworld_pb2_grpc
    from helloworld_pb2 import HelloRequest, HelloReply

    # TODO should be able to use the nameko service definition here too, just add the new base (entrypoints will be ignored)
    class greeter(helloworld_pb2_grpc.greeterServicer):
        def say_hello(self, request, context):
            return HelloReply(message="Hello, %s!" % request.name)

        def say_hello_goodbye(self, request, context):
            yield HelloReply(message="Hello, %s!" % request.name)
            yield HelloReply(message="Goodbye, %s!" % request.name)

        def say_hello_to_many(self, request, context):
            for message in request:
                yield HelloReply(message="Hi " + message.name)

        def say_hello_to_many_at_once(self, request, context):
            names = []
            for message in request:
                names.append(message.name)
            return HelloReply(message="Hi " + ", ".join(names) + "!")

    def serve():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        helloworld_pb2_grpc.add_greeterServicer_to_server(greeter(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)

    serve()
