import os
import sys
import grpc
from concurrent import futures
import time

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import example_pb2_grpc
    from example_pb2 import ExampleReply

    # TODO should be able to use the nameko service definition here too, just add the new base (entrypoints will be ignored)
    class example(example_pb2_grpc.exampleServicer):
        def unary_unary(self, request, context):
            return ExampleReply(message="Hello, %s!" % request.name)

        def unary_stream(self, request, context):
            yield ExampleReply(message="Hello, %s!" % request.name)
            yield ExampleReply(message="Goodbye, %s!" % request.name)

        def stream_stream(self, request, context):
            for message in request:
                yield ExampleReply(message="Hi " + message.name)

        def stream_unary(self, request, context):
            names = []
            for message in request:
                names.append(message.name)
            return ExampleReply(message="Hi " + ", ".join(names) + "!")

    def serve():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        example_pb2_grpc.add_exampleServicer_to_server(example(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)

    serve()
