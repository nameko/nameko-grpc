# -*- coding: utf-8 -*-
import os
import sys

from nameko_grpc.client import Client


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))

    import example_pb2
    import example_pb2_grpc

    req = example_pb2.ExampleRequest(value="pickle1")

    def gen():
        for i in range(50):
            yield example_pb2.ExampleRequest(value="pickle{}".format(i))

    with Client(example_pb2_grpc.exampleStub) as client:

        response = client.unary_unary(req)
        print(">> ", response)
        for response in client.unary_stream(req):
            print(">> ", response)
        response = client.stream_unary(gen())
        print(">> ", response)
        for response in client.stream_stream(gen()):
            print(">> ", response)
