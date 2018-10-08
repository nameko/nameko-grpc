from __future__ import print_function

import time
import grpc

import example_pb2
import example_pb2_grpc


def _name_generator(delay=0):
    names = ("Foo", "Bar", "Bat", "Baz")

    for name in names:
        yield example_pb2.ExampleRequest(value=name)
        time.sleep(delay)


if __name__ == "__main__":

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = example_pb2_grpc.exampleStub(channel)

    # response = stub.unary_unary(example_pb2.ExampleRequest(value="you", multiplier=2))
    # print("Greeter client received: " + response.message)

    # response_iterator = stub.unary_stream(example_pb2.ExampleRequest(value="y'all"))

    # for response in response_iterator:
    #     print(response.message, response.seqno)

    response_iterator = stub.stream_stream(_name_generator(delay=0.5))

    for response in response_iterator:
        print(response.message, response.seqno)

    # response = stub.stream_unary(_name_generator())
    # print(response.message)

    # -----

    # response = stub.stream_unary.future(_name_generator(delay=0.5))
    # print(response.result().message)

    # # missing tests:
    # # large request payload
    # # large response payload
    # # multiple streams
    # # standard client, entrypoint service
    # # nameko client, standard service

    # # TODO
    # # large request payload (this is actually request and response):
    # # add flow-control-ish logic to DP and see if we'd fall foul of window rules etc
    # # and whether we can recover
    # response = stub.say_hello(helloworld_pb2.HelloRequest(name="you" * 8000))
    # print(len(response.message))
