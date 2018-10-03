from __future__ import print_function

import grpc

import helloworld_pb2
import helloworld_pb2_grpc


def _name_generator():
    names = ("Foo", "Bar", "Bat", "Baz")

    for name in names:
        yield helloworld_pb2.HelloRequest(name=name)
        # sleep(0.5)


if __name__ == "__main__":

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = helloworld_pb2_grpc.greeterStub(channel)

    response = stub.say_hello(helloworld_pb2.HelloRequest(name="you"))
    print("Greeter client received: " + response.message)

    response_iterator = stub.say_hello_goodbye(
        helloworld_pb2.HelloRequest(name="y'all")
    )

    for response in response_iterator:
        print(response.message)

    response_iterator = stub.say_hello_to_many(_name_generator())

    for response in response_iterator:
        print(response.message)

    response = stub.say_hello_to_many_at_once(_name_generator())
    print(response.message)
