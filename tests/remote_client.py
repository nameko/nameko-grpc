import os
import sys
import grpc

from helpers import send, receive


if __name__ == "__main__":

    fifo_in = sys.argv[1]

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import helloworld_pb2_grpc

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = helloworld_pb2_grpc.greeterStub(channel)

    while True:
        config = receive(fifo_in)
        if config is None:
            print("END")
            break
        print(config.method_name, config.fifo_out)
        print(">> WAIT on", fifo_in)
        request = receive(fifo_in)
        print(">> REQ", request)
        response = getattr(stub, config.method_name)(request)
        print(">> RES", response)
        send(config.fifo_out, response)
