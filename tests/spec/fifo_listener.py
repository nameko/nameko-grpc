import sys
import grpc
import helloworld_pb2_grpc

from helpers import send, receive

# XXX this is broken because the helpers are harccoded to expect eventlet now

if __name__ == "__main__":

    fifo_in = sys.argv[1]

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = helloworld_pb2_grpc.greeterStub(channel)

    while True:
        config = receive(fifo_in)
        if config is None:
            break
        request = receive(fifo_in)
        response = getattr(stub, config.method_name)(request)
        send(config.fifo_out, response)
