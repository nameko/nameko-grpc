import os
import sys
import grpc

from helpers import send, receive, wrap_fifo


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import helloworld_pb2_grpc

    fifo_in_path = sys.argv[1]
    fifo_in = wrap_fifo(fifo_in_path)

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = helloworld_pb2_grpc.greeterStub(channel)

    while True:
        config = receive(fifo_in)
        if config is None:
            break
        fifo_out_path = config.fifo_out
        fifo_out = wrap_fifo(fifo_out_path)
        request = receive(fifo_in)
        response = getattr(stub, config.method_name)(request)
        send(fifo_out, response)
