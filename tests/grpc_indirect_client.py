# -*- coding: utf-8 -*-
import os
import sys
import threading

import grpc

from helpers import FifoPipe, receive, send


def call(fifo_in, fifo_out, method):
    request = receive(fifo_in)
    response = method(request)
    send(fifo_out, response)


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import example_pb2_grpc

    command_fifo_path = sys.argv[1]
    command_fifo = FifoPipe.wrap(command_fifo_path)

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = example_pb2_grpc.exampleStub(channel)

    while True:
        config = receive(command_fifo)
        if config is None:
            break

        in_fifo_path = config.in_fifo
        in_fifo = FifoPipe.wrap(in_fifo_path)

        out_fifo_path = config.out_fifo
        out_fifo = FifoPipe.wrap(out_fifo_path)

        method = getattr(stub, config.method_name)

        thread = threading.Thread(
            target=call, name=config.method_name, args=(in_fifo, out_fifo, method)
        )
        thread.start()
