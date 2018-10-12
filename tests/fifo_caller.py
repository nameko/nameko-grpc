# -*- coding: utf-8 -*-
# monkeypatch is optional; enabling here demonstrates safety of the helpers
import eventlet

eventlet.monkey_patch()
import sys
import os

from helpers import receive, send, Config, FifoPipe


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import example_pb2

    req = example_pb2.ExampleRequest(value="pickle1")

    COMMAND_FIFO = "/tmp/fifo_in"
    command_fifo = FifoPipe.wrap(COMMAND_FIFO)

    def gen():
        for i in range(50):
            yield example_pb2.ExampleRequest(value="pickle{}".format(i))

    def ping():
        while True:
            print("ping")
            eventlet.sleep(0.01)

    # eventlet.spawn(ping)
    # eventlet.sleep(.1)

    def call(method_name, request, stream_response=True):

        with FifoPipe.new("/tmp") as in_fifo:
            with FifoPipe.new("/tmp") as out_fifo:
                send(command_fifo, Config(method_name, in_fifo.path, out_fifo.path))
                send(in_fifo, request)
                result = receive(out_fifo)

                if stream_response:
                    for res in result:
                        print(">>>", res)
                else:
                    print(">>", result)

    call("unary_unary", req, stream_response=False)
    call("unary_stream", req, stream_response=True)
    call("stream_unary", gen(), stream_response=False)
    call("stream_stream", gen(), stream_response=True)

    send(command_fifo, None)
