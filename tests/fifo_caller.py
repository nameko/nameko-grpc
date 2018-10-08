# monkeypatch is optional; enabling here demonstrates safety of the helpers
import eventlet

eventlet.monkey_patch()
import sys
import os

from helpers import receive, send, Config, Fifo, wrap_fifo


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import example_pb2

    req = example_pb2.ExampleRequest(name="pickle1")

    FIFO_IN = "/tmp/fifo_in"
    FIFO_OUT = "/tmp/fifo_out"

    fifo_in = wrap_fifo(FIFO_IN)
    fifo_out = wrap_fifo(FIFO_OUT)

    def gen():
        for i in range(50):
            yield example_pb2.ExampleRequest(name="pickle{}".format(i))

    def ping():
        while True:
            print("ping")
            eventlet.sleep(0.01)

    # eventlet.spawn(ping)
    # eventlet.sleep(.1)

    def call(method_name, request, stream_response=True):

        send(fifo_in, Config(method_name, FIFO_OUT))
        send(fifo_in, request)
        result = receive(fifo_out)

        if stream_response:
            for res in result:
                print(">>>", res)
        else:
            print(">>", result)

    call("unary_unary", req, stream_response=False)
    call("unary_stream", req, stream_response=True)
    call("stream_unary", gen(), stream_response=False)
    call("stream_stream", gen(), stream_response=True)

    send(fifo_in, None)
