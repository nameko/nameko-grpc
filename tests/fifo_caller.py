# monkeypatch is optional; enabling here demonstrates safety of the helpers
import eventlet

eventlet.monkey_patch()
import sys
import os

from helpers import receive, send, Config, Fifo, wrap_fifo


if __name__ == "__main__":

    sys.path.append(os.path.join(os.path.dirname(__file__), "spec"))
    import helloworld_pb2

    req = helloworld_pb2.HelloRequest(name="pickle1")

    FIFO_IN = "/tmp/fifo_in"
    FIFO_OUT = "/tmp/fifo_out"

    fifo_in = wrap_fifo(FIFO_IN)
    fifo_out = wrap_fifo(FIFO_OUT)

    def gen():
        for i in range(50):
            yield helloworld_pb2.HelloRequest(name="pickle{}".format(i))

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

    call("say_hello", req, stream_response=False)
    call("say_hello_goodbye", req)
    call("say_hello_to_many_at_once", gen(), stream_response=False)
    call("say_hello_to_many", gen())

    send(fifo_in, None)
