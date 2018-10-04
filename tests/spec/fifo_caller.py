import eventlet

eventlet.monkey_patch()

from helpers import receive, send, Config

import helloworld_pb2

req = helloworld_pb2.HelloRequest(name="pickle1")

FIFO_IN = "/tmp/fifo_in"
FIFO_OUT = "/tmp/fifo_out"


def gen():
    for i in range(50):
        yield helloworld_pb2.HelloRequest(name="pickle{}".format(i))


def ping():
    while True:
        print("ping")
        eventlet.sleep(0.01)


# eventlet.spawn(ping)
# eventlet.sleep(.1)

# XXX this works fine, so we probably have succeeded at making the fifos eventlet-safe
# BUT we MUST use the non-eventlet enabled remote client while the caller is eventlet-safe.
# so we have to somehow make the shared helpers agnostic, or unshare them?


def call(method_name, request, stream_response=True):

    send(FIFO_IN, Config(method_name, FIFO_OUT))
    send(FIFO_IN, request)
    result = receive(FIFO_OUT)

    if stream_response:
        for res in result:
            print(">>>", res)
    else:
        print(">>", result)


call("say_hello", req, stream_response=False)
call("say_hello_goodbye", req)
call("say_hello_to_many_at_once", gen(), stream_response=False)
call("say_hello_to_many", gen())

send(FIFO_IN, None)
