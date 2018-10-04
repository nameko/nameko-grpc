import pickle
import time
import os
import uuid
from contextlib import contextmanager


class Config:
    def __init__(self, method_name, fifo_out):
        self.method_name = method_name
        self.fifo_out = fifo_out


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


@contextmanager
def temp_fifo(prefix=""):
    path = "/tmp/{}-{}".format(prefix, uuid.uuid4())
    os.mkfifo(path)
    yield path
    os.unlink(path)


def receive_stream(stream_fifo):
    while True:
        with open(stream_fifo, "rb") as in_:
            req = pickle.load(in_)
            # XXX: dedent?
            if req is None:
                break
            yield req


def receive(fifo):
    with open(fifo, "rb") as in_:
        try:
            return pickle.load(in_)
        except pickle.UnpicklingError as exc:
            stream_fifo = in_.read()
            return receive_stream(stream_fifo)


def send_stream(stream_fifo, result):
    for msg in result:
        print(">> send stream", msg, stream_fifo)
        with open(stream_fifo, "wb") as out_:
            pickle.dump(msg, out_)
            print(">> sent")
        time.sleep(0.1)
    with open(stream_fifo, "wb") as out_:
        print(">> send stream close")
        pickle.dump(None, out_)


def send(fifo, result):
    print(">> send", fifo, result)
    if isiterable(result):
        with temp_fifo("stream") as stream_fifo:
            with open(fifo, "wb") as out_:
                print(">> convert to stream", stream_fifo)
                out_.write(stream_fifo.encode("utf-8"))
            send_stream(stream_fifo, result)
    else:
        with open(fifo, "wb") as out_:
            pickle.dump(result, out_)
