import pickle
import time
import os
import uuid
from contextlib import contextmanager

from eventlet import tpool


class Config:
    def __init__(self, method_name, fifo_out):
        self.method_name = method_name
        self.fifo_out = fifo_out


class NotPickled:
    # XXX this is required because raising inside the contextmanager in _load
    # doesn't result in the file being closed (not sure why, probably because it's
    # inside tpool). a better solution would be not to use plain (unpickled) strings
    # as the message to upgrade to a stream, but something else instead. then we
    # could always load/dump and avoid using read/write at all.
    def __init__(self, msg):
        self.msg = msg


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


class Fifo:
    def __init__(self, path):
        self.fifo = path  # XXX rename: PATH

    def dump(self, value):
        with open(self.fifo, "wb") as out_:
            pickle.dump(value, out_)

    def load(self):
        with open(self.fifo, "rb") as in_:
            try:
                value = pickle.load(in_)
            except pickle.UnpicklingError:
                value = NotPickled(in_.read())
            return value

    def write(self, data):
        with open(self.fifo, "wb") as out_:
            out_.write(data)

    def read(self):
        # XXX never called anymore
        with open(self.fifo, "rb") as in_:
            data = in_.read()
            return data


def under_eventlet():
    import socket
    from eventlet.greenio.base import GreenSocket

    return issubclass(socket.socket, GreenSocket)


def wrap_fifo(path):
    wrapped = Fifo(path)
    if under_eventlet():
        wrapped = tpool.Proxy(wrapped)
    return wrapped


@contextmanager
def temp_fifo(eventlet=True):
    path = "/tmp/{}".format(uuid.uuid4())
    os.mkfifo(path)
    yield wrap_fifo(path)
    os.unlink(path)


def receive_stream(stream_fifo):
    while True:
        req = stream_fifo.load()
        if req is None:
            break
        yield req


def receive(fifo):
    loaded = fifo.load()
    if isinstance(loaded, NotPickled):
        stream_fifo_path = loaded.msg
        return receive_stream(wrap_fifo(stream_fifo_path))
    return loaded


def send_stream(stream_fifo, result):
    for msg in result:
        stream_fifo.dump(msg)
        time.sleep(0.01)
    stream_fifo.dump(None)


def send(fifo, result):
    if isiterable(result):
        with temp_fifo() as stream_fifo:
            fifo.write(stream_fifo.fifo.encode("utf-8"))
            send_stream(stream_fifo, result)
    else:
        fifo.dump(result)
