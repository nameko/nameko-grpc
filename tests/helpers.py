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


class NewStream:
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "<NewStream {}>".format(self.path)


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


class Fifo:
    def __init__(self, path):
        self.path = path

    def dump(self, value):
        with open(self.path, "wb") as out_:
            data = pickle.dumps(value)
            out_.write(data)

    def load(self):
        with open(self.path, "rb") as in_:
            data = in_.read()
            return pickle.loads(data)


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
def temp_fifo(directory, name=None):
    if name is None:
        name = str(uuid.uuid4())
    path = os.path.join(directory, name)
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
    if isinstance(loaded, NewStream):
        stream_fifo = wrap_fifo(loaded.path)
        return receive_stream(stream_fifo)
    return loaded


def send_stream(stream_fifo, result):
    for msg in result:
        stream_fifo.dump(msg)
        time.sleep(0.01)
    stream_fifo.dump(None)


def send(fifo, result):
    if isiterable(result):
        with temp_fifo(os.path.dirname(fifo.path)) as stream_fifo:
            fifo.dump(NewStream(stream_fifo.path))
            send_stream(stream_fifo, result)
    else:
        fifo.dump(result)
