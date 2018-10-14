# -*- coding: utf-8 -*-
import os
import pickle
import time
import uuid

from eventlet import tpool


class Config:
    def __init__(self, method_name, in_fifo, out_fifo):
        self.method_name = method_name
        self.in_fifo = in_fifo
        self.out_fifo = out_fifo


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


class FifoPipe:
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

    def open(self):
        os.mkfifo(self.path)

    def close(self):
        os.unlink(self.path)

    @classmethod
    def new(cls, directory, name=None):
        if name is None:
            name = str(uuid.uuid4())
        path = os.path.join(directory, name)
        return cls.wrap(path)

    @classmethod
    def wrap(cls, path):
        instance = cls(path)
        if under_eventlet():
            instance = tpool.Proxy(instance)
        return instance

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()


def under_eventlet():
    import socket
    from eventlet.greenio.base import GreenSocket

    return issubclass(socket.socket, GreenSocket)


def receive_stream(stream_fifo):
    while True:
        req = stream_fifo.load()
        if req is None:
            break
        yield req


def receive(fifo):
    loaded = fifo.load()
    if isinstance(loaded, NewStream):
        stream_fifo = FifoPipe.wrap(loaded.path)
        return receive_stream(stream_fifo)
    return loaded


def send_stream(stream_fifo, result):
    for msg in result:
        stream_fifo.dump(msg)
        time.sleep(0.01)
    stream_fifo.dump(None)


def send(fifo, result):
    if isiterable(result):
        with FifoPipe.new(os.path.dirname(fifo.path)) as stream_fifo:
            fifo.dump(NewStream(stream_fifo.path))
            send_stream(stream_fifo, result)
    else:
        fifo.dump(result)
