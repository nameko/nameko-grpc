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


# TODO: wrap this in a class and we can use tpool.proxy on it
def dump(fifo, value):
    tpool.execute(_dump, fifo, value)


def load(fifo):
    return tpool.execute(_load, fifo)


def write(fifo, data):
    tpool.execute(_write, fifo, data)


def read(fifo):
    return tpool.execute(_read, fifo)


def _dump(fifo, value):
    print(">> _dump", value, fifo)
    with open(fifo, "wb") as out_:
        pickle.dump(value, out_)


def _load(fifo):
    with open(fifo, "rb") as in_:
        try:
            value = pickle.load(in_)
        except pickle.UnpicklingError:
            value = NotPickled(in_.read())
        return value


def _write(fifo, data):
    with open(fifo, "wb") as out_:
        out_.write(data)


def _read(fifo):
    # XXX never called anymore
    with open(fifo, "rb") as in_:
        data = in_.read()
        return data


@contextmanager
def temp_fifo():
    path = "/tmp/{}".format(uuid.uuid4())
    os.mkfifo(path)
    yield path
    os.unlink(path)


def receive_stream(stream_fifo):
    while True:
        req = load(stream_fifo)
        if req is None:
            break
        yield req


def receive(fifo):
    loaded = load(fifo)
    if isinstance(loaded, NotPickled):
        stream_fifo = loaded.msg
        return receive_stream(stream_fifo)
    return loaded


def send_stream(stream_fifo, result):
    for msg in result:
        dump(stream_fifo, msg)
        time.sleep(0.01)
    dump(stream_fifo, None)


def send(fifo, result):
    if isiterable(result):
        with temp_fifo() as stream_fifo:
            write(fifo, stream_fifo.encode("utf-8"))
            send_stream(stream_fifo, result)
    else:
        dump(fifo, result)
