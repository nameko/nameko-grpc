# -*- coding: utf-8 -*-
import os
import pickle
import time
import uuid

import grpc
import wrapt
import zmq
from eventlet import tpool
from nameko.testing.utils import find_free_port

from nameko_grpc.exceptions import GrpcError


class StreamAborted(Exception):
    pass


class Config:
    def __init__(self, method_name, in_fifo, out_fifo, kwargs):
        self.method_name = method_name
        self.in_fifo = in_fifo
        self.out_fifo = out_fifo
        self.kwargs = kwargs

        # zmq
        self.req_port = self.in_fifo
        self.res_port = self.out_fifo


class NewStream:
    def __init__(self, path):
        self.path = path

        # zmq
        self.port = self.path

    def __str__(self):
        return "<NewStream {}>".format(self.path)


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


class AbortableIterator:
    class Aborted(Exception):
        pass

    def __init__(self, value):
        self.value = value
        self.aborted = False

    def abort(self):
        self.aborted = True

    def iterate(self):
        for item in self.value:
            if self.aborted:
                raise AbortableIterator.Aborted()
            yield item

    def result(self):
        if not isiterable(self.value):
            return self.value
        return self.iterate()

    @classmethod
    def wrap(cls, value):
        instance = cls(value)
        return instance.abort, instance.result()


class RaisingReceiver:
    """ Inspect and re-raise any GrpcErrors in the stream
    """

    def __init__(self, value):
        self.value = value

    def iterate(self):
        for item in self.value:
            if isinstance(item, GrpcError):
                raise item
            yield item

    def result(self):
        if not isiterable(self.value):
            if isinstance(self.value, GrpcError):
                raise self.value
            return self.value
        return self.iterate()

    @classmethod
    def wrap(cls, value):
        return cls(value).result()


class SafeSender:
    """ Catch and send any GrpcErrors in the stream
    """

    def __init__(self, value):
        self.value = value

    def iterate(self):
        try:
            for item in self.value:
                yield item
        except grpc.RpcError as exc:
            state = exc._state
            yield GrpcError(state.code, state.details, state.debug_error_string)

    def result(self):
        if not isiterable(self.value):
            return self.value
        return self.iterate()

    @classmethod
    def wrap(cls, value):
        return cls(value).result()


class RequestResponseStash:

    REQUEST = "REQ"
    RESPONSE = "RES"

    def __init__(self, path):
        self.path = path

    def write(self, value, type_):
        if not self.path:
            return

        with open(self.path, "ab") as fh:
            fh.write(pickle.dumps((type_, value)) + b"|")

    def write_request(self, req):
        self.write(req, self.REQUEST)

    def write_response(self, res):
        self.write(res, self.RESPONSE)

    def read(self):
        if not self.path or not os.path.exists(self.path):
            return []

        with open(self.path, "rb") as fh:
            data = fh.read()

        for item in data.split(b"|"):
            if item:
                yield pickle.loads(item)

    def requests(self):
        for type_, item in self.read():
            if type_ == self.REQUEST:
                yield item

    def responses(self):
        for type_, item in self.read():
            if type_ == self.RESPONSE:
                yield item


@wrapt.decorator
def instrumented(wrapped, instance, args, kwargs):
    """ Decorator for instrumenting GRPC implementation methods.

    Stores requests and responses to file for later inspection.
    """
    (request, context) = args

    stash = None

    def stashing_iterator(iterable, type_):
        for item in iterable:
            nonlocal stash
            if stash is None:
                stash = RequestResponseStash(item.stash)
            stash.write(item, type_)
            yield item

    if not isiterable(request):
        if stash is None:
            stash = RequestResponseStash(request.stash)
        stash = RequestResponseStash(request.stash)
        stash.write_request(request)
    else:
        request = stashing_iterator(request, RequestResponseStash.REQUEST)

    response = wrapped(request, context, **kwargs)

    if not isiterable(response):
        if stash is None:
            stash = RequestResponseStash(response.stash)
        stash.write_response(response)
    else:
        response = stashing_iterator(response, RequestResponseStash.RESPONSE)

    return response


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


def zreceive_stream(sock):
    while True:
        req = pickle.loads(sock.recv())
        if req is None:
            break
        yield req


def receive(fifo):
    loaded = fifo.load()
    if isinstance(loaded, NewStream):
        stream_fifo = FifoPipe.wrap(loaded.path)
        return receive_stream(stream_fifo)
    return loaded


def zreceive(context, socket):
    loaded = pickle.loads(socket.recv())
    if isinstance(loaded, NewStream):
        sock = context.socket(zmq.PULL)
        sock.connect("tcp://127.0.0.1:{}".format(loaded.port))
        return zreceive_stream(sock)
    return loaded


def send_stream(stream_fifo, result):
    try:
        for msg in result:
            stream_fifo.dump(msg)
            time.sleep(0.01)
        stream_fifo.dump(None)
    except AbortableIterator.Aborted:
        pass


def zsend_stream(sock, result):
    for msg in result:
        sock.send(pickle.dumps(msg))
    sock.send(pickle.dumps(None))


def send(fifo, result):
    if isiterable(result):
        with FifoPipe.new(os.path.dirname(fifo.path)) as stream_fifo:
            fifo.dump(NewStream(stream_fifo.path))
            send_stream(stream_fifo, result)
    else:
        fifo.dump(result)


def zsend(context, socket, result):
    if isiterable(result):
        new_port = find_free_port()
        sock = context.socket(zmq.PUSH)
        sock.bind("tcp://*:{}".format(new_port))
        socket.send(pickle.dumps(NewStream(new_port)))
        zsend_stream(sock, result)
    else:
        socket.send(pickle.dumps(result))
