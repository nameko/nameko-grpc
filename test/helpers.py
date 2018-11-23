# -*- coding: utf-8 -*-
import os
import pickle
import threading

import grpc
import wrapt
import zmq
from nameko.testing.utils import find_free_port

from nameko_grpc.exceptions import GrpcError


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


class Command:
    def __init__(self, config):
        self.config = config

    def invoke(self, conn, stub):
        method = getattr(stub, self.config.method_name)

        req_socket = conn.context.socket(zmq.PULL)
        req_socket.connect("tcp://127.0.0.1:{}".format(self.config.req_port))

        res_socket = conn.context.socket(zmq.PUSH)
        res_socket.connect("tcp://127.0.0.1:{}".format(self.config.res_port))

        request = Connection(conn.context, req_socket).receive()

        try:
            response = method(request, **self.config.kwargs)
        except grpc.RpcError as exc:
            state = exc._state
            response = GrpcError(state.code, state.details, state.debug_error_string)

        Connection(conn.context, res_socket).send(response)

    def execute(self, conn, stub):

        compression = self.config.kwargs.pop("compression", None)
        if compression:
            self.config.kwargs["metadata"] = list(
                self.config.kwargs.get("metadata", [])
            ) + [("grpc-internal-encoding-request", compression)]

        thread = threading.Thread(
            target=self.invoke, name=self.config.method_name, args=(conn, stub)
        )
        thread.start()


class Connection:
    def __init__(self, context, sock):
        self.context = context
        self.sock = sock

    def receive_stream(self):
        while True:
            req = pickle.loads(self.sock.recv())
            if req is None:
                break
            yield req

    def receive(self):
        loaded = pickle.loads(self.sock.recv())
        if isinstance(loaded, NewStream):
            sock = self.context.socket(zmq.PULL)
            sock.connect("tcp://127.0.0.1:{}".format(loaded.port))
            loaded = Connection(self.context, sock).receive_stream()
        return RaisingReceiver.wrap(loaded)

    def send_stream(self, result):
        for msg in result:
            self.sock.send(pickle.dumps(msg))
        self.sock.send(pickle.dumps(None))

    def send(self, result):
        result = SafeSender.wrap(result)
        if isiterable(result):
            new_port = find_free_port()
            sock = self.context.socket(zmq.PUSH)
            sock.bind("tcp://*:{}".format(new_port))
            self.sock.send(pickle.dumps(NewStream(new_port)))
            Connection(self.context, sock).send_stream(result)
        else:
            self.sock.send(pickle.dumps(result))


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


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
