# -*- coding: utf-8 -*-
import enum
import os
import pickle

import grpc
import wrapt
import zmq
from nameko.testing.utils import find_free_port

from nameko_grpc.exceptions import GrpcError


def isiterable(req):
    try:
        iter(req)
        return True
    except TypeError:
        return False


class Command:
    """ Encapsulates a command to run on a remote GRPC client.

    A command can be issued to a remote client, or responded to by that remote client.

    Issuing a command involves serializing it with pickle and sending it to the
    remote client via ZeroMQ (serialization is by a `RemoteClientTransport`)

    At the remote client, the received `Command` can be used to receive the request to
    issue to the GRPC server, and to return the response from the server to the caller.

    Commands have two modes: ISSUE and RESPOND. A Command is constructed in ISSUE mode,
    and converted to RESPOND mode only when it is deserialised by pickle at the remote
    client.

    When in ISSUE mode, the Command can send requests to and receive responses from the
    remote client. When in RESPOND mode, the Command can receive requests from and send
    responses back to the caller.
    """

    class Modes(enum.Enum):
        ISSUE = "issue"
        RESPOND = "respond"

    def __init__(self, method_name, kwargs, transport):
        self.method_name = method_name
        self.kwargs = kwargs
        self.transport = transport

        self.request_port = find_free_port()
        self.response_port = find_free_port()

        self.mode = Command.Modes.ISSUE

    def __getstate__(self):
        # remove the local `transport` before pickling
        state = self.__dict__.copy()
        state["transport"] = None
        return state

    def __setstate__(self, newstate):
        # set the mode to RESPOND when unpicking
        newstate["mode"] = Command.Modes.RESPOND
        self.__dict__.update(newstate)

    def issue(self):
        """ Issue this command to a remote client.

        Sends itelf over the given `transport`.
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to be issued")

        self.transport.send(self)
        assert self.transport.receive() is True

    @staticmethod
    def retrieve_commands(transport):
        """ Retrieve a series of commands over the given `transport`
        """
        while True:
            command = transport.receive()
            if command is None:
                break
            assert isinstance(command, Command)
            command.transport = transport
            yield command
            transport.send(True)

    def send_request(self, request):
        """ Send the request for this command to the remote client.

        Only available in ISSUE mode.
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to send request")

        # create a new transport and PUSH the request
        request_transport = RemoteClientTransport.bind(
            self.transport.context, zmq.PUSH, "tcp://*:{}".format(self.request_port)
        )
        request_transport.send(request)

    def get_request(self):
        """ Get the request for this command from the caller.

        Only available in RESPOND mode.
        """
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to get request")

        # create a new transport and PULL the request
        request_transport = RemoteClientTransport.connect(
            self.transport.context,
            zmq.PULL,
            "tcp://127.0.0.1:{}".format(self.request_port),
        )
        return request_transport.receive()

    def send_response(self, response):
        """ Send the response for this command
        """
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to send a response")

        # create a new transport and PUSH the response
        response_transport = RemoteClientTransport.connect(
            self.transport.context,
            zmq.PUSH,
            "tcp://127.0.0.1:{}".format(self.response_port),
        )
        response_transport.send(response)

    def get_response(self):
        """ Get the response for this command from the remote client
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to get a response")

        # create a new transport and PULL the response
        response_transport = RemoteClientTransport.bind(
            self.transport.context, zmq.PULL, "tcp://*:{}".format(self.response_port)
        )
        return response_transport.receive()


class RemoteClientTransport:
    """ Serialializes/deserializes objects through ZMQ sockets using `pickle`.
    """

    def __init__(self, sock):
        self.sock = sock

    @classmethod
    def bind(cls, context, socket_type, target):
        """ Create a new transport over a ZMQ socket of type `socket_type`, bound
        to the `target` address.
        """
        socket = context.socket(socket_type)
        socket.bind(target)
        return RemoteClientTransport(socket)

    @classmethod
    def connect(cls, context, socket_type, target):
        """ Create a new transport over a ZMQ socket of type `socket_type`, connected
        to the `target` address.
        """
        socket = context.socket(socket_type)
        socket.connect(target)
        return RemoteClientTransport(socket)

    @property
    def context(self):
        return self.sock.context

    def receive_stream(self):
        while True:
            req = pickle.loads(self.sock.recv())
            if req is None:
                break
            yield req

    def receive(self):
        loaded = pickle.loads(self.sock.recv())
        if isinstance(loaded, NewStream):
            stream_transport = RemoteClientTransport.connect(
                self.context, zmq.PULL, "tcp://127.0.0.1:{}".format(loaded.port)
            )
            loaded = stream_transport.receive_stream()

        return RaisingReceiver.wrap(loaded)

    def send_stream(self, result):
        for msg in result:
            self.sock.send(pickle.dumps(msg))
        self.sock.send(pickle.dumps(None))

    def send(self, result):
        result = SafeSender.wrap(result)
        if isiterable(result):
            new_port = find_free_port()
            self.sock.send(pickle.dumps(NewStream(new_port)))
            stream_transport = RemoteClientTransport.bind(
                self.context, zmq.PUSH, "tcp://*:{}".format(new_port)
            )
            stream_transport.send_stream(result)
        else:
            self.sock.send(pickle.dumps(result))


class NewStream:
    def __init__(self, path):
        self.path = path

        # zmq
        self.port = self.path

    def receive(self, sock):
        while True:
            req = pickle.loads(sock.recv())
            if req is None:
                break
            yield req

    def send(self, sock, result):
        for msg in result:
            sock.send(pickle.dumps(msg))
        sock.send(pickle.dumps(None))

    def __str__(self):
        return "<NewStream {}>".format(self.path)


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
