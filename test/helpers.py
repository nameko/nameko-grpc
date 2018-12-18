# -*- coding: utf-8 -*-
import os
import pickle
import threading

import grpc
import wrapt
import zmq
from nameko.testing.utils import find_free_port

from nameko_grpc.constants import Cardinality
from nameko_grpc.exceptions import GrpcError


class RemoteClientTransport:
    """ Serialializes/deserializes objects through ZMQ sockets using `pickle`.
    """

    ENDSTREAM = "endstream"

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

    def receive(self):
        loaded = pickle.loads(self.sock.recv())
        if isinstance(loaded, GrpcError):
            raise loaded
        return loaded

    def receive_stream(self):
        while True:
            item = self.receive()
            if item == self.ENDSTREAM:
                break
            yield item

    def send(self, result):
        self.sock.send(pickle.dumps(result))

    def send_stream(self, result):
        try:
            for item in result:
                self.send(item)
        except grpc.RpcError as exc:
            state = exc._state
            error = GrpcError(state.code, state.details, state.debug_error_string)
            self.send(error)
        self.send(self.ENDSTREAM)


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

    END = "endcommands"

    class Modes:
        ISSUE = "issue"
        RESPOND = "respond"

    def __init__(self, method_name, cardinality, kwargs, transport):
        self.method_name = method_name
        self.cardinality = cardinality
        self.kwargs = kwargs
        self.transport = transport

        self.request_port = find_free_port()
        self.response_port = find_free_port()
        self.metadata_port = find_free_port()

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
            if command == Command.END:
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

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
            threading.Thread(
                target=request_transport.send_stream, args=(request,)
            ).start()
        else:
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

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
            return request_transport.receive_stream()
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

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
            threading.Thread(
                target=response_transport.send_stream, args=(response,)
            ).start()
        else:
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

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
            return response_transport.receive_stream()
        return response_transport.receive()

    def send_metadata(self, metadata):
        """ Send the response metadata for this command
        """
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to send metadata")

        # create a new transport and PUSH the metadata
        metadata_transport = RemoteClientTransport.connect(
            self.transport.context,
            zmq.PUSH,
            "tcp://127.0.0.1:{}".format(self.metadata_port),
        )
        metadata_transport.send(metadata)

    def get_metadata(self):
        """ Get the response metadata for this command from the remote client
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to get a metadata")

        # create a new transport and PULL the metadata
        metadata_transport = RemoteClientTransport.bind(
            self.transport.context, zmq.PULL, "tcp://*:{}".format(self.metadata_port)
        )
        return metadata_transport.receive()


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

    # TODO stop inferring cardinality with isiterable; pass it in instead?

    (request, context) = args

    stash = None

    def isiterable(obj):
        try:
            iter(obj)
            return True
        except TypeError:
            return False

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
