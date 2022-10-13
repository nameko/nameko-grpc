# -*- coding: utf-8 -*-
import base64
import json
import os
import pickle
import threading
import time

import grpc
import wrapt
import zmq
from google.rpc import status_pb2

from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GRPC_DETAILS_METADATA_KEY, GrpcError


def status_from_metadata(metadata):
    for key, value in metadata:
        if key == GRPC_DETAILS_METADATA_KEY:
            return status_pb2.Status.FromString(value)


def extract_metadata(context):
    """Extracts invocation metadata from `context` and returns it as JSON.

    Binary headers are included as base64 encoded strings.
    Duplicate header values are joined as comma separated, preserving order.
    """
    metadata = {}
    for key, value in context.invocation_metadata():
        if key.endswith("-bin"):
            value = base64.b64encode(value).decode("utf-8")
        if key in metadata:
            metadata[key] = "{},{}".format(metadata[key], value)
        else:
            metadata[key] = value
    return json.dumps(metadata)


def maybe_echo_metadata(context):
    """Copy metadata from request to response."""
    initial_metadata = []
    trailing_metadata = []

    for key, value in context.invocation_metadata():
        if key.startswith("echo-header"):
            initial_metadata.append((key[12:], value))
        elif key.startswith("echo-trailer"):
            trailing_metadata.append((key[13:], value))

    if initial_metadata:
        context.send_initial_metadata(initial_metadata)
    if trailing_metadata:
        context.set_trailing_metadata(trailing_metadata)


def maybe_sleep(request):
    if request.delay:
        time.sleep(request.delay / 1000)


class RemoteClientTransport:
    """Serialializes/deserializes objects through ZMQ sockets using `pickle`."""

    ENDSTREAM = "endstream"

    def __init__(self, sock):
        self.sock = sock

    @classmethod
    def bind(cls, context, socket_type, target):
        """Create a new transport over a ZMQ socket of type `socket_type`, bound
        to the `target` address.
        """
        socket = context.socket(socket_type)
        socket.bind(target)
        return RemoteClientTransport(socket)

    @classmethod
    def bind_to_free_port(cls, context, socket_type, target):
        socket = context.socket(socket_type)
        port = socket.bind_to_random_port(target)
        return RemoteClientTransport(socket), port

    @classmethod
    def connect(cls, context, socket_type, target):
        """Create a new transport over a ZMQ socket of type `socket_type`, connected
        to the `target` address.
        """
        socket = context.socket(socket_type)
        socket.connect(target)
        return RemoteClientTransport(socket)

    @property
    def context(self):
        return self.sock.context

    def receive(self, close=False):
        loaded = pickle.loads(self.sock.recv())
        try:
            if isinstance(loaded, GrpcError):
                raise loaded
            return loaded
        finally:
            if close:
                self.close()

    def receive_stream(self):
        while True:
            item = self.receive()
            if item == self.ENDSTREAM:
                break
            yield item
        self.close()

    def send(self, result, close=False):
        self.sock.send(pickle.dumps(result))
        if close:
            self.close()

    def send_stream(self, result):
        try:
            for item in result:
                self.send(item)
        except grpc.RpcError as exc:
            state = exc._state
            error = GrpcError(
                state.code, state.details, status_from_metadata(state.trailing_metadata)
            )
            self.send(error)
        self.send(self.ENDSTREAM, close=True)

    def close(self):
        self.sock.close()


class Command:
    """Encapsulates a command to run on a remote GRPC client.

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

        self.request_port = None
        self.response_port = None
        self.metadata_port = None

        self._request_transport = None
        self._response_transport = None
        self._metadata_transport = None

        self.mode = Command.Modes.ISSUE

    def __getstate__(self):
        # remove the local transports before pickling
        state = self.__dict__.copy()
        state["transport"] = None
        state["_request_transport"] = None
        state["_response_transport"] = None
        state["_metadata_transport"] = None
        return state

    def __setstate__(self, newstate):
        # set the mode to RESPOND when unpickling
        newstate["mode"] = Command.Modes.RESPOND
        self.__dict__.update(newstate)

    def issue(self):
        """Issue this command to a remote client.

        Sends itelf over the given `transport`.
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to be issued")

        # ensure transports are established and bound to ports before transmitting
        assert self.request_transport
        assert self.response_transport
        assert self.metadata_transport

        self.transport.send(self)
        assert self.transport.receive() is True

    @staticmethod
    def retrieve_commands(transport):
        """Retrieve a series of commands over the given `transport`"""
        # this is actually just receive_stream...
        while True:
            command = transport.receive()
            if command == Command.END:
                break
            assert isinstance(command, Command)
            command.transport = transport
            yield command
            transport.send(True)
        transport.close()

    def bind_transport(self, socket_type):
        """Bind a new transport using the given `socket_type`."""
        transport, port = RemoteClientTransport.bind_to_free_port(
            self.transport.context, socket_type, "tcp://127.0.0.1"
        )
        return transport, port

    def connect_transport(self, socket_type, port):
        """Connect a new transport to `port` using the given `socket_type`."""
        transport = RemoteClientTransport.connect(
            self.transport.context, socket_type, "tcp://127.0.0.1:{}".format(port)
        )
        return transport

    @property
    def request_transport(self):
        """Bind or connect the request transport.

        Depending on the mode of this command, either binds a new transport to a free
        port, or connects to the established port.
        """
        if self._request_transport is None:
            if self.mode is Command.Modes.ISSUE:
                transport, self.request_port = self.bind_transport(zmq.PUSH)
            else:
                transport = self.connect_transport(zmq.PULL, self.request_port)
            self._request_transport = transport
        return self._request_transport

    @property
    def response_transport(self):
        """Bind or connect the response transport.

        Depending on the mode of this command, either binds a new transport to a free
        port, or connects to the established port.
        """
        if self._response_transport is None:
            if self.mode is Command.Modes.ISSUE:
                transport, self.response_port = self.bind_transport(zmq.PULL)
            else:
                transport = self.connect_transport(zmq.PUSH, self.response_port)
            self._response_transport = transport
        return self._response_transport

    @property
    def metadata_transport(self):
        """Bind or connect the metadata transport.

        Depending on the mode of this command, either binds a new transport to a free
        port, or connects to the established port.
        """
        if self._metadata_transport is None:
            if self.mode is Command.Modes.ISSUE:
                transport, self.metadata_port = self.bind_transport(zmq.PULL)
            else:
                transport = self.connect_transport(zmq.PUSH, self.metadata_port)
            self._metadata_transport = transport
        return self._metadata_transport

    def send_request(self, request):
        """Send the request for this command to the remote client.

        Only available in ISSUE mode.
        """
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to send request")

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
            threading.Thread(
                target=self.request_transport.send_stream, args=(request,)
            ).start()
        else:
            self.request_transport.send(request, close=True)

    def get_request(self):
        """Get the request for this command from the caller.

        Only available in RESPOND mode.
        """
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to get request")

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
            return self.request_transport.receive_stream()
        return self.request_transport.receive(close=True)

    def send_response(self, response):
        """Send the response for this command"""
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to send a response")

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
            threading.Thread(
                target=self.response_transport.send_stream, args=(response,)
            ).start()
        else:
            self.response_transport.send(response, close=True)

    def get_response(self):
        """Get the response for this command from the remote client"""
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to get a response")

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
            return self.response_transport.receive_stream()
        return self.response_transport.receive(close=True)

    def send_metadata(self, metadata):
        """Send the response metadata for this command"""
        if self.mode is not Command.Modes.RESPOND:
            raise ValueError("Command must be in RESPOND mode to send metadata")

        self.metadata_transport.send(metadata, close=True)

    def get_metadata(self):
        """Get the response metadata for this command from the remote client"""
        if self.mode is not Command.Modes.ISSUE:
            raise ValueError("Command must be in ISSUE mode to get a metadata")

        return self.metadata_transport.receive(close=True)


class Stash:

    REQUEST = "REQ"
    RESPONSE = "RES"

    def __init__(self, path):
        self.path = path

    def write(self, value, type_):
        if not self.path:
            return

        with open(self.path, "ab") as fh:
            fh.write(pickle.dumps((type_, value)) + b"|")

    def stash_request(self, request):
        self.write(request, self.REQUEST)
        return request

    def stash_request_stream(self, iterator):
        for request in iterator:
            self.write(request, self.REQUEST)
            yield request

    def stash_response(self, response):
        self.write(response, self.RESPONSE)
        return response

    def stash_response_stream(self, iterator):
        for response in iterator:
            self.write(response, self.RESPONSE)
            yield response

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
    """Decorator for instrumenting GRPC implementation methods.

    Stores requests and responses to file for later inspection.
    """
    request, context = args

    stash_metadata = dict(context.invocation_metadata()).get("stash")
    if stash_metadata:
        stash_path, cardinality = json.loads(stash_metadata)
    else:
        stash_path, cardinality = None, None

    stash = Stash(stash_path)

    if cardinality in (Cardinality.STREAM_UNARY.value, Cardinality.STREAM_STREAM.value):
        request = stash.stash_request_stream(request)
    else:
        request = stash.stash_request(request)

    response = wrapped(request, context, **kwargs)

    if cardinality in (Cardinality.UNARY_STREAM.value, Cardinality.STREAM_STREAM.value):
        response = stash.stash_response_stream(response)
    else:
        response = stash.stash_response(response)

    return response
