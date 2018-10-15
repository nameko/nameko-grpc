# -*- coding: utf-8 -*-
import itertools
import socket
import threading
from collections import deque
from logging import getLogger
from urllib.parse import urlparse

from nameko_grpc.connection import ConnectionManager
from nameko_grpc.constants import Cardinality
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream


log = getLogger(__name__)


USER_AGENT = "grpc-python-nameko/0.0.1"


class ClientConnectionManager(ConnectionManager):
    """
    An object that manages a single HTTP/2 connection on a GRPC client.

    Extends the base `ConnectionManager` to make outbound GRPC requests.
    """

    def __init__(self, sock, stub):
        super().__init__(sock, client_side=True)

        self.stub = stub  # XXX no longer used.

        self.pending_requests = deque()

        self.counter = itertools.count(start=1, step=2)

    def on_iteration(self):
        """ On each iteration of the event loop, also initiate any pending requests.
        """
        self.send_pending_requests()
        super().on_iteration()

    def send_request(self, request_headers):
        """ Called by the client to invoke a GRPC method.

        Establish a `SendStream` to send the request payload and `ReceiveStream`
        for receiving the eventual response. `SendStream` and `ReceiveStream` are
        returned to the client for providing the request payload and iterating
        over the response.

        Invocations are queued and sent on the next iteration of the event loop.
        """
        stream_id = next(self.counter)

        self.pending_requests.append((stream_id, request_headers))

        request_stream = SendStream(stream_id)
        response_stream = ReceiveStream(stream_id)
        self.receive_streams[stream_id] = response_stream
        self.send_streams[stream_id] = request_stream

        return request_stream, response_stream

    def send_pending_requests(self):
        """ Initiate requests for any pending invocations.

        Sends initial headers and any request data that is ready to be sent.
        """
        while self.pending_requests:
            stream_id, request_headers = self.pending_requests.popleft()

            log.debug("initiating request, new stream %s", stream_id)

            self.conn.send_headers(stream_id, request_headers)
            self.send_data(stream_id)


class Future:
    def __init__(self, response, cardinality):
        self.response = response
        self.cardinality = cardinality

    def result(self):
        response = self.response
        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            response = next(response)
        return response


class Method:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __call__(self, request):
        return self.future(request).result()

    def future(self, request):
        inspector = Inspector(self.client.stub)

        cardinality = inspector.cardinality_for_method(self.name)
        input_type = inspector.input_type_for_method(self.name)
        output_type = inspector.output_type_for_method(self.name)

        request_headers = [
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", urlparse(self.client.target).hostname),
            (":path", "/{}/{}".format(inspector.service_name, self.name)),
            # TODO timeout support
            # ("grpc-timeout", "5M"),
            ("te", "trailers"),
            ("content-type", "application/grpc+proto"),
            ("user-agent", USER_AGENT),
            # TODO compression support
            ("grpc-encoding", "identity"),  # gzip, deflate, snappy
            (
                "grpc-message-type",
                "{}.{}".format(inspector.service_name, input_type.__name__),
            ),
            # TODO compression support
            ("grpc-accept-encoding", "identity"),  # gzip, deflate, snappy
            # TODO applicatiom headers
            # application headers, base64 or binary
        ]

        if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
            request = (request,)
        resp = self.client.invoke(request_headers, output_type, request)

        return Future(resp, cardinality)


class Proxy:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        return Method(self.client, name)


class Client:
    """ Standalone GRPC client that uses native threads.
    """

    manager = None

    def __init__(self, target, stub):
        self.target = target
        self.stub = stub

    def __enter__(self):
        return self.start()

    def __exit__(self, *args):
        self.stop()

    def start(self):
        sock = socket.socket()
        target = urlparse(self.target)
        sock.connect((target.hostname, target.port or 50051))

        self.manager = ClientConnectionManager(sock, self.stub)
        threading.Thread(target=self.manager.run_forever).start()

        return Proxy(self)

    def stop(self):
        if self.manager:
            self.manager.stop()  # TODO make blocking
        # TODO socket tidyup, after manager has stopped

    def invoke(self, request_headers, output_type, request):
        send_stream, response_stream = self.manager.send_request(request_headers)
        response_stream.message_type = output_type
        threading.Thread(target=send_stream.populate, args=(request,)).start()
        return response_stream
