from nameko.extensions import DependencyProvider

import select
import socket
from eventlet.event import Event
import eventlet

from collections import deque
from h2.errors import PROTOCOL_ERROR  # changed under h2 from 2.6.4?
from nameko_grpc.connection import ConnectionManager
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.constants import Cardinality
import itertools
from logging import getLogger


log = getLogger(__name__)


class ClientConnectionManager(ConnectionManager):
    """
    An object that manages a single HTTP/2 connection on a GRPC client.

    Extends the base `ConnectionManager` to make outbound GRPC requests.
    """

    def __init__(self, sock, stub):
        super().__init__(sock, client_side=True)

        self.stub = stub

        self.pending_requests = deque()

        self.counter = itertools.count(start=1, step=2)

    def on_idle_iteration(self):
        """ Extend `ConnectionManager.on_idle_iteration` to also send any pending
        requests.
        """
        self.send_pending_requests()
        super().on_idle_iteration()

    def invoke_method(self, method_name):
        """ Called by the client to invoke a GRPC method.

        Establish a `SendStream` to send the request payload and `ReceiveStream`
        for receiving the eventual response. `SendStream` and `ReceiveStream` are
        returned to the client for providing the request payload and iterating
        over the response.

        Invocations are queued and sent on the next idle iteration of the event loop.
        XXX (this is bad; there should be more hooks)
        """
        stream_id = next(self.counter)

        inspector = Inspector(self.stub)
        output_type = inspector.output_type_for_method(method_name)

        self.pending_requests.append((stream_id, method_name))

        receive_stream = ReceiveStream(stream_id, output_type)
        self.receive_streams[stream_id] = receive_stream

        send_stream = SendStream(stream_id)
        self.send_streams[stream_id] = send_stream

        return send_stream, receive_stream

    def send_pending_requests(self):
        """ Initiate requests for any pending invocations.

        Sends initial headers and any request data that is ready to be sent.
        """
        while self.pending_requests:
            stream_id, method_name = self.pending_requests.popleft()

            log.debug("initiating request to %s, new stream %s", method_name, stream_id)

            request_headers = [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "127.0.0.1"),
                (":path", "/example/{}".format(method_name)),
                ("te", "trailers"),
                ("content-type", "application/grpc"),
                ("user-agent", "nameko-grc-proxy"),
                ("grpc-accept-encoding", "identity,deflate,gzip"),
                ("accept-encoding", "identity,gzip"),
            ]

            self.conn.send_headers(stream_id, request_headers)
            self.send_data(stream_id)


class GrpcProxy(DependencyProvider):
    def __init__(self, stub, **kwargs):
        self.stub = stub
        self.inspector = Inspector(self.stub)
        super().__init__(**kwargs)

    def setup(self):

        sock = socket.socket()
        sock.connect(("127.0.0.1", 50051))

        self.manager = ClientConnectionManager(sock, self.stub)
        self.container.spawn_managed_thread(self.manager.run_forever)

    def invoke(self, method_name, request):

        send_stream, response_stream = self.manager.invoke_method(method_name)
        self.container.spawn_managed_thread(
            lambda: send_stream.populate(request), identifier="populate_request"
        )
        return response_stream

    def get_dependency(self, worker_ctx):
        class Result:
            def __init__(self, response, cardinality):
                self.response = response
                self.cardinality = cardinality

            def result(self):
                response = self.response
                if self.cardinality in (
                    Cardinality.STREAM_UNARY,
                    Cardinality.UNARY_UNARY,
                ):
                    response = next(response)
                return response

        class Method:
            def __init__(self, invoke, stub, name):
                self.invoke = invoke
                self.stub = stub
                self.name = name

            def __call__(self, request):
                return self.future(request).result()

            def future(self, request):
                inspector = Inspector(self.stub)
                cardinality = inspector.cardinality_for_method(self.name)

                if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
                    request = (request,)
                resp = self.invoke(self.name, request)

                return Result(resp, cardinality)

        class Proxy:
            def __init__(self, invoke, stub):
                self.invoke = invoke
                self.stub = stub

            def __getattr__(self, name):
                return Method(self.invoke, self.stub, name)

        return Proxy(self.invoke, self.stub)
