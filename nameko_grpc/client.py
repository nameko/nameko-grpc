# -*- coding: utf-8 -*-
import itertools
import socket
import threading
import time
from collections import OrderedDict, deque
from logging import getLogger
from urllib.parse import urlparse

from grpc import StatusCode
from h2.errors import PROTOCOL_ERROR  # changed under h2 from 2.6.4?

from nameko_grpc.connection import ConnectionManager
from nameko_grpc.constants import Cardinality
from nameko_grpc.exceptions import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.timeout import bucket_timeout


log = getLogger(__name__)


USER_AGENT = "grpc-python-nameko/0.0.1"


class ClientConnectionManager(ConnectionManager):
    """
    An object that manages a single HTTP/2 connection on a GRPC client.

    Extends the base `ConnectionManager` to make outbound GRPC requests.
    """

    def __init__(self, sock):
        super().__init__(sock, client_side=True)

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

    def response_received(self, headers, stream_id):
        """ Called when a response is received on a stream.

        If the headers contain an error, we should raise it here.
        """
        super().response_received(headers, stream_id)
        self.handle_status(headers, stream_id)

    def trailers_received(self, headers, stream_id):
        """ Called when trailers are received on a stream.

        If the trailers contain an error, we should raise it here.
        """
        super().trailers_received(headers, stream_id)
        self.handle_status(headers, stream_id)

    def handle_status(self, headers, stream_id):
        """ Handle the status of a GRPC stream.
        """
        response_stream = self.receive_streams.get(stream_id)
        if response_stream is None:
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        headers = OrderedDict(headers)
        status = int(headers.get("grpc-status", 0))
        if status > 0:
            exc = GrpcError.from_headers(headers)
            response_stream.close(exc)

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

    def __call__(self, request, timeout=None):
        return self.future(request, timeout=timeout).result()

    def future(self, request, timeout=None):
        inspector = Inspector(self.client.stub)

        cardinality = inspector.cardinality_for_method(self.name)
        input_type = inspector.input_type_for_method(self.name)
        output_type = inspector.output_type_for_method(self.name)
        service_name = inspector.service_name

        request_headers = [
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", urlparse(self.client.target).hostname),
            (":path", "/{}/{}".format(inspector.service_name, self.name)),
            ("te", "trailers"),
            ("content-type", "application/grpc+proto"),
            ("user-agent", USER_AGENT),
            # TODO compression support
            ("grpc-encoding", "identity"),  # gzip, deflate, snappy
            ("grpc-message-type", "{}.{}".format(service_name, input_type.__name__)),
            # TODO compression support
            ("grpc-accept-encoding", "identity"),  # gzip, deflate, snappy
            # TODO applicatiom headers
            # application headers, base64 or binary
        ]

        if timeout is not None:
            request_headers.append(("grpc-timeout", bucket_timeout(timeout)))

        if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
            request = (request,)

        response_stream = self.client.invoke(request_headers, request, timeout)

        return Future(response_stream.consume(output_type), cardinality)


class Proxy:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        return Method(self.client, name)


class Client:
    """ Standalone GRPC client that uses native threads.
    """

    manager = None
    sock = None

    def __init__(self, target, stub):
        self.target = target
        self.stub = stub

    def __enter__(self):
        return self.start()

    def __exit__(self, *args):
        self.stop()

    def start(self):
        target = urlparse(self.target)

        self.sock = socket.socket()
        self.sock.connect((target.hostname, target.port or 50051))

        self.manager = ClientConnectionManager(self.sock)
        threading.Thread(target=self.manager.run_forever).start()

        return Proxy(self)

    def stop(self):
        if self.manager:
            self.manager.stop()
            self.sock.close()

    def timeout(self, send_stream, response_stream, deadline):
        start = time.time()
        while True:
            elapsed = time.time() - start
            if elapsed > deadline:
                exc = GrpcError(
                    status=StatusCode.DEADLINE_EXCEEDED,
                    details="Deadline Exceeded",
                    debug_error_string="<traceback>",
                )
                response_stream.close(exc)
                send_stream.close()
            time.sleep(0.001)

    def invoke(self, request_headers, request, timeout):
        send_stream, response_stream = self.manager.send_request(request_headers)
        if timeout:
            threading.Thread(
                target=self.timeout, args=(send_stream, response_stream, timeout)
            ).start()
        threading.Thread(target=send_stream.populate, args=(request,)).start()
        return response_stream
