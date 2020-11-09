# -*- coding: utf-8 -*-
import itertools
import socket
import threading
import time
from collections import deque
from logging import getLogger
from urllib.parse import urlparse

from grpc import StatusCode
from h2.errors import ErrorCodes

from nameko_grpc.compression import SUPPORTED_ENCODINGS, UnsupportedEncoding
from nameko_grpc.connection import ConnectionManager
from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.ssl import SslConfig
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.timeout import bucket_timeout


log = getLogger(__name__)


USER_AGENT = "grpc-python-nameko/0.0.1"
CONTENT_TYPE = "application/grpc+proto"


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

        request_stream = SendStream(stream_id)
        response_stream = ReceiveStream(stream_id)
        self.receive_streams[stream_id] = response_stream
        self.send_streams[stream_id] = request_stream

        request_stream.headers.set(*request_headers)

        self.pending_requests.append(stream_id)

        return request_stream, response_stream

    def response_received(self, event):
        """ Called when a response is received on a stream.

        If the headers contain an error, we should raise it here.
        """
        super().response_received(event)

        stream_id = event.stream_id
        response_stream = self.receive_streams.get(stream_id)
        if response_stream is None:
            self.conn.reset_stream(stream_id, error_code=ErrorCodes.PROTOCOL_ERROR)
            return

        headers = response_stream.headers

        if int(headers.get("grpc-status", 0)) > 0:
            error = GrpcError.from_headers(headers)
            response_stream.close(error)

    def trailers_received(self, event):
        """ Called when trailers are received on a stream.

        If the trailers contain an error, we should raise it here.
        """
        super().trailers_received(event)

        stream_id = event.stream_id
        response_stream = self.receive_streams.get(stream_id)
        if response_stream is None:
            self.conn.reset_stream(stream_id, error_code=ErrorCodes.PROTOCOL_ERROR)
            return

        trailers = response_stream.trailers

        if int(trailers.get("grpc-status", 0)) > 0:
            error = GrpcError.from_headers(trailers)
            response_stream.close(error)

    def send_pending_requests(self):
        """ Initiate requests for any pending invocations.

        Sends initial headers and any request data that is ready to be sent.
        """
        while self.pending_requests:
            stream_id = self.pending_requests.popleft()

            log.debug("initiating request, new stream %s", stream_id)

            # send headers immediately rather than waiting for data. this ensures
            # streams are established with increasing stream ids regardless of when
            # the request data is available
            self.send_headers(stream_id, immediate=True)
            self.send_data(stream_id)

    def send_data(self, stream_id):
        try:
            super().send_data(stream_id)
        except UnsupportedEncoding:

            response_stream = self.receive_streams[stream_id]
            request_stream = self.send_streams[stream_id]

            error = GrpcError(
                status=StatusCode.UNIMPLEMENTED,
                details="Algorithm not supported: {}".format(request_stream.encoding),
            )
            response_stream.close(error)
            request_stream.close()


class Future:
    def __init__(self, response_stream, output_type, cardinality):
        self.response_stream = response_stream
        self.output_type = output_type
        self.cardinality = cardinality

    def initial_metadata(self):
        return self.response_stream.headers.for_application

    def trailing_metadata(self):
        return self.response_stream.trailers.for_application

    def result(self):
        response = self.response_stream.consume(self.output_type)
        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            response = next(response)
        return response


class Method:
    def __init__(self, client, name, extra_metadata=None):
        self.client = client
        self.name = name
        self.extra_metadata = extra_metadata or []

    def __call__(self, request, **kwargs):
        return self.future(request, **kwargs).result()

    def future(self, request, timeout=None, compression=None, metadata=None):
        inspector = Inspector(self.client.stub)

        cardinality = inspector.cardinality_for_method(self.name)
        input_type = inspector.input_type_for_method(self.name)
        output_type = inspector.output_type_for_method(self.name)
        service_name = inspector.service_name

        compression = compression or self.client.default_compression
        if compression not in SUPPORTED_ENCODINGS:
            log.warning(
                "Invalid compression algorithm: '{}'. Ignoring.".format(compression)
            )
            compression = self.client.default_compression

        scheme = "https" if self.client.ssl else "http"

        request_headers = [
            (":method", "POST"),
            (":scheme", scheme),
            (":authority", urlparse(self.client.target).hostname),
            (":path", "/{}/{}".format(inspector.service_name, self.name)),
            ("te", "trailers"),
            ("content-type", CONTENT_TYPE),
            ("user-agent", USER_AGENT),
            ("grpc-encoding", compression),
            ("grpc-message-type", "{}.{}".format(service_name, input_type.__name__)),
            ("grpc-accept-encoding", ",".join(SUPPORTED_ENCODINGS)),
        ]

        if metadata is not None:
            metadata = metadata[:]
        else:
            metadata = []

        metadata.extend(self.extra_metadata)

        for key, value in metadata:
            request_headers.append((key, value))

        if timeout is not None:
            request_headers.append(("grpc-timeout", bucket_timeout(timeout)))

        if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
            request = (request,)

        response_stream = self.client.invoke(request_headers, request, timeout)

        return Future(response_stream, output_type, cardinality)


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

    def __init__(
        self,
        target,
        stub,
        compression_algorithm="none",
        compression_level="high",
        ssl=False,
    ):
        self.target = target
        self.stub = stub
        self.compression_algorithm = compression_algorithm
        self.compression_level = compression_level  # NOTE not used
        self.ssl = SslConfig(ssl)

    def __enter__(self):
        return self.start()

    def __exit__(self, *args):
        self.stop()

    @property
    def default_compression(self):
        if self.compression_algorithm != "none":
            return self.compression_algorithm
        return "identity"

    def connect(self):
        target = urlparse(self.target)
        sock = socket.create_connection((target.hostname, target.port or 50051))

        if self.ssl:
            context = self.ssl.client_context()
            sock = context.wrap_socket(
                sock=sock, server_hostname=target.hostname, suppress_ragged_eofs=True
            )
        return sock

    def start(self):
        self.sock = self.connect()
        self.manager = ClientConnectionManager(self.sock)
        threading.Thread(target=self.manager.run_forever).start()

        return Proxy(self)

    def stop(self):
        if self.manager:
            self.manager.stop()
            self.sock.close()

    def timeout(self, send_stream, response_stream, deadline):
        start = time.time()
        # TODO timeout thread should terminate if (one or both?) streams are closed
        while True:
            elapsed = time.time() - start
            if elapsed > deadline:
                error = GrpcError(
                    status=StatusCode.DEADLINE_EXCEEDED, details="Deadline Exceeded"
                )
                response_stream.close(error)
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
