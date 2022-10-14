# -*- coding: utf-8 -*-
import threading
import time
from logging import getLogger
from urllib.parse import urlparse

from grpc import StatusCode

from nameko_grpc.channel import ClientChannel
from nameko_grpc.compression import SUPPORTED_ENCODINGS
from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.ssl import SslConfig
from nameko_grpc.timeout import bucket_timeout


log = getLogger(__name__)


USER_AGENT = "grpc-python-nameko/0.0.1"
CONTENT_TYPE = "application/grpc+proto"


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


class ClientBase:

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

    def spawn_thread(self, target, args=(), kwargs=None, name=None):
        raise NotImplementedError

    @property
    def default_compression(self):
        if self.compression_algorithm != "none":
            return self.compression_algorithm
        return "identity"

    def start(self):
        self.channel = ClientChannel(self.target, self.ssl, self.spawn_thread)
        self.channel.start()

    def stop(self):
        self.channel.stop()

    def timeout(self, send_stream, response_stream, deadline):
        start = time.time()
        while True:
            if send_stream.closed and response_stream.closed:
                break
            elapsed = time.time() - start
            if elapsed > deadline:
                error = GrpcError(
                    code=StatusCode.DEADLINE_EXCEEDED, message="Deadline Exceeded"
                )
                response_stream.close(error)
                send_stream.close()
                break
            time.sleep(0.001)

    def invoke(self, request_headers, request, timeout):
        send_stream, response_stream = self.channel.send_request(request_headers)
        if timeout:
            self.spawn_thread(
                target=self.timeout,
                args=(send_stream, response_stream, timeout),
                name=f"client timeout [{request}]",
            )
        self.spawn_thread(
            target=send_stream.populate,
            args=(request,),
            name=f"populate request [{request}]",
        )
        return response_stream


class Client(ClientBase):
    """Standalone gRPC client that uses native threads."""

    def __enter__(self):
        return self.start()

    def __exit__(self, *args):
        self.stop()

    def start(self):
        super().start()
        return Proxy(self)

    def spawn_thread(self, target, args=(), kwargs=None, name=None):
        threading.Thread(target=target, args=args, kwargs=kwargs, name=name).start()
