# -*- coding: utf-8 -*-
import socket
import time
from logging import getLogger
from urllib.parse import urlparse

from grpc import StatusCode
from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientConnectionManager, Method
from nameko_grpc.errors import GrpcError


log = getLogger(__name__)


class Proxy:
    def __init__(self, client, context_data):
        self.client = client
        self.context_data = context_data

    def __getattr__(self, name):
        return Method(self.client, name, context_data=self.context_data)


class GrpcProxy(DependencyProvider):

    manager = None
    sock = None

    def __init__(
        self,
        target,
        stub,
        compression_algorithm="none",
        compression_level="high",  # NOTE not used
        **kwargs
    ):
        self.target = target
        self.stub = stub
        self.compression_algorithm = compression_algorithm
        self.compression_level = compression_level
        super().__init__(**kwargs)

    def start(self):

        target = urlparse(self.target)

        self.sock = socket.socket()
        self.sock.connect((target.hostname, target.port or 50051))

        self.manager = ClientConnectionManager(self.sock)
        self.container.spawn_managed_thread(self.manager.run_forever)

    def stop(self):
        self.manager.stop()
        self.sock.close()

    @property
    def default_compression(self):
        if self.compression_algorithm != "none":
            return self.compression_algorithm
        return "identity"

    def timeout(self, send_stream, response_stream, deadline):
        start = time.time()
        while True:
            elapsed = time.time() - start
            if elapsed > deadline:
                error = GrpcError(
                    status=StatusCode.DEADLINE_EXCEEDED, details="Deadline Exceeded"
                )
                response_stream.close(error)
                send_stream.close()
                break
            time.sleep(0.001)

    def invoke(self, request_headers, request, timeout):

        send_stream, response_stream = self.manager.send_request(request_headers)
        if timeout:
            self.container.spawn_managed_thread(
                lambda: self.timeout(send_stream, response_stream, timeout),
                identifier="client_timeout",
            )
        self.container.spawn_managed_thread(
            lambda: send_stream.populate(request), identifier="populate_request"
        )
        return response_stream

    def get_dependency(self, worker_ctx):
        return Proxy(self, worker_ctx.context_data)
