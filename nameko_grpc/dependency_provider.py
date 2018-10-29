# -*- coding: utf-8 -*-
import socket
import time
from logging import getLogger
from urllib.parse import urlparse

from grpc import StatusCode
from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientConnectionManager, Proxy
from nameko_grpc.exceptions import GrpcError
from nameko_grpc.streams import ReceiveStream, SendStream


log = getLogger(__name__)


class GrpcProxy(DependencyProvider):

    manager = None
    sock = None

    def __init__(self, target, stub, **kwargs):
        self.target = target
        self.stub = stub
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

    # XXX should use the DP client as a parameter to the client fixture to avoid
    # accidentally not covering it.

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
                try:
                    response_stream.close(exc)
                except ReceiveStream.Closed:  # XXX not a thing; do we need this?
                    pass  # already completed
                try:
                    send_stream.close()
                except SendStream.Closed:
                    pass  # already sent all the data
                break
            time.sleep(0.001)

    def invoke(self, request_headers, output_type, request, timeout):

        send_stream, response_stream = self.manager.send_request(request_headers)
        response_stream.message_type = output_type
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
        return Proxy(self)
