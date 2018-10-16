# -*- coding: utf-8 -*-
import socket
from logging import getLogger
from urllib.parse import urlparse

from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientConnectionManager, Proxy


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

    def invoke(self, request_headers, output_type, request):

        send_stream, response_stream = self.manager.send_request(request_headers)
        response_stream.message_type = output_type
        self.container.spawn_managed_thread(
            lambda: send_stream.populate(request), identifier="populate_request"
        )
        return response_stream

    def get_dependency(self, worker_ctx):
        return Proxy(self)
