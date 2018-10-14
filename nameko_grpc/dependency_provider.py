# -*- coding: utf-8 -*-
import socket
from logging import getLogger

from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientConnectionManager, Proxy


log = getLogger(__name__)


class GrpcProxy(DependencyProvider):
    def __init__(self, stub, **kwargs):
        self.stub = stub
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
        return Proxy(self.invoke, self.stub)
