# -*- coding: utf-8 -*-
import socket
from logging import getLogger

from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientConnectionManager, Proxy


log = getLogger(__name__)


class GrpcProxy(DependencyProvider):
    def __init__(self, host, stub, port=50051, **kwargs):
        self.host = host
        self.stub = stub
        self.port = port
        super().__init__(**kwargs)

    def start(self):

        sock = socket.socket()
        sock.connect((self.host, self.port))

        self.manager = ClientConnectionManager(sock, self.stub)
        self.container.spawn_managed_thread(self.manager.run_forever)

    def stop(self):
        self.manager.stop()

    def invoke(self, method_name, request):

        send_stream, response_stream = self.manager.invoke_method(method_name)
        self.container.spawn_managed_thread(
            lambda: send_stream.populate(request), identifier="populate_request"
        )
        return response_stream

    def get_dependency(self, worker_ctx):
        return Proxy(self.invoke, self.stub)
