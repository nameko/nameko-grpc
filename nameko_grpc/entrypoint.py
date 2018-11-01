# -*- coding: utf-8 -*-
import time
from collections import OrderedDict
from functools import partial
from logging import getLogger

import eventlet
from grpc import StatusCode
from h2.exceptions import StreamClosedError
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import Entrypoint, SharedExtension

from nameko_grpc.connection import ConnectionManager
from nameko_grpc.exceptions import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.timeout import unbucket_timeout

from .constants import Cardinality


log = getLogger(__name__)


class ServerConnectionManager(ConnectionManager):
    """
    An object that manages a single HTTP/2 connection on a GRPC server.

    Extends the base `ConnectionManager` to handle incoming GRPC requests.
    """

    def __init__(self, sock, handle_request):
        super().__init__(sock, client_side=False)
        self.handle_request = handle_request

    def request_received(self, headers, stream_id):
        """ Receive a GRPC request and pass it to the GrpcServer to fire any
        appropriate entrypoint.

        Establish a `ReceiveStream` to receive the request payload and `SendStream`
        for sending the eventual response.
        """
        super().request_received(headers, stream_id)

        headers = OrderedDict(headers)

        request_stream = ReceiveStream(stream_id)
        response_stream = SendStream(stream_id)
        self.receive_streams[stream_id] = request_stream
        self.send_streams[stream_id] = response_stream

        try:
            self.handle_request(headers, request_stream, response_stream)
            response_headers = (
                (":status", "200"),
                ("content-type", "application/grpc+proto"),
                # TODO compression support
                ("grpc-encoding", "identity"),  # gzip, deflate, snappy
            )
            self.conn.send_headers(stream_id, response_headers, end_stream=False)
        except GrpcError as error:
            response_headers = [(":status", "200")]
            response_headers.extend(error.as_headers())
            self.conn.send_headers(stream_id, response_headers, end_stream=True)

    def send_data(self, stream_id):
        try:
            super().send_data(stream_id)
        except GrpcError as error:
            response_headers = error.as_headers()
            try:
                self.conn.send_headers(stream_id, response_headers, end_stream=True)
            except StreamClosedError:
                pass

    def end_stream(self, stream_id):
        """ Close the outbound response stream with trailers containing the status
        of the GRPC request.

        Only called when the stream ends successfully, hence status is always good.
        """
        response_headers = (("grpc-status", "0"),)
        try:
            self.conn.send_headers(stream_id, response_headers, end_stream=True)
        except StreamClosedError:
            pass


class GrpcServer(SharedExtension):
    def __init__(self):
        super(GrpcServer, self).__init__()
        self.is_accepting = True
        self.entrypoints = {}

    @property
    def bind_addr(self):
        host = self.container.config.get("GRPC_BIND_HOST", "0.0.0.0")
        port = self.container.config.get("GRPC_BIND_PORT", 50051)
        return host, port

    def register(self, entrypoint):
        self.entrypoints[entrypoint.method_path] = entrypoint

    def unregister(self, entrypoint):
        self.entrypoints.pop(entrypoint.method_path, None)

    def timeout(self, request_stream, response_stream, deadline):
        start = time.time()
        while True:
            elapsed = time.time() - start
            if elapsed > deadline:
                request_stream.close()
                # XXX does server actually need to do this according to the spec?
                # perhaps we could just _stop_ processing (somehow?)
                exc = GrpcError(
                    status=StatusCode.DEADLINE_EXCEEDED,
                    details="Deadline Exceeded",
                    debug_error_string="<traceback>",
                )
                response_stream.close(exc)
                break
            time.sleep(0.001)

    def handle_request(self, headers, request_stream, response_stream):
        try:
            method_path = headers[":path"]
            entrypoint = self.entrypoints[method_path]
            request_stream.message_type = entrypoint.input_type
        except KeyError:
            raise GrpcError(
                status=StatusCode.UNIMPLEMENTED,
                details="Method not found!",
                debug_error_string="<traceback>",
            )

        timeout = headers.get("grpc-timeout")
        if timeout:
            timeout = unbucket_timeout(timeout)
            self.container.spawn_managed_thread(
                partial(self.timeout, request_stream, response_stream, timeout)
            )

        self.container.spawn_managed_thread(
            partial(entrypoint.handle_request, request_stream, response_stream)
        )

    def run(self):
        while self.is_accepting:
            new_sock, _ = self.server_socket.accept()
            manager = ServerConnectionManager(new_sock, self.handle_request)
            self.container.spawn_managed_thread(manager.run_forever)

    def start(self):
        self.server_socket = eventlet.listen(self.bind_addr)
        # work around https://github.com/celery/kombu/issues/838
        self.server_socket.settimeout(None)
        self.container.spawn_managed_thread(self.run)

    def stop(self):
        self.is_accepting = False
        self.server_socket.close()
        super(GrpcServer, self).stop()

    def kill(self):
        # TODO extension should have a default kill?
        self.stop()


class Grpc(Entrypoint):

    grpc_server = GrpcServer()

    def __init__(self, stub, **kwargs):
        self.stub = stub
        super().__init__(**kwargs)

    @property
    def method_path(self):
        if self.is_bound():  # TODO why is this not a property?
            return Inspector(self.stub).path_for_method(self.method_name)

    @property
    def input_type(self):
        if self.is_bound():
            return Inspector(self.stub).input_type_for_method(self.method_name)

    @property
    def output_type(self):
        if self.is_bound():
            return Inspector(self.stub).output_type_for_method(self.method_name)

    @property
    def cardinality(self):
        if self.is_bound():
            return Inspector(self.stub).cardinality_for_method(self.method_name)

    def setup(self):
        self.grpc_server.register(self)

    def stop(self):
        self.grpc_server.unregister(self)

    def handle_request(self, request_stream, response_stream):

        # where does this come from?
        context = None

        request = request_stream

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.UNARY_UNARY):
            request = next(request)

        args = (request, context)
        kwargs = {}

        # context_data = self.unpack_message_headers(message)
        context_data = {}

        handle_result = partial(self.handle_result, response_stream)
        try:
            self.container.spawn_worker(
                self,
                args,
                kwargs,
                context_data=context_data,
                handle_result=handle_result,
            )
        except ContainerBeingKilled:
            raise GrpcError(
                status=StatusCode.UNAVAILABLE,
                detail="Server shutting down",
                debug_error_string="<traceback>",
            )

    def handle_result(self, response_stream, worker_ctx, result, exc_info):

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            result = (result,)

        response_stream.populate(result)

        return result, exc_info


grpc = Grpc.decorator
