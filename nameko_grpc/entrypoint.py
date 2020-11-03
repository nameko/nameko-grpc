# -*- coding: utf-8 -*-
import time
import types
from functools import partial
from logging import getLogger

import eventlet
from grpc import StatusCode
from nameko import config
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import Entrypoint, SharedExtension, register_entrypoint

from nameko_grpc.compression import SUPPORTED_ENCODINGS, select_algorithm
from nameko_grpc.connection import ConnectionManager
from nameko_grpc.constants import Cardinality
from nameko_grpc.context import GrpcContext, context_data_from_metadata
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.ssl import SslConfig
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.timeout import unbucket_timeout


log = getLogger(__name__)


class ServerConnectionManager(ConnectionManager):
    """
    An object that manages a single HTTP/2 connection on a GRPC server.

    Extends the base `ConnectionManager` to handle incoming GRPC requests.
    """

    def __init__(self, sock, handle_request):
        super().__init__(sock, client_side=False)
        self.handle_request = handle_request

    def request_received(self, event):
        """ Receive a GRPC request and pass it to the GrpcServer to fire any
        appropriate entrypoint.

        Establish a `ReceiveStream` to receive the request payload and `SendStream`
        for sending the eventual response.
        """
        super().request_received(event)

        stream_id = event.stream_id

        request_stream = ReceiveStream(stream_id)
        response_stream = SendStream(stream_id)
        self.receive_streams[stream_id] = request_stream
        self.send_streams[stream_id] = response_stream

        request_stream.headers.set(*event.headers, from_wire=True)

        compression = select_algorithm(
            request_stream.headers.get("grpc-accept-encoding"),
            request_stream.headers.get("grpc-encoding"),
        )

        try:
            response_stream.headers.set(
                (":status", "200"),
                ("content-type", "application/grpc+proto"),
                ("grpc-accept-encoding", ",".join(SUPPORTED_ENCODINGS)),
                # TODO support server changing compression later
                ("grpc-encoding", compression),
            )
            response_stream.trailers.set(("grpc-status", "0"))
            self.handle_request(request_stream, response_stream)

        except GrpcError as error:
            response_stream.trailers.set((":status", "200"), *error.as_headers())
            self.end_stream(stream_id)

    def send_data(self, stream_id):
        try:
            super().send_data(stream_id)
        except GrpcError as error:
            send_stream = self.send_streams.get(stream_id)
            send_stream.trailers.set(*error.as_headers())
            self.end_stream(stream_id)


class GrpcServer(SharedExtension):
    def __init__(self):
        super(GrpcServer, self).__init__()
        self.is_accepting = True
        self.entrypoints = {}

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
                # perhaps we could just close the stream.
                error = GrpcError(
                    status=StatusCode.DEADLINE_EXCEEDED, details="Deadline Exceeded"
                )
                response_stream.close(error)
                break
            time.sleep(0.001)

    def handle_request(self, request_stream, response_stream):
        try:
            method_path = request_stream.headers.get(":path")
            entrypoint = self.entrypoints[method_path]
        except KeyError:
            raise GrpcError(
                status=StatusCode.UNIMPLEMENTED, details="Method not found!"
            )

        encoding = request_stream.headers.get("grpc-encoding", "identity")
        if encoding not in SUPPORTED_ENCODINGS:
            raise GrpcError(
                status=StatusCode.UNIMPLEMENTED,
                details="Algorithm not supported: {}".format(encoding),
            )

        timeout = request_stream.headers.get("grpc-timeout")
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

    def listen(self):

        host = config.get("GRPC_BIND_HOST", "0.0.0.0")
        port = config.get("GRPC_BIND_PORT", 50051)
        ssl = SslConfig(config.get("GRPC_SSL"))

        sock = eventlet.listen((host, port))
        # work around https://github.com/celery/kombu/issues/838
        sock.settimeout(None)

        if ssl:
            context = ssl.server_context()
            sock = context.wrap_socket(
                sock=sock, server_side=True, suppress_ragged_eofs=True,
            )

        return sock

    def start(self):
        self.server_socket = self.listen()
        self.container.spawn_managed_thread(self.run)

    def stop(self):
        self.is_accepting = False
        self.server_socket.close()
        super(GrpcServer, self).stop()

    def kill(self):
        self.stop()


class Grpc(Entrypoint):

    grpc_server = GrpcServer()

    def __init__(self, stub, **kwargs):
        super().__init__(**kwargs)
        self.stub = stub

    @property
    def method_path(self):
        if self.is_bound():
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

    @classmethod
    def implementing(cls, stub):
        return partial(cls.decorator, stub)

    @classmethod
    def decorator(cls, stub, *args, **kwargs):
        """ Override Entrypoint.decorator to ensure `stub` is passed to instance.

        Would be nicer if Nameko had a better mechanism for this.
        """

        def registering_decorator(fn, args, kwargs):
            instance = cls(stub, *args, **kwargs)
            register_entrypoint(fn, instance)
            return fn

        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            # usage without arguments to the decorator:
            # @foobar
            # def spam():
            #     pass
            return registering_decorator(args[0], args=(), kwargs={})
        else:
            # usage with arguments to the decorator:
            # @foobar('shrub', ...)
            # def spam():
            #     pass
            return partial(registering_decorator, args=args, kwargs=kwargs)

    def setup(self):
        self.grpc_server.register(self)

    def stop(self):
        self.grpc_server.unregister(self)

    def handle_request(self, request_stream, response_stream):

        request = request_stream.consume(self.input_type)

        if self.cardinality in (Cardinality.UNARY_STREAM, Cardinality.UNARY_UNARY):
            request = next(request)

        context = GrpcContext(request_stream, response_stream)

        args = (request, context)
        kwargs = {}

        context_data = context_data_from_metadata(context.invocation_metadata())

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
                status=StatusCode.UNAVAILABLE, details="Server shutting down"
            )

    def handle_result(self, response_stream, worker_ctx, result, exc_info):

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            result = (result,)

        if exc_info is None:
            try:
                response_stream.populate(result)
            except Exception as exception:
                message = "Exception iterating responses: {}".format(exception)

                error = GrpcError(status=StatusCode.UNKNOWN, details=message)
                response_stream.close(error)
        else:
            error = GrpcError(
                status=StatusCode.UNKNOWN,
                details="Exception calling application: {}".format(exc_info[1]),
            )
            response_stream.close(error)

        return result, exc_info
