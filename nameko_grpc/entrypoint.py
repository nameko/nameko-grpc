# -*- coding: utf-8 -*-
import sys
import time
import types
from functools import partial
from logging import getLogger

from grpc import StatusCode
from nameko import config
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import Entrypoint, SharedExtension, register_entrypoint

from nameko_grpc.channel import ServerChannel
from nameko_grpc.compression import SUPPORTED_ENCODINGS
from nameko_grpc.constants import Cardinality
from nameko_grpc.context import GrpcContext, context_data_from_metadata
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc.ssl import SslConfig
from nameko_grpc.timeout import unbucket_timeout


log = getLogger(__name__)


class GrpcServer(SharedExtension):
    def __init__(self):
        super(GrpcServer, self).__init__()
        self.entrypoints = {}

    def register(self, entrypoint):
        self.entrypoints[entrypoint.method_path] = entrypoint

    def unregister(self, entrypoint):
        self.entrypoints.pop(entrypoint.method_path, None)

    def timeout(self, request_stream, response_stream, deadline):
        start = time.time()
        while True:
            if request_stream.closed and response_stream.closed:
                break
            elapsed = time.time() - start
            if elapsed > deadline:
                request_stream.close()
                error = GrpcError(
                    code=StatusCode.DEADLINE_EXCEEDED, message="Deadline Exceeded"
                )
                response_stream.close(error)
                break
            time.sleep(0.001)

    def handle_request(self, request_stream, response_stream):
        try:
            method_path = request_stream.headers.get(":path")
            entrypoint = self.entrypoints[method_path]
        except KeyError:
            raise GrpcError(code=StatusCode.UNIMPLEMENTED, message="Method not found!")

        encoding = request_stream.headers.get("grpc-encoding", "identity")
        if encoding not in SUPPORTED_ENCODINGS:
            raise GrpcError(
                code=StatusCode.UNIMPLEMENTED,
                message="Algorithm not supported: {}".format(encoding),
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

    def setup(self):
        host = config.get("GRPC_BIND_HOST", "0.0.0.0")
        port = config.get("GRPC_BIND_PORT", 50051)
        ssl = SslConfig(config.get("GRPC_SSL"))

        def spawn_thread(target, args=(), kwargs=None, name=None):
            self.container.spawn_managed_thread(
                lambda: target(*args, **kwargs or {}), identifier=name
            )

        self.channel = ServerChannel(host, port, ssl, spawn_thread, self.handle_request)

    def start(self):
        self.channel.start()

    def stop(self):
        self.channel.stop()
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
        """Override Entrypoint.decorator to ensure `stub` is passed to instance.

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
            try:
                request = next(request)
            except Exception:
                exc_info = sys.exc_info()
                message = "Exception deserializing request!"
                error = GrpcError.from_exception(
                    exc_info,
                    code=StatusCode.INTERNAL,
                    message=message,
                )
                response_stream.close(error)
                return

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
            raise GrpcError(code=StatusCode.UNAVAILABLE, message="Server shutting down")

    def handle_result(self, response_stream, worker_ctx, result, exc_info):

        if self.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            result = (result,)

        if exc_info is None:
            try:
                response_stream.populate(result)
            except Exception as exception:
                message = "Exception iterating responses: {}".format(exception)
                error = GrpcError.from_exception(sys.exc_info(), message=message)

                response_stream.close(error)
        else:
            message = "Exception calling application: {}".format(exc_info[1])
            error = GrpcError.from_exception(exc_info, message=message)

            response_stream.close(error)

        return result, exc_info
