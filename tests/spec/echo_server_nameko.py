# -*- coding: utf-8 -*-
from nameko_grpc.entrypoint import Grpc

import echo_server_pb2_grpc
from echo_server_pb2 import EchoReply


grpc = Grpc.decorator(echo_server_pb2_grpc.echoStub)


class echo:
    name = "echo"

    @grpc
    def foo(self, request, context):
        return EchoReply(message=request.value)
