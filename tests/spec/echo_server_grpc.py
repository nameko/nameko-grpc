# -*- coding: utf-8 -*-
import echo_server_pb2_grpc
from echo_pb2 import EchoReply


class echo(echo_server_pb2_grpc.echoServicer):
    def foo(self, request, context):
        return EchoReply(message=request.value)
