# -*- coding: utf-8 -*-
import base64
import json

from nameko.extensions import DependencyProvider

from nameko_grpc.entrypoint import Grpc

import advanced_pb2_grpc
from advanced_pb2 import SimpleReply


grpc = Grpc.implementing(advanced_pb2_grpc.advancedStub)


class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("utf-8")
        return json.JSONEncoder.default(self, obj)


class ContextData(DependencyProvider):
    def get_dependency(self, worker_ctx):
        return worker_ctx.context_data


class advanced:
    name = "advanced"

    context_data = ContextData()

    @grpc
    def unary_unary(self, request, context):
        message = json.dumps(self.context_data, cls=BytesEncoder)
        return SimpleReply(message=message)
