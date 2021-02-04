# -*- coding: utf-8 -*-
from logging import getLogger

from nameko import config
from nameko.extensions import DependencyProvider

from nameko_grpc.client import ClientBase, Method
from nameko_grpc.context import metadata_from_context_data


log = getLogger(__name__)


class Proxy:
    def __init__(self, client, context_data):
        self.client = client
        self.context_data = context_data

    def __getattr__(self, name):
        extra_metadata = metadata_from_context_data(self.context_data)
        return Method(self.client, name, extra_metadata)


class GrpcProxy(ClientBase, DependencyProvider):
    def __init__(self, *args, **kwargs):
        ssl = kwargs.pop("ssl", config.get("GRPC_SSL"))
        super().__init__(*args, ssl=ssl, **kwargs)

    def spawn_thread(self, target, args=(), kwargs=None, name=None):
        self.container.spawn_managed_thread(
            lambda: target(*args, **kwargs or {}), identifier=name
        )

    def get_dependency(self, worker_ctx):
        return Proxy(self, worker_ctx.context_data)
