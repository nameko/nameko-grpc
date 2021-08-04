# -*- coding: utf-8 -*-
# TODO would be good to have some unit tests for the channels here
import gc

import objgraph
import pytest

from nameko_grpc.client import Client


class TestDisposeServerConnectionOnExit:

    COUNT = 10

    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    def test_dispose(self, server, load_stubs, spec_dir, grpc_port, protobufs):
        """ Regression test for server connection part of
        https://github.com/nameko/nameko-grpc/issues/40
        """
        stubs = load_stubs("example")

        clients = {}
        for _ in range(self.COUNT):
            client = Client(
                "//localhost:{}".format(grpc_port),
                stubs.exampleStub,
                "none",
                "high",
                False,
            )
            proxy = client.start()
            clients[client] = proxy

        gc.collect()
        assert len(objgraph.by_type("ServerConnectionManager")) == self.COUNT

        for client, proxy in clients.items():
            response = proxy.unary_unary(protobufs.ExampleRequest(value="A"))
            assert response.message == "A"
            client.stop()

        # there is always one server connnection in memory, even if it has no referrers
        # anymore. this is something to do with the eventlet loop i think. seems to be
        # the same situation as https://stackoverflow.com/questions/1127836 except
        # i can't find the strong reference.
        gc.collect()
        assert len(objgraph.by_type("ServerConnectionManager")) == 1


class TestDisposeClientConnectionOnExit:
    COUNT = 10

    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    def test_dispose(self, server, load_stubs, spec_dir, grpc_port, protobufs):
        """ Regression test for client connection part of
        https://github.com/nameko/nameko-grpc/issues/40
        """
        stubs = load_stubs("example")

        clients = {}
        for _ in range(self.COUNT):
            client = Client(
                "//localhost:{}".format(grpc_port),
                stubs.exampleStub,
                "none",
                "high",
                False,
            )
            proxy = client.start()
            clients[client] = proxy

        gc.collect()
        assert len(objgraph.by_type("ClientConnectionManager")) == self.COUNT

        for client in clients.keys():
            client.stop()

        gc.collect()
        assert len(objgraph.by_type("ClientConnectionManager")) == 0
