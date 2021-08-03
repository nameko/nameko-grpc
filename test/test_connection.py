import pytest
import objgraph
from nameko_grpc.client import Client
from nameko.testing.waiting import wait_for_call


class TestCloseSocketOnClientExit:
    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    def test_close_socket(self, server, load_stubs, spec_dir, grpc_port, protobufs):
        """ Regression test for https://github.com/nameko/nameko-grpc/issues/39
        """
        stubs = load_stubs("example")

        client = Client(
            "//localhost:{}".format(grpc_port),
            stubs.exampleStub,
            "none",
            "high",
            False,
        )
        proxy = client.start()

        connection = objgraph.by_type("ServerConnectionManager")[0]

        response = proxy.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

        with wait_for_call(connection.sock, "close"):
            client.stop()
