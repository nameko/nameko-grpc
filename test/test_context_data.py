# -*- coding: utf-8 -*-
import json

import pytest
from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy

from nameko_grpc.dependency_provider import GrpcProxy


@pytest.fixture(autouse=True)
def stubs(load_stubs):
    return load_stubs("advanced")


@pytest.fixture(autouse=True)
def protobufs(load_protobufs):
    return load_protobufs("advanced")


class TestContextData:
    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        # only nameko server supports the features in this test
        return request.param[7:]

    @pytest.fixture
    def grpc_server(self, start_server):
        start_server("advanced")

    @pytest.fixture
    def grpc_client(self, start_client, grpc_server):
        return start_client("advanced")

    @pytest.fixture
    def amqprpc_service(
        self, container_factory, rabbit_config, grpc_server, protobufs, stubs, grpc_port
    ):
        class AmqpRpcService:
            name = "amqp"

            advanced_grpc = GrpcProxy(
                "//127.0.0.1:{}".format(grpc_port), stubs.advancedStub
            )

            @rpc
            def proxy(self):
                response = self.advanced_grpc.unary_unary(
                    protobufs.SimpleRequest(value="A"),
                    metadata=[("x", "X"), ("x-bin", b"\x0a\x0b\x0a\x0b\x0a\x0b")],
                )
                return response.message

        container = container_factory(AmqpRpcService, rabbit_config)
        container.start()

    @pytest.mark.parametrize(
        "metadata,expected_key,expected_value",
        [
            # simple
            ([("a", "A")], "a", "A"),
            # duplicate names
            ([("a", "A1"), ("a", "A2")], "a", ["A1", "A2"]),
            # # binary
            ([("a-bin", b"\x0a\x0b\x0a\x0b\x0a\x0b")], "a-bin", "CgsKCwoL"),
            # # duplicate binary
            (
                [
                    ("a-bin", b"\x01\x01\x01\x01\x01\x01"),
                    ("a-bin", b"\x02\x02\x02\x02\x02\x02"),
                ],
                "a-bin",
                ["AQEBAQEB", "AgICAgIC"],
            ),
        ],
    )
    def test_request_metadata_available_as_context_data(
        self, grpc_client, protobufs, metadata, expected_key, expected_value
    ):
        response = grpc_client.unary_unary(
            protobufs.SimpleRequest(value="A"), metadata=metadata
        )
        assert json.loads(response.message)[expected_key] == expected_value

    @pytest.mark.usefixtures("predictable_call_ids")
    def test_dependency_provider_includes_context_data_in_grpc_request_metadata(
        self, amqprpc_service, rabbit_config
    ):
        with ServiceRpcProxy("amqp", context_data={"a": "A"}) as proxy_rpc:

            context_data = json.loads(proxy_rpc.proxy())

            # added by RPC client, propagated by GRPC
            assert context_data["a"] == "A"

            # added by GRPC client as metadata
            assert context_data["x"] == "X"
            assert context_data["x-bin"] == "CgsKCwoL"  # base64-encoded for response

            # NOTE adding binary objects to context_data will cause problems for
            # extensions that can't serialize them (e.g. AMQP entrypoints in their
            # default configuration); this is a general problem when mixing extensions.
            # opportunity to improve this when nameko grows its own serialization layer

            # call id stack is propagated
            assert context_data["call_id_stack"] == [
                "standalone_rpc_client.0.0",
                "amqp.proxy.1",
                "advanced.unary_unary.2",
            ]
