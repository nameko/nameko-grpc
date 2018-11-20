# -*- coding: utf-8 -*-
import pytest
from mock import Mock
from nameko.testing.services import dummy
from nameko.testing.utils import get_extension

from nameko_grpc.dependency_provider import GrpcProxy


class TestStandard:
    def test_unary_unary(self, client, protobufs):
        response = client.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

    def test_unary_stream(self, client, protobufs):
        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", response_count=2)
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(generate_requests())
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]


class TestLarge:
    def test_large_request(self, client, protobufs):
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", blob="B" * 20000)
        )
        assert response.message == "A"

    def test_large_response(self, client, protobufs):
        multiplier = 20000
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", multiplier=multiplier)
        )
        assert response.message == "A" * multiplier


class TestFuture:
    def test_unary_unary(self, client, protobufs):
        response_future = client.unary_unary.future(protobufs.ExampleRequest(value="A"))
        response = response_future.result()
        assert response.message == "A"

    def test_unary_stream(self, client, protobufs):
        responses_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="A", response_count=2)
        )
        responses = responses_future.result()
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response_future = client.stream_unary.future(generate_requests())
        response = response_future.result()
        assert response.message == "A,B"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses_future = client.stream_stream.future(generate_requests())
        responses = responses_future.result()
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]


class TestDependencyProvider:
    @pytest.fixture
    def client(self, container_factory, stubs, server, grpc_port):
        class Service:
            name = "caller"

            example_grpc = GrpcProxy(
                "//127.0.0.1:{}".format(grpc_port), stubs.exampleStub
            )

            @dummy
            def call(self):
                pass

        container = container_factory(Service, {})
        container.start()

        grpc_proxy = get_extension(container, GrpcProxy)
        return grpc_proxy.get_dependency(Mock())

    def test_unary_unary(self, client, protobufs):
        response = client.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

    def test_unary_stream(self, client, protobufs):
        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", response_count=2)
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(generate_requests())
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]
