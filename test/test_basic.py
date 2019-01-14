# -*- coding: utf-8 -*-
import pytest


@pytest.mark.equivalence
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


@pytest.mark.equivalence
class TestLarge:
    def test_large_unary_request(self, client, protobufs):
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", blob="X" * 20000)
        )
        assert response.message == "A"

    def test_large_unary_response(self, client, protobufs):
        multiplier = 20000
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", multiplier=multiplier)
        )
        assert response.message == "A" * multiplier

    def test_large_streaming_request(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value, blob="X" * 20000)

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

    def test_large_streaming_response(self, client, protobufs):

        multiplier = 20000
        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", multiplier=multiplier, response_count=2)
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A" * multiplier, 1),
            ("A" * multiplier, 2),
        ]


@pytest.mark.equivalence
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
