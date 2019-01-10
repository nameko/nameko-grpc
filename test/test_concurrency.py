# -*- coding: utf-8 -*-
import json
import random
import string

import pytest

from nameko_grpc.constants import Cardinality


@pytest.mark.equivalence
class TestConcurrency:
    def test_unary_unary(self, client, protobufs, instrumented, client_type):

        stash_metadata = json.dumps([instrumented.path, Cardinality.UNARY_UNARY.value])

        futures = []
        for letter in string.ascii_uppercase:
            futures.append(
                client.unary_unary.future(
                    protobufs.ExampleRequest(value=letter),
                    metadata=[("stash", stash_metadata)],
                )
            )

        for index, future in enumerate(futures):
            assert future.result().message == string.ascii_uppercase[index]

        # verify messages from concurrent requests are interleaved
        # there is a 1/26! chance of concurrent requests being handled in order
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) == 26
        assert [req.value for req in captured_requests] != string.ascii_uppercase

    def test_unary_stream(self, client, protobufs, instrumented, client_type):

        stash_metadata = json.dumps([instrumented.path, Cardinality.UNARY_STREAM.value])

        futures = []
        for letter in string.ascii_uppercase:
            futures.append(
                client.unary_stream.future(
                    protobufs.ExampleRequest(value=letter, response_count=2),
                    metadata=[("stash", stash_metadata)],
                )
            )

        for index, future in enumerate(futures):
            result = list(future.result())
            responses = [(response.message, response.seqno) for response in result]
            assert responses == [
                (string.ascii_uppercase[index], 1),
                (string.ascii_uppercase[index], 2),
            ]

        # verify messages from concurrent requests are interleaved
        # there is a 1/26! chance of concurrent requests being handled in order
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) == 26
        assert [req.value for req in captured_requests] != string.ascii_uppercase

    def test_stream_unary(
        self, client, protobufs, instrumented, client_type, server_type
    ):

        stash_metadata = json.dumps([instrumented.path, Cardinality.STREAM_UNARY.value])

        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        futures = []
        for index in range(26):
            if index % 2 == 0:
                values = string.ascii_uppercase
            else:
                values = string.ascii_lowercase
            futures.append(
                client.stream_unary.future(
                    generate_requests(values), metadata=[("stash", stash_metadata)]
                )
            )

        for index, future in enumerate(futures):
            if index % 2 == 0:
                assert future.result().message == ",".join(string.ascii_uppercase)
            else:
                assert future.result().message == ",".join(string.ascii_lowercase)

        # verify messages from concurrent requests are interleaved
        # there is a 1/626! chance of concurrent requests being handled in order,
        # just check the first 26.
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) == 26 * 26
        assert [req.value for req in captured_requests[:26]] != string.ascii_uppercase

    def test_stream_stream(self, client, protobufs, instrumented, client_type):

        stash_metadata = json.dumps(
            [instrumented.path, Cardinality.STREAM_STREAM.value]
        )

        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        futures = []
        for index in range(26):
            if index % 2 == 0:
                values = string.ascii_uppercase
            else:
                values = string.ascii_lowercase
            futures.append(
                client.stream_stream.future(
                    generate_requests(values), metadata=[("stash", stash_metadata)]
                )
            )

        for index, future in enumerate(futures):
            result = list(future.result())
            responses = [(response.seqno, response.message) for response in result]
            if index % 2 == 0:
                expected = list(enumerate(string.ascii_uppercase, 1))
            else:
                expected = list(enumerate(string.ascii_lowercase, 1))
            assert responses == expected

        # verify messages from concurrent requests are interleaved
        # there is a 1/626! chance of concurrent requests being handled in order,
        # just check the first 26.
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) == 26 * 26
        assert [req.value for req in captured_requests[:26]] != string.ascii_uppercase


@pytest.mark.equivalence
class TestMultipleClients:
    def test_unary_unary(self, start_client, server, protobufs):

        futures = []
        number_of_clients = 5

        for index in range(number_of_clients):
            client = start_client("example")
            response_future = client.unary_unary.future(
                protobufs.ExampleRequest(value=string.ascii_uppercase[index])
            )
            futures.append(response_future)

        for index, future in enumerate(futures):
            response = future.result()
            assert response.message == string.ascii_uppercase[index]

    def test_unary_stream(self, start_client, server, protobufs):

        futures = []
        number_of_clients = 5

        for index in range(number_of_clients):
            client = start_client("example")
            responses_future = client.unary_stream.future(
                protobufs.ExampleRequest(
                    value=string.ascii_uppercase[index], response_count=2
                )
            )
            futures.append(responses_future)

        for index, future in enumerate(futures):
            responses = future.result()
            assert [(response.message, response.seqno) for response in responses] == [
                (string.ascii_uppercase[index], 1),
                (string.ascii_uppercase[index], 2),
            ]

    def test_stream_unary(self, start_client, server, protobufs):

        number_of_clients = 5

        def shuffled(string):
            chars = list(string)
            random.shuffle(chars)
            return chars

        streams = [shuffled(string.ascii_uppercase) for _ in range(number_of_clients)]

        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        futures = []

        for index in range(number_of_clients):
            client = start_client("example")
            response_future = client.stream_unary.future(
                generate_requests(streams[index])
            )
            futures.append(response_future)

        for index, future in enumerate(futures):
            response = future.result()
            assert response.message == ",".join(streams[index])

    def test_stream_stream(self, start_client, server, protobufs):

        number_of_clients = 5

        def shuffled(string):
            chars = list(string)
            random.shuffle(chars)
            return chars

        streams = [shuffled(string.ascii_uppercase) for _ in range(number_of_clients)]

        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        futures = []

        for index in range(number_of_clients):
            client = start_client("example")
            responses_future = client.stream_stream.future(
                generate_requests(streams[index])
            )
            futures.append(responses_future)

        for index, future in enumerate(futures):
            responses = future.result()

            received = [(response.seqno, response.message) for response in responses]
            assert received == list(enumerate(streams[index], 1))
