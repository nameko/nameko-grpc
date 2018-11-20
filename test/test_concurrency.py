# -*- coding: utf-8 -*-
import random
import string


class TestConcurrency:
    # XXX how to assert both are in flight at the same time?
    def test_unary_unary(self, client, protobufs):
        response_a_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A")
        )
        response_b_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="B")
        )
        response_a = response_a_future.result()
        response_b = response_b_future.result()
        assert response_a.message == "A"
        assert response_b.message == "B"

    def test_unary_stream(self, client, protobufs):
        responses_a_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="A", response_count=2)
        )
        responses_b_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="B", response_count=2)
        )
        responses_a = responses_a_future.result()
        responses_b = responses_b_future.result()
        # TODO add random delays and generator consumer that grabs whatever comes first
        # then verify streams are interleaved
        assert [(response.message, response.seqno) for response in responses_a] == [
            ("A", 1),
            ("A", 2),
        ]
        assert [(response.message, response.seqno) for response in responses_b] == [
            ("B", 1),
            ("B", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        # XXX any way to verify that the input streams were interleaved?
        response_1_future = client.stream_unary.future(generate_requests("AB"))
        response_2_future = client.stream_unary.future(generate_requests("XY"))
        response_1 = response_1_future.result()
        response_2 = response_2_future.result()
        assert response_1.message == "A,B"
        assert response_2.message == "X,Y"

    def test_stream_stream(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        # TODO add random delays and generator consumer that grabs whatever comes first
        # then verify streams are interleaved
        # XXX any way to verify that the input streams were interleaved? perhaos track
        # the order the generators are pulled?
        responses_1_future = client.stream_stream.future(generate_requests("AB"))
        responses_2_future = client.stream_stream.future(generate_requests("XY"))
        responses_1 = responses_1_future.result()
        responses_2 = responses_2_future.result()
        assert [(response.message, response.seqno) for response in responses_1] == [
            ("A", 1),
            ("B", 2),
        ]
        assert [(response.message, response.seqno) for response in responses_2] == [
            ("X", 1),
            ("Y", 2),
        ]


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

            expected = [(char, idx + 1) for idx, char in enumerate(streams[index])]
            received = [(response.message, response.seqno) for response in responses]

            assert received == expected
