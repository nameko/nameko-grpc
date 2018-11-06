# -*- coding: utf-8 -*-
import random
import re
import string
import time

import pytest
from grpc import StatusCode
from mock import Mock
from nameko.testing.services import dummy
from nameko.testing.utils import get_extension

from nameko_grpc.constants import Cardinality
from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.exceptions import GrpcError
from nameko_grpc.inspection import Inspector


class TestInspection:
    @pytest.fixture
    def inspector(self, stubs):
        return Inspector(stubs.exampleStub)

    def test_service_name(self, inspector):
        assert inspector.service_name == "nameko.example"

    def test_path_for_method(self, inspector):
        assert inspector.path_for_method("unary_unary") == "/nameko.example/unary_unary"
        assert (
            inspector.path_for_method("unary_stream") == "/nameko.example/unary_stream"
        )
        assert (
            inspector.path_for_method("stream_stream")
            == "/nameko.example/stream_stream"
        )
        assert (
            inspector.path_for_method("stream_unary") == "/nameko.example/stream_unary"
        )

    def test_input_type_for_method(self, inspector, protobufs):
        assert (
            inspector.input_type_for_method("unary_unary") == protobufs.ExampleRequest
        )

    def test_output_type_for_method(self, inspector, protobufs):
        assert inspector.output_type_for_method("unary_unary") == protobufs.ExampleReply

    def test_cardinality_for_method(self, inspector):
        insp = inspector
        assert insp.cardinality_for_method("unary_unary") == Cardinality.UNARY_UNARY
        assert insp.cardinality_for_method("unary_stream") == Cardinality.UNARY_STREAM
        assert insp.cardinality_for_method("stream_unary") == Cardinality.STREAM_UNARY
        assert insp.cardinality_for_method("stream_stream") == Cardinality.STREAM_STREAM


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


class TestMultipleClients:

    # TODO any point checking this with grpc client?

    def test_unary_unary(self, start_client, protobufs):

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

    def test_unary_stream(self, start_client, protobufs):

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

    def test_stream_unary(self, start_client, protobufs):

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

    def test_stream_stream(self, start_client, protobufs):

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


class TestMethodNotFound:
    @pytest.fixture(autouse=True)
    def unregister_grpc_method(self, stubs):
        with open(stubs.__file__) as fh:
            original_service = fh.read()

        pattern = re.compile(r"'not_found': grpc.\w+\(.*?\),", re.DOTALL)
        modified_service = re.sub(pattern, "", original_service)

        with open(stubs.__file__, "w") as fh:
            fh.write(modified_service)

        yield

        with open(stubs.__file__, "w") as fh:
            fh.write(original_service)

    def test_method_not_found(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.not_found(protobufs.ExampleRequest(value="hello"))
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Method not found!"


class TestDeadlineExceededAtClient:
    @pytest.fixture
    def protobufs(self, compile_proto, spec_dir):
        protobufs, _ = compile_proto("example")
        return protobufs

    def test_timeout_before_any_result(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_unary(
                protobufs.ExampleRequest(value="A", delay=1000),
                timeout=0.05,  # XXX fails when too fast; need protection
            )
        assert error.value.status == StatusCode.DEADLINE_EXCEEDED
        assert error.value.details == "Deadline Exceeded"

    def test_timeout_while_streaming_request(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                time.sleep(0.01)
                yield protobufs.ExampleRequest(value=value)

        with pytest.raises(GrpcError) as error:
            client.stream_unary(generate_requests(string.ascii_uppercase), timeout=0.05)
        assert error.value.status == StatusCode.DEADLINE_EXCEEDED
        assert error.value.details == "Deadline Exceeded"

    def test_timeout_while_streaming_result(self, client, protobufs):

        res = client.unary_stream(
            protobufs.ExampleRequest(value="A", delay=10, response_count=10),
            timeout=0.05,
        )
        with pytest.raises(GrpcError) as error:
            list(res)

        assert error.value.status == StatusCode.DEADLINE_EXCEEDED
        assert error.value.details == "Deadline Exceeded"


class TestDeadlineExceededAtServer:
    @pytest.fixture
    def protobufs(self, compile_proto, spec_dir):
        protobufs, _ = compile_proto("example")
        return protobufs

    def test_timeout_while_streaming_request(self, client, protobufs, instrumented):
        def generate_requests(values):
            for value in values:
                time.sleep(0.01)
                yield protobufs.ExampleRequest(value=value, stash=instrumented.path)

        with pytest.raises(GrpcError) as error:
            client.stream_unary(generate_requests(string.ascii_uppercase), timeout=0.05)
        assert error.value.status == StatusCode.DEADLINE_EXCEEDED
        assert error.value.details == "Deadline Exceeded"

        # server should not have recieved all the requests
        assert len(list(instrumented.requests())) < len(string.ascii_uppercase)

    def test_timeout_while_streaming_response(self, client, protobufs, instrumented):

        response_count = 10

        res = client.unary_stream(
            protobufs.ExampleRequest(
                value="A",
                delay=10,
                stash=instrumented.path,
                response_count=response_count,
            ),
            timeout=0.05,
        )
        with pytest.raises(GrpcError) as error:
            list(res)  # client will throw
        assert error.value.status == StatusCode.DEADLINE_EXCEEDED
        assert error.value.details == "Deadline Exceeded"

        time.sleep(0.5)

        # server should not continue to stream responses
        assert len(list(instrumented.responses())) < response_count

    # add extra test that does mocking and MAKES SURE nameko service is responding
    # correctly (over and above these equivalence tests)


class TestCompression:
    @pytest.fixture(params=["deflate", "gzip", "none"])
    def compression_algorithm(self, request):
        return request.param

    @pytest.fixture(params=["high", "medium", "low", "none"])
    def compression_level(self, request):
        return request.param

    @pytest.fixture
    def server(self, start_server, compression_algorithm, compression_level):
        return start_server(
            "example",
            compression_algorithm=compression_algorithm,
            compression_level=compression_level,
        )

    @pytest.fixture
    def client(self, start_client, compression_algorithm, compression_level, server):
        return start_client(
            "example",
            compression_algorithm=compression_algorithm,
            compression_level=compression_level,
        )

    def test_default_compression(self, client, protobufs):

        response = client.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", response_count=2)
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(generate_requests())
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

    @pytest.mark.parametrize("algorithm_for_call", ["gzip", "identity"])
    def test_set_different_compression_for_call(
        self, start_client, start_server, protobufs, algorithm_for_call
    ):
        start_server("example")
        client = start_client("example", compression_algorithm="deflate")

        response = client.unary_unary(
            protobufs.ExampleRequest(value="A" * 1000),
            metadata=(("grpc-internal-encoding-request", algorithm_for_call),),
        )
        assert response.message == "A" * 1000

        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A" * 1000, response_count=2),
            metadata=(("grpc-internal-encoding-request", algorithm_for_call),),
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A" * 1000, 1),
            ("A" * 1000, 2),
        ]

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(
            generate_requests(),
            metadata=(("grpc-internal-encoding-request", algorithm_for_call),),
        )
        assert response.message == "A" * 1000 + "," + "B" * 1000

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(
            generate_requests(),
            metadata=(("grpc-internal-encoding-request", algorithm_for_call),),
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A" * 1000, 1),
            ("B" * 1000, 2),
        ]

    def test_request_unsupported_algorithm(
        self, start_client, start_server, protobufs, client_type
    ):
        if client_type == "grpc":
            pytest.skip(
                "GRPC client will strip bogus compression algorithms rather than "
                "sending them to the server, so we can't run this test."
            )

        start_server("example")
        client = start_client("example", compression_algorithm="deflate")

        with pytest.raises(GrpcError) as error:
            client.unary_unary(
                protobufs.ExampleRequest(value="A" * 1000),
                metadata=(("grpc-internal-encoding-request", "bogus"),),
            )
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Deadline Exceeded"

    def test_respond_with_different_algorithm(
        self, start_client, start_server, protobufs
    ):
        start_server("example")
        client = start_client("example", compression_algorithm="deflate")

        response = client.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", response_count=2, compression="gzip")
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value, compression="gzip")

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value, compression="gzip")

        responses = client.stream_stream(generate_requests())
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

    def test_disable_compression_for_message(
        self, start_client, start_server, client_type, protobufs
    ):
        if client_type == "grpc":
            pytest.skip(
                "Not sure this is possible with non-beta grpc client. There is no way "
                "to pass the GRPCCallOptions object disabling compression"
            )
