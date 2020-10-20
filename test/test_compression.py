# -*- coding: utf-8 -*-
import gzip
import zlib

import pytest
from grpc import StatusCode
from mock import patch

from nameko_grpc.compression import (
    UnsupportedEncoding,
    compress,
    decompress,
    select_algorithm,
)
from nameko_grpc.errors import GrpcError


@pytest.mark.equivalence
class TestCompression:
    """ Some of these tests are incomplete because the standard Python GRPC client
    and/or server don't conform to the spec.

    See https://stackoverflow.com/questions/53233339/what-is-the-state-of-compression-
    for more details.
    """

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
            protobufs.ExampleRequest(value="A" * 1000), compression=algorithm_for_call
        )
        assert response.message == "A" * 1000

        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A" * 1000, response_count=2),
            compression=algorithm_for_call,
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A" * 1000, 1),
            ("A" * 1000, 2),
        ]

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(
            generate_requests(), compression=algorithm_for_call
        )
        assert response.message == "A" * 1000 + "," + "B" * 1000

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(
            generate_requests(), compression=algorithm_for_call
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A" * 1000, 1),
            ("B" * 1000, 2),
        ]

    def test_request_unsupported_algorithm_at_client(
        self, start_client, start_server, protobufs, client_type, server_type
    ):
        if client_type == "grpc":
            pytest.skip("grpc client throws assertion error")

        start_server("example")
        client = start_client("example")

        response = client.unary_unary(
            protobufs.ExampleRequest(value="A"), compression="bogus"
        )
        assert response.message == "A"

        responses = client.unary_stream(
            protobufs.ExampleRequest(value="A", response_count=2), compression="bogus"
        )
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(generate_requests(), compression="bogus")
        assert response.message == "A,B"

        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(generate_requests(), compression="bogus")
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        # TODO assert that the message is sent _without_ compression

    @patch("nameko_grpc.entrypoint.SUPPORTED_ENCODINGS", new=["deflate"])
    def test_request_unsupported_algorithm_at_server(
        self, start_client, start_server, protobufs, client_type, server_type
    ):
        """ It's not possible to run this test with the GRPC server.

        The server can be configured to disable certain compression algorithms with
        the grpc.compression_enabled_algorithms_bitset server option. Unfortunately,
        making a call with a disabled algorithm results in an unhandled exception
        rather than the error described in the spec.
        """

        if server_type == "grpc":
            pytest.skip("See docstring for details")

        start_server("example")
        client = start_client("example")

        with pytest.raises(GrpcError) as error:
            client.unary_unary(
                protobufs.ExampleRequest(value="A" * 1000), compression="gzip"
            )
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Algorithm not supported: gzip"

        res = client.unary_stream(
            protobufs.ExampleRequest(value="A" * 1000, response_count=2),
            compression="gzip",
        )
        with pytest.raises(GrpcError) as error:
            list(res)
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Algorithm not supported: gzip"

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        with pytest.raises(GrpcError) as error:
            client.stream_unary(generate_requests(), compression="gzip")
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Algorithm not supported: gzip"

        def generate_requests():
            for value in ["A" * 1000, "B" * 1000]:
                yield protobufs.ExampleRequest(value=value)

        res = client.stream_stream(generate_requests(), compression="gzip")
        with pytest.raises(GrpcError) as error:
            next(res)
        assert error.value.status == StatusCode.UNIMPLEMENTED
        assert error.value.details == "Algorithm not supported: gzip"

    def test_compression_not_required_at_client(self):
        # TODO
        pass

    def test_compression_not_required_at_server(self):
        # TODO
        pass

    def test_respond_with_different_algorithm(self):
        """ The GRPC server doesn't seem to support this behaviour. It's either
        explicitly not supported, or there is a bug that results in an error if the
        server also has a default compression algorithm set.

        The Nameko server MAY be able to support this behaviour in future, once it's
        clear from the reference implementation exactly how it should behave.

        """
        pytest.skip("See docstring for details")

    def test_disable_compression_for_message(self):
        """ The non-beta GRPC client doesn't support any way to disable compression
        for a call. Additionally, it's not clear how the client should request that
        an the next _message_ should be sent uncompressed as described in the spec.

        It is possible to call context.disable_next_message_compression() in a GRPC
        server method, but this results in an "Unallowed duplicate metadata" error.

        The Nameko server MAY be able to support this behaviour in future, once it's
        clear from the reference implementation exactly how it should behave.
        """
        pytest.skip("See docstring for details")


class TestDecompress:
    def test_preferred_algorithm(self):
        payload = b"\x00" * 1000

        assert decompress(zlib.compress(payload)) == payload

    def test_fallback_algorithm(self):
        payload = b"\x00" * 1000

        assert decompress(gzip.compress(payload)) == payload

    def test_unsupported_algorithm(self):
        payload = b"\x00" * 1000

        with pytest.raises(UnsupportedEncoding):
            decompress(payload)  # not correctly encoded


class TestCompress:
    def test_identity(self):
        payload = b"\x00" * 1000
        assert compress(payload, "identity") == (False, payload)

    def test_deflate(self):
        payload = b"\x00" * 1000
        assert compress(payload, "deflate") == (True, zlib.compress(payload))

    def test_gzip(self):
        payload = b"\x00" * 1000
        assert compress(payload, "gzip") == (True, gzip.compress(payload))

    def test_unsupported_algorithm(self):
        payload = b"\x00" * 1000

        with pytest.raises(UnsupportedEncoding):
            compress(payload, "bogus")


class TestSelectAlgorithm:
    @pytest.mark.parametrize("preferred", ["deflate", "gzip", "identity"])
    def test_preferred_algorithm_available(self, preferred):
        acceptable = "deflate,gzip,identity"
        expected = preferred
        assert select_algorithm(acceptable, preferred) == expected

    def test_preferred_algorithm_is_null(self):
        acceptable = "deflate,gzip,identity"
        preferred = None
        expected = "deflate"
        assert select_algorithm(acceptable, preferred) == expected

    def test_fallback_to_supported_algorithm(self):
        acceptable = "minify,gzip,identity"
        preferred = "minify"
        expected = "gzip"
        assert select_algorithm(acceptable, preferred) == expected

    def test_fallback_to_identity(self):
        acceptable = "minify,identity"
        preferred = "minify"
        expected = "identity"
        assert select_algorithm(acceptable, preferred) == expected

    def test_no_supported_algo(self):
        acceptable = "minify"
        preferred = "minify"
        with pytest.raises(UnsupportedEncoding):
            select_algorithm(acceptable, preferred)
