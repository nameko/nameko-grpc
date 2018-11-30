# -*- coding: utf-8 -*-
import re
import string
import time

import pytest
from grpc import StatusCode

from nameko_grpc.exceptions import GrpcError


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
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) < len(string.ascii_uppercase)

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
        captured_responses = list(instrumented.responses())
        assert len(captured_responses) < response_count

    # add extra test that does mocking and MAKES SURE nameko service is responding
    # correctly (over and above these equivalence tests)
