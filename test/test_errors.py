# -*- coding: utf-8 -*-
import json
import re
import string
import time

import pytest
from grpc import StatusCode

from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import STATUS_CODE_ENUM_TO_INT_MAP, GrpcError, register

from google.protobuf.any_pb2 import Any
from google.rpc.error_details_pb2 import DebugInfo
from google.rpc.status_pb2 import Status


@pytest.mark.equivalence
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
        assert error.value.code == StatusCode.UNIMPLEMENTED
        assert error.value.message == "Method not found!"


@pytest.mark.equivalence
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
        assert error.value.code == StatusCode.DEADLINE_EXCEEDED
        assert error.value.message == "Deadline Exceeded"

    def test_timeout_while_streaming_request(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                time.sleep(0.01)
                yield protobufs.ExampleRequest(value=value)

        with pytest.raises(GrpcError) as error:
            client.stream_unary(generate_requests(string.ascii_uppercase), timeout=0.05)
        assert error.value.code == StatusCode.DEADLINE_EXCEEDED
        assert error.value.message == "Deadline Exceeded"

    def test_timeout_while_streaming_result(self, client, protobufs):

        res = client.unary_stream(
            protobufs.ExampleRequest(value="A", delay=10, response_count=10),
            timeout=0.05,
        )
        with pytest.raises(GrpcError) as error:
            list(res)

        assert error.value.code == StatusCode.DEADLINE_EXCEEDED
        assert error.value.message == "Deadline Exceeded"


@pytest.mark.equivalence
class TestDeadlineExceededAtServer:
    @pytest.fixture
    def protobufs(self, compile_proto, spec_dir):
        protobufs, _ = compile_proto("example")
        return protobufs

    def test_timeout_while_streaming_request(self, client, protobufs, instrumented):

        stash_metadata = json.dumps([instrumented.path, Cardinality.STREAM_UNARY.value])

        def generate_requests(values):
            for value in values:
                time.sleep(0.01)
                yield protobufs.ExampleRequest(value=value)

        with pytest.raises(GrpcError) as error:
            client.stream_unary(
                generate_requests(string.ascii_uppercase),
                timeout=0.05,
                metadata=[("stash", stash_metadata)],
            )
        assert error.value.code == StatusCode.DEADLINE_EXCEEDED
        assert error.value.message == "Deadline Exceeded"

        # server should not have recieved all the requests
        captured_requests = list(instrumented.requests())
        assert len(captured_requests) < len(string.ascii_uppercase)

    def test_timeout_while_streaming_response(self, client, protobufs, instrumented):

        stash_metadata = json.dumps([instrumented.path, Cardinality.UNARY_STREAM.value])

        response_count = 10

        res = client.unary_stream(
            protobufs.ExampleRequest(
                value="A", delay=10, response_count=response_count
            ),
            timeout=0.05,
            metadata=[("stash", stash_metadata)],
        )
        with pytest.raises(GrpcError) as error:
            list(res)  # client will throw
        assert error.value.code == StatusCode.DEADLINE_EXCEEDED
        assert error.value.message == "Deadline Exceeded"

        time.sleep(0.5)

        # server should not continue to stream responses
        captured_responses = list(instrumented.responses())
        assert len(captured_responses) < response_count

    # add extra test that does mocking and MAKES SURE nameko service is responding
    # correctly (over and above these equivalence tests)


@pytest.mark.equivalence
class TestMethodException:
    def test_error_before_response(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_error(protobufs.ExampleRequest(value="A"))
        assert error.value.code == StatusCode.UNKNOWN
        assert error.value.message == "Exception calling application: boom"

    def test_error_while_streaming_response(self, client, protobufs):
        res = client.stream_error(
            protobufs.ExampleRequest(value="A", response_count=10)
        )
        recvd = []
        with pytest.raises(GrpcError) as error:
            for item in res:
                recvd.append(item)

        # the entrypoint raises after 9 yields, but in the nameko server the exception
        # will abort the stream while some previously read messages are still in
        # the buffer waiting to be sent.
        assert len(recvd) < 10

        assert error.value.code == StatusCode.UNKNOWN
        assert error.value.message == "Exception iterating responses: boom"


class TestRaiseGrpcError:
    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    def test_error_before_response(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_grpc_error(protobufs.ExampleRequest(value="A"))
        assert error.value.code == StatusCode.UNAUTHENTICATED
        assert error.value.message == "Not allowed!"

    def test_error_while_streaming_response(self, client, protobufs):
        res = client.stream_grpc_error(
            protobufs.ExampleRequest(value="A", response_count=10)
        )
        recvd = []
        with pytest.raises(GrpcError) as error:
            for item in res:
                recvd.append(item)

        # the entrypoint raises after 9 yields, but in the nameko server the exception
        # will abort the stream while some previously read messages are still in
        # the buffer waiting to be sent.
        assert len(recvd) < 10

        assert error.value.code == StatusCode.RESOURCE_EXHAUSTED
        assert error.value.message == "Out of tokens!"


@pytest.mark.equivalence
class TestErrorViaContext:
    def test_error_before_response(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_error_via_context(protobufs.ExampleRequest(value="A"))
        assert error.value.code == StatusCode.UNAUTHENTICATED
        assert error.value.message == "Not allowed!"

    def test_error_while_streaming_response(self, client, protobufs):
        res = client.stream_error_via_context(
            protobufs.ExampleRequest(value="A", response_count=10)
        )
        recvd = []
        with pytest.raises(GrpcError) as error:
            for item in res:
                recvd.append(item)

        # there is no exception raised in the entrypoint here, in contrast to
        # TestMethodException.test_error_while_streaming_response above, so we
        # are guaranteed to receive all 9 messages
        assert len(recvd) == 9

        assert error.value.code == StatusCode.RESOURCE_EXHAUSTED
        assert error.value.message == "Out of tokens!"


class TestErrorDetails:
    @pytest.fixture(params=["client=nameko", "client=dp"])
    def client_type(self, request):
        return request.param[7:]

    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    def unpack_debug_info(self, detail):
        any_ = Any()
        any_.CopyFrom(detail)
        debug_info = DebugInfo()
        any_.Unpack(debug_info)
        return debug_info

    def test_error_before_response(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_error(protobufs.ExampleRequest(value="A"))
        assert error.value.code == StatusCode.UNKNOWN
        assert error.value.message == "Exception calling application: boom"

        status = error.value.status
        debug_info = self.unpack_debug_info(status.details[0])

        assert status.code == StatusCode.UNKNOWN.value[0]
        assert status.message == "Exception calling application: boom"
        assert debug_info.stack_entries[0] == "Traceback (most recent call last):\n"
        assert debug_info.detail == "boom"

    def test_error_while_streaming_response(self, client, protobufs):
        res = client.stream_error(
            protobufs.ExampleRequest(value="A", response_count=10)
        )
        with pytest.raises(GrpcError) as error:
            list(res)

        assert error.value.code == StatusCode.UNKNOWN
        assert error.value.message == "Exception iterating responses: boom"

        status = error.value.status
        debug_info = self.unpack_debug_info(status.details[0])

        assert status.code == StatusCode.UNKNOWN.value[0]
        assert status.message == "Exception iterating responses: boom"
        assert debug_info.stack_entries[0] == "Traceback (most recent call last):\n"
        assert debug_info.detail == "boom"


class TestCustomErrorFromException:
    @pytest.fixture(params=["client=nameko", "client=dp"])
    def client_type(self, request):
        return request.param[7:]

    @pytest.fixture(params=["server=nameko"])
    def server_type(self, request):
        return request.param[7:]

    @pytest.fixture(autouse=True)
    def register_exception_handler(self):
        from example_nameko import Error

        def handler(exc_info, code=None, message=None):
            exc_type, exc, tb = exc_info

            code = code or StatusCode.PERMISSION_DENIED
            message = "Not allowed!"

            status = Status(
                code=STATUS_CODE_ENUM_TO_INT_MAP[code],
                message=message,
                details=[],  # don't include traceback
            )

            return GrpcError(code=code, message=message, status=status)

        register(Error, handler)

    def test_error_before_response(self, client, protobufs):
        with pytest.raises(GrpcError) as error:
            client.unary_error(protobufs.ExampleRequest(value="A"))
        assert error.value.code == StatusCode.PERMISSION_DENIED
        assert error.value.message == "Not allowed!"

        status = error.value.status

        assert status.code == StatusCode.PERMISSION_DENIED.value[0]
        assert status.message == "Not allowed!"
        assert not status.details

    def test_error_while_streaming_response(self, client, protobufs):
        res = client.stream_error(
            protobufs.ExampleRequest(value="A", response_count=10)
        )
        with pytest.raises(GrpcError) as error:
            list(res)

        assert error.value.code == StatusCode.PERMISSION_DENIED
        assert error.value.message == "Not allowed!"

        status = error.value.status

        assert status.code == StatusCode.PERMISSION_DENIED.value[0]
        assert status.message == "Not allowed!"
        assert not status.details
