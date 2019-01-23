# -*- coding: utf-8 -*-
""" Test integration with https://github.com/nameko/nameko-tracer
"""

import logging
import socket
from datetime import datetime

import pytest
from grpc import StatusCode
from nameko_tracer.constants import Stage

from nameko_grpc.constants import Cardinality
from nameko_grpc.context import GrpcContext
from nameko_grpc.exceptions import GrpcError
from nameko_grpc.tracer.adapter import (
    GRPC_CONTEXT,
    GRPC_REQUEST,
    GRPC_RESPONSE,
    GRPC_STREAM,
)


@pytest.fixture
def client_type():
    return "nameko"  # no point testing multiple clients


@pytest.fixture
def server(start_nameko_server):
    return start_nameko_server("tracer")


@pytest.fixture
def caplog(caplog):
    with caplog.at_level(logging.INFO):
        yield caplog


@pytest.fixture
def get_log_records(caplog):
    def is_trace(record):
        return record.name == "nameko_tracer"

    def is_request(record):
        return record.stage == Stage.request

    def is_response(record):
        return record.stage == Stage.response

    def is_stream(record):
        return getattr(record, "stream_part", False)

    def is_not_stream(record):
        return not is_stream(record)

    def match_all(*fns):
        def check(val):
            return all(fn(val) for fn in fns)

        return check

    def extract_trace(record):
        return record.nameko_trace

    def extract_records():
        trace_records = list(filter(is_trace, caplog.records))

        request_trace = extract_trace(
            next(filter(match_all(is_request, is_not_stream), trace_records))
        )
        response_trace = extract_trace(
            next(filter(match_all(is_response, is_not_stream), trace_records))
        )

        request_stream = list(
            map(extract_trace, filter(match_all(is_request, is_stream), trace_records))
        )
        response_stream = list(
            map(extract_trace, filter(match_all(is_response, is_stream), trace_records))
        )
        return request_trace, response_trace, request_stream, response_stream

    return extract_records


@pytest.fixture
def check_trace():
    def check(data, requires):
        for key, value in requires.items():
            if callable(value):
                assert value(data[key])
            else:
                assert data[key] == value

    return check


@pytest.mark.usefixtures("predictable_call_ids")
class TestEssentialFields:
    """ Verify "essential" fields are present on every log trace, including stream
    parts.

    Essential fields:

        - hostname
        - timestamp
        - entrypoint_name
        - entrypoint_type
        - service
        - cardinality
        - call_id
        - call_id_stack
        - stage
        - stream_part (for streams)
        - stream_age (for streams)

    """

    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "unary_unary",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_UNARY,
            "call_id": "example.unary_unary.0",
            "call_id_stack": ["example.unary_unary.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "unary_stream",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_STREAM,
            "call_id": "example.unary_stream.0",
            "call_id_stack": ["example.unary_stream.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 0

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "response",
                        "stream_part": index + 1,
                        "stream_age": lambda value: value > 0,
                    }
                ),
            )

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "stream_unary",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.STREAM_UNARY,
            "call_id": "example.stream_unary.0",
            "call_id_stack": ["example.stream_unary.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "request",
                        "stream_part": index + 1,
                        "stream_age": lambda value: value > 0,
                    }
                ),
            )

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "stream_stream",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.STREAM_STREAM,
            "call_id": "example.stream_stream.0",
            "call_id_stack": ["example.stream_stream.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "request",
                        "stream_part": index + 1,
                        "stream_age": lambda value: value > 0,
                    }
                ),
            )

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "response",
                        "stream_part": index + 1,
                        "stream_age": lambda value: value > 0,
                    }
                ),
            )

    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace
    ):
        request = protobufs.ExampleRequest(value="A")
        with pytest.raises(GrpcError) as error:
            client.unary_error(request)
        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception calling application: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "unary_error",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_UNARY,
            "call_id": "example.unary_error.0",
            "call_id_stack": ["example.unary_error.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_error_while_streaming_response(
        self, client, protobufs, get_log_records, check_trace
    ):

        # NOTE it's important that the server sleeps between streaming responses
        # otherwise it terminates the stream with an error before any parts of the
        # response stream are put on the wire
        request = protobufs.ExampleRequest(value="A", response_count=10, delay=10)
        responses = []
        with pytest.raises(GrpcError) as error:
            for response in client.stream_error(request):
                responses.append(response)

        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception iterating responses: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda dt: isinstance(dt, datetime),
            "entrypoint_name": "stream_error",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_STREAM,
            "call_id": "example.stream_error.0",
            "call_id_stack": ["example.stream_error.0"],
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))

        check_trace(response_trace, dict(common, **{"stage": "response"}))

        assert len(request_stream) == 0

        assert len(result_stream) == 10
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "response",
                        "stream_part": index + 1,
                        "stream_age": lambda value: value > 0,
                    }
                ),
            )


class TestCallArgsField:
    # XXX these are all a bit boring and similar; are they adding value?
    # also we should really test call args redaction
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "call_args": {"context": GRPC_CONTEXT, "request": GRPC_REQUEST},
            "call_args_redacted": False,
        }

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "call_args": {"context": GRPC_CONTEXT, "request": GRPC_REQUEST},
            "call_args_redacted": False,
        }

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        # streaming request
        common = {
            "call_args": {"context": GRPC_CONTEXT, "request": GRPC_STREAM},
            "call_args_redacted": False,
        }

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, common)

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        # streaming request
        common = {
            "call_args": {"context": GRPC_CONTEXT, "request": GRPC_STREAM},
            "call_args_redacted": False,
        }

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, common)

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)


class TestResponseFields:
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_RESPONSE,
                "response_status": "success",
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_STREAM,  # streaming response
                "response_status": None,  # still pending
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                {
                    "response": GRPC_RESPONSE,  # individual response
                    "response_status": "success",
                    "response_time": lambda value: value > 0,
                },
            )

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_RESPONSE,
                "response_status": "success",
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_STREAM,  # streaming response
                "response_status": None,  # still pending
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                {
                    "response": GRPC_RESPONSE,  # individual response
                    "response_status": "success",
                    "response_time": lambda value: value > 0,
                },
            )

    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace
    ):
        request = protobufs.ExampleRequest(value="A")
        with pytest.raises(GrpcError) as error:
            client.unary_error(request)
        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception calling application: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_RESPONSE,
                "response_status": "error",
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 0

    def test_error_while_streaming_response(
        self, client, protobufs, get_log_records, check_trace
    ):

        # NOTE it's important that the server sleeps between streaming responses
        # otherwise it terminates the stream with an error before any parts of the
        # response stream are put on the wire
        request = protobufs.ExampleRequest(value="A", response_count=10, delay=10)
        responses = []
        with pytest.raises(GrpcError) as error:
            for response in client.stream_error(request):
                responses.append(response)

        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception iterating responses: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response": GRPC_STREAM,  # streaming response
                "response_status": None,  # still pending
                "response_time": lambda value: value > 0,
            },
        )

        assert len(result_stream) == 10

        # check first 9 stream parts
        for index, trace in enumerate(result_stream[:-1]):
            check_trace(
                trace,
                {
                    "response": GRPC_RESPONSE,  # individual response
                    "response_status": "success",
                    "response_time": lambda value: value > 0,
                },
            )

        # check last stream part
        check_trace(
            result_stream[-1],
            {
                "response": GRPC_RESPONSE,  # XXX wrong
                "response_status": "error",
                "response_time": lambda value: value > 0,
            },
        )


class TestGrpcRequestFields:
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_request": request}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_request": request}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_request": GRPC_STREAM}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, {"grpc_request": requests[index]})

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_request": GRPC_STREAM}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, {"grpc_request": requests[index]})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)


class TestGrpcResponseFields:
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": response})

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": GRPC_STREAM})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, {"grpc_response": responses[index]})

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": response})

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": GRPC_STREAM})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, {"grpc_response": responses[index]})


class TestGrpcContextFields:
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_context": lambda value: isinstance(value, GrpcContext)}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_context": lambda value: isinstance(value, GrpcContext)}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 0

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)

    def test_stream_unary(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_context": lambda value: isinstance(value, GrpcContext)}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, common)

        assert len(result_stream) == 0

    def test_stream_stream(self, client, protobufs, get_log_records, check_trace):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_context": lambda value: isinstance(value, GrpcContext)}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, common)

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)


class TestExceptionFields:
    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace
    ):
        request = protobufs.ExampleRequest(value="A")
        with pytest.raises(GrpcError) as error:
            client.unary_error(request)
        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception calling application: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(
            response_trace,
            {
                "response_status": "error",
                "exception_value": "boom",
                "exception_type": "Error",
                "exception_path": "example_nameko.Error",
                "exception_args": ["boom"],
                "exception_traceback": lambda tb: 'raise Error("boom")' in tb,
                "exception_expected": True,
            },
        )

        assert len(result_stream) == 0

    def test_error_while_streaming_response(
        self, client, protobufs, get_log_records, check_trace
    ):

        # NOTE it's important that the server sleeps between streaming responses
        # otherwise it terminates the stream with an error before any parts of the
        # response stream are put on the wire
        request = protobufs.ExampleRequest(value="A", response_count=10, delay=10)
        responses = []
        with pytest.raises(GrpcError) as error:
            for response in client.stream_error(request):
                responses.append(response)

        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception iterating responses: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"response": GRPC_STREAM, "response_status": None})

        assert len(result_stream) == 10

        # check first 9 stream parts
        for index, trace in enumerate(result_stream[:-1]):
            check_trace(
                trace, {"response": GRPC_RESPONSE, "response_status": "success"}
            )

        # check last stream part
        check_trace(
            result_stream[-1],
            {
                "response": GRPC_RESPONSE,
                "response_status": "error",
                "exception_value": "boom",
                "exception_type": "Error",
                "exception_path": "example_nameko.Error",
                "exception_args": ["boom"],
                "exception_traceback": lambda tb: 'raise Error("boom")' in tb,
                "exception_expected": True,
            },
        )
