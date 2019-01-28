# -*- coding: utf-8 -*-
""" Test integration with https://github.com/nameko/nameko-tracer
"""

import json
import logging
import socket
from datetime import datetime

import pytest
from google.protobuf.json_format import MessageToJson
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
from nameko_grpc.tracer.formatter import GrpcJsonFormatter


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

        request_trace = next(
            filter(match_all(is_request, is_not_stream), trace_records)
        )
        response_trace = next(
            filter(match_all(is_response, is_not_stream), trace_records)
        )

        request_stream = list(filter(match_all(is_request, is_stream), trace_records))
        response_stream = list(filter(match_all(is_response, is_stream), trace_records))
        return request_trace, response_trace, request_stream, response_stream

    return extract_records


@pytest.fixture
def check_trace():
    def check(record, requires):
        data = record.nameko_trace
        for key, value in requires.items():
            if callable(value):
                assert value(data, key)
            else:
                assert data[key] == value

    return check


@pytest.fixture
def check_format():

    formatter = GrpcJsonFormatter()

    def check(record, requires):

        formatted = formatter.format(record)
        data = json.loads(formatted)
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
        - context_data

    """

    def test_unary_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request, metadata=[("foo", "bar")])
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "unary_unary",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_UNARY,
            "call_id": "example.unary_unary.0",
            "call_id_stack": ["example.unary_unary.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_UNARY"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_UNARY"},
        )

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request, metadata=[("foo", "bar")]))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "unary_stream",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_STREAM,
            "call_id": "example.unary_stream.0",
            "call_id_stack": ["example.unary_stream.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_STREAM"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_STREAM"},
        )

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
                        "stream_age": lambda data, key: data[key] > 0,
                    }
                ),
            )

    def test_stream_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests, metadata=[("foo", "bar")])
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "stream_unary",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.STREAM_UNARY,
            "call_id": "example.stream_unary.0",
            "call_id_stack": ["example.stream_unary.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "STREAM_UNARY"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "STREAM_UNARY"},
        )

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "request",
                        "stream_part": index + 1,
                        "stream_age": lambda data, key: data[key] > 0,
                    }
                ),
            )

        assert len(result_stream) == 0

    def test_stream_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        responses = list(client.stream_stream(requests, metadata=[("foo", "bar")]))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "stream_stream",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.STREAM_STREAM,
            "call_id": "example.stream_stream.0",
            "call_id_stack": ["example.stream_stream.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "STREAM_STREAM"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "STREAM_STREAM"},
        )

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(
                trace,
                dict(
                    common,
                    **{
                        "stage": "request",
                        "stream_part": index + 1,
                        "stream_age": lambda data, key: data[key] > 0,
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
                        "stream_age": lambda data, key: data[key] > 0,
                    }
                ),
            )

    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        with pytest.raises(GrpcError) as error:
            client.unary_error(request, metadata=[("foo", "bar")])
        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception calling application: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "unary_error",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_UNARY,
            "call_id": "example.unary_error.0",
            "call_id_stack": ["example.unary_error.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_UNARY"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_UNARY"},
        )

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_error_while_streaming_response(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):

        # NOTE it's important that the server sleeps between streaming responses
        # otherwise it terminates the stream with an error before any parts of the
        # response stream are put on the wire
        request = protobufs.ExampleRequest(value="A", response_count=10, delay=10)
        responses = []
        with pytest.raises(GrpcError) as error:
            for response in client.stream_error(request, metadata=[("foo", "bar")]):
                responses.append(response)

        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception iterating responses: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {
            "hostname": socket.gethostname(),
            "timestamp": lambda data, key: isinstance(data[key], datetime),
            "entrypoint_name": "stream_error",
            "entrypoint_type": "Grpc",
            "service": "example",
            "cardinality": Cardinality.UNARY_STREAM,
            "call_id": "example.stream_error.0",
            "call_id_stack": ["example.stream_error.0"],
            "context_data": {"foo": "bar"},
        }

        check_trace(request_trace, dict(common, **{"stage": "request"}))
        check_format(
            request_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_STREAM"},
        )

        check_trace(response_trace, dict(common, **{"stage": "response"}))
        check_format(
            response_trace,
            {"context_data": '{"foo": "bar"}', "cardinality": "UNARY_STREAM"},
        )

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
                        "stream_age": lambda data, key: data[key] > 0,
                    }
                ),
            )


class TestCallArgsField:
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
                "response_time": lambda data, key: data[key] > 0,
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
                "response_time": lambda data, key: data[key] > 0,
            },
        )

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                {
                    "response": GRPC_RESPONSE,  # individual response
                    "response_status": "success",
                    "response_time": lambda data, key: data[key] > 0,
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
                "response_time": lambda data, key: data[key] > 0,
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
                "response_time": lambda data, key: data[key] > 0,
            },
        )

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(
                trace,
                {
                    "response": GRPC_RESPONSE,  # individual response
                    "response_status": "success",
                    "response_time": lambda data, key: data[key] > 0,
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
                "response": lambda data, key: key not in data,
                "response_status": "error",
                "response_time": lambda data, key: data[key] > 0,
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
                "response_time": lambda data, key: data[key] > 0,
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
                    "response_time": lambda data, key: data[key] > 0,
                },
            )

        # check last stream part
        check_trace(
            result_stream[-1],
            {
                "response": lambda data, key: key not in data,
                "response_status": "error",
                "response_time": lambda data, key: data[key] > 0,
            },
        )


class TestGrpcRequestFields:
    def test_unary_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common_trace = {"grpc_request": request}
        common_format = {"grpc_request": MessageToJson(request)}

        check_trace(request_trace, common_trace)
        check_format(request_trace, common_format)

        check_trace(response_trace, common_trace)
        check_format(response_trace, common_format)

        assert len(request_stream) == 0

        assert len(result_stream) == 0

    def test_unary_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common_trace = {"grpc_request": request}
        common_format = {"grpc_request": MessageToJson(request)}

        check_trace(request_trace, common_trace)
        check_format(request_trace, common_format)

        check_trace(response_trace, common_trace)
        check_format(response_trace, common_format)

        assert len(request_stream) == 0

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(response_trace, common_trace)
            check_format(response_trace, common_format)

    def test_stream_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_request": GRPC_STREAM}

        check_trace(request_trace, common)
        check_format(response_trace, common)

        check_trace(response_trace, common)
        check_format(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, {"grpc_request": requests[index]})
            check_format(trace, {"grpc_request": MessageToJson(requests[index])})

        assert len(result_stream) == 0

    def test_stream_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
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
        check_format(request_trace, common)

        check_trace(response_trace, common)
        check_format(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, {"grpc_request": requests[index]})
            check_format(trace, {"grpc_request": MessageToJson(requests[index])})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)
            check_format(trace, common)


class TestGrpcResponseFields:
    def test_unary_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": response})
        check_format(response_trace, {"grpc_response": MessageToJson(response)})

        assert len(result_stream) == 0

    def test_unary_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A", response_count=2)
        responses = list(client.unary_stream(request))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": GRPC_STREAM})
        check_format(response_trace, {"grpc_response": GRPC_STREAM})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, {"grpc_response": responses[index]})
            check_format(trace, {"grpc_response": MessageToJson(responses[index])})

    def test_stream_unary(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        requests = list(generate_requests())
        response = client.stream_unary(requests)
        assert response.message == "A,B"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": response})
        check_format(response_trace, {"grpc_response": MessageToJson(response)})

        assert len(result_stream) == 0

    def test_stream_stream(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
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
        check_format(response_trace, {"grpc_response": GRPC_STREAM})

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, {"grpc_response": responses[index]})
            check_format(trace, {"grpc_response": MessageToJson(responses[index])})

    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace
    ):
        request = protobufs.ExampleRequest(value="A")
        with pytest.raises(GrpcError) as error:
            client.unary_error(request)
        assert error.value.status == StatusCode.UNKNOWN
        assert error.value.details == "Exception calling application: boom"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        check_trace(response_trace, {"grpc_response": None})

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
            response_trace, {"grpc_response": GRPC_STREAM}  # streaming response
        )

        assert len(result_stream) == 10

        # check first 9 stream parts
        for index, trace in enumerate(result_stream[:-1]):
            check_trace(trace, {"grpc_response": responses[index]})

        # check last stream part
        check_trace(result_stream[-1], {"grpc_response": None})


class TestGrpcContextField:
    def test_unary_unary(self, client, protobufs, get_log_records, check_trace):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(request)
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        common = {"grpc_context": lambda data, key: isinstance(data[key], GrpcContext)}

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

        common = {"grpc_context": lambda data, key: isinstance(data[key], GrpcContext)}

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

        common = {"grpc_context": lambda data, key: isinstance(data[key], GrpcContext)}

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

        common = {"grpc_context": lambda data, key: isinstance(data[key], GrpcContext)}

        check_trace(request_trace, common)

        check_trace(response_trace, common)

        assert len(request_stream) == 2
        for index, trace in enumerate(request_stream):
            check_trace(trace, common)

        assert len(result_stream) == 2
        for index, trace in enumerate(result_stream):
            check_trace(trace, common)

    def test_invocation_metadata(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(
            request, metadata=[("foo", "foo1"), ("foo", "foo2")]
        )
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        expected = json.dumps(
            {
                "request_metadata": [("foo", "foo1"), ("foo", "foo2")],
                "response_headers": [],
                "response_trailers": [],
            }
        )

        check_format(request_trace, {"grpc_context": expected})

        check_format(response_trace, {"grpc_context": expected})

    def test_response_headers(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(
            request, metadata=[("echo-header-foo", "foo1"), ("echo-header-foo", "foo2")]
        )
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        expected = json.dumps(
            {
                "request_metadata": [
                    ("echo-header-foo", "foo1"),
                    ("echo-header-foo", "foo2"),
                ],
                "response_headers": [("foo", "foo1"), ("foo", "foo2")],
                "response_trailers": [],
            }
        )

        check_format(request_trace, {"grpc_context": expected})

        check_format(response_trace, {"grpc_context": expected})

    def test_response_trailers(
        self, client, protobufs, get_log_records, check_trace, check_format
    ):
        request = protobufs.ExampleRequest(value="A")
        response = client.unary_unary(
            request,
            metadata=[("echo-trailer-foo", "foo1"), ("echo-trailer-foo", "foo2")],
        )
        assert response.message == "A"

        request_trace, response_trace, request_stream, result_stream = get_log_records()

        expected = json.dumps(
            {
                "request_metadata": [
                    ("echo-trailer-foo", "foo1"),
                    ("echo-trailer-foo", "foo2"),
                ],
                "response_headers": [],
                "response_trailers": [("foo", "foo1"), ("foo", "foo2")],
            }
        )

        check_format(request_trace, {"grpc_context": expected})

        check_format(response_trace, {"grpc_context": expected})


class TestExceptionFields:
    def test_error_before_response(
        self, client, protobufs, get_log_records, check_trace, check_format
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
                "exception_value": "boom",
                "exception_type": "Error",
                "exception_path": "example_nameko.Error",
                "exception_args": ["boom"],
                "exception_traceback": (
                    lambda data, key: 'raise Error("boom")' in data[key]
                ),
                "exception_expected": True,
            },
        )

        check_format(response_trace, {"exception_args": json.dumps(["boom"])})

        assert len(result_stream) == 0

    def test_error_while_streaming_response(
        self, client, protobufs, get_log_records, check_trace, check_format
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
            check_trace(trace, {"exception_value": lambda data, key: key not in data})

        # check last stream part
        check_trace(
            result_stream[-1],
            {
                "exception_value": "boom",
                "exception_type": "Error",
                "exception_path": "example_nameko.Error",
                "exception_args": ["boom"],
                "exception_traceback": (
                    lambda data, key: 'raise Error("boom")' in data[key]
                ),
                "exception_expected": True,
            },
        )
        check_format(result_stream[-1], {"exception_args": json.dumps(["boom"])})
