# -*- coding: utf-8 -*-
import subprocess

import pytest


@pytest.fixture(autouse=True, scope="session")
def interop_protos(compile_proto):
    compile_proto("empty")
    compile_proto("messages")
    compile_proto("interop")


@pytest.fixture
def grpc_server(start_grpc_server):
    return start_grpc_server("TestService", "interop")


@pytest.fixture
def nameko_server(start_nameko_server):
    return start_nameko_server("TestService", "interop")


@pytest.fixture
def server(request, server_type):
    if server_type == "grpc":
        if request.config.option.server not in ("grpc", "all"):
            pytest.skip("grpc server not requested")
        return request.getfixturevalue("grpc_server")
    if server_type == "nameko":
        if request.config.option.server not in ("nameko", "all"):
            pytest.skip("nameko server not requested")
        return request.getfixturevalue("nameko_server")


@pytest.mark.parametrize(
    "testcase,skip_if_grpc,skip_if_nameko",
    [
        ("large_unary", False, False),
        ("unimplemented_service", False, False),
        ("unimplemented_method", False, False),
        ("client_streaming", False, False),
        ("server_streaming", False, False),
        ("server_compressed_unary", True, True),  # compression not supported
        ("server_compressed_streaming", True, True),  # compression not supported
        ("client_compressed_unary", False, False),
        ("client_compressed_streaming", True, True),  # both servers hang, not sure why
        ("empty_unary", False, False),
        ("empty_stream", False, False),
        ("long_lived_channel", True, True),  # takes forever
        ("cacheable_unary", True, True),  # irrelevant, no caching proxy in use
        ("timeout_on_sleeping_server", False, False),
        ("slow_consumer", False, False),
        ("half_duplex", False, False),
        ("ping_pong", False, False),
        ("rpc_soak", False, False),
        ("channel_soak", False, False),
        ("cancel_after_begin", False, False),
        ("cancel_after_first_response", False, False),
        ("custom_metadata", False, False),
        ("status_code_and_message", False, False),
    ],
)
def test_interop(
    server, grpc_port, testcase, skip_if_grpc, skip_if_nameko, server_type
):
    if server_type == "nameko" and skip_if_nameko:
        pytest.skip("Server not compatible")
    if server_type == "grpc" and skip_if_grpc:
        pytest.skip("Server not compatible")

    args = [
        "docker",
        "run",
        "nameko/nameko-grpc-interop",
        "-server_port",
        str(grpc_port),
        "-test_case",
        testcase,
    ]
    assert subprocess.call(args) == 0
