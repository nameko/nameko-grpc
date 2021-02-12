# -*- coding: utf-8 -*-
import sys
import threading
from importlib import import_module

import grpc
import zmq
from grpc._cython.cygrpc import CompressionAlgorithm, CompressionLevel

from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GrpcError

from helpers import Command, RemoteClientTransport, status_from_metadata


def execute(command, stub):
    method = getattr(stub, command.method_name)

    request = command.get_request()

    compression = command.kwargs.pop("compression", None)
    if compression:
        command.kwargs["metadata"] = list(command.kwargs.get("metadata", [])) + [
            ("grpc-internal-encoding-request", compression)
        ]

    response_metadata = {}

    try:
        if command.cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            response_future = method.future(request, **command.kwargs)
            response_metadata["code"] = response_future.code()
            response_metadata["details"] = response_future.details()
            response_metadata["initial_metadata"] = list(
                map(tuple, response_future.initial_metadata())
            )
            response_metadata["trailing_metadata"] = list(
                map(tuple, response_future.trailing_metadata())
            )
            response = response_future.result()
        else:
            # .future() interface for RPCs with STREAM responses not supported
            response = method(request, **command.kwargs)
    except grpc.RpcError as exc:
        state = exc._state
        response_metadata["code"] = state.code
        response_metadata["details"] = state.details

        response = GrpcError(
            state.code, state.details, status_from_metadata(state.trailing_metadata)
        )

    command.send_response(response)
    command.send_metadata(response_metadata)


if __name__ == "__main__":

    port = sys.argv[1]
    secure = sys.argv[2]

    proto_name = sys.argv[3]
    service_name = sys.argv[4]

    compression_algorithm = sys.argv[5]
    compression_level = sys.argv[6]

    grpc_module = import_module("{}_pb2_grpc".format(proto_name))
    stub_cls = getattr(grpc_module, "{}Stub".format(service_name))

    zmq_port = sys.argv[7]

    channel_options = [
        (
            "grpc.default_compression_algorithm",
            getattr(CompressionAlgorithm, compression_algorithm),
        ),
        (
            "grpc.default_compression_level",
            getattr(CompressionLevel, compression_level),
        ),
    ]

    if secure == "secure":
        with open("test/certs/server.crt", "rb") as f:
            creds = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(
            "localhost:{}".format(port), creds, options=channel_options
        )
    else:
        channel = grpc.insecure_channel(
            "127.0.0.1:{}".format(port), options=channel_options
        )
    stub = stub_cls(channel)

    transport = RemoteClientTransport.connect(
        zmq.Context(), zmq.REP, "tcp://127.0.0.1:{}".format(zmq_port)
    )

    for command in Command.retrieve_commands(transport):
        threading.Thread(target=execute, args=(command, stub)).start()
