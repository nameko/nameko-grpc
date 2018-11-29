# -*- coding: utf-8 -*-
import sys
import threading
from importlib import import_module

import grpc
import zmq
from grpc._cython.cygrpc import CompressionAlgorithm, CompressionLevel

from nameko_grpc.exceptions import GrpcError

from helpers import Command, RemoteClientTransport


def execute(command, stub):
    method = getattr(stub, command.method_name)

    request = command.get_request()

    compression = command.kwargs.pop("compression", None)
    if compression:
        command.kwargs["metadata"] = list(command.kwargs.get("metadata", [])) + [
            ("grpc-internal-encoding-request", compression)
        ]

    try:
        response = method(request, **command.kwargs)
    except grpc.RpcError as exc:
        state = exc._state
        response = GrpcError(state.code, state.details, state.debug_error_string)

    command.send_response(response)


if __name__ == "__main__":

    port = sys.argv[1]

    spec_path = sys.argv[2]
    sys.path.append(spec_path)

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

    channel = grpc.insecure_channel(
        "127.0.0.1:{}".format(port), options=channel_options
    )
    stub = stub_cls(channel)

    transport = RemoteClientTransport.connect(
        zmq.Context(), zmq.REP, "tcp://127.0.0.1:{}".format(zmq_port)
    )

    for command in Command.retrieve_commands(transport):
        threading.Thread(target=execute, args=(command, stub)).start()
