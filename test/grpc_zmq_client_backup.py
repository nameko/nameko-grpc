# -*- coding: utf-8 -*-
import sys
import threading
from importlib import import_module

import grpc
import zmq
from grpc._cython.cygrpc import CompressionAlgorithm, CompressionLevel

from nameko_grpc.exceptions import GrpcError

from helpers import SafeSender, zreceive, zsend


def call(context, req_socket, res_socket, method, kwargs):
    request = zreceive(context, req_socket)

    try:
        response = method(request, **kwargs)
    except grpc.RpcError as exc:
        state = exc._state
        response = GrpcError(state.code, state.details, state.debug_error_string)

    response = SafeSender.wrap(response)
    zsend(context, res_socket, response)


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

    context = zmq.Context()  # move out of __main__ to be a global?
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:{}".format(zmq_port))

    while True:
        #  Wait for next request from client
        config = zreceive(context, socket)

        if config is None:
            break

        # TODO probably safer to create these inside the thread, not here.
        req_socket = context.socket(zmq.PULL)
        req_socket.connect("tcp://127.0.0.1:{}".format(config.req_port))

        res_socket = context.socket(zmq.PUSH)
        res_socket.connect("tcp://127.0.0.1:{}".format(config.res_port))

        method = getattr(stub, config.method_name)

        compression = config.kwargs.pop("compression", None)
        if compression:
            config.kwargs["metadata"] = list(config.kwargs.get("metadata", [])) + [
                ("grpc-internal-encoding-request", compression)
            ]

        thread = threading.Thread(
            target=call,
            name=config.method_name,
            args=(context, req_socket, res_socket, method, config.kwargs),
        )
        thread.start()
        zsend(context, socket, True)
