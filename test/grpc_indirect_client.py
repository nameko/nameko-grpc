# -*- coding: utf-8 -*-
import sys
from importlib import import_module

import grpc
import zmq
from grpc._cython.cygrpc import CompressionAlgorithm, CompressionLevel

from helpers import Connection


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

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://127.0.0.1:{}".format(zmq_port))

    conn = Connection(context, socket)

    while True:
        #  Wait for next request from client
        command = conn.receive()
        if command is None:
            break

        command.execute(conn, stub)  # doesn't seem like a brilliant api...
        # instead perhaps do conn.execute(command)
        # or here, even just conn.run()  << receives, executes
        conn.send(True)
