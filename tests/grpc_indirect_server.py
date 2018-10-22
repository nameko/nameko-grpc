# -*- coding: utf-8 -*-
import sys
import time
from concurrent import futures
from importlib import import_module

import grpc


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


if __name__ == "__main__":

    source_dir = sys.argv[1]
    sys.path.append(source_dir)

    proto_name = sys.argv[2]
    service_name = sys.argv[3]

    service_module = import_module("{}_grpc".format(proto_name))
    service_cls = getattr(service_module, service_name)

    grpc_module = import_module("{}_pb2_grpc".format(proto_name))
    add_servicer = getattr(grpc_module, "add_{}Servicer_to_server".format(service_name))

    def serve():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_servicer(service_cls(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)

    serve()
