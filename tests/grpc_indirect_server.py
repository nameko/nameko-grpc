# -*- coding: utf-8 -*-
import sys
import time
from concurrent import futures
from importlib import import_module

import grpc


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


if __name__ == "__main__":

    service_path = sys.argv[1]
    source_dir = sys.argv[2]

    sys.path.append(source_dir)

    module_path, service_name = service_path.split(".")

    service_module = import_module(module_path)
    service_cls = getattr(service_module, service_name)

    grpc_module = import_module("{}_pb2_grpc".format(service_name))
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
