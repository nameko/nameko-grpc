# -*- coding: utf-8 -*-
from example_nameko import example

from nameko_grpc.tracer import GrpcTracer


class tracer(example):

    tracer = GrpcTracer()
