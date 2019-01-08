# -*- coding: utf-8 -*-


class GrpcContext:
    def __init__(self, request_stream, response_stream):
        self.request_stream = request_stream
        self.response_stream = response_stream

    def set_code(self, code):
        self.response_stream.trailers.set(("grpc-status", str(code)))

    def set_details(self, details):
        self.response_stream.trailers.set(("grpc-message", details))

    def invocation_metadata(self):
        return self.request_stream.headers.for_application

    def send_initial_metadata(self, metadata):
        self.response_stream.headers.set(*metadata)

    def set_trailing_metadata(self, metadata):
        self.response_stream.trailers.set(*metadata)
