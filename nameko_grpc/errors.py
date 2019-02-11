# -*- coding: utf-8 -*-
from grpc._common import (
    CYGRPC_STATUS_CODE_TO_STATUS_CODE,
    STATUS_CODE_TO_CYGRPC_STATUS_CODE,
)


class GrpcError(Exception):
    def __init__(self, status, details, debug_error_string=""):
        self.status = status
        self.details = details
        self.debug_error_string = debug_error_string

    def as_headers(self):
        headers = (
            # ("content-length", "0"),
            ("grpc-status", str(STATUS_CODE_TO_CYGRPC_STATUS_CODE[self.status])),
            ("grpc-message", self.details),
        )
        return headers

    @staticmethod
    def from_headers(headers):
        status = int(headers.get("grpc-status"))
        message = headers.get("grpc-message")

        return GrpcError(
            status=CYGRPC_STATUS_CODE_TO_STATUS_CODE[status], details=message
        )

    def __str__(self):
        return (
            "<RPC terminated with:\n"
            "\tstatus = {}\n"
            '\tdetails = "{}"\n'
            '\tdebug_error_string = "{}"\n'
            ">".format(self.status, self.details, self.debug_error_string)
        )
