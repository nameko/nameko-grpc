# -*- coding: utf-8 -*-
from grpc._common import (
    CYGRPC_STATUS_CODE_TO_STATUS_CODE,
    STATUS_CODE_TO_CYGRPC_STATUS_CODE,
)
from grpc import StatusCode
from google.protobuf.any_pb2 import Any
from google.rpc.error_details_pb2 import DebugInfo
from google.rpc.status_pb2 import Status
import traceback

registry = {}

GRPC_DETAILS_METADATA_KEY = "grpc-status-details-bin"


class GrpcError(Exception):
    def __init__(self, status, message, details=None):
        self.status = status
        self.message = message
        self.details = details

    def as_headers(self):
        """ Dehydrate this instance to headers to be sent as trailing metadata.
        """
        headers = {
            # ("content-length", "0"),
            "grpc-status": str(STATUS_CODE_TO_CYGRPC_STATUS_CODE[self.status]),
            "grpc-message": self.message,
        }
        if self.details:
            headers[GRPC_DETAILS_METADATA_KEY] = self.details.SerializeToString()
        return list(headers.items())

    @staticmethod
    def from_headers(headers):
        """ Rehydrate a new instance from headers received as trailing metadata.
        """
        status = int(headers.get("grpc-status"))
        message = headers.get("grpc-message")
        details = headers.get(GRPC_DETAILS_METADATA_KEY)

        return GrpcError(
            status=CYGRPC_STATUS_CODE_TO_STATUS_CODE[status],
            message=message,
            details=Status.FromString(details) if details else None,
        )

    @staticmethod
    def from_exception(exc_info, status=None, message=None):
        """ Create a new GrpcError instance representing an underlying exception.
        The `status` and `message` can be passed to this function.

        By default, a `google.rpc.Status` message will be generated capturing the
        debug info of the underyling traceback. See `default_error_from_exception`.

        This can be overridden by registering a new callable against a given exception
        type. See `register`.
        """
        exc, exc_type, tb = exc_info

        error_from_exception = registry.get(exc_type, default_error_from_exception)
        return error_from_exception(exc_info, status, message)

    def __str__(self):
        return (
            "<RPC terminated with:\n"
            "\tstatus = {}\n"
            '\tmessage = "{}"\n'
            '\tdetails = "{}"\n'
            ">".format(self.status, self.message, self.details)
        )


def register(exc_type, custom_error_from_exception):
    """ Register a custom implementation to generate a GrpcError from an underlying
    exception, by exception type.

    Must be a callable with a signature matching `default_error_from_exception`.
    """
    registry[exc_type] = custom_error_from_exception


def unregister(exc_type):
    """ Unregister a custom implementation.
    """
    registry.pop(exc_type, None)


def default_error_from_exception(exc_info, status=None, message=None):
    """ Create a new GrpcError instance representing an underlying exception.
    The `status` and `message` can be passed to this function.

    A `google.rpc.Status` message will be generated capturing the debug info of the
    underyling traceback.
    """
    exc, exc_type, tb = exc_info

    detail = Any()
    detail.Pack(
        DebugInfo(stack_entries=traceback.format_exception(*exc_info), detail=str(exc),)
    )
    status = status or StatusCode.UNKNOWN
    message = message or str(exc)

    rpc_status = Status(
        code=STATUS_CODE_TO_CYGRPC_STATUS_CODE[status],
        message=message,
        details=[detail],
    )

    return GrpcError(status=status, message=message, details=rpc_status)
