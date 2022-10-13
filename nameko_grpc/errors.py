# -*- coding: utf-8 -*-
import traceback

from grpc import StatusCode
from nameko import config

from google.protobuf.any_pb2 import Any
from google.rpc.error_details_pb2 import DebugInfo
from google.rpc.status_pb2 import Status


STATUS_CODE_INT_TO_ENUM_MAP = {item.value[0]: item for item in StatusCode}
STATUS_CODE_ENUM_TO_INT_MAP = {item: item.value[0] for item in StatusCode}

GRPC_DETAILS_METADATA_KEY = "grpc-status-details-bin"


class GrpcError(Exception):
    def __init__(self, code, message, status=None):
        self.code = code
        self.message = message
        self.status = status

    def as_headers(self):
        """Dehydrate this instance to headers to be sent as trailing metadata."""
        headers = {
            # ("content-length", "0"),
            "grpc-status": str(STATUS_CODE_ENUM_TO_INT_MAP[self.code]),
            "grpc-message": self.message,
        }
        if self.status:
            headers[GRPC_DETAILS_METADATA_KEY] = self.status.SerializeToString()
        return list(headers.items())

    @classmethod
    def from_headers(cls, headers):
        """Rehydrate a new instance from headers received as trailing metadata."""
        code = int(headers.get("grpc-status"))
        message = headers.get("grpc-message")
        status = headers.get(GRPC_DETAILS_METADATA_KEY)

        return cls(
            code=STATUS_CODE_INT_TO_ENUM_MAP[code],
            message=message,
            status=Status.FromString(status) if status else None,
        )

    @classmethod
    def from_status(cls, status):
        return cls(
            code=STATUS_CODE_INT_TO_ENUM_MAP[status.code],
            message=status.message,
            status=status,
        )

    @staticmethod
    def from_exception(exc_info, code=None, message=None):
        """Utility method to create a new GrpcError instance representing an
        underlying exception. Useful in try/except clauses.

        By default, a `google.rpc.Status` message will be generated capturing the
        debug info of the underyling traceback. See `default_error_from_exception`.

        This can be overridden by registering a new callable against a given exception
        type. See `register_exception_handler`.
        """
        exc_type, exc, tb = exc_info

        error_from_exception = registry.get(exc_type, default_error_from_exception)
        return error_from_exception(exc_info, code, message)

    def __str__(self):
        return (
            "<RPC terminated with:\n"
            "\tcode = {}\n"
            '\tmessage = "{}"\n'
            '\tstatus = "{}"\n'
            ">".format(self.code, self.message, self.status)
        )


def make_status(code, message, details=None):
    return Status(code=code.value[0], message=message, details=details or [])


def register_exception_handler(exc_type, custom_error_from_exception):
    """Register a custom implementation to generate a GrpcError from an underlying
    exception, by exception type.

    Must be a callable with a signature matching `default_error_from_exception`.
    """
    registry[exc_type] = custom_error_from_exception


def unregister_expection_handler(exc_type):
    """Unregister a custom implementation."""
    registry.pop(exc_type, None)


def default_error_from_exception(exc_info, code=None, message=None):
    """Create a new GrpcError instance representing an underlying exception.

    If the `GRPC_DEBUG` key is set in the Nameko config, the `status` message will
    capture the underyling traceback in a `google.rpc.error_details.DebugInfo` message.
    """
    exc_type, exc, tb = exc_info

    code = code or StatusCode.UNKNOWN
    message = message or str(exc)

    status = Status(code=STATUS_CODE_ENUM_TO_INT_MAP[code], message=message)

    if config.get("GRPC_DEBUG"):
        debug_info = Any()
        debug_info.Pack(
            DebugInfo(
                stack_entries=traceback.format_exception(*exc_info),
                detail=str(exc),
            )
        )
        status.details.append(debug_info)

    return GrpcError(code=code, message=message, status=status)


def grpc_error_passthrough_exception_handler(exc_info, code, message):
    exc_type, exc, tb = exc_info
    return exc


registry = {GrpcError: grpc_error_passthrough_exception_handler}
