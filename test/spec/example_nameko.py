# -*- coding: utf-8 -*-
import base64
import json
import time

from nameko_grpc.entrypoint import Grpc

from helpers import instrumented

import example_pb2_grpc
from example_pb2 import ExampleReply


# TODO move stash to context once applciation headers are implemented


grpc = Grpc.decorator(example_pb2_grpc.exampleStub)


def extract_metadata(context):
    """ Extracts invocation metadata from `context` and returns it as JSON.

    Binary headers are included as base64 encoded strings.
    Duplicate header values are joined as comma separated, preserving order.
    """
    metadata = {}
    for key, value in context.invocation_metadata():
        if key.endswith("-bin"):
            value = base64.b64encode(value).decode("utf-8")
        if key in metadata:
            metadata[key] = "{},{}".format(metadata[key], value)
        else:
            metadata[key] = value
    return json.dumps(metadata)


def maybe_echo_metadata(context):
    """ Copy metadata from request to response.
    """
    return  # XXX

    initial_metadata = []
    trailing_metadata = []

    for key, value in context.invocation_metadata():
        if key.startswith("echo-header"):
            initial_metadata.append((key[12:], value))
        elif key.startswith("echo-trailer"):
            trailing_metadata.append((key[13:], value))

    if initial_metadata:
        context.send_initial_metadata(initial_metadata)
    if trailing_metadata:
        context.set_trailing_metadata(trailing_metadata)


def maybe_sleep(request):
    if request.delay:
        time.sleep(request.delay / 1000)


class example:
    name = "example"

    @grpc
    @instrumented
    def unary_unary(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        maybe_sleep(request)
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message, stash=request.stash, metadata=metadata)

    @grpc
    @instrumented
    def unary_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        message = request.value * (request.multiplier or 1)
        for i in range(request.response_count):
            maybe_sleep(request)
            yield ExampleReply(
                message=message, seqno=i + 1, stash=request.stash, metadata=metadata
            )

    @grpc
    @instrumented
    def stream_unary(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        messages = []
        stash = None
        for index, req in enumerate(request):
            stash = req.stash
            maybe_sleep(req)
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return ExampleReply(message=",".join(messages), stash=stash, metadata=metadata)

    @grpc
    @instrumented
    def stream_stream(self, request, context):
        metadata = extract_metadata(context)
        maybe_echo_metadata(context)
        for index, req in enumerate(request):
            maybe_sleep(req)
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(
                message=message, seqno=index + 1, stash=req.stash, metadata=metadata
            )
