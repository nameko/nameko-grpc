# -*- coding: utf-8 -*-
import logging
import sys
from datetime import datetime

from nameko_tracer import Tracer, constants

from nameko_grpc.constants import Cardinality


logger = logging.getLogger(__name__)

GRPC_ADAPTER = {
    "nameko_grpc.entrypoint.Grpc": ("nameko_grpc.tracer.GrpcEntrypointAdapter")
}


class GrpcTracer(Tracer):
    """ Extend nameko_tracer.Tracer to add support for the Grpc entrypoint, including
    streaming requests and responses.
    """

    def setup(self):
        self.configure_adapter_types(GRPC_ADAPTER)
        super().setup()

    def log_request(self, worker_ctx):
        request, context = worker_ctx.args

        cardinality = worker_ctx.entrypoint.cardinality

        request_stream = None
        if cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
            request_stream = request.tee()

        timestamp = datetime.utcnow()
        self.worker_timestamps[worker_ctx] = timestamp

        extra = {
            "stage": constants.Stage.request,
            "worker_ctx": worker_ctx,
            "timestamp": timestamp,
        }
        try:
            adapter = self.adapter_factory(worker_ctx)
            adapter.info("[%s] entrypoint call trace", worker_ctx.call_id, extra=extra)
        except Exception:
            logger.warning("Failed to log entrypoint trace", exc_info=True)

        if request_stream:
            self.container.spawn_managed_thread(
                lambda: self.log_request_stream(worker_ctx, request_stream)
            )

    def log_result(self, worker_ctx, result, exc_info):

        cardinality = worker_ctx.entrypoint.cardinality

        timestamp = datetime.utcnow()
        worker_setup_timestamp = self.worker_timestamps[worker_ctx]
        response_time = (timestamp - worker_setup_timestamp).total_seconds()

        result_stream = None

        if exc_info is None and cardinality in (
            Cardinality.UNARY_STREAM,
            Cardinality.STREAM_STREAM,
        ):
            result_stream = result.tee()

        extra = {
            "stage": constants.Stage.response,
            "worker_ctx": worker_ctx,
            "result": result,
            "exc_info_": exc_info,
            "timestamp": timestamp,
            "response_time": response_time,
        }

        try:
            adapter = self.adapter_factory(worker_ctx)
            if exc_info:
                adapter.warning(
                    "[%s] entrypoint result trace", worker_ctx.call_id, extra=extra
                )
            else:
                adapter.info(
                    "[%s] entrypoint result trace", worker_ctx.call_id, extra=extra
                )
        except Exception:
            logger.warning("Failed to log entrypoint trace", exc_info=True)

        if result_stream:
            self.container.spawn_managed_thread(
                lambda: self.log_result_stream(worker_ctx, result_stream)
            )

    def log_request_stream(self, worker_ctx, request_stream):

        stream_start = datetime.utcnow()

        for index, request in enumerate(request_stream, start=1):

            timestamp = datetime.utcnow()
            stream_age = (timestamp - stream_start).total_seconds()

            extra = {
                "stage": constants.Stage.request,
                "worker_ctx": worker_ctx,
                "timestamp": timestamp,
                "stream_age": stream_age,
                "stream_part": index,
                "request": request,
            }
            try:
                adapter = self.adapter_factory(worker_ctx)
                adapter.info(
                    "[%s] entrypoint call trace [stream_part %s]",
                    worker_ctx.call_id,
                    index,
                    extra=extra,
                )
            except Exception:
                logger.warning("Failed to log entrypoint trace", exc_info=True)

    def log_result_stream(self, worker_ctx, result_stream):

        stream_start = datetime.utcnow()
        worker_setup_timestamp = self.worker_timestamps[worker_ctx]

        try:
            for index, result in enumerate(result_stream, start=1):
                timestamp = datetime.utcnow()
                stream_age = (timestamp - stream_start).total_seconds()
                response_time = (timestamp - worker_setup_timestamp).total_seconds()

                extra = {
                    "stage": constants.Stage.response,
                    "worker_ctx": worker_ctx,
                    "result": result,
                    "exc_info_": None,
                    "timestamp": timestamp,
                    "response_time": response_time,
                    "stream_age": stream_age,
                    "stream_part": index,
                }
                try:
                    adapter = self.adapter_factory(worker_ctx)
                    adapter.info(
                        "[%s] entrypoint result trace [stream_part %s]",
                        worker_ctx.call_id,
                        index,
                        extra=extra,
                    )
                except Exception:
                    logger.warning("Failed to log entrypoint trace", exc_info=True)

        except Exception:

            timestamp = datetime.utcnow()
            stream_age = (timestamp - stream_start).total_seconds()
            response_time = (timestamp - worker_setup_timestamp).total_seconds()

            # NB> this is _idential_ to block above; all the cleverness to extract
            # rhe traceback is already in the adapter
            # (stream part not identical, actually. nor log level)
            extra = {
                "stage": constants.Stage.response,
                "worker_ctx": worker_ctx,
                "result": None,
                "exc_info_": sys.exc_info(),
                "timestamp": timestamp,
                "response_time": response_time,
                "stream_age": stream_age,
                "stream_part": index + 1,
            }
            try:
                adapter = self.adapter_factory(worker_ctx)
                adapter.warning(
                    "[%s] entrypoint result trace [stream_part %s]",
                    worker_ctx.call_id,
                    index,
                    extra=extra,
                )
            except Exception:
                logger.warning("Failed to log entrypoint trace", exc_info=True)

    def worker_setup(self, worker_ctx):
        """ Log entrypoint call details
        """
        self.log_request(worker_ctx)

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        """ Log entrypoint result details
        """
        self.log_result(worker_ctx, result, exc_info)
