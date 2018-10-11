from nameko.extensions import DependencyProvider

import socket
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
    ResponseReceived,
    DataReceived,
    StreamEnded,
    RemoteSettingsChanged,
    WindowUpdated,
    SettingsAcknowledged,
    TrailersReceived,
)
from eventlet.event import Event
import eventlet

from collections import deque
from h2.errors import PROTOCOL_ERROR  # changed under h2 from 2.6.4?
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.constants import Cardinality
import itertools


class ClientConnectionManager(object):
    """
    An object that manages a single HTTP/2 connection on a GRPC client.
    """

    def __init__(self, sock, stub):
        self.sock = sock
        self.stub = stub

        config = H2Configuration(client_side=True)
        self.conn = H2Connection(config=config)

        self.receive_streams = {}
        self.send_streams = {}

        self.pending_requests = deque()

        self.counter = itertools.count(start=1, step=2)

    def run_forever(self):
        self.conn.initiate_connection()
        self.sock.sendall(self.conn.data_to_send())

        while True:

            self.sock.settimeout(0.01)
            try:
                # XXX does receiving ANY data prevent the timeout?
                data = self.sock.recv(65535)
                if not data:
                    break
                events = self.conn.receive_data(data)
            except socket.timeout:
                events = []
                self.bump()

            for event in events:
                if isinstance(event, ResponseReceived):
                    self.response_received(event.headers, event.stream_id)
                elif isinstance(event, DataReceived):
                    self.data_received(event.data, event.stream_id)
                elif isinstance(event, RemoteSettingsChanged):
                    self.settings_changed(event)
                elif isinstance(event, StreamEnded):
                    self.stream_ended(event.stream_id)
                elif isinstance(event, WindowUpdated):
                    self.window_updated(event.stream_id)
                elif isinstance(event, SettingsAcknowledged):
                    pass
                elif isinstance(event, TrailersReceived):
                    pass

            self.sock.sendall(self.conn.data_to_send())

    def bump(self):
        # XXX rename, formalise
        self.send_pending_requests()
        for stream_id in list(self.send_streams.keys()):
            self.send_data(stream_id)

    def invoke_method(self, method_name):

        stream_id = next(self.counter)
        print(">> queue_request", method_name, stream_id)

        inspector = Inspector(self.stub)
        output_type = inspector.output_type_for_method(method_name)

        self.pending_requests.append((stream_id, method_name))

        receive_stream = ReceiveStream(stream_id, output_type)
        self.receive_streams[stream_id] = receive_stream

        send_stream = SendStream(stream_id)
        self.send_streams[stream_id] = send_stream

        return send_stream, receive_stream

    def response_received(self, headers, stream_id):
        print(">> response recvd", stream_id)

        # inspector = Inspector(self.stub)
        # output_type = inspector.output_type_for_method(self.method_name)

        # stream = ReceiveStream(stream_id, output_type)
        # self.receive_streams[stream_id] = stream

    def stream_ended(self, stream_id):
        print(">> response stream ended", stream_id)
        receive_stream = self.receive_streams.pop(stream_id)
        receive_stream.close()

    def data_received(self, data, stream_id):
        print(">> response data recvd", data, stream_id)

        receive_stream = self.receive_streams.get(stream_id)
        if receive_stream is None:
            # data for unknown stream, exit?
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        receive_stream.write(data)

    def settings_changed(self, event):
        print(">> settings changed")
        self.send_pending_requests()

    def window_updated(self, stream_id):
        print(">> window updated")
        self.send_pending_requests()
        self.send_data(stream_id)

    def send_pending_requests(self):

        print(">> send pending requests", self.pending_requests)

        while self.pending_requests:
            stream_id, method_name = self.pending_requests.popleft()

            print(">> sending request", stream_id)

            request_headers = [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "127.0.0.1"),
                (":path", "/example/{}".format(method_name)),
                ("te", "trailers"),
                ("content-type", "application/grpc"),
                ("user-agent", "nameko-grc-proxy"),
                ("grpc-accept-encoding", "identity,deflate,gzip"),
                ("accept-encoding", "identity,gzip"),
            ]

            self.conn.send_headers(stream_id, request_headers)
            self.send_data(stream_id)

    def send_data(self, stream_id):

        send_stream = self.send_streams.get(stream_id)

        if not send_stream:
            # send_data may be called after everything is already sent?
            return

        window_size = self.conn.local_flow_control_window(stream_id=stream_id)
        max_frame_size = self.conn.max_outbound_frame_size

        for chunk in send_stream.read(window_size, max_frame_size):
            self.conn.send_data(stream_id=stream_id, data=chunk)

        if send_stream.exhausted:
            self.conn.end_stream(stream_id=stream_id)
            self.send_streams.pop(stream_id)

        self.sock.sendall(self.conn.data_to_send())


class GrpcProxy(DependencyProvider):
    def __init__(self, stub, **kwargs):
        self.stub = stub
        self.inspector = Inspector(self.stub)
        super().__init__(**kwargs)

    def setup(self):

        sock = socket.socket()
        sock.connect(("127.0.0.1", 50051))

        self.manager = ClientConnectionManager(sock, self.stub)
        self.container.spawn_managed_thread(self.manager.run_forever)

    def invoke(self, method_name, request):

        send_stream, response_stream = self.manager.invoke_method(method_name)
        self.container.spawn_managed_thread(
            lambda: send_stream.populate(request), identifier="populate_request"
        )
        return response_stream.messages()

    def get_dependency(self, worker_ctx):
        class Result:
            def __init__(self, response, cardinality):
                self.response = response
                self.cardinality = cardinality

            def result(self):
                response = self.response
                if self.cardinality in (
                    Cardinality.STREAM_UNARY,
                    Cardinality.UNARY_UNARY,
                ):
                    response = next(response)
                return response

        class Method:
            def __init__(self, invoke, stub, name):
                self.invoke = invoke
                self.stub = stub
                self.name = name

            def __call__(self, request):
                return self.future(request).result()

            def future(self, request):
                inspector = Inspector(self.stub)
                cardinality = inspector.cardinality_for_method(self.name)

                if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
                    request = (request,)
                resp = self.invoke(self.name, request)

                return Result(resp, cardinality)

        class Proxy:
            def __init__(self, invoke, stub):
                self.invoke = invoke
                self.stub = stub

            def __getattr__(self, name):
                return Method(self.invoke, self.stub, name)

        return Proxy(self.invoke, self.stub)
