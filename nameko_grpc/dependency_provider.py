from nameko.extensions import DependencyProvider

from functools import partial
import socket
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
    ResponseReceived,
    DataReceived,
    StreamEnded,
    RemoteSettingsChanged,
    WindowUpdated,
)
from eventlet.event import Event

from h2.errors import PROTOCOL_ERROR  # changed under h2 from 2.6.4?
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream, SendStream
from nameko_grpc.constants import Cardinality
import itertools


class ClientConnectionManager(object):
    """
    An object that manages a single HTTP/2 connection on a GRPC client.
    """

    def __init__(self, sock, method_name, request, stub):
        self.sock = sock
        self.method_name = method_name
        self.request = request
        self.stub = stub

        config = H2Configuration(client_side=True)
        self.conn = H2Connection(config=config)
        self.streams = {}
        self.send_streams = {}
        self.request_made = False
        self.response_ready = Event()
        self.counter = itertools.count(start=1)

    def run_forever(self):
        self.conn.initiate_connection()
        self.sock.sendall(self.conn.data_to_send())

        while True:
            data = self.sock.recv(65535)
            if not data:
                break

            # XXX what happens when we 'idle'? could we be deadlocked waiting to make
            # a new request?

            events = self.conn.receive_data(data)
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
                else:
                    print(">> ", event)

            self.sock.sendall(self.conn.data_to_send())

    def response_received(self, headers, stream_id):
        print(">> response recvd", stream_id)

        inspector = Inspector(self.stub)
        output_type = inspector.output_type_for_method(self.method_name)

        stream = ReceiveStream(stream_id, output_type)
        self.streams[stream_id] = stream

    def stream_ended(self, stream_id):
        print(">> response stream ended", stream_id)
        stream = self.streams.pop(stream_id)
        stream.close()

        # XXX actually, some responses may be ready way before this
        self.response_ready.send(stream.messages())

    def data_received(self, data, stream_id):
        print(">> response data recvd", data, stream_id)

        stream = self.streams.get(stream_id)
        if stream is None:
            # data for unknown stream, exit?
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        stream.write(data)

    def settings_changed(self, event):
        # XXX is this the correct hook for sending the request?
        # (probably, but we should be getting requests to send from outside)
        if not self.request_made:
            self.send_request()

    def window_updated(self, stream_id):
        pass
        # if there are any pending requests, send them
        # if there are any sending streams, continue sending them
        self.send_data(stream_id)

    def send_request(self):
        stream_id = next(self.counter)
        print(">> sending request", stream_id)

        # TODO should be sharing a single connection

        request_headers = [
            (":method", "GET"),
            (":scheme", "http"),
            (":authority", "127.0.0.1"),
            (":path", "/example/{}".format(self.method_name)),
            ("te", "trailers"),
            ("content-type", "application/grpc"),
            ("user-agent", "nameko-grc-proxy"),
            ("grpc-accept-encoding", "identity,deflate,gzip"),
            ("accept-encoding", "identity,gzip"),
        ]

        self.conn.send_headers(stream_id, request_headers)
        self.request_made = True  # request made _for stream_
        # (we can use streams dict to track this)

        self.send_streams[stream_id] = SendStream(stream_id)
        self.send_data(stream_id)

    def send_data(self, stream_id):

        send_stream = self.send_streams.get(stream_id)

        if not send_stream:
            # send_data may be called after everything is already sent?
            return

        # XXX what about when self.request blocks? don't want to wait here
        # until the input stream has finished.
        # can we not put requests _directly_ into the stream?
        for request in self.request:
            send_stream.put(request)
        send_stream.close()

        final_send = True  # XXX faking it until we have a better hook

        window_size = self.conn.local_flow_control_window(stream_id=stream_id)
        max_frame_size = self.conn.max_outbound_frame_size

        for chunk in send_stream.read(window_size, max_frame_size, blocking=final_send):
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

    def invoke(self, method_name, request):

        # TODO do we need a new socket every time?
        # how to get multiple concurrent requests sharing the same underlying connection?
        sock = socket.socket()
        sock.connect(("127.0.0.1", 50051))

        cardinality = self.inspector.cardinality_for_method(method_name)

        # TODO case where method doesn't exist

        if cardinality in (Cardinality.UNARY_UNARY, Cardinality.UNARY_STREAM):
            request = (request,)

        manager = ClientConnectionManager(sock, method_name, request, self.stub)
        self.container.spawn_managed_thread(manager.run_forever)

        response = manager.response_ready.wait()

        if cardinality in (Cardinality.STREAM_UNARY, Cardinality.UNARY_UNARY):
            response = next(response)

        return response

    def get_dependency(self, worker_ctx):
        class Proxy:
            def __init__(self, invoke):
                self.invoke = invoke

            def __getattr__(self, name):
                return partial(self.invoke, name)

        return Proxy(self.invoke)
