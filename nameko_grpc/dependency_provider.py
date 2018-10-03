from nameko.extensions import DependencyProvider

import struct
import socket
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ResponseReceived, DataReceived, StreamEnded, SettingsAcknowledged
from eventlet.event import Event

from h2.errors import PROTOCOL_ERROR
from nameko_grpc.inspection import Inspector
from nameko_grpc.streams import ReceiveStream
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

            events = self.conn.receive_data(data)

            for event in events:
                if isinstance(event, ResponseReceived):
                    self.response_received(event.headers, event.stream_id)
                elif isinstance(event, DataReceived):
                    self.data_received(event.data, event.stream_id)
                elif isinstance(event, SettingsAcknowledged):
                    self.settings_ackd(event)
                elif isinstance(event, StreamEnded):
                    self.stream_ended(event.stream_id)

            self.sock.sendall(self.conn.data_to_send())

    def response_received(self, headers, stream_id):
        print(">> response recvd", stream_id)

        inspector = Inspector(self.stub)
        output_type = inspector.output_type_for_method(self.method_name)

        # NEED
        # 1. somewhere to store incoming data for this response
        # 2. a way to chunk yhat data into actually messsages (could do this inline?)
        # 3. a way to get messages to caller; could block on actual messages arriving

        stream = ReceiveStream(stream_id, output_type)
        self.streams[stream_id] = stream

    def stream_ended(self, stream_id):
        print(">> response stream ended", stream_id)
        stream = self.streams.pop(stream_id)
        stream.close()
        self.response_ready.send(stream.messages())

    def data_received(self, data, stream_id):
        print(">> response data recvd", data, stream_id)

        stream = self.streams.get(stream_id)
        if stream is None:
            # data for unknown stream, exit?
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        stream.write(data)

    def settings_ackd(self, event):
        if not self.request_made:
            self.send_request()

    def send_request(self):
        stream_id = next(self.counter)
        print(">> sending request", stream_id)

        # TODO should be sharing a single connection

        request_headers = [
            (":method", "GET"),
            (":scheme", "http"),
            (":authority", "127.0.0.1"),
            (":path", "/greeter/{}".format(self.method_name)),
            ("te", "trailers"),
            ("content-type", "application/grpc"),
            ("user-agent", "nameko-grc-proxy"),
            ("grpc-accept-encoding", "identity,deflate,gzip"),
            ("accept-encoding", "identity,gzip"),
        ]

        self.conn.send_headers(stream_id, request_headers)
        self.request_made = True

        # TODO flow control

        for request in self.request:

            request_bin = request.SerializeToString()
            header = struct.pack("?", False) + struct.pack(">I", len(request_bin))
            body = header + request_bin

            self.conn.send_data(stream_id=stream_id, data=body)

        self.conn.end_stream(stream_id=stream_id)

        self.sock.sendall(self.conn.data_to_send())


class GrpcProxy(DependencyProvider):
    def __init__(self, stub, **kwargs):
        self.stub = stub
        super().__init__(**kwargs)

    def invoke(self, method_name, request):

        sock = socket.socket()
        sock.connect(("127.0.0.1", 50051))

        manager = ClientConnectionManager(sock, method_name, request, self.stub)
        self.container.spawn_managed_thread(manager.run_forever)

        return manager.response_ready.wait()

    def get_dependency(self, worker_ctx):
        class Foo:
            def __init__(self, invoke):
                self.invoke = invoke

            def say_hello(self, request):
                responses = self.invoke("say_hello", (request,))
                return next(responses)

            def say_hello_goodbye(self, request):
                responses = self.invoke("say_hello_goodbye", (request,))
                for response in responses:
                    yield response

            def say_hello_to_many(self, request):
                responses = self.invoke("say_hello_to_many", request)
                for response in responses:
                    yield response

            def say_hello_to_many_at_once(self, request):
                responses = self.invoke("say_hello_to_many_at_once", request)
                return next(responses)

        return Foo(self.invoke)
