# -*- coding: utf-8 -*-
import select
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import (
    ResponseReceived,
    RequestReceived,
    DataReceived,
    StreamEnded,
    RemoteSettingsChanged,
    WindowUpdated,
    SettingsAcknowledged,
    TrailersReceived,
)
from logging import getLogger

log = getLogger(__name__)


SELECT_TIMEOUT = 0.01


class ConnectionManager(object):
    """
    Base class for managing a single GRPC HTTP/2 connection.

    Provides the H2 event loop that manages the connection over a socket. Methods for
    handling each HTTP2 event are fully or partially implemented and can be extended
    by subclasses.
    """

    def __init__(self, sock, client_side):
        self.sock = sock

        config = H2Configuration(client_side=client_side)
        self.conn = H2Connection(config=config)

        self.receive_streams = {}
        self.send_streams = {}

    def run_forever(self):
        """ Event loop.
        """
        self.conn.initiate_connection()

        while True:

            self.on_iteration()
            self.sock.sendall(self.conn.data_to_send())

            ready = select.select([self.sock], [], [], SELECT_TIMEOUT)
            if not ready[0]:
                continue

            data = self.sock.recv(65535)
            if not data:
                break
            events = self.conn.receive_data(data)

            for event in events:
                if isinstance(event, RequestReceived):
                    self.request_received(event.headers, event.stream_id)
                if isinstance(event, ResponseReceived):
                    self.response_received(event.headers, event.stream_id)
                elif isinstance(event, DataReceived):
                    self.data_received(event.data, event.stream_id)
                elif isinstance(event, StreamEnded):
                    self.stream_ended(event.stream_id)
                elif isinstance(event, WindowUpdated):
                    self.window_updated(event.stream_id)
                elif isinstance(event, RemoteSettingsChanged):
                    self.settings_changed(event)
                elif isinstance(event, SettingsAcknowledged):
                    self.settings_acknowledged(event)
                elif isinstance(event, TrailersReceived):
                    self.trailers_received(event.stream_id)

    def on_iteration(self):
        """ Called on every iteration of the event loop.

        If there are any open `SendStream`s with data to send, try to send it.
        """
        for stream_id in list(self.send_streams.keys()):
            self.send_data(stream_id)

    def request_received(self, headers, stream_id):
        """ Called when a request is received on a stream.

        Subclasses should extend this method to handle the request accordingly.
        """
        log.debug("request received, stream %s", stream_id)

    def response_received(self, headers, stream_id):
        """ Called when a response is received on a stream.

        Subclasses should extend this method to handle the response accordingly.
        """
        log.debug("response received, stream %s", stream_id)

    def data_received(self, data, stream_id):
        """ Called when data is received on a stream.

        If there is any open `ReceiveStream`, write the data to it.
        """
        log.debug("data received on stream %s: %s...", stream_id, data[:100])

        receive_stream = self.receive_streams.get(stream_id)
        if receive_stream is None:
            # data for unknown stream, exit?
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        receive_stream.write(data)

    def window_updated(self, stream_id):
        """ Called when the flow control window for a stream is changed.

        Any data waiting to be sent on the stream may fit in the window now.
        """
        log.debug("window updated, stream %s", stream_id)
        self.send_data(stream_id)

    def stream_ended(self, stream_id):
        """ Called when an incoming stream ends.

        Close any `ReceiveStream` that was opened for this stream.
        """
        log.debug("stream ended, stream %s", stream_id)
        receive_stream = self.receive_streams.pop(stream_id)
        if receive_stream:
            receive_stream.close()

    def settings_changed(self, event):
        log.debug("settings changed")

    def settings_acknowledged(self, event):
        log.debug("settings acknowledged")

    def trailers_received(self, stream_id):
        log.debug("trailers received, stream %s", stream_id)

    def send_data(self, stream_id):
        """ Attempt to send any pending data on a stream.

        Up to the current flow-control window size bytes may be sent. If the
        `SendStream` is exhausted (no more data to send), the stream is closed.
        """
        send_stream = self.send_streams.get(stream_id)

        if not send_stream:
            # window updates trigger sending of data, but can happen after a stream
            # has been completely sent
            return

        window_size = self.conn.local_flow_control_window(stream_id=stream_id)
        max_frame_size = self.conn.max_outbound_frame_size

        for chunk in send_stream.read(window_size, max_frame_size):
            log.debug("sending data on stream %s: %s...", stream_id, chunk[:100])
            self.conn.send_data(stream_id=stream_id, data=chunk)

        if send_stream.exhausted:
            log.debug("closing exhausted stream, stream %s", stream_id)
            self.end_stream(stream_id)
            self.send_streams.pop(stream_id)

    def end_stream(self, stream_id):
        """ End an outbound stream.
        """
        self.conn.end_stream(stream_id=stream_id)
