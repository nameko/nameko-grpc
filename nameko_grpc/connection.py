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
    """

    def __init__(self, sock, client_side):
        self.sock = sock

        config = H2Configuration(client_side=client_side)
        self.conn = H2Connection(config=config)

        self.receive_streams = {}
        self.send_streams = {}

    def run_forever(self):
        self.conn.initiate_connection()
        self.sock.sendall(self.conn.data_to_send())

        while True:

            ready = select.select([self.sock], [], [], SELECT_TIMEOUT)
            if not ready[0]:
                self.on_idle_iteration()
                events = []
            else:
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

    def on_idle_iteration(self):
        for stream_id in list(self.send_streams.keys()):
            self.send_data(stream_id)

    def request_received(self, headers, stream_id):
        log.debug("request received, stream %s", stream_id)

    def response_received(self, headers, stream_id):
        log.debug("response received, stream %s", stream_id)

    def stream_ended(self, stream_id):
        log.debug("stream ended, stream %s", stream_id)
        receive_stream = self.receive_streams.pop(stream_id)
        receive_stream.close()

    def data_received(self, data, stream_id):
        log.debug("data received on stream %s: %s...", stream_id, data[:100])

        receive_stream = self.receive_streams.get(stream_id)
        if receive_stream is None:
            # data for unknown stream, exit?
            self.conn.reset_stream(stream_id, error_code=PROTOCOL_ERROR)
            return

        receive_stream.write(data)

    def settings_changed(self, event):
        log.debug("settings changed")

    def window_updated(self, stream_id):
        log.debug("window updated, stream %s", stream_id)
        self.send_data(stream_id)

    def send_data(self, stream_id):

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

    def end_stream(self, stream_id):
        self.conn.end_stream(stream_id=stream_id)
        self.send_streams.pop(stream_id)
