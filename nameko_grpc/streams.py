# -*- coding: utf-8 -*-
import struct
from queue import Empty, Queue

from nameko_grpc.compression import compress, decompress
from nameko_grpc.errors import GrpcError
from nameko_grpc.headers import HeaderManager


HEADER_LENGTH = 5

STREAM_END = object()


class ByteBuffer:
    def __init__(self):
        self.bytes = bytearray()

    def peek(self, view=None):
        if view is None:
            view = slice(0, len(self.bytes))
        return self.bytes[view]

    def discard(self, max_bytes=None):
        self.read(max_bytes)

    def read(self, max_bytes=None):
        if max_bytes is None:
            max_bytes = len(self.bytes)
        length = min(max_bytes, len(self.bytes))
        data = self.bytes[:length]
        self.bytes = self.bytes[length:]
        return data

    def write(self, data):
        self.bytes.extend(data)

    def empty(self):
        return len(self.bytes) == 0

    def __len__(self):
        return len(self.bytes)


class StreamBase:
    def __init__(self, stream_id):
        self.stream_id = stream_id

        self.headers = HeaderManager()
        self.trailers = HeaderManager()

        self.queue = Queue()
        self.buffer = ByteBuffer()
        self.closed = False

    @property
    def exhausted(self):
        """ A stream is exhausted if it is closed and there are no more messages to be
        consumed or bytes to be read.
        """
        return self.closed and self.queue.empty() and self.buffer.empty()

    def close(self, error=None):
        """ Close this stream, preventing further messages or data to be added.

        If closed with an error, the error will be raised when reading
        or consuming from this stream.
        """
        if error:
            assert isinstance(error, GrpcError)

        self.closed = True
        self.queue.put(error or STREAM_END)


class ReceiveStream(StreamBase):
    """ An HTTP2 stream that receives data as bytes to be iterated over as GRPC
    messages.
    """

    def write(self, data):
        """ Write data to this stream, separating it into message-sized chunks.
        """
        if self.closed:
            return

        self.buffer.write(data)
        while True:

            if len(self.buffer) < HEADER_LENGTH:
                break

            compressed_flag = struct.unpack("?", self.buffer.peek(slice(0, 1)))[0]
            message_length = struct.unpack(">I", self.buffer.peek(slice(1, 5)))[0]

            if len(self.buffer) < HEADER_LENGTH + message_length:
                break

            self.buffer.discard(HEADER_LENGTH)
            message_data = bytes(self.buffer.read(message_length))
            self.queue.put((compressed_flag, message_data))

    def consume(self, message_type):
        """ Consume the data in this stream by yielding `message_type` messages,
        or raising if the stream was closed with an error.
        """
        while True:
            item = self.queue.get()
            if isinstance(item, GrpcError):
                raise item
            elif item is STREAM_END:
                break

            compressed, message_data = item
            if compressed:
                message_data = decompress(message_data)

            message = message_type()
            message.ParseFromString(message_data)

            yield message


class SendStream(StreamBase):
    """ An HTTP2 stream that receives data as GRPC messages to be read as chunks of
    bytes.
    """

    def __init__(self, *args, **kwargs):
        self.headers_sent = False
        super().__init__(*args, **kwargs)

    @property
    def encoding(self):
        return self.headers.get("grpc-encoding")

    def populate(self, iterable):
        """ Populate this stream with an iterable of messages.
        """
        for item in iterable:
            if self.closed:
                return
            self.queue.put(item)
        self.close()

    def headers_to_send(self, defer_until_data=True):
        """ Return any headers to be sent with this stream.

        Headers may only be transmitted before any data is sent.
        This state is maintained by only returning headers from this method once.

        When `defer_until_data` is true, no headers are returned until there is at least
        one message ready to be sent. This allows for header values to be changed until
        the last possible moment (enabling the server to change encoding, for example).
        """
        if self.headers_sent or len(self.headers) == 0:
            return False

        if defer_until_data and self.queue.empty():
            return False

        self.headers_sent = True
        return self.headers.for_wire

    def trailers_to_send(self):
        """ Return any trailers to be sent after this stream.
        """
        if len(self.trailers) == 0:
            return False

        return self.trailers.for_wire

    def flush_queue_to_buffer(self):
        """ Write the bytes from any messages in the queue to the buffer.
        """
        while True:
            try:
                message = self.queue.get_nowait()
            except Empty:
                break

            # any error should be raised immediately
            if isinstance(message, GrpcError):
                raise message

            # add the bytes from the message to the buffer
            if message and message != STREAM_END:
                body = message.SerializeToString()
                compressed, body = compress(body, self.encoding)

                data = (
                    struct.pack("?", compressed) + struct.pack(">I", len(body)) + body
                )
                self.buffer.write(data)

    def read(self, max_bytes, chunk_size):
        """ Read up to `max_bytes` from the stream, yielding up to `chunk_size`
        bytes at a time.
        """
        sent = 0

        while sent < max_bytes:

            # ensure any messages enqueued during reading are flushed
            self.flush_queue_to_buffer()

            # chunk data out of buffer
            max_read = min(chunk_size, max_bytes - sent)
            chunk = self.buffer.read(max_read)
            if not chunk:
                break  # buffer is empty
            sent += len(chunk)
            yield chunk
