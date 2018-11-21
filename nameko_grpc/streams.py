# -*- coding: utf-8 -*-
import struct
from queue import Empty, Queue

from nameko_grpc.compression import compress, decompress


HEADER_LENGTH = 5

STREAM_END = object()


class ByteBuffer:
    def __init__(self):
        self.bytes = bytearray()

    def peek(self, slice):
        return self.bytes[slice]

    def discard(self, max_bytes):
        self.read(max_bytes)

    def read(self, max_bytes):
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

        self.queue = Queue()
        self.buffer = ByteBuffer()
        self.closed = False

    @property
    def exhausted(self):
        return self.closed and self.queue.empty() and self.buffer.empty()

    def close(self, exception=None):
        """ Close this stream.

        If closed with an exception, the exception will be raised when reading
        or consuming from this stream.
        """
        self.closed = True
        self.queue.put(exception or STREAM_END)


class ReceiveStream(StreamBase):
    """ A stream that receives data as bytes to be iterated over as GRPC messages.
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
        or raising if the stream was closed with an exception.
        """
        while True:
            item = self.queue.get()
            if isinstance(item, Exception):
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
    """ A stream that receives data as GRPC messages to be read as chunks of bytes.
    """

    def __init__(self, stream_id, encoding):
        super().__init__(stream_id)
        self.encoding = encoding

    def populate(self, iterable):
        """ Populate this stream with an iterable of messages.
        """
        for item in iterable:
            if self.closed:
                return
            self.queue.put(item)
        self.close()

    def read(self, max_bytes, chunk_size):
        """ Read up to `max_bytes` from the stream, yielding up to `chunk_size`
        bytes at a time.
        """
        sent = 0

        while len(self.buffer) >= chunk_size and sent < max_bytes:
            chunk = self.buffer.read(chunk_size)
            sent += len(chunk)
            yield chunk

        while True:
            try:
                message = self.queue.get_nowait()
            except Empty:
                break
            if message is STREAM_END:
                break
            if isinstance(message, Exception):
                raise message

            body = message.SerializeToString()
            compressed, body = compress(body, self.encoding)

            data = struct.pack("?", compressed) + struct.pack(">I", len(body)) + body
            self.buffer.write(data)

            while sent < max_bytes:
                max_read = min(chunk_size, max_bytes - sent)
                chunk = self.buffer.read(max_read)
                if not chunk:
                    break  # no more data to send
                sent += len(chunk)
                yield chunk
