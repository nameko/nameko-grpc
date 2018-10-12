# -*- coding: utf-8 -*-
from eventlet.queue import Queue, Empty
import struct


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


class ReceiveStream:
    def __init__(self, stream_id, message_type):
        self.stream_id = stream_id
        self.message_type = message_type

        self.message_queue = Queue()
        self.buffer = ByteBuffer()

    def close(self):
        self.message_queue.put(STREAM_END)

    def write(self, data):

        self.buffer.write(data)

        if len(self.buffer) < HEADER_LENGTH:
            return

        compressed_flag = struct.unpack("?", self.buffer.peek(slice(0, 1)))[0]
        message_length = struct.unpack(">I", self.buffer.peek(slice(1, 5)))[0]

        # TODO handle compression
        if compressed_flag:
            raise NotImplementedError

        if len(self.buffer) < HEADER_LENGTH + message_length:
            return

        self.buffer.discard(HEADER_LENGTH)

        message = self.message_type()
        message.ParseFromString(bytes(self.buffer.read(message_length)))

        self.message_queue.put(message)

    def __iter__(self):
        return self

    def __next__(self):
        message = self.message_queue.get()
        if message is STREAM_END:
            raise StopIteration()
        return message


class SendStream:
    class Closed(Exception):
        pass

    def __init__(self, stream_id):
        self.stream_id = stream_id

        self.queue = Queue()
        self.buffer = ByteBuffer()
        self.closed = False

    @property
    def exhausted(self):
        return self.closed and self.queue.empty() and self.buffer.empty()

    def close(self):
        self.closed = True
        self.queue.put(STREAM_END)

    def populate(self, iterable):
        for item in iterable:
            self.put(item)
        self.close()

    def put(self, message):
        if self.closed:
            raise SendStream.Closed()
        self.queue.put(message)

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

            body = message.SerializeToString()
            data = struct.pack("?", False) + struct.pack(">I", len(body)) + body
            self.buffer.write(data)

            while sent < max_bytes:
                chunk = self.buffer.read(chunk_size)
                if not chunk:
                    break  # no more data to send
                sent += len(chunk)
                yield chunk
