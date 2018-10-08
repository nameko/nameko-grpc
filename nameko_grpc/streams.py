from eventlet.queue import Queue
import struct
from collections import deque


class ReceiveStream:
    def __init__(self, stream_id, message_type):
        self.stream_id = stream_id
        self.message_type = message_type

        self.message_queue = Queue()
        self.buffer = bytearray()

    def close(self):
        self.message_queue.put(None)

    def write(self, data):

        self.buffer.extend(data)

        if len(self.buffer) < 5:
            return

        compressed_flag = struct.unpack("?", self.buffer[0:1])[0]
        message_length = struct.unpack(">I", self.buffer[1:5])[0]

        # TODO handle compression
        if compressed_flag:
            raise NotImplementedError

        if len(self.buffer) < 5 + message_length:
            return

        limit = 5 + message_length
        message = self.message_type()
        message.ParseFromString(bytes(self.buffer[5:limit]))
        self.buffer = self.buffer[limit:]  # or reset?

        self.message_queue.put(message)

    def messages(self):
        while True:
            message = self.message_queue.get()
            if message is None:
                break
            yield message


# class ByteBuffer:
#     def __init__(self):
#         self.buffer = []

#     def empty(self):
#         return not len(self.buffer)


class SendStream:
    class Closed(Exception):
        pass

    def __init__(self, stream_id):
        self.stream_id = stream_id

        self.queue = Queue()
        self.buffer = []
        self.closed = False

    @property
    def exhausted(self):
        return self.closed and self.queue.empty()

    def close(self):
        self.closed = True
        self.queue.put(None)

    def put(self, message):
        if self.closed:
            raise SendStream.Closed()
        self.queue.put(message)

    def messages(self):
        # TODO: make internal?
        while True:
            message = self.queue.get()
            if message is None:
                break
            yield message

    def read(self, max_bytes):
        """ Read up to `max_bytes` from the stream, blocking until sufficient messages
        are received or the stream is closed.

        If more bytes are received than can be returned within `max_bytes`, they are
        buffered until the next call to `read`.
        """
        sent_bytes = 0

        # XXX this is broken when an individual message is longer than max_bytes
        while self.buffer:
            data = self.buffer.pop(0)

            length = len(data)
            if sent_bytes + length < max_bytes:
                sent_bytes += length
                yield data
            else:
                self.buffer.append(data)
                break

        for message in self.messages():
            body = message.SerializeToString()
            data = struct.pack("?", False) + struct.pack(">I", len(body)) + body

            length = len(data)
            if sent_bytes + length < max_bytes:
                sent_bytes += length
                split = int(len(data) / 2)
                yield data[:split]
                yield data[split:]
                # yield data
            else:
                self.buffer.append(data)
                break
