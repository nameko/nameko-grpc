from eventlet.queue import Queue
import struct


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

        if len(self.buffer) < message_length:
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


class SendStream:
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.message_queue = Queue()

    def close(self):
        self.message_queue.put(None)

    def put(self, message):
        self.message_queue.put(message)

    def messages(self):
        while True:
            message = self.message_queue.get()
            if message is None:
                break
            yield message

    def read(self):
        for message in self.messages():
            body = message.SerializeToString()
            data = struct.pack("?", False) + struct.pack(">I", len(body)) + body
            yield data
