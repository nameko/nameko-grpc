from eventlet.queue import Queue, Empty
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


class ByteBuffer:
    def __init__(self):
        self.bytes = bytearray()

    def read(self, max_bytes):
        length = min(max_bytes, len(self.bytes))
        data = self.bytes[:length]
        self.bytes = self.bytes[length:]
        return data

    def write(self, data):
        self.bytes.extend(data)

    def empty(self):
        return not len(self.bytes)


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
        self.queue.put(None)

    def put(self, message):
        if self.closed:
            raise SendStream.Closed()
        self.queue.put(message)

    def messages(self):
        # TODO: nobody actually calls this anymore?
        while True:
            message = self.queue.get()
            if message is None:
                break
            yield message

    def get(self, blocking):
        if blocking:
            message = self.queue.get()
        else:
            try:
                message = self.queue.get_nowait()
            except Empty:
                message = None
        return message

    def read(self, max_bytes, blocking=True):
        """ Read up to `max_bytes` from the stream, blocking until sufficient messages
        are received or the stream is closed.

        If more bytes are received than can be returned within `max_bytes`, they are
        buffered until the next call to `read`.
        """
        sent = 0
        while not self.buffer.empty():
            data = self.buffer.read(max_bytes - sent)
            sent += len(data)
            yield data

        while True:
            message = self.get(blocking)
            print(">> SendStream.read", message)
            if message is None:
                break  # end

            body = message.SerializeToString()
            data = struct.pack("?", False) + struct.pack(">I", len(body)) + body
            self.buffer.write(data)

            while not self.buffer.empty():
                data = self.buffer.read(max_bytes - sent)
                sent += len(data)
                yield data
