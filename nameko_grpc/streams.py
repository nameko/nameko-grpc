from eventlet.queue import Queue, Empty
import struct


class ReceiveStream:
    def __init__(self, stream_id, message_type):
        self.stream_id = stream_id
        self.message_type = message_type

        self.message_queue = Queue()
        self.buffer = bytearray()  # TODO use ByteBuffer

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
        return len(self.bytes) == 0

    def __len__(self):
        return len(self.bytes)


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

    def populate(self, generator):
        for item in generator:
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
            if message is None:  # TODO use sentinel
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
