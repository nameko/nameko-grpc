from eventlet.queue import Queue
import struct

# main changes are to move the buffer manipulation out of here.

# Stream object is:
# somewhere to store incoming data
# some iterator of messages, for the client to consume
# somewhere to store outgoing messages
# some iterator for chunks of data to send?

# perhaps the iterators should be separate.


class Stream:
    def __init__(self, stream_id, request_type):
        self.stream_id = stream_id
        self.request_type = request_type

        self._requests = Queue()
        self._responses = Queue()
        self.buffer = bytearray()

    def close(self):
        self._requests.put(None)

    def put_data(self, data):
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

        message = self.request_type()
        message.ParseFromString(bytes(self.buffer[5:limit]))
        self.buffer = self.buffer[limit:]  # or reset?

        self._requests.put(message)

    def requests(self):
        while True:
            message = self._requests.get()
            if message is None:
                break
            yield message


class ResponseStream:
    # TODO: rename: not a stream
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.queue = Queue()

    def put(self, response):
        self.queue.put(response)

    def responses(self):
        while True:
            message = self.queue.get()
            if message is None:
                break
            yield message
