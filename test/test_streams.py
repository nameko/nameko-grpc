# -*- coding: utf-8 -*-
import struct

import pytest
from mock import Mock, call, patch

from nameko_grpc.errors import GrpcError
from nameko_grpc.streams import (
    STREAM_END,
    ByteBuffer,
    ReceiveStream,
    SendStream,
    StreamBase,
)


class TestByteBuffer:
    def test_peek(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.peek(slice(0, 1)) == b"a"
        assert buffer.peek(slice(3, 6)) == b"def"
        assert buffer.peek(slice(-2, -1)) == b"h"

        assert buffer.read() == b"abcdefghi"

    def test_peek_all(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.peek() == b"abcdefghi"
        assert buffer.read() == b"abcdefghi"

    def test_discard(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.discard(3) is None
        assert buffer.read() == b"defghi"

    def test_discard_all(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.discard() is None
        assert buffer.read() == b""

    def test_read(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.read(3) == b"abc"
        assert buffer.read() == b"defghi"

    def test_read_all(self):
        buffer = ByteBuffer()
        buffer.write(b"abcdefghi")

        assert buffer.read() == b"abcdefghi"
        assert buffer.read() == b""

    def test_write(self):
        buffer = ByteBuffer()

        buffer.write(b"abc")
        assert buffer.peek() == b"abc"
        buffer.write(b"def")
        assert buffer.peek() == b"abcdef"

    def test_empty(self):
        buffer = ByteBuffer()

        assert buffer.empty() is True
        buffer.write(b"abc")
        assert buffer.empty() is False
        buffer.discard()
        assert buffer.empty() is True

    def test_len(self):
        buffer = ByteBuffer()

        assert len(buffer) == 0
        buffer.write(b"abc")
        assert len(buffer) == 3


class TestStreamBase:
    def test_exhausted(self):
        stream = StreamBase(1)
        stream.buffer.write(b"abc")
        assert not stream.exhausted

        stream.close()
        assert stream.closed
        assert not stream.exhausted

        stream.queue.get()
        assert stream.queue.empty()
        assert not stream.exhausted

        stream.buffer.discard()
        assert stream.buffer.empty()
        assert stream.exhausted

    def test_close(self):
        stream = StreamBase(1)
        stream.close()

        assert stream.closed
        assert stream.queue.get() == STREAM_END

    def test_close_with_error(self):
        stream = StreamBase(1)
        error = GrpcError("boom", "details", "error string")
        stream.close(error)

        assert stream.closed
        assert stream.queue.get() == error

    def test_close_with_non_error(self):
        stream = StreamBase(1)
        error = Exception("boom")

        with pytest.raises(AssertionError):
            stream.close(error)


class TestReceiveStream:
    def test_write_to_closed_stream(self):
        stream = ReceiveStream(1)

        assert stream.buffer.empty()
        stream.close()
        stream.write(b"\x00\x00\x00")
        assert stream.buffer.empty()

    def test_write_less_bytes_than_header(self):
        stream = ReceiveStream(1)
        stream.write(b"\x00\x00\x00")

        assert stream.queue.empty()
        assert stream.buffer.peek() == b"\x00\x00\x00"

    def test_write_less_bytes_than_one_message(self):
        stream = ReceiveStream(1)
        stream.write(b"\x00\x00\x00\x01\x00\xff\xff\xff")

        assert stream.queue.empty()
        assert stream.buffer.peek() == b"\x00\x00\x00\x01\x00\xff\xff\xff"

    def test_write_more_bytes_than_one_message(self):
        stream = ReceiveStream(1)
        # incompressed single byte message, followed by two more bytes of /xff
        stream.write(b"\x00\x00\x00\x00\x01\xff\xff\xff")
        # single byte message is queued
        assert stream.queue.get() == (False, b"\xff")
        # following two bytes remain in the buffer
        assert stream.buffer.peek() == b"\xff\xff"

    def test_write_multiple_messages(self):
        stream = ReceiveStream(1)
        for _ in range(10):
            stream.write(b"\x00\x00\x00\x00\x01\xff")  # 10 single byte messages

        assert stream.queue.qsize() == 10
        assert len(stream.buffer) == 0

    def test_consume_grpc_error(self):
        stream = ReceiveStream(1)
        error = GrpcError("boom", "details", "message")
        stream.queue.put(error)

        message_type = Mock()

        with pytest.raises(GrpcError):
            next(stream.consume(message_type))

    def test_consume_end_of_stream(self):
        stream = ReceiveStream(1)
        stream.close()

        message_type = Mock()

        assert list(stream.consume(message_type)) == []

    def test_consume_uncompressed_message(self):
        stream = ReceiveStream(1)

        message_data = b"x"
        message_type = Mock()
        message = message_type()

        stream.queue.put((False, message_data))
        stream.close()  # close stream so that consume exits

        assert list(stream.consume(message_type)) == [message]
        assert message.ParseFromString.call_args_list == [call(message_data)]

    @patch("nameko_grpc.streams.decompress")
    def test_consume_compressed_message(self, decompress):
        stream = ReceiveStream(1)

        message_data = b"x"
        message_type = Mock()
        message = message_type()

        stream.queue.put((True, message_data))
        stream.close()  # close stream so that consume exits

        assert list(stream.consume(message_type)) == [message]
        assert message.ParseFromString.call_args_list == [
            call(decompress(message_data))
        ]

    @patch("nameko_grpc.streams.decompress")
    def test_consume_multiple_messages(self, decompress):
        stream = ReceiveStream(1)

        message_data = b"x"
        message_type = Mock()
        message = message_type()

        stream.queue.put((False, message_data))
        stream.queue.put((True, message_data))
        stream.close()  # close stream so that consume exits

        assert list(stream.consume(message_type)) == [message, message]
        assert message.ParseFromString.call_args_list == [
            call(message_data),
            call(decompress(message_data)),
        ]


class TestSendStream:
    def test_populate(self):
        stream = SendStream(1)
        stream.populate(range(10))

        assert stream.closed
        assert stream.queue.qsize() == 11

    def test_populate_closed_stream(self):
        stream = SendStream(1)
        stream.close()

        assert stream.closed
        stream.populate(range(10))
        assert stream.queue.qsize() == 1


class TestSendStreamHeadersToSend:
    def test_no_headers(self):
        stream = SendStream(1)

        assert len(stream.headers) == 0
        assert stream.headers_to_send(False) is False

    def test_empty_queue(self):
        stream = SendStream(1)
        stream.headers.set(("foo", "bar"))

        assert stream.queue.qsize() == 0
        assert stream.headers_to_send(True) is False
        assert stream.headers_to_send(False) == [(b"foo", b"bar")]

    def test_mark_as_sent(self):
        stream = SendStream(1)
        stream.headers.set(("foo", "bar"))

        assert stream.headers_to_send(False) == [(b"foo", b"bar")]  # marks as sent
        assert stream.headers_to_send(False) is False  # previously sent

    def test_defer_until_data(self):
        stream = SendStream(1)
        stream.headers.set(("foo", "bar"))

        assert stream.headers_to_send(True) is False  # defer until data
        stream.queue.put(Mock())

        assert stream.queue.qsize() == 1
        assert stream.headers_to_send(True) == [(b"foo", b"bar")]


class TestSendStreamTrailersToSend:
    def test_no_trailers(self):
        stream = SendStream(1)

        assert len(stream.trailers) == 0
        assert stream.trailers_to_send() is False

    def test_send_trailers(self):
        stream = SendStream(1)
        stream.trailers.set(("foo", "bar"))

        assert stream.trailers_to_send() == [(b"foo", b"bar")]


@pytest.fixture
def generate_messages():
    with patch("nameko_grpc.streams.compress") as compress:
        compress.side_effect = lambda body, _: (False, body)

        def generate(count, length):
            """ Generate a series of mock messages.

            If `count` is 2 and `length` is 4, when passed to `stream.populate`,
            two messages with the following payload will be added to the stream's
            queue.

                #1. b`\x00\x00\x00\x00`
                #2. b`\x01\x01\x01\x01`
            """
            messages = []
            for index in range(count):
                message = Mock()
                message.SerializeToString.return_value = bytes([index] * length)
                messages.append(message)
            return messages

        yield generate


class TestSendStreamFlushQueueToBuffer:
    def test_empty_queue(self):
        stream = SendStream(1)
        assert stream.queue.qsize() == 0

        stream.flush_queue_to_buffer()
        assert stream.buffer.empty()

    def test_messages_on_queue(self, generate_messages):
        stream = SendStream(1)
        stream.populate(generate_messages(count=2, length=20))

        header = struct.pack("?", False) + struct.pack(">I", 20)

        stream.flush_queue_to_buffer()
        assert stream.buffer.peek() == header + b"\x00" * 20 + header + b"\x01" * 20

    def test_stream_closed(self, generate_messages):
        stream = SendStream(1)
        stream.populate(generate_messages(count=2, length=20))

        header = struct.pack("?", False) + struct.pack(">I", 20)

        stream.flush_queue_to_buffer()
        assert stream.buffer.peek() == header + b"\x00" * 20 + header + b"\x01" * 20

        stream.flush_queue_to_buffer()  # stream closed; no-op
        assert stream.buffer.peek() == header + b"\x00" * 20 + header + b"\x01" * 20

    def test_error_on_queue(self, generate_messages):
        stream = SendStream(1)
        stream.populate(generate_messages(count=2, length=20))

        error = GrpcError("boom", "details", "error string")
        stream.close(error)

        with pytest.raises(GrpcError):
            stream.flush_queue_to_buffer()


class TestSendStreamRead:
    def test_no_data(self):
        stream = SendStream(1)

        max_bytes = 10
        chunk_size = 10

        assert stream.buffer.empty()
        assert list(stream.read(max_bytes, chunk_size)) == []

    def test_less_than_one_chunk_of_data(self):
        stream = SendStream(1)
        stream.buffer.write(b"abc")

        max_bytes = 10
        chunk_size = 5

        assert list(stream.read(max_bytes, chunk_size)) == [b"abc"]
        assert stream.buffer.empty()

    def test_more_than_one_chunk_of_data(self):
        stream = SendStream(1)
        stream.buffer.write(b"abcdefghijklm")

        max_bytes = 10
        chunk_size = 5

        assert list(stream.read(max_bytes, chunk_size)) == [b"abcde", b"fghij"]
        assert stream.buffer.peek() == b"klm"

    def test_less_than_max_bytes_of_data(self):
        stream = SendStream(1)
        stream.buffer.write(b"abcdefghijklm")

        max_bytes = 20
        chunk_size = 5

        assert list(stream.read(max_bytes, chunk_size)) == [b"abcde", b"fghij", b"klm"]
        assert stream.buffer.empty()

    def test_more_than_max_bytes_of_data(self):
        stream = SendStream(1)
        stream.buffer.write(b"abcdefghijklm")

        max_bytes = 10
        chunk_size = 5

        assert list(stream.read(max_bytes, chunk_size)) == [b"abcde", b"fghij"]
        assert stream.buffer.peek() == b"klm"

    def test_chunk_greater_than_max_bytes(self):
        stream = SendStream(1)
        stream.buffer.write(b"abcdefghijklm")

        max_bytes = 5
        chunk_size = 10

        assert list(stream.read(max_bytes, chunk_size)) == [b"abcde"]
        assert stream.buffer.peek() == b"fghijklm"

    def test_stream_closed(self):
        stream = SendStream(1)

        max_bytes = 10
        chunk_size = 5

        stream.close()
        assert list(stream.read(max_bytes, chunk_size)) == []

    def test_stream_closed_with_error(self):
        stream = SendStream(1)

        error = GrpcError("boom", "details", "error string")
        stream.close(error)

        max_bytes = 10
        chunk_size = 5

        with pytest.raises(GrpcError):
            next(stream.read(max_bytes, chunk_size))

    def test_multiple_small_messages(self, generate_messages):
        stream = SendStream(1)
        stream.populate(generate_messages(count=100, length=1))

        header = struct.pack("?", False) + struct.pack(">I", 1)
        max_bytes = 20
        chunk_size = 10

        chunks = list(stream.read(max_bytes, chunk_size))
        expected = [
            # 5 bytes header + 1 byte payload + 4 bytes of next header
            header + b"\x00" + header[:4],
            # remaining 1 byte of header + 1 byte payload
            # + 5 bytes header + 1 byte payload + 2 bytes of next header
            header[4:] + b"\x01" + header + b"\x02" + header[:2],
        ]
        assert chunks == expected

        assert sum(map(len, chunks)) == max_bytes

        # queue is emptied into buffer
        assert len(stream.buffer) == 100 * (5 + 1) - max_bytes  # 580 bytes left
        assert stream.queue.qsize() == 0

    def test_multiple_large_messages(self, generate_messages):
        stream = SendStream(1)
        stream.populate(generate_messages(count=100, length=200))

        header = struct.pack("?", False) + struct.pack(">I", 200)
        max_bytes = 50
        chunk_size = 10

        chunks = list(stream.read(max_bytes, chunk_size))
        expected = [
            header + b"\x00\x00\x00\x00\x00",  # 5 bytes header + 5 bytes payload
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",  # 10 bytes payload
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",  # 10 bytes payload
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",  # 10 bytes payload
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",  # 10 bytes payload
        ]
        assert chunks == expected

        assert sum(map(len, chunks)) == max_bytes

        # queue is emptied into buffer
        assert len(stream.buffer) == 100 * (5 + 200) - max_bytes  # 20450 bytes left
        assert stream.queue.qsize() == 0

    def test_data_in_buffer_and_messages_in_queue(self, generate_messages):
        stream = SendStream(1)
        stream.buffer.write(b"\xff\xff\xff\xff\xff")
        stream.populate(generate_messages(count=10, length=10))

        header = struct.pack("?", False) + struct.pack(">I", 10)
        max_bytes = 10
        chunk_size = 10

        chunks = list(stream.read(max_bytes, chunk_size))
        expected = [b"\xff\xff\xff\xff\xff" + header]
        assert chunks == expected

        assert sum(map(len, chunks)) == max_bytes

        # queue is emptied into buffer
        assert len(stream.buffer) == 5 + 10 * (5 + 10) - max_bytes  # 145 bytes left
        assert stream.queue.qsize() == 0
