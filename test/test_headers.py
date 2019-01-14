# -*- coding: utf-8 -*-
import base64

import pytest

from nameko_grpc.headers import (
    HeaderManager,
    check_decoded,
    check_encoded,
    comma_join,
    decode_header,
    encode_header,
    filter_headers_for_application,
    sort_headers_for_wire,
)


class TestEncodeHeader:
    def test_binary(self):
        assert encode_header(("foo-bin", b"123")) == (
            b"foo-bin",
            base64.b64encode(b"123"),
        )

    def test_string_value(self):
        assert encode_header(("foo", "123")) == (b"foo", b"123")


class TestDecodeHeader:
    def test_binary(self):
        assert decode_header((b"foo-bin", base64.b64encode(b"123"))) == (
            "foo-bin",
            b"123",
        )

    def test_string_value(self):
        assert decode_header((b"foo", b"123")) == ("foo", "123")


class TestFilterHeadersForApplication:
    def test_no_application(self):
        headers = [(":status", "1"), ("content-type", "2"), ("grpc-foo", "3")]
        assert filter_headers_for_application(headers) == []

    def test_all_application(self):
        headers = [("foo", "1"), ("bar", "2"), ("baz", "3")]
        assert filter_headers_for_application(headers) == headers

    def test_filter(self):
        headers = [
            ("foo", "1"),
            (":status", "1"),
            ("bar", "2"),
            ("content-type", "2"),
            ("baz", "3"),
        ]
        assert filter_headers_for_application(headers) == [
            ("foo", "1"),
            ("bar", "2"),
            ("baz", "3"),
        ]


class TestSortHeadersForWire:
    def test_empty(self):
        unsorted = []
        for_wire = []
        assert sort_headers_for_wire(unsorted) == for_wire

    def test_already_sorted(self):
        unsorted = [
            (":status", "1"),
            ("content-type", "2"),
            ("grpc-foo", "3"),
            ("other", "4"),
        ]
        for_wire = [
            (":status", "1"),
            ("content-type", "2"),
            ("grpc-foo", "3"),
            ("other", "4"),
        ]
        assert sort_headers_for_wire(unsorted) == for_wire

    def test_sort(self):
        unsorted = [
            ("content-type", "2"),
            (":status", "1"),
            ("other", "4"),
            ("grpc-foo", "3"),
        ]
        for_wire = [
            (":status", "1"),
            ("content-type", "2"),
            ("grpc-foo", "3"),
            ("other", "4"),
        ]
        assert sort_headers_for_wire(unsorted) == for_wire

    def test_multi_sort(self):
        unsorted = [
            ("content-type", "1"),
            ("te", "2"),
            (":status", "3"),
            (":authority", "4"),
            ("other", "5"),
            ("grpc-foo", "6"),
            ("grpc-bar", "7"),
            ("more", "8"),
            (":method", "9"),
        ]
        for_wire = [
            (":status", "3"),
            (":authority", "4"),
            (":method", "9"),
            ("content-type", "1"),
            ("te", "2"),
            ("grpc-foo", "6"),
            ("grpc-bar", "7"),
            ("other", "5"),
            ("more", "8"),
        ]
        assert sort_headers_for_wire(unsorted) == for_wire


class TestCheckEncoded:
    def test_empty(self):
        assert check_encoded([]) is None

    def test_good(self):
        assert check_encoded([(b"foo", b"bar")]) is None

    def test_bad_name(self):
        with pytest.raises(AssertionError):
            check_encoded([("foo", b"bar")])

    def test_bad_value(self):
        with pytest.raises(AssertionError):
            check_encoded([(b"foo", "bar")])


class TestCheckDecoded:
    def test_empty(self):
        assert check_decoded([]) is None

    def test_good(self):
        assert check_decoded([("foo", "bar")]) is None

    def test_bad_name(self):
        with pytest.raises(AssertionError):
            check_decoded([(b"foo", "bar")])

    def test_bad_value(self):
        with pytest.raises(AssertionError):
            check_decoded([("foo", b"bar")])

    def test_good_binary(self):
        assert check_decoded([("foo-bin", b"bar")]) is None

    def test_bad_binary(self):
        with pytest.raises(AssertionError):
            check_decoded([("foo-bin", "bar")])


class TestCommaJoin:
    def test_string(self):
        assert comma_join(["foo", "bar"]) == "foo,bar"

    def test_bytes(self):
        assert comma_join([b"foo", b"bar"]) == b"foo,bar"

    def test_mixed(self):
        with pytest.raises(TypeError):
            comma_join([b"foo", "bar"])


class TestHeaderManager:
    def test_get(self):
        manager = HeaderManager()
        manager.set(("foo", "bar"))

        assert manager.get("foo") == "bar"

    def test_get_with_default(self):
        manager = HeaderManager()

        assert manager.get("foo", "baz") == "baz"

    def test_get_multi(self):
        manager = HeaderManager()
        manager.set(("foo", "bar"), ("foo", "baz"))

        assert manager.get("foo") == "bar,baz"

    def test_set(self):
        manager = HeaderManager()
        manager.set(("x", "y"), ("foo", "bar"))

        assert manager.get("x") == "y"
        assert manager.get("foo") == "bar"
        manager.set(("foo", "baz"))  # clears existing foo, only
        assert manager.get("foo") == "baz"
        assert manager.get("x") == "y"

    def test_set_from_wire(self):
        manager = HeaderManager()

        manager.set((b"foo", b"bar"), from_wire=True)
        assert manager.get("foo") == "bar"

    def test_append(self):
        manager = HeaderManager()
        manager.set(("x", "y"), ("foo", "bar"))

        assert manager.get("x") == "y"
        assert manager.get("foo") == "bar"
        manager.append(("foo", "baz"))  # appends to foo
        assert manager.get("foo") == "bar,baz"
        assert manager.get("x") == "y"

    def test_append_from_wire(self):
        manager = HeaderManager()
        manager.set(("foo", "bar"))

        manager.append((b"foo", b"baz"), from_wire=True)
        assert manager.get("foo") == "bar,baz"

    def test_for_wire(self):
        manager = HeaderManager()
        manager.set(("x", "y"), ("foo", "bar"))

        assert manager.for_wire == [(b"x", b"y"), (b"foo", b"bar")]
