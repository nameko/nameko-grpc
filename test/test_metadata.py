# -*- coding: utf-8 -*-
import base64
import json


class TestRequestMetadata:
    def test_custom_string_metadata(self, client, protobufs):
        foo = "bar"
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A"), metadata=[("foo", foo)]
        )
        assert response.message == "A"
        assert json.loads(response.metadata)["foo"] == foo

    def test_custom_string_metadata_duplicates(self, client, protobufs):
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A"),
            metadata=[("foo", "bar1"), ("foo", "bar2")],
        )
        assert response.message == "A"
        # order is preserved
        assert json.loads(response.metadata)["foo"] == "bar1,bar2"

    def test_custom_binany_metadata(self, client, protobufs):
        foo = b"\x0a\x0b\x0a\x0b\x0a\x0b"
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A"), metadata=[("foo-bin", foo)]
        )
        assert response.message == "A"
        assert json.loads(response.metadata)["foo-bin"] == base64.b64encode(foo).decode(
            "utf-8"
        )

    def test_custom_binany_metadata_duplicates(self, client, protobufs):
        foo1 = b"\x0a\x0b\x0a\x0b\x0a\x0b"
        foo2 = b"\x0b\x0a\x0b\x0a\x0b\x0a"
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A"),
            metadata=[("foo-bin", foo1), ("foo-bin", foo2)],
        )
        assert response.message == "A"

        # order is preserved
        expected = ",".join(
            [
                base64.b64encode(foo1).decode("utf-8"),
                base64.b64encode(foo2).decode("utf-8"),
            ]
        )
        assert json.loads(response.metadata)["foo-bin"] == expected


class TestResponseMetadata:
    def test_custom_string_metadata(self, client, protobufs):
        header = "header"
        trailer = "trailer"
        response_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A"),
            metadata=[("echo-header-foo", header), ("echo-trailer-foo", trailer)],
        )
        assert response_future.result().message == "A"
        assert dict(response_future.initial_metadata())["foo"] == header
        assert dict(response_future.trailing_metadata())["foo"] == trailer

    def test_custom_string_metadata_duplicates(self, client, protobufs):
        header1 = "header1"
        header2 = "header2"
        trailer1 = "trailer1"
        trailer2 = "trailer2"
        response_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A"),
            metadata=[
                ("echo-header-foo", header1),
                ("echo-header-foo", header2),
                ("echo-trailer-foo", trailer1),
                ("echo-trailer-foo", trailer2),
            ],
        )
        assert response_future.result().message == "A"
        assert response_future.initial_metadata() == [
            ("foo", header1),
            ("foo", header2),
        ]
        assert response_future.trailing_metadata() == [
            ("foo", trailer1),
            ("foo", trailer2),
        ]

    def test_custom_binary_metadata(self, client, protobufs):
        header = b"\x0a\x0b\x0a\x0b\x0a\x0b"
        trailer = b"\x0b\x0a\x0b\x0a\x0b\x0a"
        response_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A"),
            metadata=[
                ("echo-header-foo-bin", header),
                ("echo-trailer-foo-bin", trailer),
            ],
        )
        assert response_future.result().message == "A"
        assert dict(response_future.initial_metadata())["foo-bin"] == header
        assert dict(response_future.trailing_metadata())["foo-bin"] == trailer

    def test_custom_binary_metadata_duplicates(self, client, protobufs):
        header1 = b"\x0a\x0b\x0a\x0b\x0a\x0b"
        header2 = b"\x0b\x0a\x0b\x0a\x0b\x0a"
        trailer1 = b"\x0c\x0d\x0c\x0d\x0c\x0d"
        trailer2 = b"\x0d\x0c\x0d\x0c\x0d\x0c"
        response_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A"),
            metadata=[
                ("echo-header-foo-bin", header1),
                ("echo-header-foo-bin", header2),
                ("echo-trailer-foo-bin", trailer1),
                ("echo-trailer-foo-bin", trailer2),
            ],
        )
        assert response_future.result().message == "A"
        assert response_future.initial_metadata() == [
            ("foo-bin", header1),
            ("foo-bin", header2),
        ]
        assert response_future.trailing_metadata() == [
            ("foo-bin", trailer1),
            ("foo-bin", trailer2),
        ]

    # XXX what about streaming responses? nameko may be able to support it
