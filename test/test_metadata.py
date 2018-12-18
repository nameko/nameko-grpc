# -*- coding: utf-8 -*-
import base64
import json


class TestMetadata:
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
