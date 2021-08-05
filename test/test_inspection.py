# -*- coding: utf-8 -*-
import pytest

from nameko_grpc.constants import Cardinality
from nameko_grpc.inspection import Inspector


class TestInspection:
    @pytest.fixture
    def inspector(self, stubs):
        return Inspector(stubs.exampleStub)

    def test_service_name(self, inspector):
        assert inspector.service_name == "nameko.example"

    def test_path_for_method(self, inspector):
        assert inspector.path_for_method("unary_unary") == "/nameko.example/unary_unary"
        assert (
            inspector.path_for_method("unary_stream") == "/nameko.example/unary_stream"
        )
        assert (
            inspector.path_for_method("stream_stream")
            == "/nameko.example/stream_stream"
        )
        assert (
            inspector.path_for_method("stream_unary") == "/nameko.example/stream_unary"
        )

    def test_input_type_for_method(self, inspector, protobufs):
        assert (
            inspector.input_type_for_method("unary_unary") == protobufs.ExampleRequest
        )

    def test_output_type_for_method(self, inspector, protobufs):
        assert inspector.output_type_for_method("unary_unary") == protobufs.ExampleReply

    def test_cardinality_for_method(self, inspector):
        insp = inspector
        assert insp.cardinality_for_method("unary_unary") == Cardinality.UNARY_UNARY
        assert insp.cardinality_for_method("unary_stream") == Cardinality.UNARY_STREAM
        assert insp.cardinality_for_method("stream_unary") == Cardinality.STREAM_UNARY
        assert insp.cardinality_for_method("stream_stream") == Cardinality.STREAM_STREAM

    def test_cache_is_keyed_on_stub(self, load_stubs):
        inspector1 = Inspector(load_stubs("example").exampleStub)
        inspector2 = Inspector(load_stubs("advanced").advancedStub)

        assert inspector1.service_name == "nameko.example"
        assert inspector2.service_name == "nameko.advanced"
