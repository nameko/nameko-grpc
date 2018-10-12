# -*- coding: utf-8 -*-
import os
import subprocess
import sys
import time
from importlib import import_module

import pytest
from mock import Mock
from nameko.testing.services import dummy
from nameko.testing.utils import get_extension

from nameko_grpc.constants import Cardinality
from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.inspection import Inspector

from helpers import Config, FifoPipe, receive, send


last_modified = os.path.getmtime


@pytest.fixture
def compile_proto():
    def codegen(service_name):
        spec_dir = os.path.join(os.path.dirname(__file__), "spec")
        proto_path = os.path.join(spec_dir, "{}.proto".format(service_name))
        proto_last_modified = last_modified(proto_path)

        for generated_file in (
            "{}_pb2.py".format(service_name),
            "{}_pb2_grpc.py".format(service_name),
        ):
            generated_path = os.path.join(spec_dir, generated_file)
            if (
                not os.path.exists(generated_path)
                or last_modified(generated_path) < proto_last_modified
            ):
                protoc_args = [
                    "-I{}".format(spec_dir),
                    "--python_out",
                    spec_dir,
                    "--grpc_python_out",
                    spec_dir,
                    proto_path,
                ]
                # protoc.main is confused by absolute paths, so use subprocess instead
                python_args = ["python", "-m", "grpc_tools.protoc"] + protoc_args
                subprocess.call(python_args)

        if spec_dir not in sys.path:
            sys.path.append(spec_dir)

        protobufs = import_module("{}_pb2".format(service_name))
        stubs = import_module("{}_pb2_grpc".format(service_name))

        return protobufs, stubs

    return codegen


@pytest.fixture
def protobufs(compile_proto):
    protobufs, _ = compile_proto("example")
    return protobufs


@pytest.fixture
def stubs(compile_proto):
    _, stubs = compile_proto("example")
    return stubs


@pytest.fixture
def grpc_server():
    """ Standard GRPC server, running in another process
    """
    server_script = os.path.join(os.path.dirname(__file__), "grpc_server.py")
    with subprocess.Popen([sys.executable, server_script]) as proc:
        # wait until server has started
        time.sleep(0.5)
        yield
        proc.terminate()


@pytest.fixture
def grpc_client(stubs, tmpdir):
    """ Standard GRPC client, running in another process
    """
    # TODO allow multiple clients in the same test
    with FifoPipe.new(tmpdir.strpath) as command_fifo:

        client_script = os.path.join(
            os.path.dirname(__file__), "grpc_indirect_client.py"
        )
        with subprocess.Popen([sys.executable, client_script, command_fifo.path]):

            fifos = []

            def new_fifo():
                fifo = FifoPipe.new(tmpdir.strpath)
                fifos.append(fifo)
                fifo.open()
                return fifo

            class Result:
                def __init__(self, fifo):
                    self.fifo = fifo

                def result(self):
                    return receive(self.fifo)

            class Method:
                def __init__(self, name):
                    self.name = name

                def __call__(self, request):
                    return self.future(request).result()

                def future(self, request):
                    in_fifo = new_fifo()
                    out_fifo = new_fifo()
                    send(command_fifo, Config(self.name, in_fifo.path, out_fifo.path))
                    send(in_fifo, request)
                    return Result(out_fifo)

            class Client:
                def __getattr__(self, name):
                    return Method(name)

            yield Client()
            send(command_fifo, None)

            for fifo in fifos:
                fifo.close()


@pytest.fixture
def service(container_factory, protobufs, stubs):

    from nameko_service import ExampleService

    container = container_factory(ExampleService, {})
    container.start()

    return container


@pytest.fixture
def dependency_provider_client(container_factory, stubs):
    class Service:
        name = "caller"

        example_grpc = GrpcProxy(stubs.exampleStub)

        @dummy
        def call(self):
            pass

    container = container_factory(Service, {})
    container.start()

    grpc_proxy = get_extension(container, GrpcProxy)
    return grpc_proxy.get_dependency(Mock())


class TestInspection:
    @pytest.fixture
    def inspector(self, stubs):
        return Inspector(stubs.exampleStub)

    def test_path_for_method(self, inspector):
        assert inspector.path_for_method("unary_unary") == "/example/unary_unary"
        assert inspector.path_for_method("unary_stream") == "/example/unary_stream"
        assert inspector.path_for_method("stream_stream") == "/example/stream_stream"
        assert inspector.path_for_method("stream_unary") == "/example/stream_unary"

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


@pytest.fixture(params=["grpc_server", "nameko_server"])
def server(request):
    if "grpc" in request.param:
        if request.config.option.server not in ("grpc", "all"):
            pytest.skip("grpc server not requested")
        request.getfixturevalue("grpc_server")
    elif "nameko" in request.param:
        if request.config.option.server not in ("nameko", "all"):
            pytest.skip("nameko server not requested")
        request.getfixturevalue("service")


@pytest.fixture(params=["grpc_client", "nameko_client"])
def client(request, server):
    if "grpc" in request.param:
        if request.config.option.client not in ("grpc", "all"):
            pytest.skip("grpc client not requested")
        return request.getfixturevalue("grpc_client")
    elif "nameko" in request.param:
        if request.config.option.client not in ("nameko", "all"):
            pytest.skip("nameko client not requested")
        return request.getfixturevalue("dependency_provider_client")


class TestStandard:
    def test_unary_unary(self, client, protobufs):
        response = client.unary_unary(protobufs.ExampleRequest(value="A"))
        assert response.message == "A"

    def test_unary_stream(self, client, protobufs):
        responses = client.unary_stream(protobufs.ExampleRequest(value="A"))
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response = client.stream_unary(generate_requests())
        assert response.message == "A,B"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses = client.stream_stream(generate_requests())
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]


class TestLarge:
    def test_large_request(self, client, protobufs):
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", blob="B" * 20000)
        )
        assert response.message == "A"

    def test_large_response(self, client, protobufs):
        multiplier = 20000
        response = client.unary_unary(
            protobufs.ExampleRequest(value="A", multiplier=multiplier)
        )
        assert response.message == "A" * multiplier


class TestFuture:
    def test_unary_unary(self, client, protobufs):
        response_future = client.unary_unary.future(protobufs.ExampleRequest(value="A"))
        response = response_future.result()
        assert response.message == "A"

    def test_unary_stream(self, client, protobufs):
        responses_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="A")
        )
        responses = responses_future.result()
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("A", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        response_future = client.stream_unary.future(generate_requests())
        response = response_future.result()
        assert response.message == "A,B"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for value in ["A", "B"]:
                yield protobufs.ExampleRequest(value=value)

        responses_future = client.stream_stream.future(generate_requests())
        responses = responses_future.result()
        assert [(response.message, response.seqno) for response in responses] == [
            ("A", 1),
            ("B", 2),
        ]


class TestConcurrency:
    # XXX how to assert both are in flight at the same time?

    def test_unary_unary(self, client, protobufs):
        response_a_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A")
        )
        response_b_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="B")
        )
        response_a = response_a_future.result()
        response_b = response_b_future.result()
        assert response_a.message == "A"
        assert response_b.message == "B"

    def test_unary_stream(self, client, protobufs):
        responses_a_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="A")
        )
        responses_b_future = client.unary_stream.future(
            protobufs.ExampleRequest(value="B")
        )
        responses_a = responses_a_future.result()
        responses_b = responses_b_future.result()
        # TODO add random delays and generator consumer that grabs whatever comes first
        # then verify streams are interleaved
        assert [(response.message, response.seqno) for response in responses_a] == [
            ("A", 1),
            ("A", 2),
        ]
        assert [(response.message, response.seqno) for response in responses_b] == [
            ("B", 1),
            ("B", 2),
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        # XXX any way to verify that the input streams were interleaved?
        response_1_future = client.stream_unary.future(generate_requests("AB"))
        response_2_future = client.stream_unary.future(generate_requests("XY"))
        response_1 = response_1_future.result()
        response_2 = response_2_future.result()
        assert response_1.message == "A,B"
        assert response_2.message == "X,Y"

    def test_stream_stream(self, client, protobufs):
        def generate_requests(values):
            for value in values:
                yield protobufs.ExampleRequest(value=value)

        # TODO add random delays and generator consumer that grabs whatever comes first
        # then verify streams are interleaved
        # XXX any way to verify that the input streams were interleaved? perhaos track
        # the order the generators are pulled?
        responses_1_future = client.stream_stream.future(generate_requests("AB"))
        responses_2_future = client.stream_stream.future(generate_requests("XY"))
        responses_1 = responses_1_future.result()
        responses_2 = responses_2_future.result()
        assert [(response.message, response.seqno) for response in responses_1] == [
            ("A", 1),
            ("B", 2),
        ]
        assert [(response.message, response.seqno) for response in responses_2] == [
            ("X", 1),
            ("Y", 2),
        ]


# class TestMultipleClients:
#     pass

# class TestTimeouts:
#     pass
