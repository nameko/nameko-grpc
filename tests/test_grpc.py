import pytest
import os
import subprocess
import sys
import time
from functools import partial
from importlib import import_module
from nameko.testing.services import entrypoint_hook, dummy

from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.inspection import Inspector
from nameko_grpc.constants import Cardinality

from helpers import receive, send, temp_fifo, Config

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
    with temp_fifo(tmpdir.strpath) as fifo_in:
        with temp_fifo(tmpdir.strpath) as fifo_out:

            client_script = os.path.join(
                os.path.dirname(__file__), "grpc_indirect_client.py"
            )
            with subprocess.Popen([sys.executable, client_script, fifo_in.path]):

                class Client:
                    def call(self, name, request):
                        send(fifo_in, Config(name, fifo_out.path))
                        send(fifo_in, request)
                        return receive(fifo_out)

                    def __getattr__(self, name):
                        return partial(self.call, name)

                yield Client()
                send(fifo_in, None)


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
        def call(self, method_name, request):
            return getattr(self.example_grpc, method_name)(request)

    container = container_factory(Service, {})
    container.start()

    class Client:
        def call(self, name, request):
            with entrypoint_hook(container, "call") as hook:
                return hook(name, request)

        def __getattr__(self, name):
            return partial(self.call, name)

    yield Client()


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
        # pytest.skip("pass")
        request.getfixturevalue("grpc_server")
    elif "nameko" in request.param:
        pytest.skip("pass")
        request.getfixturevalue("service")


@pytest.fixture(params=["grpc_client", "nameko_client"])
def client(request, server):
    if "grpc" in request.param:
        pytest.skip("pass")
        return request.getfixturevalue("grpc_client")
    elif "nameko" in request.param:
        # pytest.skip("pass")
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
    def test_unary_unary(self, client, server):
        response_future = client.unary_unary.future(
            protobufs.ExampleRequest(value="A", blob="B" * 20000)
        )
        response = response_future.get()
        assert response.message == "A"


# class TestConcurrency:
#     def test_concurrent_unary_requests(self):
#         pass

#     def test_concurrent_stream_requests(self):
#         pass
