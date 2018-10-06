import pytest
import os
import subprocess
import sys
from importlib import import_module
from nameko.testing.services import entrypoint_hook, dummy

from nameko_grpc.entrypoint import Grpc
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
    protobufs, _ = compile_proto("helloworld")
    return protobufs


@pytest.fixture
def stubs(compile_proto):
    _, stubs = compile_proto("helloworld")
    return stubs


@pytest.fixture
def grpc_server():
    """ Standard GRPC server, running in another process
    """
    server_script = os.path.join(os.path.dirname(__file__), "server.py")
    with subprocess.Popen([sys.executable, server_script]) as proc:
        yield
        proc.terminate()


@pytest.fixture
def grpc_client(stubs, tmpdir):
    """ Standard GRPC client, running in another process
    """
    with temp_fifo(tmpdir.strpath) as fifo_in:
        with temp_fifo(tmpdir.strpath) as fifo_out:

            client_script = os.path.join(os.path.dirname(__file__), "remote_client.py")
            with subprocess.Popen([sys.executable, client_script, fifo_in.path]):

                class Method:
                    def __init__(self, name):
                        self.name = name

                    def __call__(self, request):
                        send(fifo_in, Config(self.name, fifo_out.path))
                        send(fifo_in, request)
                        return receive(fifo_out)

                class Client:
                    def __getattr__(self, name):
                        return Method(name)

                yield Client()
                send(fifo_in, None)


@pytest.fixture
def service(container_factory, protobufs, stubs):

    HelloReply = protobufs.HelloReply

    grpc = Grpc.decorator(stubs.greeterStub)

    class Service:
        name = "greeter"

        @grpc
        def say_hello(self, request, context):
            return HelloReply(message="Hello, %s!" % request.name)

        @grpc
        def say_hello_goodbye(self, request, context):
            yield HelloReply(message="Hello, %s!" % request.name)
            yield HelloReply(message="Goodbye, %s!" % request.name)

        @grpc
        def say_hello_to_many(self, request, context):
            for message in request:
                yield HelloReply(message="Hi " + message.name)

        @grpc
        def say_hello_to_many_at_once(self, request, context):
            names = []
            for message in request:
                names.append(message.name)

            return HelloReply(message="Hi " + ", ".join(names) + "!")

    container = container_factory(Service, {})
    container.start()


class TestInspection:
    @pytest.fixture
    def inspector(self, stubs):
        return Inspector(stubs.greeterStub)

    def test_path_for_method(self, inspector):
        assert inspector.path_for_method("say_hello") == "/greeter/say_hello"
        assert (
            inspector.path_for_method("say_hello_goodbye")
            == "/greeter/say_hello_goodbye"
        )
        assert (
            inspector.path_for_method("say_hello_to_many")
            == "/greeter/say_hello_to_many"
        )
        assert (
            inspector.path_for_method("say_hello_to_many_at_once")
            == "/greeter/say_hello_to_many_at_once"
        )

    def test_input_type_for_method(self, inspector, protobufs):
        assert inspector.input_type_for_method("say_hello") == protobufs.HelloRequest

    def test_output_type_for_method(self, inspector, protobufs):
        assert inspector.output_type_for_method("say_hello") == protobufs.HelloReply

    def test_cardinality_for_method(self, inspector):
        assert inspector.cardinality_for_method("say_hello") == Cardinality.UNARY_UNARY
        assert (
            inspector.cardinality_for_method("say_hello_goodbye")
            == Cardinality.UNARY_STREAM
        )
        assert (
            inspector.cardinality_for_method("say_hello_to_many")
            == Cardinality.STREAM_STREAM
        )
        assert (
            inspector.cardinality_for_method("say_hello_to_many_at_once")
            == Cardinality.STREAM_UNARY
        )


@pytest.mark.usefixtures("service")
class TestDependencyProvider:
    @pytest.fixture
    def caller(self, container_factory, protobufs, stubs):
        class Caller:
            name = "caller"

            greeter_grpc = GrpcProxy(stubs.greeterStub)

            @dummy
            def call_say_hello(self, name):
                return self.greeter_grpc.say_hello(protobufs.HelloRequest(name=name))

            @dummy
            def call_say_hello_goodbye(self, name):
                return self.greeter_grpc.say_hello_goodbye(
                    protobufs.HelloRequest(name=name)
                )

            @dummy
            def call_say_hello_to_many_at_once(self, *names):
                return self.greeter_grpc.say_hello_to_many_at_once(
                    protobufs.HelloRequest(name=name) for name in names
                )

            @dummy
            def call_say_hello_to_many(self, *names):
                return self.greeter_grpc.say_hello_to_many(
                    protobufs.HelloRequest(name=name) for name in names
                )

        container = container_factory(Caller, {})
        container.start()

        yield container

    def test_unary_unary(self, caller):

        with entrypoint_hook(caller, "call_say_hello") as hook:
            response = hook(name="Matt")
        assert response.message == "Hello, Matt!"

    def test_unary_stream(self, caller):

        with entrypoint_hook(caller, "call_say_hello_goodbye") as hook:
            responses = hook("Matt")
        assert [response.message for response in responses] == [
            "Hello, Matt!",
            "Goodbye, Matt!",
        ]

    def test_stream_unary(self, caller):

        with entrypoint_hook(caller, "call_say_hello_to_many_at_once") as hook:
            response = hook("Matt", "Josie")
        assert response.message == "Hi Matt, Josie!"

    def test_stream_stream(self, caller):

        with entrypoint_hook(caller, "call_say_hello_to_many") as hook:
            responses = hook("Matt", "Josie")
        assert [response.message for response in responses] == ["Hi Matt", "Hi Josie"]


class TestStandard:
    @pytest.fixture
    def client(self, grpc_client):
        return grpc_client

    @pytest.fixture(params=["grpc", "nameko"], autouse=True)
    def server(self, request):
        if request.param == "grpc":
            request.getfixturevalue("grpc_server")
        elif request.param == "nameko":
            request.getfixturevalue("service")

    def test_unary_unary(self, client, protobufs):
        response = client.say_hello(protobufs.HelloRequest(name="you"))
        assert response.message == "Hello, you!"

    def test_unary_stream(self, client, protobufs):
        responses = client.say_hello_goodbye(protobufs.HelloRequest(name="you"))
        assert [response.message for response in responses] == [
            "Hello, you!",
            "Goodbye, you!",
        ]

    def test_stream_unary(self, client, protobufs):
        def generate_requests():
            for name in ["Bill", "Bob"]:
                yield protobufs.HelloRequest(name=name)

        response = client.say_hello_to_many_at_once(generate_requests())
        assert response.message == "Hi Bill, Bob!"

    def test_stream_stream(self, client, protobufs):
        def generate_requests():
            for name in ["Bill", "Bob"]:
                yield protobufs.HelloRequest(name=name)

        responses = client.say_hello_to_many(generate_requests())
        assert [response.message for response in responses] == ["Hi Bill", "Hi Bob"]
