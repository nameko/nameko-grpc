# -*- coding: utf-8 -*-
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from importlib import import_module

import pytest
from eventlet.green import zmq
from mock import Mock
from nameko.testing.services import dummy
from nameko.testing.utils import find_free_port, get_extension

from nameko_grpc.client import Client
from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.inspection import Inspector

from helpers import Command, RemoteClientTransport, Stash


def pytest_addoption(parser):

    parser.addoption(
        "--client",
        action="store",
        type="choice",
        choices=["nameko", "dp", "grpc", "all"],
        dest="client",
        default="all",
        help="Use this client type",
    )

    parser.addoption(
        "--server",
        action="store",
        type="choice",
        choices=["nameko", "grpc", "all"],
        dest="server",
        default="all",
        help="Use this server type",
    )


@pytest.fixture(scope="session")
def spec_dir(tmpdir_factory):
    master = os.path.join(os.path.dirname(__file__), "spec")
    temp = tmpdir_factory.mktemp("spec")
    for filename in os.listdir(master):
        path = os.path.join(master, filename)
        if os.path.isfile(path):
            copy = temp.join(filename)
            with open(path) as file:
                copy.write(file.read())

    sys.path.append(temp.strpath)
    yield temp
    sys.path.remove(temp.strpath)


@pytest.fixture(scope="session")
def compile_proto(spec_dir):

    spec_path = spec_dir.strpath

    def codegen(proto_name):

        proto_path = os.path.join(spec_path, "{}.proto".format(proto_name))
        proto_last_modified = os.path.getmtime(proto_path)

        for generated_file in (
            "{}_pb2.py".format(proto_name),
            "{}_pb2_grpc.py".format(proto_name),
        ):
            generated_path = os.path.join(spec_path, generated_file)
            if (
                not os.path.exists(generated_path)
                or os.path.getmtime(generated_path) < proto_last_modified
            ):
                protoc_args = [
                    "-I{}".format(spec_path),
                    "--python_out",
                    spec_path,
                    "--grpc_python_out",
                    spec_path,
                    proto_path,
                ]
                # protoc.main is confused by absolute paths, so use subprocess instead
                python_args = ["python", "-m", "grpc_tools.protoc"] + protoc_args
                subprocess.call(python_args)

        protobufs = import_module("{}_pb2".format(proto_name))
        stubs = import_module("{}_pb2_grpc".format(proto_name))

        return protobufs, stubs

    return codegen


@pytest.fixture(scope="session")
def load_protobufs(compile_proto):
    def load(name):
        protobufs, _ = compile_proto(name)
        return protobufs

    return load


@pytest.fixture(scope="session")
def load_stubs(compile_proto):
    def load(name):
        _, stubs = compile_proto(name)
        return stubs

    return load


@pytest.fixture(autouse=True, scope="session")
def example_proto(compile_proto):
    compile_proto("example")


@pytest.fixture
def spawn_process():

    procs = []

    def spawn(*args, env=None):
        popen_args = [sys.executable]
        popen_args.extend(args)
        procs.append(subprocess.Popen(popen_args, env=env))

    yield spawn

    for proc in procs:
        proc.terminate()


@pytest.fixture
def grpc_port():
    return find_free_port()


@pytest.fixture
def start_grpc_server(compile_proto, spawn_process, spec_dir, grpc_port):

    server_script = os.path.join(os.path.dirname(__file__), "grpc_indirect_server.py")

    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
    ):
        if proto_name is None:
            proto_name = service_name

        env = os.environ.copy()
        env["PYTHONPATH"] = spec_dir.strpath

        spawn_process(
            server_script,
            str(grpc_port),
            proto_name,
            service_name,
            compression_algorithm,
            compression_level,
            env=env,
        )

        # wait for server to start
        while True:
            try:
                sock = socket.socket()
                sock.connect(("127.0.0.1", grpc_port))
                sock.close()
                break
            except socket.error:
                time.sleep(0.1)

    yield make


@pytest.fixture
def start_grpc_client(load_stubs, spawn_process, spec_dir, grpc_port):

    client_script = os.path.join(os.path.dirname(__file__), "grpc_indirect_client.py")

    clients = []

    context = zmq.Context()

    class Result:
        _metadata = None

        def __init__(self, command):
            self.command = command

        @property
        def metadata(self):
            if self._metadata is None:
                self._metadata = self.command.get_metadata()
            return self._metadata

        def code(self):
            return self.metadata.get("code")

        def details(self):
            return self.metadata.get("details")

        def initial_metadata(self):
            return self.metadata.get("initial_metadata")

        def trailing_metadata(self):
            return self.metadata.get("trailing_metadata")

        def result(self):
            return self.command.get_response()

    class Method:
        def __init__(self, client, name):
            self.client = client
            self.name = name

        def __call__(self, request, **kwargs):
            return self.future(request, **kwargs).result()

        def future(self, request, **kwargs):
            inspector = Inspector(self.client.stub)

            cardinality = inspector.cardinality_for_method(self.name)

            command = Command(self.name, cardinality, kwargs, self.client.transport)
            command.issue()
            threading.Thread(target=command.send_request, args=(request,)).start()
            return Result(command)

    class Client:
        def __init__(self, stub, transport):
            self.stub = stub
            self.transport = transport

        def __getattr__(self, name):
            return Method(self, name)

        def shutdown(self):
            self.transport.send(Command.END, close=True)

    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
    ):
        if proto_name is None:
            proto_name = service_name

        stubs = load_stubs(proto_name)
        stub_cls = getattr(stubs, "{}Stub".format(service_name))

        transport, zmq_port = RemoteClientTransport.bind_to_free_port(
            context, zmq.REQ, "tcp://127.0.0.1"
        )

        env = os.environ.copy()
        env["PYTHONPATH"] = spec_dir.strpath

        spawn_process(
            client_script,
            str(grpc_port),
            proto_name,
            service_name,
            compression_algorithm,
            compression_level,
            str(zmq_port),
            env=env,
        )

        client = Client(stub_cls, transport)
        clients.append(client)
        return client

    yield make

    # shut down indirect clients
    for client in clients:
        client.shutdown()


@pytest.fixture
def start_nameko_server(spec_dir, container_factory, grpc_port):
    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
        config=None,
    ):
        if proto_name is None:
            proto_name = service_name

        service_module = import_module("{}_nameko".format(proto_name))
        service_cls = getattr(service_module, service_name)

        if config is None:
            config = {}

        config.update(
            {
                "GRPC_BIND_PORT": grpc_port,
                "GRPC_COMPRESSION_ALGORITHM": compression_algorithm,
                "GRPC_COMPRESSION_LEVEL": compression_level,
            }
        )

        container = container_factory(service_cls, config)
        container.start()

        return container

    yield make


@pytest.fixture
def start_nameko_client(load_stubs, spec_dir, grpc_port):

    clients = []

    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
    ):
        if proto_name is None:
            proto_name = service_name

        stubs = load_stubs(proto_name)
        stub_cls = getattr(stubs, "{}Stub".format(service_name))
        client = Client(
            "//127.0.0.1:{}".format(grpc_port),
            stub_cls,
            compression_algorithm,
            compression_level,
        )
        clients.append(client)
        return client.start()

    yield make

    for client in clients:
        client.stop()


@pytest.fixture
def start_dependency_provider(load_stubs, spec_dir, grpc_port, container_factory):
    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
    ):
        if proto_name is None:
            proto_name = service_name

        stubs = load_stubs(proto_name)
        stub_cls = getattr(stubs, "{}Stub".format(service_name))

        class Service:
            name = "caller"

            example_grpc = GrpcProxy(
                "//127.0.0.1:{}".format(grpc_port),
                stub_cls,
                compression_algorithm=compression_algorithm,
                compression_level=compression_level,
            )

            @dummy
            def call(self):
                pass

        container = container_factory(Service, {})
        container.start()

        grpc_proxy = get_extension(container, GrpcProxy)
        return grpc_proxy.get_dependency(Mock(context_data={}))

    yield make


@pytest.fixture(params=["server|grpc", "server|nameko"])
def server_type(request):
    return request.param[7:]


@pytest.fixture
def start_server(request, server_type):
    if server_type == "grpc":
        if request.config.option.server not in ("grpc", "all"):
            pytest.skip("grpc server not requested")
        return request.getfixturevalue("start_grpc_server")
    if server_type == "nameko":
        if request.config.option.server not in ("nameko", "all"):
            pytest.skip("nameko server not requested")
        return request.getfixturevalue("start_nameko_server")


@pytest.fixture(params=["client|grpc", "client|nameko", "client|dp"])
def client_type(request):
    return request.param[7:]


@pytest.fixture
def start_client(request, client_type):
    if client_type == "grpc":
        if request.config.option.client not in ("grpc", "all"):
            pytest.skip("grpc client not requested")
        return request.getfixturevalue("start_grpc_client")
    if client_type == "nameko":
        if request.config.option.client not in ("nameko", "all"):
            pytest.skip("nameko client not requested")
        return request.getfixturevalue("start_nameko_client")
    if client_type == "dp":
        if request.config.option.client not in ("dp", "all"):
            pytest.skip("dp client not requested")
        return request.getfixturevalue("start_dependency_provider")


@pytest.fixture
def instrumented(tmpdir_factory):
    stashes = tmpdir_factory.mktemp("instrument_stashes")
    stash_file = stashes.join(str(uuid.uuid4()))
    return Stash(stash_file.strpath)


@pytest.fixture
def server(start_server):
    return start_server("example")


@pytest.fixture
def client(start_client, server):
    return start_client("example")


@pytest.fixture
def stubs(load_stubs):
    return load_stubs("example")


@pytest.fixture
def protobufs(load_protobufs):
    return load_protobufs("example")
