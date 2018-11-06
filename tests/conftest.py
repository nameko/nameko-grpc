# -*- coding: utf-8 -*-
import os
import subprocess
import sys
import threading
import time
import uuid
from importlib import import_module

import pytest
from nameko.testing.utils import find_free_port

from nameko_grpc.client import Client

from helpers import (
    AbortableIterator,
    Config,
    FifoPipe,
    RaisingReceiver,
    RequestResponseStash,
    receive,
    send,
)


def pytest_addoption(parser):

    parser.addoption(
        "--client",
        action="store",
        type="choice",
        choices=["nameko", "grpc", "all"],
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


@pytest.fixture
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


@pytest.fixture
def make_fifo(tmpdir):

    fifos = []

    def make():
        fifo = FifoPipe.new(tmpdir.strpath)
        fifos.append(fifo)
        fifo.open()
        return fifo

    yield make

    for fifo in fifos:
        fifo.close()


@pytest.fixture
def spawn_process():

    procs = []

    def spawn(*args):
        popen_args = [sys.executable]
        popen_args.extend(args)
        procs.append(subprocess.Popen(popen_args))

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
        compile_proto(proto_name)

        spawn_process(
            server_script,
            str(grpc_port),
            spec_dir.strpath,
            proto_name,
            service_name,
            compression_algorithm,
            compression_level,
        )
        # wait until server has started
        time.sleep(0.5)

    yield make


@pytest.fixture
def start_grpc_client(
    compile_proto, tmpdir, make_fifo, spawn_process, spec_dir, grpc_port
):

    client_script = os.path.join(os.path.dirname(__file__), "grpc_indirect_client.py")

    client_fifos = []
    aborts = []

    def make_abortable(value):
        abort, iterable = AbortableIterator.wrap(value)
        aborts.append(abort)
        return iterable

    class Result:
        def __init__(self, fifo):
            self.fifo = fifo

        def result(self):
            res = receive(self.fifo)
            return RaisingReceiver.wrap(res)

    class Method:
        def __init__(self, fifo, name):
            self.fifo = fifo
            self.name = name

        def __call__(self, request, **kwargs):
            return self.future(request, **kwargs).result()

        def future(self, request, **kwargs):
            in_fifo = make_fifo()
            out_fifo = make_fifo()
            send(self.fifo, Config(self.name, in_fifo.path, out_fifo.path, kwargs))
            request = make_abortable(request)
            threading.Thread(target=send, args=(in_fifo, request)).start()
            return Result(out_fifo)

    class Client:
        def __init__(self, fifo):
            self.fifo = fifo

        def __getattr__(self, name):
            return Method(self.fifo, name)

    def make(
        service_name,
        proto_name=None,
        compression_algorithm="none",
        compression_level="high",
    ):
        if proto_name is None:
            proto_name = service_name
        compile_proto(proto_name)

        client_fifo = make_fifo()
        client_fifos.append(client_fifo)

        spawn_process(
            client_script,
            str(grpc_port),
            spec_dir.strpath,
            proto_name,
            service_name,
            compression_algorithm,
            compression_level,
            client_fifo.path,
        )

        return Client(client_fifo)

    yield make

    # abort any streams still sending
    for abort in aborts:
        abort()

    # shut down indirect clients
    for fifo in client_fifos:
        send(fifo, None)


@pytest.fixture
def start_nameko_server(compile_proto, spec_dir, container_factory, grpc_port):
    def make(service_name, proto_name=None):
        if proto_name is None:
            proto_name = service_name
        compile_proto(proto_name)
        service_module = import_module("{}_nameko".format(proto_name))
        service_cls = getattr(service_module, service_name)

        container = container_factory(service_cls, {"GRPC_BIND_PORT": grpc_port})
        container.start()

        return container

    yield make


@pytest.fixture
def start_nameko_client(compile_proto, spec_dir, grpc_port):

    clients = []

    def make(service_name, proto_name=None):
        if proto_name is None:
            proto_name = service_name
        _, stubs = compile_proto(proto_name)

        stub_cls = getattr(stubs, "{}Stub".format(service_name))
        client = Client("//127.0.0.1:{}".format(grpc_port), stub_cls)
        clients.append(client)
        return client.start()

    yield make

    for client in clients:
        client.stop()


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


@pytest.fixture(params=["client|grpc", "client|nameko"])
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


@pytest.fixture
def server(start_server):
    return start_server("example")


@pytest.fixture
def client(start_client, server):
    return start_client("example")


@pytest.fixture
def stubs(compile_proto):
    _, stubs = compile_proto("example")
    return stubs


@pytest.fixture
def protobufs(compile_proto):
    protobufs, _ = compile_proto("example")
    return protobufs


@pytest.fixture
def instrumented(tmpdir_factory):
    stashes = tmpdir_factory.mktemp("instrument_stashes")
    stash_file = stashes.join(str(uuid.uuid4()))
    return RequestResponseStash(stash_file.strpath)
