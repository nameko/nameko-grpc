# -*- coding: utf-8 -*-
import importlib
import inspect
from functools import lru_cache

from google.protobuf import descriptor
from mock import Mock

from nameko_grpc.constants import Cardinality


@lru_cache()
class Inspector:
    _stub_module = None
    _protobufs_module = None
    _service_descriptor = None
    _method_descriptors = None
    _cardinality_map = None

    def __init__(self, stub):
        self.stub = stub

    @property
    def stub_module(self):
        if self._stub_module is None:
            self._stub_module = importlib.import_module(self.stub.__module__)
        return self._stub_module

    @property
    def protobufs_module(self):
        if self._protobufs_module is None:
            self._protobufs_module = importlib.import_module(self.stub.__module__[:-5])
        return self._protobufs_module

    @property
    def service_descriptor(self):
        if self._service_descriptor is None:
            self._service_descriptor = inspect.getmembers(
                self.protobufs_module,
                lambda member: isinstance(member, descriptor.ServiceDescriptor)
                and member.name == self.stub.__name__[:-4],
            )[0][1]
        return self._service_descriptor

    @property
    def method_descriptors(self):
        if self._method_descriptors is None:
            self._method_descriptors = {
                name: descriptor
                for name, descriptor in self.service_descriptor.methods_by_name.items()
            }
        return self._method_descriptors

    @property
    def cardinality_map(self):
        if self._cardinality_map is None:
            cmap = {}

            mock_channel = Mock()
            self.stub(mock_channel)

            for (method_path,), _ in mock_channel.unary_unary.call_args_list:
                cmap[method_path.split("/")[-1]] = Cardinality.UNARY_UNARY

            for (method_path,), _ in mock_channel.unary_stream.call_args_list:
                cmap[method_path.split("/")[-1]] = Cardinality.UNARY_STREAM

            for (method_path,), _ in mock_channel.stream_unary.call_args_list:
                cmap[method_path.split("/")[-1]] = Cardinality.STREAM_UNARY

            for (method_path,), _ in mock_channel.stream_stream.call_args_list:
                cmap[method_path.split("/")[-1]] = Cardinality.STREAM_STREAM

            self._cardinality_map = cmap

        return self._cardinality_map

    @property
    def service_name(self):
        return self.service_descriptor.full_name

    def get_symbol(self, name):
        return self.protobufs_module._sym_db.GetSymbol(name)

    def path_for_method(self, method_name):
        return "/{}/{}".format(self.service_name, method_name)

    def input_type_for_method(self, method_name):
        return self.get_symbol(
            self.method_descriptors[method_name].input_type.full_name
        )

    def output_type_for_method(self, method_name):
        return self.get_symbol(
            self.method_descriptors[method_name].output_type.full_name
        )

    def cardinality_for_method(self, method_name):
        return self.cardinality_map[method_name]
