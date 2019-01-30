# nameko-grpc

This is a prototype implementation of a GRPC server and client for use in [nameko](https://nameko.io) microservices.

All four of the request-response patterns are implemented and tested:

1. unary-unary
2. unary-stream
3. stream-unary
4. stream-stream

Asynchronous calls are also supported for every pattern.

Python 3.4+ is supported.

## Installation

```
$ pip install nameko-grpc
```

## Example

### Server

Example Nameko service that can respond to GRPC requests:

``` python
from example_pb2 import ExampleReply
from example_pb2_grpc import exampleStub


grpc = Grpc.implementing(exampleStub)


class ExampleService:
    name = "example"

    @grpc
    def unary_unary(self, request, context):
        message = request.value * (request.multiplier or 1)
        return ExampleReply(message=message)

    @grpc
    def unary_stream(self, request, context):
        message = request.value * (request.multiplier or 1)
        yield ExampleReply(message=message, seqno=1)
        yield ExampleReply(message=message, seqno=2)

    @grpc
    def stream_unary(self, request, context):
        messages = []
        for req in request:
            message = req.value * (req.multiplier or 1)
            messages.append(message)

        return ExampleReply(message=",".join(messages))

    @grpc
    def stream_stream(self, request, context):
        for index, req in enumerate(request):
            message = req.value * (req.multiplier or 1)
            yield ExampleReply(message=message, seqno=index + 1)

```

### Client

Example Nameko service that can make GRPC requests:

``` python
from example_pb2 import ExampleReply
from example_pb2_grpc import exampleStub

from nameko.rpc import rpc


class ClientService:
    name = "client"

    example_grpc = GrpcProxy("//127.0.0.1", exampleStub)

    @rpc
    def method(self):
        responses = self.example_grpc.unary_stream(ExampleRequest(value="A"))
        for response in responses:
            print(response.message)

```

Example standalone client, can be used with or without Eventlet:

``` python
from example_pb2 import ExampleReply
from example_pb2_grpc import exampleStub

from nameko_grpc.client import Client

with Client("//127.0.0.1", exampleStub) as client:
    responses = client.unary_stream(ExampleRequest(value="A"))
    for response in responses:
        print(response.message)

```

### Protobuf

The protobuf for the above examples is:

```
syntax = "proto3";

package nameko;

service example {
  rpc unary_unary (ExampleRequest) returns (ExampleReply) {}
  rpc unary_stream (ExampleRequest) returns (stream ExampleReply) {}
  rpc stream_unary (stream ExampleRequest) returns (ExampleReply) {}
  rpc stream_stream (stream ExampleRequest) returns (stream ExampleReply) {}
}

message ExampleRequest {
  string value = 1;
  int32 multiplier = 2;
}


message ExampleReply {
  string message = 1;
  int32 seqno = 2;
}

```

## Style

The example protobufs in this repo use `snake_case` for method names as per the Nameko conventions rather than `CamelCase` as per GRPC. This is not mandatory -- decorated method names simply match to the methods defined in the protobufs; similarly for service names.

## More

* Context
* using the client; metadata etc.
* Compression
* Errors
* Timeouts

## Tests

Most tests are run against every permutation of GRPC server/client to Nameko server/client. This roughly demonstrates equivalence between the two implementations. These tests are marked with the "equivalence" pytest marker.

Additionally, we run the interop tests from the offical GRPC repo, which are used to verify compatibility between language implementations. The Nameko GRPC implementation supports every feature that the official Python GRPC implementation does. These tests are marked with the "interop" pytest marker.

The `test/spec` directory contains the protobufs and server implementations used in the various tests.

### Running the tests

Clone or download the reposistory, and ensure the development dependencies are installed:

```
$ pip install nameko-grpc[dev]
```

Then run the tests:

```
$ pytest test
```

The interop tests require docker. They use the image at https://hub.docker.com/r/nameko/nameko-grpc-interop which contains the pre-built C++ interop client. To run all tests excluding the interop tests:

```
$ pytest test -m "not interop"
```


## Implementation Notes

GRPC requests and responses are implemented as HTTP2 requests and responses, so nameko-grpc relies heavily on the [hyper-h2](https://python-hyper.org/projects/h2/en/stable/) library. H2 is a finite state-machine implementation of the HTTP2 protocol, and its documentation is very good. The code in nameko-grpc is much more understandable when you're familiar with h2.

Much of the heavy-lifting in nameko-grpc is done by either the server or client subclasses of `ConnectionManager`. A `ConnectionManager` handles a single HTTP2 connection, and implements the handlers for each HTTP2 event on that connection (e.g. `request_received` or `stream_ended`). See:

* nameko_grpc/client.py::ClientConnectionManager
* nameko_grpc/entrypoint.py::ServerConnectionManager
* nameko_grpc/connection.py::ConnectionManager

The next most significant module is `nameko_grpc/streams.py`. This module contains the `SendStream` and `ReceiveStream` classes, which represent an HTTP2 stream that is being sent or received, respectively. `ReceiveStream`s receive data as bytes from a `ConnectionManager`, and parses them into a stream of GRPC messages. `SendStream`s do the opposite, encoding GRPC messages into bytes that can be sent across an HTTP2 connection.

The GRPC Entrypoint is a normal Nameko entrypoint that executes a service method when an appropriate request is made. The entrypoint deals with a `ReceiveStream` object encapulating the request, and a `SendStream` object that accepts the response. The streams are managed by a shared `GrpcServer`, which accepts incoming connections and wraps each in a `ServerConnectionManager`.

The standalone Client is a small wrapper around a `ClientConnectionManager`. The Client simply creates a socket connection and then hands it to the connection manager. When a method is invoked on the client, the connection manager initates an appropriate request. The headers for that request describe the method being invoked, encodings, message types etc. This logic is all encapulated into the `Method` class.

The GRPC DependencyProvider is a normal Nameko DependencyProvider, which is also just a small wrapper around a `ClientConnectionManager`. It functions in exactly the same manner as the standalone Client.

The Entrypoint and both clients implement timeout logic and will raise a `Deadline Exceeded` error if the call has not been completed before the deadline is reached.


## Equivalence tests notes

To demonstrate equivalence between the nameko-grpc implementations and the standard GRPC implementations, all tests marked with the `equivalence` marker run against every permuation of:

* GRPC standard server (Python implementation) or
* Nameko server

and

* GRPC standard client (Python implementation) or
* Nameko standalone client or
* Nameko DepdendencyProvider client

Nameko uses Eventlet for concurrency, which is incompatible with the standard GRPC server and client. Consequently, these must be run in a separate process and somehow communicated with in order to make assertions about the behaviour of the standard implementation.

The scripts which run the out-of-process client and server can be found in `test/grpc_indirect_client.py` and `test/grpc_indirect_server.py`

The communication is done with ZeroMQ. The logic for this is contained within the  `RemoteClientTransport` and `Command` classes within `test/helpers.py`, and the `start_grpc_client` and `start_grpc_server` fixtures in `test/conftest.py`.

In the future this arrangement would allow us to run equivalence tests against a different (more feature-complete) standard GRPC implementation.
