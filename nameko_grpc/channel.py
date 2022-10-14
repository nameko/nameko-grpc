# -*- coding: utf-8 -*-
import queue
import socket
import weakref
from logging import getLogger
from urllib.parse import urlparse

import eventlet

from nameko_grpc.connection import ClientConnectionManager, ServerConnectionManager


log = getLogger(__name__)

CONNECT_TIMEOUT = 5


class ClientConnectionPool:
    """Simple connection pool for clients.

    Accepts a list of targets and will maintain a connection to each of them,
    round-robining requests between them.

    Currently expects each target to be a valid argument to `urllib.parse.urlparse`.
    If the ClientChannel becomes more complex to support pluggable resolvers and
    load-balancing, `targets` will need more structure. Something like:

        target:
           ip_address: ...
           port: ...
           hostname: ... (for ssl verification)
           service config: ... (maybe)

    """

    def __init__(self, targets, ssl, spawn_thread):
        self.targets = targets
        self.ssl = ssl
        self.spawn_thread = spawn_thread

        self.connections = queue.Queue()

    def connect(self, target):
        sock = socket.create_connection(
            (target.hostname, target.port or 50051), timeout=CONNECT_TIMEOUT
        )

        if self.ssl:
            context = self.ssl.client_context()
            sock = context.wrap_socket(
                sock=sock, server_hostname=target.hostname, suppress_ragged_eofs=True
            )

        sock.settimeout(60)  # XXX needed and/or correct value?
        connection = ClientConnectionManager(sock)
        self.connections.put(weakref.ref(connection))

        def run_with_reconnect():
            connection.run_forever()
            if self.run:
                self.connect(target)

        self.spawn_thread(
            target=run_with_reconnect, name=f"grpc client connection [{target}]"
        )

    def get(self):
        while True:
            connection_weakref = self.connections.get()
            conn = connection_weakref()
            if conn and conn.alive:
                self.connections.put(connection_weakref)
                return conn

    def start(self):
        self.run = True
        for target in self.targets:
            self.connect(urlparse(target))

    def stop(self):
        self.run = False
        while not self.connections.empty():
            connection_weakref = self.connections.get()
            conn = connection_weakref()
            if conn:
                conn.stop()


class ClientChannel:
    """Simple client channel.

    Channels could eventually suppport pluggable resolvers and load-balancing.
    """

    def __init__(self, target, ssl, spawn_thread):
        self.conn_pool = ClientConnectionPool([target], ssl, spawn_thread)

    def start(self):
        self.conn_pool.start()

    def stop(self):
        self.conn_pool.stop()

    def send_request(self, request_headers):
        return self.conn_pool.get().send_request(request_headers)


class ServerConnectionPool:
    """Simple connection pool for servers.

    Just accepts new connections and allows them to run until close.
    """

    def __init__(self, host, port, ssl, spawn_thread, handle_request):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.spawn_thread = spawn_thread
        self.handle_request = handle_request

        self.connections = queue.Queue()

    def listen(self):
        sock = eventlet.listen((self.host, self.port))
        sock.settimeout(None)

        if self.ssl:
            context = self.ssl.server_context()
            sock = context.wrap_socket(
                sock=sock,
                server_side=True,
                suppress_ragged_eofs=True,
            )

        return sock

    def run(self):
        while self.is_accepting:
            sock, _ = self.listening_socket.accept()
            sock.settimeout(60)  # XXX needed and/or correct value?

            connection = ServerConnectionManager(sock, self.handle_request)
            self.connections.put(weakref.ref(connection))
            self.spawn_thread(
                target=connection.run_forever, name=f"grpc server connection [{sock}]"
            )

    def start(self):
        self.listening_socket = self.listen()
        self.is_accepting = True
        self.spawn_thread(
            target=self.run, name=f"grpc server accept [{self.listening_socket}]"
        )

    def stop(self):
        self.is_accepting = False
        while not self.connections.empty():
            connection_weakref = self.connections.get()
            conn = connection_weakref()
            if conn is not None:
                conn.stop()
        self.listening_socket.close()


class ServerChannel:
    """Simple server channel encapsulating incoming connection management."""

    def __init__(self, host, port, ssl, spawn_thread, handle_request):
        self.conn_pool = ServerConnectionPool(
            host, port, ssl, spawn_thread, handle_request
        )

    def start(self):
        self.conn_pool.start()

    def stop(self):
        self.conn_pool.stop()
