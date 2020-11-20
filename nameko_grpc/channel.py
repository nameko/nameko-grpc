# -*- coding: utf-8 -*-
import queue
import socket
from functools import partial
from urllib.parse import urlparse

from nameko_grpc.connection import ClientConnectionManager


# DNS RESOLVER
# look up address, maybe get back multiple ips
# maybe add the same ip multiple times to maintain parallel connections
# round robin between multiple ips

# CONNECTION POOL
# keep connections alive (conns do this themselves?)
# [x] replace dead connections
# refresh resolution?
# [x] round robin between multiple connections


class ConnectionPool:
    def __init__(self, targets, ssl, spawn_thread):
        self.targets = targets
        self.ssl = ssl
        self.spawn_thread = spawn_thread

        self.connections = queue.SimpleQueue()

    def connect(self, target):
        print(">> connecting to", target)
        sock = socket.create_connection((target.hostname, target.port or 50051))

        if self.ssl:
            context = self.ssl.client_context()
            sock = context.wrap_socket(
                sock=sock, server_hostname=target.hostname, suppress_ragged_eofs=True
            )

        connection = ClientConnectionManager(sock)
        self.connections.put(connection)

        cb = partial(self.handle_connection_termination, connection, target)
        self.spawn_thread(target=connection.run_forever, callback=cb)

    def handle_connection_termination(self, connection, target, res, exc_info):
        print(">> connection terminating", res, exc_info, connection, target)
        connection.dead = True
        print("deading conncetion")
        if self.run:
            print(">> reconnecting to", target)
            self.connect(target)

    def get(self):
        while True:
            conn = self.connections.get()
            if not getattr(conn, "dead", False):
                self.connections.put(conn)
                return conn
            else:
                print("junking dead connection")

    def start(self):
        self.run = True
        for target in self.targets:
            self.connect(urlparse(target))

    def stop(self):
        print("connection pool stop")
        self.run = False
        while not self.connections.empty():
            self.connections.get().stop()


class ClientChannel:
    """ Simple client channel using DNS resolution and round robin balancing.

    Channels could eventually suppport pluggable resolvers and load-balancing.
    """

    def __init__(self, target, ssl, spawn_thread):
        self.target = target
        self.ssl = ssl
        self.connection_pool = ConnectionPool([self.target], self.ssl, spawn_thread)

    def start(self):
        self.connection_pool.start()

    def stop(self):
        self.connection_pool.stop()

    def send_request(self, request_headers):
        connection = self.connection_pool.get()
        return connection.send_request(request_headers)
