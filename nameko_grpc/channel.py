from urllib.parse import urlparse
import socket
from nameko_grpc.connection import ClientConnectionManager


class ClientChannel:
    """ Simple client channel. DNS resolution only and no load-balancing.
    """

    def __init__(self, target, ssl, spawn_thread):
        self.target = target
        self.ssl = ssl
        self.spawn_thread = spawn_thread
        self.connections = []

    def connect(self):
        target = urlparse(self.target)
        sock = socket.create_connection((target.hostname, target.port or 50051))

        if self.ssl:
            context = self.ssl.client_context()
            sock = context.wrap_socket(
                sock=sock, server_hostname=target.hostname, suppress_ragged_eofs=True
            )
        return sock

    def start(self):
        sock = self.connect()
        manager = ClientConnectionManager(sock)
        self.spawn_thread(target=manager.run_forever)

        self.connections.append((manager, sock))

    def stop(self):
        while self.connections:
            manager, sock = self.connections.pop()
            manager.stop()
            sock.close()

    def send_request(self, request_headers):
        manager, _ = self.connections[0]
        return manager.send_request(request_headers)
