import os
import selectors
import socket

from collections import defaultdict

from .logging import get_logger
from .registry import Registry

DEFAULT_BLOCK_SIZE = 16384


class ServerCollector:
    def __init__(self, sock_path, ready):
        self.logger = get_logger(__name__, type(self))
        self.sock_path = sock_path

        self.ready = ready
        self.running = False
        self.write_fd = None
        self.registry = None

    def send(self, message):
        offset = os.write(self.write_fd, message)
        while offset < len(message):
            offset += os.write(self.write_fd, message[offset:])

    def query(self, timeout=None):
        return self.registry.serialize()

    def start(self):
        self.running = True
        if os.path.exists(self.sock_path):
            self.logger.debug("Removing socket file.")
            os.unlink(self.sock_path)

        self.registry = Registry()
        self.buffers = defaultdict(bytes)
        server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_sock.bind(self.sock_path)
        server_sock.listen(1)
        server_sock.setblocking(False)
        self.logger.debug("Listening on %r.", self.sock_path)

        read_fd, write_fd = _, self.write_fd = os.pipe()
        selector = selectors.DefaultSelector()
        selector.register(server_sock, selectors.EVENT_READ, self.on_accept)
        selector.register(read_fd, selectors.EVENT_READ, self.on_read_from_pipe)

        self.ready.set()
        while self.running:
            events = selector.select(timeout=1)
            for key, mask in events:
                key.data(selector, key.fileobj)

        self.logger.debug("Closing pipes...")
        selector.unregister(read_fd)
        os.close(write_fd)
        os.close(read_fd)

        self.logger.debug("Closing server socket...")
        selector.unregister(server_sock)
        server_sock.close()

        self.logger.debug("Closing selector...")
        selector.close()
        self.logger.debug("Collector server stopped.")

    def stop(self, block=True):
        self.logger.debug("Stopping collector server...")
        self.running = False

    def on_accept(self, selector, server_sock):
        client_sock, address = server_sock.accept()
        client_sock.setblocking(False)
        self.logger.debug("Accepted connection with fd %r.", client_sock.fileno())
        selector.register(client_sock, selectors.EVENT_READ, self.on_read_from_sock)

    def on_read_from_sock(self, selector, sock):
        fd = sock.fileno()
        data = sock.recv(DEFAULT_BLOCK_SIZE)
        if not data:
            self.logger.debug("Closing connection with fd %r.", fd)
            selector.unregister(sock)
            sock.close()
            return

        self.buffers[fd] += data
        self.on_read_from_buffer(sock, fd)

    def on_read_from_pipe(self, selector, fd, *_):
        data = os.read(fd, DEFAULT_BLOCK_SIZE)
        if not data:
            self.logger.debug("Closing read pipe with fd %r.", fd)
            selector.unregister(fd)
            os.close(fd)
            return

        self.buffers[fd] += data
        self.on_read_from_buffer(None, fd)

    def on_read_from_buffer(self, sock, fd):
        while True:
            buff = self.buffers[fd]
            if not buff:
                return

            elif buff.startswith(b"?"):
                data = self.registry.serialize()
                self.buffers[fd] = buff[1:]
                sock.sendall(b"$" + str(len(data)).encode("ascii") + b"\0" + data.encode("utf-8") + b"\0")
                continue

            elif not buff.startswith(b"$"):
                self.logger.warning("Malformed message from client with fd %r: %r", fd, buff)
                del self.buffers[fd]
                return

            try:
                marker = buff.index(b"\0")
                message_len = int(buff[1:marker])
            except ValueError:
                self.logger.warning("Malformed message from client with fd %r: %r", fd, buff)
                del self.buffers[fd]
                return
            except IndexError:
                self.logger.debug("Waiting for more data from fd %r...", fd)
                return

            buff = buff[marker + 1:]
            if len(buff) < message_len:
                return

            message = buff[:message_len]
            self.buffers[fd] = buff[message_len:]
            self.on_handle_message(sock, message)

    def on_handle_message(self, socket, message):
        operation, *args = [arg.decode("utf-8") for arg in message.split(b"\0")]
        getattr(self.registry, operation)(*args)
