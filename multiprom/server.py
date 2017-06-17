import os
import selectors
import socket

from collections import defaultdict
from threading import Lock

from .logging import get_logger
from .registry import Registry

DEFAULT_BLOCK_SIZE = 16384


class ServerCollector:
    def __init__(self, sock_path, ready):
        self.logger = get_logger(__name__, type(self))
        self.sock_path = sock_path

        self.lock = Lock()
        self.ready = ready
        self.running = False
        self.registry = None

    def send(self, message):
        try:
            _, message = message.split(b"\0", 1)
            self.on_handle_message(None, message)
        except IndexError:
            self.logger.warning("Attempted to send invalid message: %r", message)

    def query(self, timeout=None):
        return self.registry.serialize()

    def start(self):
        self.running = True
        if os.path.exists(self.sock_path):
            self.logger.debug("Removing socket file.")
            os.unlink(self.sock_path)

        self.registry = Registry()
        self.buffers = defaultdict(bytes)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(self.sock_path)
        sock.listen(1)
        sock.setblocking(False)
        self.logger.debug("Listening on %r.", self.sock_path)

        selector = selectors.DefaultSelector()
        selector.register(sock, selectors.EVENT_READ, self.on_accept)

        self.ready.set()
        while self.running:
            events = selector.select(timeout=1)
            for key, mask in events:
                key.data(selector, key.fileobj)

        self.logger.debug("Closing server socket...")
        selector.unregister(sock)
        sock.close()
        selector.close()
        self.logger.debug("Collector server stopped.")

    def stop(self, block=True):
        self.logger.debug("Stopping collector server...")
        self.running = False

    def on_accept(self, selector, server_sock):
        sock, address = server_sock.accept()
        sock.setblocking(False)
        self.logger.debug("Accepted connection with fd %r.", sock.fileno())
        selector.register(sock, selectors.EVENT_READ, self.on_read_from_sock)

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

    def on_read_from_buffer(self, sock, fd):
        while True:
            buff = self.buffers[fd]
            if not buff:
                return

            elif buff[0] == 63:  # ?-mark
                data = self.registry.serialize()
                self.buffers[fd] = buff[1:]
                sock.sendall(str(len(data)).encode("ascii") + b"\0" + data.encode("utf-8") + b"\0")
                continue

            elif buff[0] != 36:  # $-sign
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
        with self.lock:
            operation, *args = [arg.decode("utf-8") for arg in message.split(b"\0")]
            getattr(self.registry, operation)(*args)
