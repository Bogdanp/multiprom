import socket
import time

from queue import Empty, Queue

from .logging import get_logger
from .server import DEFAULT_BLOCK_SIZE


class ClientCollector:
    def __init__(self, sock_path, ready):
        self.logger = get_logger(__name__, type(self))
        self.sock_path = sock_path

        self.ready = ready
        self.queue = Queue()
        self.running = False

    def send(self, message):
        self.queue.put(message)

    def query(self, timeout=None):
        self.sock.sendall(b"?")
        buff = b""
        while True:
            buff += self.sock.recv(DEFAULT_BLOCK_SIZE)
            try:
                marker = buff.index(b"\0")
                message_len = int(buff[:marker])
            except ValueError:
                self.logger.warning("Malformed message from server: %r", buff)
                return b""
            except IndexError:
                self.logger.debug("Waiting for more data from server...")
                continue

            if len(buff[marker + 2:]) < message_len + 2:
                continue

            buff = buff[marker + 2:]
            return buff[:message_len].decode("utf-8")

    def start(self):
        attempts = 0
        while True:
            try:
                self.logger.debug("Connecting to collector server.")
                self.sock = sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(self.sock_path)
                break
            except OSError:
                if attempts >= 5:  # TODO: un-hardcode this
                    raise

                self.logger.warning("Failed to connect to collector server. Retrying...")
                time.sleep(min(0.125 * 2 ** attempts, 2))
                attempts += 1

        self.running = True
        self.ready.set()
        while self.running:
            try:
                message = self.queue.get(timeout=1)
                sock.sendall(message)
                self.queue.task_done()
            except Empty:
                pass

        self.logger.debug("Closing client socket...")
        sock.close()
        self.logger.debug("Collector client stopped.")

    def stop(self, block=True):
        self.logger.debug("Stopping collector client...")
        if block:
            self.logger.debug("Waiting for metrics queue to be drained...")
            self.queue.join()

        self.running = False
