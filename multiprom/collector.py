import fcntl
import os

from contextlib import contextmanager
from threading import Event, Thread

from .client import ClientCollector
from .logging import get_logger
from .metrics import Counter
from .server import ServerCollector


DEFAULT_LOCK_PATH = os.getenv("MULTIPROM_LOCK_PATH", "/tmp/multiprom.lock")
DEFAULT_SOCK_PATH = os.getenv("MULTIPROM_SOCK_PATH", "/tmp/multiprom.sock")


class Collector(Thread):
    """Collectors collect metrics.

    Examples:

      >>> collector = Collector(namespace="myapp")
      >>> collector.start()
      >>> total_requests = collector.counter("requests_total", description="The total number of requests.")
      >>> total_requests.inc(hostname=os.gethostname())
    """

    def __init__(self, *, namespace=None, lock_path=DEFAULT_LOCK_PATH, sock_path=DEFAULT_SOCK_PATH):
        super().__init__(daemon=True, target=self._run_thread)

        self.logger = get_logger(__name__, type(self))
        self.namespace = namespace
        self.lock_path = lock_path
        self.sock_path = sock_path

        self.collector_ready = Event()
        self.collector_impl = None

    def counter(self, metric, *, description=None):
        """Instantiate a counter metric.

        Parameters:
          metric(str): The name of this metric.
          description(str): An optional description for the metric.

        Returns:
          Metric
        """
        if self.namespace:
            metric = f"{self.namespace}_{metric}"
        return Counter(self, metric, description)

    def send(self, message, timeout=None):
        """Send a message to this collector.

        Parameters:
          timeout(float)

        Parameters:
          message(bytes)
        """
        if not self.collector_impl or not self.collector_ready.is_set():
            self.logger.debug("Waiting for collector to become available...")
            self.collector_ready.wait(timeout=timeout)
        self.collector_impl.send(message)

    def query(self, timeout=None):
        """Query this collector for its metrics.

        Parameters:
          timeout(float)

        Returns:
          bytes: The metrics in the Prometheus text format.
        """
        if not self.collector_impl or not self.collector_ready.is_set():
            self.logger.debug("Waiting for collector to become available...")
            self.collector_ready.wait(timeout=timeout)
        return self.collector_impl.query()

    def stop(self):
        """Stop the collector.
        """
        if self.collector_impl:
            self.collector_impl.stop()
            self.join()

    def _run_thread(self):
        with flock(self.lock_path) as acquired:
            if not acquired:
                self.logger.debug("Could not acquire lock. Running in client mode.")
                implementation = ClientCollector
            else:
                self.logger.debug("Lock acquired. Running in server mode.")
                implementation = ServerCollector

            self.collector_impl = implementation(self.sock_path, self.collector_ready)
            self.collector_impl.start()


@contextmanager
def flock(path):
    """Attempt to acquire a POSIX file lock.
    """
    with open(path, "w+") as lf:
        try:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            yield acquired

        except OSError:
            acquired = False
            yield acquired

        finally:
            if acquired:
                fcntl.flock(lf, fcntl.LOCK_UN)
