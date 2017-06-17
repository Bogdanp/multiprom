import os

from multiprom import Collector
from multiprocessing import Pool


# Given that I have processes that can perform work
def inc_worker(metric, increments=100, increment_by=1):
    import logging
    import os
    import time

    from multiprom import Collector

    logfmt = "[%(asctime)s] [PID %(process)d] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logfmt)

    collector = Collector()
    collector.start()

    # And each process increments a counter ten times
    total_requests = collector.counter(metric)
    for _ in range(increments):
        total_requests.inc(increment_by, pid=os.getpid())

    # And each process gives the server enough time to spawn
    time.sleep(1)

    # Then the process exits cleanly
    collector.stop()

    # And returns its pid
    return os.getpid()


def test_can_collect_metrics_across_processes():
    # Given that I have a pool of processes that increment a counter
    futures = []
    with Pool(processes=8) as pool:
        # And that I have a collector
        collector = Collector()
        collector.start()

        for _ in range(8):
            futures.append(pool.apply_async(inc_worker, args=("requests_total",)))

        # If I wait for all of them to complete their work
        pids = []
        for future in futures:
            pids.append(future.get(timeout=10))

        # Then query the collector
        data = collector.query()
        collector.stop()

        # I expect the metrics to have been recorded
        assert data.startswith("# TYPE requests_total counter\n")
        for pid in pids:
            assert f'\nrequests_total{{pid="{pid}"}} 100.0' in data
