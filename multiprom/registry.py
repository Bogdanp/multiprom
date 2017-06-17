from collections import defaultdict, namedtuple
from time import time

from .logging import get_logger


class Entry(namedtuple("Entry", ("name", "description", "values"))):
    value_type = None

    def __new__(cls, name, description=None, values=None):
        values = values or defaultdict(cls.value_type)
        return super().__new__(cls, name, description, values)

    @property
    def kind(self):  # pragma: no cover
        raise NotImplementedError

    def serialize(self, timestamp):
        lines = [f"# TYPE {self.name} {self.kind}"]
        if self.description:
            lines.append(f"# HELP {self.name} {self.description}")
        return lines


class CounterEntry(Entry):
    value_type = float

    @property
    def kind(self):
        return "counter"

    def inc(self, n, labels):
        self.values[labels] += n

    def serialize(self, timestamp):
        lines = super().serialize(timestamp)
        for label, value in self.values.items():
            if label == "":
                name = self.name
            else:
                name = self.name + "{" + label + "}"

            lines.append(f"{name} {value} {timestamp}")
        return lines


_entries_by_kind = {
    "counter": CounterEntry,
}


class Registry:
    def __init__(self):
        self.logger = get_logger(__name__, type(self))
        self.metrics = defaultdict(Entry)

    def serialize(self):
        timestamp = int(time() * 1000)
        lines = []
        for metric in self.metrics.values():
            lines.extend(metric.serialize(timestamp))

        response = "\n".join(lines) + "\n"
        return response

    def reg(self, kind, metric, description=None):
        clazz = _entries_by_kind.get(kind)
        if not clazz:
            self.logger.warning("Tried to register unsupported kind %r.", kind)
            return

        entry = self.metrics.get(metric)
        if entry is None or not issubclass(type(entry), clazz):
            self.metrics[metric] = clazz(metric, description)

    def inc(self, metric, n, labels=""):
        entry = self.metrics.get(metric)
        if entry is None:
            self.logger.warning("Tried to increment unregistered metric %r.", metric)
            return

        elif type(entry) not in (CounterEntry,):
            self.logger.warning("Tried to increment metric of type %r.", type(entry).__name__)
            return

        try:
            entry.inc(float(n), labels)
        except ValueError as e:
            self.logger.warning("Failed to increment metric %r: %s", metric, e)
