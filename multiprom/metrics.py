class Metric:
    def __init__(self, collector, name, description):
        self.collector = collector
        self.name = name
        self.description = description
        self.register()

    @property
    def kind(self):  # pragma: no cover
        raise NotImplementedError

    def register(self):
        """Register this metric with its collector.
        """
        self.collector.send(encode("reg", self.kind, self.name, self.description))


class Counter(Metric):
    @property
    def kind(self):
        return "counter"

    def inc(self, n=1, **labels):
        """Increment this counter by the given amount.

        Parameters:
          n(int or float): This must be a positive amount.
        """
        assert n >= 0, "amounts must be positive"
        message = encode("inc", self.name, str(n), **labels)
        self.collector.send(message)


def encode(operation, *args, **labels):
    """Encode a message so that it can be sent over the wire.

    Parameters:
      operation(str)
      \*args(tuple[str])
      \**labels(dict)

    Returns:
      bytes
    """
    sorted_labels = []
    for name in sorted(labels):
        sorted_labels.append(f'{name}="{labels[name]}"')

    if sorted_labels:
        args += (",".join(sorted_labels),)

    message = b"\r\n".join((arg or "").encode("utf-8") for arg in (operation, *args))
    message_len = str(len(message)).encode("utf-8")
    return b"$" + message_len + b"\r\n" + message + b"\r\n\r\n"
